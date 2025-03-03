from typing import List
from datetime import date
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy import func
from sqlalchemy import desc

# from sqlalchemy import inspect
from sqlalchemy.exc import InvalidRequestError
from timpani.app_cfg import TimpaniAppCfg
from timpani.content_store.content_store_interface import ContentStoreInterface
from timpani.content_store.content_cluster import ContentCluster
from timpani.content_store.content_item import ContentItem
from timpani.content_store.item_state_model import ContentItemState
from timpani.content_store.content_store_obj import ContentStoreObject
from timpani.content_store.content_keyword import ContentKeyword
from timpani.conductor.process_state import ProcessState

from timpani.processing_sequences.workflow_manager import WorkflowManager

import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()


class ContentStore(ContentStoreInterface):
    """
    Primary source of truth for content items, their processing state, cluster
    membership, and any other state the system needs to track. Assumed to wrap a
    relational database with an ORM layer.

    Also has the business logic for operations involving objects in the store

    Failing any database operation (flushing, etc) should throw an Exception,
    which should be handled in level above. If it is not recoverable (i.e this
    update was the result of an incoming callback) the item will not progress to
    the next state in its update sequence. This means that the application will
    need to pick it up again and retry the process

    Imports and instantiates all of the known state models from known workflows
    registered in WorkflowManager that they can be instantiated when loading back from db.

    NOTE: would be nice to refactor all of the 'biz logic' of updates to a
    different class somehow, but maybe not possible since it is intertwined
    with database transactions
    """

    app_cfg = TimpaniAppCfg()

    # this should only be used for admin operations (db migrations, etc)
    # NOTE: contains PWD so do not log
    PG_ADMIN_CONNECT_STR = (
        f"postgresql://{app_cfg.content_store_admin_user}:{app_cfg.content_store_admin_pwd}"
        + f"@{app_cfg.content_store_db_rw_endpoint}/{app_cfg.content_store_db}"
    )

    # this is for normal read and write operations from applications
    # NOTE: contains PWD so do not log
    PG_CONNECT_STR = (
        f"postgresql://{app_cfg.content_store_user}:{app_cfg.content_store_pwd}"
        + f"@{app_cfg.content_store_db_rw_endpoint}/{app_cfg.content_store_db}"
    )

    # this is for read only operations (can be from a read replica)
    # NOTE: contains PWD so do not log
    RO_PG_CONNECT_STR = (
        f"postgresql://{app_cfg.content_store_user}:{app_cfg.content_store_pwd}"
        + f"@{app_cfg.content_store_db_ro_endpoint}/{app_cfg.content_store_db}"
    )

    # this creates dummy db for testing
    SQLITE_INMEMORY_CONNECT_STR = "sqlite+pysqlite:///:memory:"

    engine = None
    ro_engine = None

    # instantiate all of the state models we are likely to need
    known_content_item_states = []
    for workflow_cls in WorkflowManager.REGISTRED_WORKFLOWS:
        state_model = workflow_cls.get_state_model()
        known_content_item_states.append(state_model)

    def init_db_engine(self, connect_string=None, ro_connect_string=None, debug=False):
        """
        # TODO: When running on a DB cluster, any reads related to modification
        *must* occur on the writer instances endpoint to avoid race conditions between
        updates.  Replace with init_rw_db_engine and init_ro_db_engine
        Default is to use postgres cluster with endpoints  defined
        by environment variables
        NOTE: this sets both a read-only (RO) connection and read-write (RW) connection
        but it only returns a reference to the RW connection for testing.
        NOTE: to run on SQLite in-memory test db, use the SQLITE_INMEMORY_CONNECT_STR for
        both connect strings.
        """

        if connect_string is None:
            logging.debug("Content store defaulting to postgres connection")
            connect_string = self.PG_CONNECT_STR
        self.engine = create_engine(
            connect_string,
            echo=debug,
            # pool size needs to be > number of processing threads
            pool_size=25,
            max_overflow=15,
        )
        if ro_connect_string is None:
            ro_connect_string = self.RO_PG_CONNECT_STR
        self.ro_engine = create_engine(
            ro_connect_string,
            echo=debug,
            # pool size needs to be > number of processing threads
            pool_size=25,
            max_overflow=15,
        )
        # only return the RW engine (this is only used for testing)
        return self.engine

    # --- operations on clusters and items --

    def initialize_item(self, item: ContentItem, state=None, force_overwrite=False):
        """
        Add a newly created content item into the database,
        initializing an appropriate state and return the
        modified item.

        However, in order to support idempotent updates, if an item with matching
        raw_content_id and source_field for the workspace exists, the new record will be ignored.
        UNLESSS the existing recoord is in the FAILED state or force_overwrite,
        in which case it will be deleted (along with states and cluster memberships)
        and the new item will be added.
        NOTE: if there is no associated state (some test cases), the item will be overwritten
        NOTE: in the case of multiple objects with same id (an unanticipated error state),
        only one is checked
        """
        with Session(self.engine, expire_on_commit=False) as session:
            # check if an item with the same content id already exists
            # and update it if in failed state, otherwise ignoring
            # so that we can run indempotent updates
            # TODO: should we be doing this with a compound primary key instead?
            existing_item = (
                session.query(ContentItem)
                .where(ContentItem.workspace_id == item.workspace_id)
                .where(ContentItem.raw_content_id == item.raw_content_id)
                .where(ContentItem.source_field == item.source_field)
                .first()
            )

            if existing_item is not None:
                if existing_item.content_item_state_id is not None:
                    # check if the item had an associated state
                    existing_state = session.get(
                        ContentItemState, existing_item.content_item_state_id
                    )
                    if (
                        existing_state is None
                        or existing_state.current_state == ContentItemState.STATE_FAILED
                    ) or force_overwrite is True:
                        # need to delete the previous object, along with states and cluster membership
                        self.delete_item(existing_item)
                    else:
                        # skip inserting this record
                        # TODO: how to count and report on how many records we skip?
                        logging.warning(  # TODO: this might make too many log messages
                            f"Ignoring init of raw_content_id {item.raw_content_id} "
                            + f"because it conflicts with existing item {existing_item.content_item_id}"
                        )
                        return None
                # TODO: could skip warning if they were equal? Also could load but put in failed state if they were not equal?

            session.add(item)
            if state is None:
                state = ContentItemState()
            session.add(state)
            session.flush()
            assert state.state_id is not None
            item.content_item_state_id = state.state_id
            session.commit()
            session.refresh(item)
            session.expunge(item)
            return item

    def transition_item_state(self, item: ContentItem, state: str) -> ContentItemState:
        """
        Check that this is validate state, etc, and make the update
        to the new state, returning the updated state object.
        NOTE: should be called when state has been achieved
        """
        # TODO: this needs to support batch
        with Session(self.engine, expire_on_commit=False) as session:
            # get the state corresponding to the item
            # this is a polymorphic class
            # https://docs.sqlalchemy.org/en/13/orm/inheritance_loading.html
            item_state = session.get(ContentItemState, item.content_item_state_id)
            # TODO: need to cast to appropriate subclass so that it knows the right set of states

            item_state.transitionTo(state)
            session.commit()
            session.expunge(item_state)
            return item_state

    def start_transition_to_state(self, item: ContentItem, state: str):
        """
        Check that transition is permissable, and updates transition
        start time to indicate transition in progress.
        Also increments the transition_num counter, even if transition fails
        so that the item will go into failed state after too many attempts
        NOTE: should be called before state transition is attempted to record timing
        """
        # TODO: this needs to support batch
        with Session(self.engine, expire_on_commit=False) as session:
            # get the state corresponding to the item
            item_state = session.get(ContentItemState, item.content_item_state_id)
            # increment the transition num to track the attempt (even if it fails)
            item_state.transition_num = item_state.transition_num + 1
            session.commit()
            self.validate_transition(item, state)
            # mark that a transition is in proggress
            item_state.startTransition()
            session.commit()
            session.expunge(item_state)
            return item_state

    def validate_transition(self, item: ContentItem, state: str):
        """
        Check if transition to state would be allowable given
        the current state of the content item
        """
        with Session(self.engine, expire_on_commit=False) as session:
            if state is None:
                return True
            else:
                # get the state corresponding to the item
                item_state = session.get(ContentItemState, item.content_item_state_id)
                # check if this will be an allowable transition
                allowable_transition = item_state.isTransitionAllowed(
                    item_state.current_state, state
                )
                if not allowable_transition:
                    logging.warning(
                        f"requested transition for content item {item.content_item_id} from state "
                        + f"{item_state.current_state} to state {state} is not allowed by state model {item_state.state_model_name}"
                    )
                return allowable_transition

    def update_item(self, item: ContentItem, state=None):
        """
        Update the database state with any changes that have been made to the item.
        If state is not None, the update will be rejected if it corresponds to
        a state which has already been reached or is not permitted
        TODO: save_item() probably a better name for this?
        """
        with Session(self.engine, expire_on_commit=False) as session:
            item = session.merge(item)
            if self.validate_transition(item, state):
                session.commit()
                # TODO: just use transition_item_state with try-catch instead of validating?
                if state is not None:
                    self.transition_item_state(item, state)
            else:
                # update corresponded to a bad state so skip it
                session.rollback()
            session.expunge(item)  # detach from the session
            return item

    def refresh_object(self, object: ContentStoreObject):
        """
        Refresh the object with fresh state that may have changed in
        the database and return it.
        """
        with Session(self.engine, expire_on_commit=True) as session:
            obj = session.merge(object)

            try:
                session.refresh(obj)
                session.expunge(obj)
                return obj
            except InvalidRequestError as e:
                # if it is not persistant, it must have been deleted
                # and cannot be refreshed
                logging.warning(f"unable to refresh, possibly deleted object {obj}:{e}")
            return None

    def _update_unique_item_count(
        self,
        session: Session,
        cluster: ContentCluster,
    ):
        """
        Update the unique item count with number of non (textutally) identical items
        NOTE: this uses a session because it is only called internally
        TODO: should this happen on the content or raw_content?
        """
        session.flush()
        result = (
            session.query(ContentItem)
            .where(ContentItem.content_cluster_id == cluster.content_cluster_id)
            .where(ContentItem.workspace_id == cluster.workspace_id)
            .distinct(ContentItem.content)
            .count()
        )
        cluster.num_items_unique = result
        session.flush()
        return cluster

    def _select_exemplar_item(
        self,
        session: Session,
        cluster: ContentCluster,
        item=None,
    ):
        """
        NOTE: this uses a session because it is only called internally
        Assign an item to be exemplar of the cluster, or chose it from internal logic
        (oldest item in cluster for now)
        NOTE: which date should it sort by? using raw created as expected to be idempotent on reload
        TODO: ideally median, but that need to be computed elsewhere

        """
        # with Session(self.engine, expire_on_commit=False) as session:
        # session.merge(cluster)
        session.flush()
        if item is not None:
            # confirm that the item is in the cluster
            assert (
                item.content_cluster_id == cluster.content_cluster_id
            ), f"item {item.content_item_id} cannont be an exemplar because it does not belong to cluster {cluster.content_cluster_id}"
            cluster.exemplar_item_id = item.content_item_id
        else:
            # find the oldest item mapped to the cluster
            item = (
                session.query(ContentItem)
                .where(ContentItem.content_cluster_id == cluster.content_cluster_id)
                .where(ContentItem.workspace_id == cluster.workspace_id)
                .order_by(ContentItem.raw_created_at)
                .first()
            )
            if item is None:
                # this shouldn't happen (because cluster should have been deleted),
                # but maybe something else deleted so there are no items left for cluster
                logging.warning(
                    f"Unable to select exemplar for cluster because no items map to cluster {cluster.content_cluster_id}"
                )
                cluster.exemplar_item_id = None
            else:
                session.flush()
                cluster.exemplar_item_id = item.content_item_id
        session.flush()
        return item

    def cluster_items(
        self,
        first_item: ContentItem,
        second_item: ContentItem = None,
    ) -> ContentCluster:
        """
        Add first_item item to the second item's cluster *by updating the item(s)*
        - If second_item is None, a new cluster will be created for the first item and returned.
        - If the first item already has a cluster, it will be moved to the second item's cluster
        (and the first cluster possibly deleted if empty) UNLESS first item is already in second item cluster
        - If the second item does not have a cluster (initial boundry condition) it will
        be created and both items inserted
        NOTE: items will be refreshed to match state in database to resolve concurrency
        """
        with Session(self.engine, expire_on_commit=False) as session:
            item1 = session.merge(first_item)
            session.refresh(item1)
            # print(f"item1 has cluster {item1.content_cluster_id}")
            if second_item is not None:
                item2 = session.merge(second_item)
                session.refresh(item2)
                # check if it is *already* in the second item's cluster
                if (
                    item1.content_cluster_id == item2.content_cluster_id
                    and item1.content_cluster_id is not None
                ):
                    # no updates are required, exit here
                    # print("no cluster update needed")
                    cluster2 = session.get(ContentCluster, item2.content_cluster_id)
                    session.expunge(cluster2)
                    return cluster2

            # if the first item already has a cluster, we will remove from it
            if item1.content_cluster_id is not None:
                cluster1 = session.get(ContentCluster, item1.content_cluster_id)
                item1.content_cluster_id = None
                #  decrement the count on the cluster
                cluster1.num_items -= 1
                # print("A num items decremented")
                session.flush()
                # if it is the last item, remove the cluster
                if cluster1.num_items <= 0:
                    session.delete(
                        cluster1
                    )  # TODO: may not be thread safe if something else updating cluster?
                else:
                    # if it is the exemplar, remove it and pick a new one
                    if cluster1.exemplar_item_id == item1.content_item_id:
                        self._select_exemplar_item(session, cluster1)
                    # recompute the number of unique items for cluster1
                    self._update_unique_item_count(session, cluster1)

            if second_item is not None:
                # check items are compatible for clustering
                # an item cannot cluster with itself
                assert (
                    item1.content_item_id != item2.content_item_id
                ), f"item {item1.content_item_id} cannot be put into a cluster with itself"
                # confirm workspace ids match
                assert (
                    item1.workspace_id == item2.workspace_id
                ), f"first_item workspace_id {item1.workspace_id} does not match second_item workspace_id {item2.workspace_id}"

                if item2.content_cluster_id is None:
                    # item 2 was not in a cluster yet (initial state)
                    cluster2 = ContentCluster()
                    session.add(cluster2)
                    session.flush()  # so id will be created
                    # do bookeeping for adding item2 to cluster2
                    cluster2.num_items += 1
                    cluster2.num_items_added += 1
                    cluster2.num_items_unique += 1  # because only one
                    # print(f"A num items incremted item2 is {item2.content_item_id}")
                    item2.content_cluster_id = cluster2.content_cluster_id
                    # TODO: we should be able to compute stress since we already know similarity for the pair?
                    cluster2.stress_score = 0.0
                    cluster2.priority_score = 0.1
                else:
                    cluster2 = session.get(ContentCluster, item2.content_cluster_id)

            else:
                # 2nd item is None so we will just be creating a new cluster for the first item alone
                cluster2 = ContentCluster()
                session.add(cluster2)
                session.flush()  # so id will be created

            assert cluster2.content_cluster_id is not None
            # TODO: need to record a cluster history event to support trends
            # https://meedan.atlassian.net/browse/CV2-3531

            # do bookeeping for adding item1 into cluster2
            cluster2.num_items += 1
            cluster2.num_items_added += 1

            # print(f"B num items incremted")
            cluster2.workspace_id = item1.workspace_id
            item1.content_cluster_id = cluster2.content_cluster_id

            if cluster2.num_items == 1:
                # since it is a new cluster, we know exemplar
                self._select_exemplar_item(session, cluster2, item1)
                # since there will be only item in the cluster, the stress is now zero
                cluster2.stress_score = 0.0
                # low priority for re evalutation
                cluster2.priority_score = 0.0
                cluster2.num_items_unique = 1
            else:
                self._select_exemplar_item(session, cluster2)
                # we don't know what the stress is, so mark that it should be calculated by bumping up priority score
                cluster2.priority_score += 0.1
                self._update_unique_item_count(session, cluster2)

            session.commit()
            session.refresh(cluster2)
            session.expunge(cluster2)
            return cluster2

    def update_cluster(
        self,
        cluster: ContentCluster,
    ):
        """
        Update the database state with any changes that have been made to the cluster.
        """
        with Session(self.engine, expire_on_commit=False) as session:
            cluster = session.merge(cluster)
            session.commit()
            session.expunge(cluster)  # detach from the session
            return cluster

    def get_priority_clusters(self, workspace_id, batch_size=1000):
        """
        Yield a series of clusters that, according to our heuristics based on priority, age etc,
        should be re-evaluated or computed. This runs on the read-only cluster endpoint
        NOTE: clusters with priority zero will be excluded
        NOTE: this can be expensive if we have to sort all of the clusters by priority
        """
        # TODO: weight priority also by age so older items get a bump
        with Session(self.ro_engine, expire_on_commit=False) as session:
            # get al of the items that are not in the inactive states
            query = (
                select(ContentCluster)
                .where(ContentCluster.workspace_id == workspace_id)
                .where(ContentCluster.priority_score > 0.0)
                .order_by(desc(ContentCluster.priority_score))
                .limit(batch_size)
            )

            for row in session.execute(query):
                cluster = row.ContentCluster
                # detach from database before returning
                session.expunge(cluster)
                yield cluster

    def get_item(self, content_item_id: int) -> ContentItem:
        """
        Return a ContentItems with the appropriate id (if any)
        or none if none match
        TODO: confirm that fields have been lazy loaded?
        """
        with Session(self.engine, expire_on_commit=False) as session:
            found_item = None
            if content_item_id is not None:
                item = session.get(ContentItem, content_item_id)
                if item is not None:
                    # make sure all attributes are loaded
                    found_item = item
                    session.refresh(found_item)
                    session.expunge(found_item)
            return found_item

    def get_item_state(self, content_item_state_id: int) -> ContentItemState:
        """
        Return a ContentItemState with the appropriate id (if any)
        TODO: error if none match?
        TODO: confirm that fields have been lazy loaded?
        """
        assert content_item_state_id is not None
        with Session(self.engine, expire_on_commit=False) as session:
            state = session.get(ContentItemState, content_item_state_id)
            if state is None:
                logging.error(
                    f"no match for content_item_state_id {content_item_state_id}"
                )
            session.expunge(state)
            return state

    # TODO: add a new function that, given a list of content items
    # reuturns the (possibly negative), duration of current transition
    # so we don't have to look up states one-at-a-time

    def get_item_cluster_size(self, content_item_id: int):
        """
        returns the size of the cluster associated with the content item
        """
        content_item_id = int(content_item_id)
        with Session(self.ro_engine, expire_on_commit=False) as session:
            # get al of the items that are not in the inactive states
            query = (
                select(ContentCluster.num_items)
                .join(
                    ContentItem,
                    ContentItem.content_cluster_id == ContentCluster.content_cluster_id,
                )
                .where(ContentItem.content_item_id == content_item_id)
            )
            result = session.execute(query).scalar()
            # if no cluster exists yet return zero
            if result is None:
                result = 0
            return result

    def get_item_state_summary(self, workspace_id=None):
        """
        Return a array with summary of states that are present and number of
        items in each state
        NOTE: This is an analytics query, so it uses the RO endpoint
        """

        with Session(self.ro_engine, expire_on_commit=False) as session:
            query = (
                session.query(
                    ContentItemState.current_state,
                    func.count(ContentItemState.current_state),
                )
                .join(
                    ContentItem,
                    ContentItem.content_item_state_id == ContentItemState.state_id,
                )
                .filter()
                .group_by(ContentItemState.current_state)
            )
            if workspace_id is not None:
                query = query.where(ContentItem.workspace_id == workspace_id)
            results = query.all()

            # convert to non-ORM object to return data
            summary = [tuple(row) for row in results]
            return summary

    def get_cluster_items(self, cluster: ContentCluster) -> List[ContentItem]:
        """
        Query database to return all of the items that name
        the cluster as their own. This returns a generator
        that yields ContentItems because it could be big
        TODO: batch size
        """
        with Session(self.engine, expire_on_commit=False) as session:
            query = select(ContentItem).where(
                ContentItem.content_cluster_id == cluster.content_cluster_id
            )

            for row in session.execute(query):
                yield row.ContentItem

    def merge_clusters(
        self,
        source_cluster_id: int,
        target_cluster_id: int,
    ):
        """
        Merge together two clusters that have be determined to be overlapping
        by inserting items from source cluster into target cluster. Source cluster
        is expected to be deleted as a side effect, target clustered will be returned
        """
        with Session(self.engine, expire_on_commit=False) as session:
            # check that they match the given workspace id
            source = session.get(ContentCluster, source_cluster_id)
            target = session.get(ContentCluster, target_cluster_id)
            assert (
                source.workspace_id == target.workspace_id
            ), f"source workspace_id {source.workspace_id} does not match target workspace_id {target.workspace_id}"

            # get the targer exemplar
            target_exemplar = session.get(ContentItem, target.exemplar_item_id)
            # iterate through all the items in source and move them to target
            # one at a time is probably slow because many internal updates
            query = select(ContentItem).where(
                ContentItem.content_cluster_id == source.content_cluster_id
            )
            for row in session.execute(query):
                self.cluster_items(row.ContentItem, target_exemplar)

            # return the updated target cluster
            session.refresh(target)
            return session.expunge(target)

    def delete_item(self, item: ContentItem):
        """
        Remove item and associated states from database and update clusters appropriately.
        NOTE: If there is an "inflight" item B that is to be inserted
        in item A's cluster, but item A gets deleted, the operation should
        either fail to retry, or succeed by creating a new cluster. (as long as
        writes and reads happen on the same RW instance)
        """
        with Session(self.engine, expire_on_commit=False) as session:
            item = session.merge(item)
            session.refresh(item)
            item_state_id = item.content_item_state_id

            # handle cluster update item was part of
            if item.content_cluster_id is not None:
                cluster = session.get(ContentCluster, item.content_cluster_id)
                # if it is the exemplar, remove it
                item.content_cluster_id = None
                #  decrement the count on the cluster
                cluster.num_items -= 1
                # print(f"B num items decremented: {cluster.num_items}")
                # if it is the last item, remove the cluster
                if cluster.num_items <= 0:
                    # TODO: may not be thread safe if something else updating cluster?
                    session.delete(cluster)
                else:
                    self._update_unique_item_count(session, cluster)
                    if cluster.exemplar_item_id == item.content_item_id:
                        cluster.exemplar_item_id = None
                        item.content_cluster_id = None
                        session.flush()
                        self._select_exemplar_item(session, cluster)

            # remove item state
            if item_state_id is not None:
                item.content_item_state_id = None
                item_state = session.get(ContentItemState, item_state_id)
                session.delete(item_state)

            # remove any keywords linked to the item
            query = (
                select(ContentKeyword)
                .where(ContentKeyword.content_item_id == item.content_item_id)
                .where(ContentKeyword.workspace_id == item.workspace_id)
            )
            for row in session.execute(query):
                keyword = row.ContentKeyword
                # detach from database before returning
                session.delete(keyword)

            session.delete(item)

            session.commit()

    # TODO: check for items with too many or too old transitions and put them in failed state

    def get_items_before(self, workspace_id, earliest: date, chunk_size=10000):
        """
        Yield a sequence of content items that have a creation date before `earliest`,
        usually to determine which content should be deleted.
        Filteres on content published date (not ingest date) and workspace id
        NOTE: assumes we don't need second sync accuracy, so uses RO cluster
        """
        # NOTE: we don't delete in this operation because we may also need
        # to delete from other data stores
        # check for items earlier than earliest
        with Session(self.ro_engine, expire_on_commit=False) as session:
            # get all of the items that are not in the inactive states
            query = (
                select(ContentItem)
                .where(ContentItem.workspace_id == workspace_id)
                .where(ContentItem.content_published_date < earliest)
                .limit(chunk_size)
                .order_by(func.random())
            )
            if workspace_id is not None:
                query = query.where(ContentItem.workspace_id == workspace_id)

            for row in session.execute(query):
                item = row.ContentItem
                # detach from database before returning
                session.expunge(item)
                yield item

    def get_items_in_progress(
        self, workspace_id=None, batch_state=None, chunk_size=10000
    ):
        """
        Yield a sequence of content items that have a state that is not
        one of the active states
        (undefined, completed, failed)
        optionally filters based on workspace id.
        By default, only fetches first 10k records to avoid OOM.
        Ordering is randomized so we don't get stuck on failing state and
        in theory multiple instances of the process could run at the same time and rarely collide
        TODO: optionaly order by content item publish date (needs index) to prioritize old content
        NOTE: workflows can add arbitrary additional states, this is just conerned
        with starting and ending states
        NOTE: does not use RO cluster due to risk of missing a recent update
        """
        with Session(self.engine, expire_on_commit=False) as session:
            # get all of the items that are not in the inactive states

            query = select(ContentItem).join(
                ContentItemState,
                ContentItem.content_item_state_id == ContentItemState.state_id,
            )
            # either we are pulling a specific state, or skipping some specific states
            if batch_state is None:
                query = query.where(  # can't figure out 'not in []' in sql alchemy
                    (ContentItemState.current_state != ContentItemState.STATE_UNDEFINED)
                    & (ContentItemState.current_state != ContentItemState.STATE_FAILED)
                    & (
                        ContentItemState.current_state
                        != ContentItemState.STATE_COMPLETED
                    )
                )
            else:
                query = query.where(ContentItemState.current_state == batch_state)

            # add the limiting and randomization
            query = query.limit(chunk_size).order_by(func.random())

            if workspace_id is not None:
                query = query.where(ContentItem.workspace_id == workspace_id)

            for row in session.execute(query):
                item = row.ContentItem
                # detach from database before returning
                session.expunge(item)
                yield item

    def attach_keyword(
        self, item: ContentItem, keyword_model_name: str, keyword_text: str, score=1.0
    ):
        """
        Create a keyword object and attach to the content_item
        """
        with Session(self.engine, expire_on_commit=False) as session:
            item = session.merge(item)
            keyword = ContentKeyword(
                workspace_id=item.workspace_id,
                keyword_model_name=keyword_model_name,
                content_item_id=item.content_item_id,
                keyword_text=keyword_text,
                keyword_score=score,
                content_published_date=item.content_published_date,
            )
            session.add(keyword)
            session.commit()
            session.expunge(keyword)
            return keyword

    def get_keywords_for_item(self, item: ContentItem, keyword_model_name: str = None):
        """
        Return a list of keyword items associated with the content item, optionally filtering
        by the name of the model
        """
        with Session(self.engine, expire_on_commit=False) as session:
            # TODO: filtering to only completed staets?
            item = session.merge(item)
            query = select(ContentKeyword).where(
                ContentKeyword.content_item_id == item.content_item_id
            )
            if keyword_model_name is not None:
                query = query.where(
                    ContentKeyword.keyword_model_name == keyword_model_name
                )

            for row in session.execute(query):
                keyword = row.ContentKeyword
                # detach from database before returning
                session.expunge(keyword)
                yield keyword

    def serialize_object(self, obj: ContentStoreObject):
        """
        Dump the content store object (content_item or cluster)
        into a dict representaiton suitible for json serialization.
        NOTE: does not check or refresh state against database
        """
        with Session(self.engine, expire_on_commit=False) as session:
            obj = session.merge(obj)
            dump_content = obj.schema().dump(obj)
            return dump_content

    def deserialize_object(self, dump: dict, cls: ContentStoreObject):
        """
        Convert a dumped dict representation of a content item into the
        appropriate object in the database by attempting to map to the
        indicated class
        """
        with Session(self.engine, expire_on_commit=False) as session:
            obj = cls.schema().load(dump, session=session)
            session.add(obj)
            session.commit()
            session.refresh(obj)
            session.expunge(obj)
            return obj

    def deprecated_get_items_as_rows(
        self,
        workspace_id,
        chunk_size=1000,
        max_limit=10000,
        published_range_start=None,
        published_range_end=None,
    ):
        """
        Yields data describing the content items (including some joind data)
        as a dict to be converted into a row in table or data frame
        TODO: need chunk size so we don't run out of memory
        """
        # TODO: validate workspace_id access here?
        # TODO: there are probably much more efficient ways to return data
        # TODO: how do we manage max limit if needing more records?
        with Session(self.ro_engine, expire_on_commit=False) as session:
            # get all of the items that are not in the inactive states
            # TODO: state restrictions
            offset = 0
            while offset < max_limit:
                query = (
                    select(ContentItem)
                    .where(ContentItem.workspace_id == workspace_id)
                    .order_by(ContentItem.content_item_id)  # primary key
                    .offset(offset)
                    .limit(min(chunk_size, max_limit))
                )
                # filter for date range if included
                if published_range_start is not None or published_range_end is not None:
                    query = query.where(
                        ContentItem.content_published_date > published_range_start
                    ).where(ContentItem.content_published_date <= published_range_end)
                num_records = 0
                for row in session.execute(query):
                    # detach from database before
                    # and serilize into dict before returning
                    row_dict = self.serialize_object(row.ContentItem)
                    num_records += 1
                    yield row_dict
                if num_records == 0:
                    # query return no more values
                    break
                # increment for next chunk
                offset += chunk_size

    def record_process_state(self, state: ProcessState):
        with Session(self.engine, expire_on_commit=False) as session:
            session.add(state)
            session.commit()

    def erase_workspace(self, workspace_id: str, source_id: str):
        """
        Permenantly deletes all of the objects associated with a specific workspace_id
        Normally used for removing test data, but would also be used if partner requests removal
        TODO: this uses raw SQL instead of ORM functions.
        """
        # logging these as warning just because it is scary
        logging.warning(f"deleting all content from workspace {workspace_id}")
        with Session(self.engine, expire_on_commit=False) as session:
            # need to delete content from previous run in content_store
            # first  null back references from clusters to items so that they can be deleted
            session.execute(
                text(
                    f"update content_cluster cc set exemplar_item_id=NULL where workspace_id='{workspace_id}'"
                )
            )
            # null references from items to clusters
            session.execute(
                text(
                    f"update content_item set content_cluster_id = NULL where source_id='{source_id}' and workspace_id='{workspace_id}'"
                )
            )

            # TODO: delete keywords

            # delete clusters
            num_deleted_clusters = session.execute(
                text(
                    f"delete from content_cluster cc where workspace_id='{workspace_id}'"
                )
            )
            logging.warning(
                f"Deleted {num_deleted_clusters} clusters from workspace {workspace_id}"
            )

            # Delete corresponding state records for items
            num_deleted_states = session.execute(
                text(
                    f"""delete from content_item_state cis
                using content_item ci
                where ci.content_item_state_id = cis.state_id
                and ci.source_id='{source_id}' and ci.workspace_id='{workspace_id}'
                """
                )
            )
            logging.warning(
                f"Deleted {num_deleted_states} item states from workspace {workspace_id}"
            )

            # Delete content items
            num_deleted_items = session.execute(
                text(
                    f"delete from content_item where source_id='{source_id}' and workspace_id='{workspace_id}'"
                )
            )
            logging.warning(
                f"Deleted {num_deleted_items} content_item from workspace {workspace_id}"
            )

            num_orphan_states = session.execute(
                text(
                    # delete the state records that do not join anymore (should be none)
                    """delete from content_item_state where not exists
                    (
                        select state_id from content_item_state
                        join content_item on content_item_state_id = state_id
                    )
                    """
                )
            )
            logging.warning(
                f"Deleted {num_orphan_states} orphan states from workspace {workspace_id}"
            )

            session.commit()

    def dangerously_run_sql(self, sql: str):
        """
        Execute arbitrary sql against the database
        Note: this is REALY DANGEROUS because it doesn't go through the ORM
        and state may not be updated properly.
        Should only be used in debugging and testing, all production
        code should have its own function in ContentStore
        """
        assert (
            TimpaniAppCfg().deploy_env_label != "live"
        ), "directly running sql is not permitted in live environment"

        with Session(self.engine, expire_on_commit=True) as session:
            results = session.execute(text(sql))
            session.commit()
            return results
