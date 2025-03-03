import json
import datetime
from datetime import timedelta
from timpani.processing_sequences.workflow import Workflow
from timpani.content_store.content_item import ContentItem
from timpani.content_store.item_state_model import ContentItemState
from timpani.content_store.content_store_interface import ContentStoreInterface
from timpani.raw_store.item import Item
from timpani.conductor.actions.delay import DelayingAction

from timpani.conductor.actions.logging import LoggingAction

from sqlalchemy import orm

import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()


class TestContentItemState(ContentItemState):
    """
    Extend the content item state model with aditional states,
    but don't require any additional dependencies (for testing)
    """

    # define additional states for this model
    STATE_PLACEHOLDER = "placeholder"
    STATE_DELAYED = "delayed"
    test_states = [STATE_PLACEHOLDER, STATE_DELAYED]
    test_transitions = {
        ContentItemState.STATE_READY: [
            STATE_PLACEHOLDER,
            ContentItemState.STATE_FAILED,
        ],
        STATE_PLACEHOLDER: [
            STATE_DELAYED,
            ContentItemState.STATE_FAILED,
        ],
        STATE_DELAYED: [
            ContentItemState.STATE_COMPLETED,
            ContentItemState.STATE_FAILED,
        ],
    }

    def __init__(self):
        super().__init__()
        self.reload_states()

    # make sure we can instantiate the correct subclass from orm
    __mapper_args__ = {
        "polymorphic_on": "state_model_name",
        "polymorphic_identity": "TestContentItemState",
    }

    @orm.reconstructor
    def reload_states(self):
        """
        Need to make sure the correct state mapping is recreated when the
        object is extracted from database
        https://docs.sqlalchemy.org/en/13/orm/constructors.html
        """
        super().reload_states()
        self.valid_states, self.valid_transitions = super().update_state_model(
            additional_states=self.test_states,
            additional_transitions=self.test_transitions,
        )


class TestWorkflow(Workflow):
    """
    Define a the processing sequence for content items
    that just does vectorization and clustering
    mostly used for testing
    TODO: probably need seperate workflows per content source
    """

    def __init__(self, content_store: ContentStoreInterface) -> None:
        # pass the reference to the content store to the super class
        super().__init__(content_store=content_store)

        self.logging_action = LoggingAction()
        self.batch_delay = DelayingAction()

    def get_name(self):
        """
        Return the slug id for the workflow
        """
        return "test_workflow"

    @classmethod
    def get_state_model(cls):
        return TestContentItemState()

    def extract_items(self, raw_item: Item, date_id=None) -> ContentItem:
        """
        create a ContentItem from a RawStore Item and return it
        """
        # check what kind of raw item it is (twitter, youtube)
        # extract the appropriate fields from the raw item

        # check that it is a known source
        assert raw_item.source_id in ["faker_testing", "junkipedia"]
        # TODO seperate workflows per content source

        items = []

        # if we are unable to  extract the data log, and
        # create the item in a failed state
        try:
            # TODO: there are multple fields we could extract content from
            # author, title
            # TODO: not sure yet the best way to handle branching for multiple content types
            if raw_item.source_id == "faker_testing":
                all_text = raw_item.content["title"]
                published_at = raw_item.created_at
                url = None
            else:
                # junkipedia
                all_text = raw_item.content["attributes"]["search_data_fields"][
                    "all_text"
                ]
                published_at = datetime.datetime.strptime(
                    raw_item.content["attributes"]["published_at"],
                    #  "2023-05-03T06:15:02.000Z"
                    "%Y-%m-%dT%H:%M:%S.%fZ",
                )

                url = raw_item.content["attributes"]["search_data_fields"]["url"]

            item = ContentItem(
                run_id=raw_item.run_id,
                source_id=raw_item.source_id,
                workspace_id=raw_item.workspace_id,
                query_id=raw_item.query_id,
                date_id=date_id,  # TODO: do we need date_id maybe s3 path is more useful?
                raw_content_id=raw_item.content_id,
                raw_created_at=raw_item.created_at,
                raw_content=all_text,
                content_published_date=published_at,
                content_published_url=url,
            )
            items.append(item)
        except KeyError as e:  # TODO: other types of exceptions here, date format
            item = ContentItem(
                run_id=raw_item.run_id,
                source_id=raw_item.source_id,
                workspace_id=raw_item.workspace_id,
                query_id=raw_item.query_id,
                date_id=date_id,  # TODO: do we need date_id maybe s3 path is more useful?
                raw_content_id=raw_item.content_id,
                raw_created_at=raw_item.created_at,
                content_published_date=raw_item.created_at,
                # store the unparsed content for debugging
                raw_content=json.dumps(raw_item.content),
                # mark the content as blank, so that the processer knows to insert
                # it in an error state
                # TODO: should have dedicated field for this? or use blank?
                content="",
            )

            logging.warning(f"Unable to parse raw item: {e} {item}")
            items.append(item)

        return items

    def is_batch_transition_from(self, transition_state_name: str):
        """
        Hack to deal with the fact that processor needs to know if it
        should call all the items corresponding to a transition individually
        (in parallel threads), or in a single batch
        TODO: eventually call everything in batch
        NOTE: I don't like that this is stored seperately from the state and
        transistions, makes it really easy to mess up :-(
        """
        # has to name what it is transitioning FROM
        if transition_state_name == TestContentItemState.STATE_PLACEHOLDER:
            # state delayed is a batch action
            return True
        return False

    def next_state(self, items: list[ContentItem], state_name=None):
        """
        Apply the next step in the transformation for the content item.
        """

        # for the moment, most batches are (can only be) single items
        item = items[0]
        # assume that state_name applies to everything in the batch, but if not, look it up
        if state_name is None:
            state = self.content_store.get_item_state(item.content_item_state_id)
            state_name = state.current_state
            logging.debug(
                f"next state called for item {item.content_item_id} state {state.current_state}"
            )

        match state_name:
            case TestContentItemState.STATE_READY:
                assert (
                    len(items) == 1
                ), "state transition cannot dispatch batch with multiple items"
                # if it is in ready state, send it to 'placeholder'
                # (not doing real operations in this test)
                self.content_store.start_transition_to_state(
                    item, TestContentItemState.STATE_PLACEHOLDER
                )
                self.logging_action.log_content_item(
                    item,
                    target_state=TestContentItemState.STATE_PLACEHOLDER,
                    store=self.content_store,
                )
                # this is a hack to make a test work: we can force the
                # item to raise an exception and get put into the error
                # state by passing in blank content
                assert item.content != ""

            case TestContentItemState.STATE_PLACEHOLDER:
                # logging.debug(f"Delaying {len(items)} items")
                # simulate a long running blocking process called in batch
                for item in items:
                    self.content_store.start_transition_to_state(
                        item, TestContentItemState.STATE_DELAYED
                    )
                self.batch_delay.delay_content_items(
                    content_items=items,
                    target_state=TestContentItemState.STATE_DELAYED,
                    store=self.content_store,
                    delay_seconds=1,
                )

            case TestContentItemState.STATE_DELAYED:
                assert (
                    len(items) == 1
                ), "state transition cannot dispatch batch with multiple items"
                # our work here is done
                self.content_store.start_transition_to_state(
                    item, TestContentItemState.STATE_COMPLETED
                )
                self.content_store.transition_item_state(
                    item, TestContentItemState.STATE_COMPLETED
                )

            case _:  # nothing matched
                logging.error(
                    f"Item {item.content_item_id} state {state.current_state} does not match states for workflow {self.get_name}"
                )
                return Workflow.ERROR
        return Workflow.SUCCESS
        # TODO: how do we know it has failed?  because we keep revisiting states, or some kind of timeout from initial creation

    def get_item_live_duration(self) -> timedelta:
        """
        Return the duration that items in the workflow will be considered
        active before they are removed. None indicates that records should
        not be removed
        """
        return timedelta(days=1)
