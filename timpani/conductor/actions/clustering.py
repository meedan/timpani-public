from operator import itemgetter
from collections import namedtuple
from timpani.vector_store.alegre_store_wrapper import AlegreVectorStoreService
from timpani.content_store.content_store_interface import ContentStoreInterface
from timpani.content_store.content_item import ContentItem
from timpani.content_store.content_cluster import ContentCluster
from timpani.content_store.item_state_model import ContentItemState

import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()

# class for associating content items with scores and cluster size
ScoredItemRelation = namedtuple("ScoredItemRelation", "score id size")


class AlegreClusteringAction(object):
    """
    Implements a heuristic and threshold for grouping together
    content items according to the similarity of their vectors
    stored in the alegre content store.
    """

    SIMILARITY_THRESHOLD = 0.95
    STRESS_SCORE_INCREMENT = 0.1

    def __init__(
        self,
        content_store: ContentStoreInterface,
        similarity_threshold=SIMILARITY_THRESHOLD,
    ) -> None:
        if similarity_threshold is not None:
            assert similarity_threshold <= 1.0
            assert similarity_threshold >= 0.0
            self.SIMILARITY_THRESHOLD = similarity_threshold

        self.vector_store = AlegreVectorStoreService()
        self.content_store = content_store

    @staticmethod
    def get_name() -> str:
        """
        Return an id string that can be used as a key to uniquely identify this action
        """
        return "alegre_clustering"

    def get_sorted_scored_items(self, scored_item_ids):
        """
        Sort the ids by score then cluster size, tiebreaking on id.
        (queries content store to get sizes)
        scores expected to range 0-2
        TODO: we are already hitting db to get cluster size, should get cluster_id at the
        same time so that we can use it in evaluating priority
        """
        ids_scored_sized = []
        for scored_item in scored_item_ids:
            cluster_size = int(self.content_store.get_item_cluster_size(scored_item.id))
            ids_scored_sized.append(
                ScoredItemRelation(scored_item.score, scored_item.id, cluster_size)
            )

        ids_scored_sized.sort(key=itemgetter(0, 2, 1), reverse=True)
        return ids_scored_sized

    def add_item_to_best_cluster(
        self,
        item: ContentItem,
        target_state: ContentItemState,
        scored_item_ids=None,
    ) -> ContentCluster:
        """
        Ask the vector store to get the set of similar items based on the
        model vector and apply the clustering heuristic and update the
        cluster state  in the content store
        TODO: in the short term this is happening without callbacks, in the long term this
        would use the vector for the content item and would be a two step process involving callback
        """
        # TODO: higher stress if more similar items exist

        # if this was not from callback, need to make the request
        if scored_item_ids is None:
            scored_item_ids = self.vector_store.request_similar_content_item_ids(
                item, threshold=self.SIMILARITY_THRESHOLD
            )
        logging.debug(f"Found {len(scored_item_ids)} ids from Alegre clustering")
        # retrieve content clusters from content_store corresponding to ids
        # so that we can tiebreak identically scored clusters using cluster size
        # to avoid fragmentation
        ids_scored_sized = self.get_sorted_scored_items(scored_item_ids)

        cluster_item = None  # maybe nothing matches
        if len(ids_scored_sized) > 0:
            # for now, ordered assume first item is best match
            # .. but don't self match
            for candidate in ids_scored_sized:
                # these are (score,id,size) tuples
                candidate_id = int(candidate.id)
                if candidate_id != int(item.content_item_id):
                    # assumes list sorted by score, size, id so we started at highest
                    cluster_item = self.content_store.get_item(candidate_id)
                    if cluster_item is None:
                        # something is wrong so skip and try another
                        logging.error(
                            f"Unable to fetch similar item {candidate_id} from content_store"
                        )
                    else:
                        # check if item is already in a cluster
                        # in order to manage state and avoid fragmentation, we should only make comparisons with items
                        # that are already in clusters
                        if candidate.size > 0:
                            # assume we found an acceptable match
                            logging.debug(
                                f"similar item selected for item {item.content_item_id}: {cluster_item.content_item_id}"
                            )
                            break
        else:
            logging.debug(f"no similar item selected for item {item.content_item_id}")

        # either add to cluster that the item is in, or create a new cluster
        # with just the one item
        cluster = self.content_store.cluster_items(
            first_item=item, second_item=cluster_item
        )

        # TODO: better cluster update logic: if there were multiple similar items found,
        # and they do not all land in the same cluster, set stress higher

        # if the cluster size is > 2, increase the likelyhood that it should be checked for stress
        # TO
        if cluster.num_items > 2:
            cluster.stress_score += self.STRESS_SCORE_INCREMENT
            cluster = self.content_store.update_cluster(cluster)

        # mark state update if requested
        if target_state is not None:
            self.content_store.transition_item_state(item, target_state)
        return cluster
