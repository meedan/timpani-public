import json

from timpani.content_store.content_item import ContentItem
from timpani.content_store.content_store_interface import ContentStoreInterface
from timpani.raw_store.item import Item

# from timpani.model_service.means_tokens_vectorization_alegre_wrapper import (
#    MeansTokensAlegreVectorizationModelService,
# )
from timpani.model_service.paraphrase_multilingual_vectorization_alegre_wrapper import (
    ParaphraseMultilingualAlegreVectorizationModelService,
)
from timpani.processing_sequences.default_workflow import (
    DefaultWorkflow,
    DefaultContentItemState,
)
from timpani.conductor.actions.clustering import AlegreClusteringAction

import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()

# we are resycling the state model from Default workflow for now
# need to include these imports if more states added
# from sqlalchemy import orm


class MeedanTSEWorkflow(DefaultWorkflow):
    """
    Define the processing sequence for Meedan content items
    that just does vectorization and clustering
    mostly used for testing
    """

    SIMILARITY_THRESHOLD = 0.875

    def __init__(self, content_store: ContentStoreInterface) -> None:
        # pass the reference to the content store to the super class
        super().__init__(content_store=content_store)
        self.means_tokens_model = ParaphraseMultilingualAlegreVectorizationModelService  # MeansTokensAlegreVectorizationModelService()
        self.clustering_action = AlegreClusteringAction(
            self.content_store, similarity_threshold=self.SIMILARITY_THRESHOLD
        )

    def get_name(self):
        """
        Return the slug id for the workflow
        """
        return "meedan_tse"

    @classmethod
    def get_state_model(cls):
        return DefaultContentItemState()

    def extract_items(self, raw_item: Item, date_id=None) -> ContentItem:
        """
        create a ContentItem from a RawStore Item and return it
        """
        # TODO: check what kind of raw item it is (
        assert raw_item.source_id in ["s3_csv_tse_tipline"]

        items = []

        # if we are unable to  extract the data log, and
        # create the item in a failed state
        try:
            # Skip all the items that are not text (video, audio, image)
            if raw_item.content["request_type"] != "text":
                logging.debug(
                    f"skipping content item {raw_item.content['id']} because it has request_type {raw_item.content['request_type']}"
                )
                return items

            # There are multple fields we could extract content from
            # we are using "text", because "clean_text" sometimes had too much text removed in
            # upstream preprocessing (we will do those steps later as a filter)
            text = raw_item.content["content"]

            published = raw_item.content["created_at"]

            item = ContentItem(
                run_id=raw_item.run_id,
                source_id=raw_item.source_id,
                workspace_id=raw_item.workspace_id,
                query_id=raw_item.query_id,
                date_id=date_id,
                raw_content_id=raw_item.content_id,
                raw_created_at=raw_item.created_at,
                raw_content=text,
                content_published_date=published,
            )
            items.append(item)
        except KeyError as e:
            item = ContentItem(
                run_id=raw_item.run_id,
                source_id=raw_item.source_id,
                workspace_id=raw_item.workspace_id,
                query_id=raw_item.query_id,
                date_id=date_id,
                raw_content_id=raw_item.content_id,
                raw_created_at=raw_item.created_at,
                content_published_date=raw_item.created_at,
                # store the unparsed content for debugging
                raw_content=json.dumps(raw_item.content),
                # mark the content as blank, so that the processer knows to insert
                # it in an error state
                content="",
            )

            logging.warning(f"Unable to parse raw item: {e} {item}")
            items.append(item)

        return items

    def next_state(self, items: list[ContentItem], state_name=None):
        """
        Apply the next step in the transformation for the content item
        NOTE initially we are using the state processing from DefaultWorkflow
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
            # For the first pass, we are not adding any new states beyound what Default
            # already supports

            # TODO: will eventually need an intermediate VECTOR_STORED state to handle callback of vector model and store it?

            # remaining states will fall through to the DefaultWorkflow

            case (
                _
            ):  # nothing matched so check if it matches any actions in the super class
                super().next_state(items=items, state_name=state_name)
        return DefaultWorkflow.SUCCESS
