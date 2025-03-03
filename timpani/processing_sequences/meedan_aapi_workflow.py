import json
from sqlalchemy import orm
from timpani.content_store.content_item import ContentItem
from timpani.content_store.content_store_interface import ContentStoreInterface
from timpani.raw_store.item import Item

from timpani.conductor.transforms.twitter import TwitterTextTransforms

# from timpani.model_service.means_tokens_vectorization_alegre_wrapper import (
#    MeansTokensAlegreVectorizationModelService,
# )
from timpani.model_service.paraphrase_multilingual_vectorization_alegre_wrapper import (
    ParaphraseMultilingualAlegreVectorizationModelService,
)
from timpani.processing_sequences.default_workflow import (
    DefaultWorkflow,
)
from timpani.content_store.item_state_model import ContentItemState
from timpani.conductor.actions.clustering import AlegreClusteringAction

import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()

# we are resycling the state model from Default workflow for now
# need to include these imports if more states added
# from sqlalchemy import orm


class AAPIContentItemState(ContentItemState):
    """
    Extend the content item state model with aditional states.
    Similar to default model, but includes text transform
    """

    # define additional states for this model
    STATE_TEXT_TRANSFORMED = "text_transformed"
    STATE_VECTORIZED = "vectorized"
    STATE_CLUSTERED = "clustered"
    aapi_states = [STATE_TEXT_TRANSFORMED, STATE_VECTORIZED, STATE_CLUSTERED]
    aapi_transitions = {
        ContentItemState.STATE_READY: [
            STATE_TEXT_TRANSFORMED,
            ContentItemState.STATE_FAILED,
        ],
        STATE_TEXT_TRANSFORMED: [
            STATE_VECTORIZED,
            ContentItemState.STATE_FAILED,
        ],
        STATE_VECTORIZED: [STATE_CLUSTERED, ContentItemState.STATE_FAILED],
        STATE_CLUSTERED: [
            ContentItemState.STATE_COMPLETED,
            ContentItemState.STATE_FAILED,
        ],
    }

    # make sure we can instantiate the correct subclass from orm
    __mapper_args__ = {
        "polymorphic_on": "state_model_name",
        "polymorphic_identity": "AAPIContentItemState",
    }

    def __init__(self):
        super().__init__()
        self.reload_states()

    @orm.reconstructor
    def reload_states(self):
        """
        add the aditional states to the model
        """
        super().reload_states()
        self.valid_states, self.valid_transitions = self.update_state_model(
            additional_states=self.aapi_states,
            additional_transitions=self.aapi_transitions,
        )


class MeedanAAPIWorkflow(DefaultWorkflow):
    """
    Define the processing sequence for Meedan content items
    that just does vectorization and clustering
    mostly used for testing
    """

    SIMILARITY_THRESHOLD = 0.875

    def __init__(self, content_store: ContentStoreInterface) -> None:
        # pass the reference to the content store to the super class
        super().__init__(content_store=content_store)

        self.twitter_transform = TwitterTextTransforms()

        # TODO: I don't think this should have initialized references to the models, pass them in?
        self.vecotorization_model = (
            ParaphraseMultilingualAlegreVectorizationModelService()
            # MeansTokensAlegreVectorizationModelService()
        )
        self.clustering_action = AlegreClusteringAction(
            self.content_store, similarity_threshold=self.SIMILARITY_THRESHOLD
        )

    def get_name(self):
        """
        Return the slug id for the workflow
        """
        return "meedan_aapi"

    @classmethod
    def get_state_model(cls):
        return AAPIContentItemState()

    def extract_items(self, raw_item: Item, date_id=None) -> ContentItem:
        """
        create a ContentItem from a RawStore Item and return it
        """
        # TODO: check what kind of raw item it is (twitter, youtube)
        assert raw_item.source_id in ["s3_csv_aapi_tweets"]

        items = []

        # if we are unable to  extract the data log, and
        # create the item in a failed state
        try:
            # Skip items with is_retweet since most of that content is not relevent at the moment
            if raw_item.content["is_retweet"] == "t":
                logging.info(
                    f"skipping content item {raw_item.content['conversation_id']} because is_retweet"
                )
                return items

            # There are multple fields we could extract content from
            # we are using "text", because "clean_text" sometimes had too much text removed in
            # upstream preprocessing (we will do those steps later as a filter)
            text = raw_item.content["text"]

            # Looks like https://twitter.com/twitter/status/<conversation_id> would work
            url = "https://twitter.com/twitter/status/" + raw_item.content["id"]

            # We believe these come from twitter's auto language detection, using a schema close to the google one
            lang_id = raw_item.content["lang"]

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
                content_published_url=url,
                content_language_code=lang_id,
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
                # TODO: should have dedicated field for this? or use blank?
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

            case ContentItemState.STATE_READY:
                assert (
                    len(items) == 1
                ), "state transition cannot dispatch batch with multiple items"

                # if it is in ready state, apply text transformations
                self.content_store.start_transition_to_state(
                    item, AAPIContentItemState.STATE_TEXT_TRANSFORMED
                )
                item.content = self.twitter_transform.transform_content(item.content)
                # update item state with transformed text
                item = self.content_store.update_item(
                    item, AAPIContentItemState.STATE_TEXT_TRANSFORMED
                )
            case AAPIContentItemState.STATE_TEXT_TRANSFORMED:
                assert (
                    len(items) == 1
                ), "state transition cannot dispatch batch with multiple items"
                # then send it to be vectorized by the multilingual means tokens model
                self.content_store.start_transition_to_state(
                    item, AAPIContentItemState.STATE_VECTORIZED
                )
                self.vecotorization_model.vectorize_content_item(
                    item, target_state=AAPIContentItemState.STATE_VECTORIZED
                )

            # remaining states (VECTORIZED, CLUSTERED, COMPLETED, will fall through to the DefaultWorkflow

            case (
                _
            ):  # nothing matched so check if it matches any actions in the super class
                super().next_state(items=items, state_name=state_name)
        return DefaultWorkflow.SUCCESS
