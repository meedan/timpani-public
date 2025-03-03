import json
from datetime import datetime

from timpani.processing_sequences.workflow import Workflow
from timpani.content_store.content_item import ContentItem
from timpani.content_store.item_state_model import ContentItemState
from timpani.content_store.content_store_interface import ContentStoreInterface
from timpani.raw_store.item import Item
from timpani.conductor.actions.hashtag_keywords import BasicKeywordsExtrator
from timpani.model_service.paraphrase_multilingual_vectorization_alegre_wrapper import (
    ParaphraseMultilingualAlegreVectorizationModelService,
)
from timpani.conductor.actions.clustering import AlegreClusteringAction
from timpani.model_service.yake_presto_service import YakePrestoService

from sqlalchemy import orm

import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()


class MeedanContentItemState(ContentItemState):
    """
    Extend the content item state model with aditional states
    """

    # define additional states for this model
    STATE_KEYWORDED = "keyworded"
    STATE_VECTORIZED = "vectorized"
    STATE_CLUSTERED = "clustered"
    STATE_HASHTAGED = "hashtaged"
    meedan_states = [
        STATE_KEYWORDED,
        STATE_VECTORIZED,
        STATE_CLUSTERED,
        STATE_HASHTAGED,
    ]
    meedan_transitions = {
        ContentItemState.STATE_READY: [
            STATE_KEYWORDED,
            ContentItemState.STATE_FAILED,
        ],
        STATE_KEYWORDED: [
            STATE_VECTORIZED,
            ContentItemState.STATE_FAILED,
        ],
        STATE_VECTORIZED: [STATE_CLUSTERED, ContentItemState.STATE_FAILED],
        STATE_CLUSTERED: [
            STATE_HASHTAGED,
            ContentItemState.STATE_FAILED,
        ],
        STATE_HASHTAGED: [
            ContentItemState.STATE_COMPLETED,
            ContentItemState.STATE_FAILED,
        ],
    }

    # make sure we can instantiate the correct subclass from orm
    __mapper_args__ = {
        "polymorphic_on": "state_model_name",
        "polymorphic_identity": "MeedanContentItemState",
    }

    def __init__(self):
        super().__init__()
        self.reload_states()

    @orm.reconstructor
    def reload_states(self):
        """
        Need to make sure the correct state mapping is recreated when the
        object is extracted from database
        https://docs.sqlalchemy.org/en/13/orm/constructors.html
        """
        super().reload_states()
        self.valid_states, self.valid_transitions = self.update_state_model(
            additional_states=self.meedan_states,
            additional_transitions=self.meedan_transitions,
        )


class MeedanWorkflow(Workflow):
    """
    Define the processing sequence for Meedan content items
    that just does vectorization and clustering
    mostly used for testing
    """

    def __init__(self, content_store: ContentStoreInterface) -> None:
        # pass the reference to the content store to the super class
        super().__init__(content_store=content_store)

        self.vector_model = (
            ParaphraseMultilingualAlegreVectorizationModelService()
            # MeansTokensAlegreVectorizationModelService()
        )
        self.clustering_action = AlegreClusteringAction(self.content_store)
        self.keyword_extractor = BasicKeywordsExtrator()
        self.yake_keywords = YakePrestoService()

    def get_name(self):
        """
        Return the slug id for the workflow
        """
        return "meedan"

    @classmethod
    def get_state_model(cls):
        return MeedanContentItemState()

    def extract_items(self, raw_item: Item, date_id=None) -> ContentItem:
        """
        create a ContentItem from a RawStore Item and return it
        """
        # TODO: check what kind of raw item it is (twitter, youtube)
        assert raw_item.source_id in ["junkipedia"]

        items = []

        try:
            # There are multple fields we could extract content from so we put them into
            # a list, and create and create additional items for each
            # These are pulling from a junkipeida-standarized data element,
            # TODO: other workflows using same process, should use the same code.
            content_texts = []
            if raw_item.content["attributes"]["search_data_fields"][
                "description"
            ] not in [None, "", "null"]:
                content_texts.append(
                    # tuple with the field name and value
                    (
                        "description",
                        raw_item.content["attributes"]["search_data_fields"][
                            "description"
                        ],
                    )
                )
            if raw_item.content["attributes"]["search_data_fields"][
                "post_title"
            ] not in [None, "", "null"]:
                content_texts.append(
                    (
                        "post_title",
                        raw_item.content["attributes"]["search_data_fields"][
                            "post_title"
                        ],
                    )
                )
            if raw_item.content["attributes"]["search_data_fields"][
                "transcript_text"
            ] not in [None, "", " ", "null"]:
                content_texts.append(
                    (
                        "transcript_text",
                        raw_item.content["attributes"]["search_data_fields"][
                            "transcript_text"
                        ],
                    )
                )

            published_at = datetime.strptime(
                raw_item.content["attributes"]["published_at"],
                # format "2023-05-03T06:15:02.000Z"
                "%Y-%m-%dT%H:%M:%S.%fZ",
            )

            url = raw_item.content["attributes"]["search_data_fields"]["url"]

            # this is to use the language code reported by the platform (vs an internal classifier)
            source_lang_code = None
            if raw_item.content["attributes"]["search_data_fields"][
                "language_code"
            ] not in [None, "", "null"]:
                source_lang_code = raw_item.content["attributes"]["search_data_fields"][
                    "language_code"
                ]

            # now loop and create an item for each of the content_texts found
            for content_text in content_texts:
                item = ContentItem(
                    run_id=raw_item.run_id,
                    source_id=raw_item.source_id,
                    workspace_id=raw_item.workspace_id,
                    query_id=raw_item.query_id,
                    date_id=date_id,
                    raw_content_id=raw_item.content_id,
                    raw_created_at=raw_item.created_at,
                    raw_content=content_text[1],
                    source_field=content_text[0],
                    content_published_date=published_at,
                    content_published_url=url,
                    content_language_code=source_lang_code,
                )
                items.append(item)
        # TODO: other types of exceptions here, date format
        # TODO: also, if there are no non-blank fields to extract, should we count as "unprocessable"
        except KeyError as e:
            item = ContentItem(
                run_id=raw_item.run_id,
                source_id=raw_item.source_id,
                workspace_id=raw_item.workspace_id,
                query_id=raw_item.query_id,
                date_id=date_id,
                raw_content_id=raw_item.content_id,
                raw_created_at=raw_item.created_at,
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
            case MeedanContentItemState.STATE_READY:
                assert (
                    len(items) == 1
                ), "state transition cannot dispatch batch with multiple items"
                # if it is in ready state, send it to be vectorized
                # by the multilingual means tokens model
                self.content_store.start_transition_to_state(
                    item, MeedanContentItemState.STATE_KEYWORDED
                )
                self.yake_keywords.submit_to_presto_model(
                    content_items=items,
                    target_state=MeedanContentItemState.STATE_KEYWORDED,
                )

            case MeedanContentItemState.STATE_KEYWORDED:
                assert (
                    len(items) == 1
                ), "state transition cannot dispatch batch with multiple items"
                # if it is in ready state, send it to be vectorized
                # by the multilingual means tokens model
                self.content_store.start_transition_to_state(
                    item, MeedanContentItemState.STATE_VECTORIZED
                )
                self.vector_model.vectorize_content_item(
                    item, target_state=MeedanContentItemState.STATE_VECTORIZED
                )

            # TODO: will eventually need an intermediate VECTOR_STORED state to handle callback of vector model and store it?

            case MeedanContentItemState.STATE_VECTORIZED:
                assert (
                    len(items) == 1
                ), "state transition cannot dispatch batch with multiple items"
                # if vectorized, send it for clustering
                self.content_store.start_transition_to_state(
                    item, MeedanContentItemState.STATE_CLUSTERED
                )
                self.clustering_action.add_item_to_best_cluster(
                    item, MeedanContentItemState.STATE_CLUSTERED
                )

            case MeedanContentItemState.STATE_CLUSTERED:
                assert (
                    len(items) == 1
                ), "state transition cannot dispatch batch with multiple items"
                # if vectorized, send it for clustering
                self.content_store.start_transition_to_state(
                    item, MeedanContentItemState.STATE_HASHTAGED
                )
                self.keyword_extractor.add_keywords_to_item(
                    item, MeedanContentItemState.STATE_HASHTAGED
                )

            case MeedanContentItemState.STATE_HASHTAGED:
                assert (
                    len(items) == 1
                ), "state transition cannot dispatch batch with multiple items"
                # our work here is done
                self.content_store.start_transition_to_state(
                    item, MeedanContentItemState.STATE_COMPLETED
                )
                self.content_store.transition_item_state(
                    item, MeedanContentItemState.STATE_COMPLETED
                )

            case _:  # nothing matched
                logging.error(
                    f"Item {item.content_item_id} state {state.current_state} does not match states for workflow {self.get_name}"
                )
        # TODO: how do we know it has failed?  because we keep revisiting states, or some kind of timeout from initial creation
        return Workflow.SUCCESS
