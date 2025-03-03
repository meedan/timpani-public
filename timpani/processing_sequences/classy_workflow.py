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

# from timpani.model_service.classycat_wrapper import ClassycatWrapper
from timpani.model_service.classycat_presto_service import ClassycatPrestoService

from sqlalchemy import orm

import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()


class ClassyContentItemState(ContentItemState):
    """
    Extend the content item state model with aditional states for experimental
    Classycat categorization keywords
    """

    # define additional states for this model
    STATE_VECTORIZED = "vectorized"
    STATE_CLUSTERED = "clustered"
    STATE_HASHTAGED = "hashtaged"
    STATE_CATEGORIZED = "categorized"
    classy_states = [
        STATE_VECTORIZED,
        STATE_CLUSTERED,
        STATE_HASHTAGED,
        STATE_CATEGORIZED,
    ]
    classy_transitions = {
        ContentItemState.STATE_READY: [
            STATE_VECTORIZED,
            ContentItemState.STATE_FAILED,
        ],
        STATE_VECTORIZED: [STATE_CLUSTERED, ContentItemState.STATE_FAILED],
        STATE_CLUSTERED: [
            STATE_HASHTAGED,
            ContentItemState.STATE_FAILED,
        ],
        STATE_HASHTAGED: [
            STATE_CATEGORIZED,
            ContentItemState.STATE_FAILED,
        ],
        STATE_CATEGORIZED: [
            ContentItemState.STATE_COMPLETED,
            ContentItemState.STATE_FAILED,
        ],
    }

    # make sure we can instantiate the correct subclass from orm
    __mapper_args__ = {
        "polymorphic_on": "state_model_name",
        "polymorphic_identity": "ClassyContentItemState",
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
        self.update_state_model(
            additional_states=self.classy_states,
            additional_transitions=self.classy_transitions,
        )


class ClassyWorkflow(Workflow):
    """
    Define the processing sequence for Meedan content items
    that does vectorization and clustering as well as
    hashtag keywords and classycat categorization keywords
    """

    CLUSTERING_TIMEOUT_WINDOW = (
        120  # how long to wait before giving up on clustering job
    )

    def __init__(
        self, content_store: ContentStoreInterface, classycat_schema_name: str
    ) -> None:
        # pass the reference to the content store to the super class
        super().__init__(content_store=content_store)

        self.vector_model = ParaphraseMultilingualAlegreVectorizationModelService()
        self.clustering_action = AlegreClusteringAction(self.content_store)
        self.keyword_extractor = BasicKeywordsExtrator()
        # self.classycat = ClassycatWrapper(default_schema_name=classycat_schema_name)
        self.classycat = ClassycatPrestoService()

    def get_name(self):
        """
        Return the slug id for the workflow
        """
        return "classy"

    @classmethod
    def get_state_model(cls):
        return ClassyContentItemState()

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
        if transition_state_name == ClassyContentItemState.STATE_HASHTAGED:
            # STATE_CATEGORIZED is a batch action
            return True
        return False

    def next_state(self, items: list[ContentItem], state_name=None):
        """
        Apply the next step in the transformation for the content item or batch of items
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

            case ClassyContentItemState.STATE_READY:
                assert (
                    len(items) == 1
                ), "state transition cannot dispatch batch with multiple items"
                # if it is in ready state, send it to be vectorized
                self.content_store.start_transition_to_state(
                    item, ClassyContentItemState.STATE_VECTORIZED
                )
                self.vector_model.vectorize_content_item(
                    item, target_state=ClassyContentItemState.STATE_VECTORIZED
                )

            # TODO: will eventually need an intermediate VECTOR_STORED state to handle callback of vector model and store it?

            case ClassyContentItemState.STATE_VECTORIZED:
                assert (
                    len(items) == 1
                ), "vectorized state transition cannot dispatch batch with multiple items"
                # if vectorized, send it for clustering
                self.content_store.start_transition_to_state(
                    item, ClassyContentItemState.STATE_CLUSTERED
                )
                self.clustering_action.add_item_to_best_cluster(
                    item, ClassyContentItemState.STATE_CLUSTERED
                )

            case ClassyContentItemState.STATE_CLUSTERED:
                assert (
                    len(items) == 1
                ), "clustred state transition cannot dispatch batch with multiple items"
                # if vectorized, send it for clustering
                self.content_store.start_transition_to_state(
                    item, ClassyContentItemState.STATE_HASHTAGED
                )
                self.keyword_extractor.add_keywords_to_item(
                    item, ClassyContentItemState.STATE_HASHTAGED
                )

            case ClassyContentItemState.STATE_HASHTAGED:
                # if hashtaged, send it for class categorization
                # this can handle invidual or batch
                # TODO: need to start transition to state for multiple items
                for item in items:
                    self.content_store.start_transition_to_state(
                        item, ClassyContentItemState.STATE_CATEGORIZED
                    )
                # submit to model in batch mode
                self.classycat.submit_to_presto_model(
                    items, ClassyContentItemState.STATE_CATEGORIZED
                )

            case ClassyContentItemState.STATE_CATEGORIZED:
                assert (
                    len(items) == 1
                ), "clustred state transition cannot dispatch batch with multiple items"
                # our work here is done
                self.content_store.transition_item_state(
                    item, ClassyContentItemState.STATE_COMPLETED
                )

            case _:  # nothing matched
                logging.error(
                    f"Item {item.content_item_id} state {state.current_state} does not match states for workflow {self.get_name}"
                )
                return Workflow.ERROR

        return Workflow.SUCCESS  # indicate status per item

    def check_state_timeout(self, state: ContentItemState):
        """
        NOTE: we need a longer timeout window for classification
        state transition
        Returns True if state has recently started a transition within
        the timeout window, indicating that a transition is most likely
        still in process and the next state transition should not start.
        Intended to allow multiple processes to manage state transitions
        """
        # TODO: this needs to handle batch, and do lookup without ContentItemState object?

        # check if transition marked in progress
        if state.transition_start > state.transition_end:
            # check if within window
            duration = datetime.utcnow() - state.transition_start
            timeout_window = Workflow.STATE_TIMEOUT_DURATION
            # if we are transitioning to CATEGORIZED allow more time
            if state.current_state == ClassyContentItemState.STATE_HASHTAGED:
                timeout_window = self.CLUSTERING_TIMEOUT_WINDOW
            if duration.total_seconds() < timeout_window:
                return True
        return False
