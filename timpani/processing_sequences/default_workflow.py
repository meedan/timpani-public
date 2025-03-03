import json
from datetime import datetime
from timpani.processing_sequences.workflow import Workflow
from timpani.content_store.content_item import ContentItem
from timpani.content_store.item_state_model import ContentItemState
from timpani.content_store.content_store_interface import ContentStoreInterface

# from timpani.model_service.means_tokens_vectorization_alegre_wrapper import (
#    MeansTokensAlegreVectorizationModelService,
# )
from timpani.model_service.paraphrase_multilingual_vectorization_alegre_wrapper import (
    ParaphraseMultilingualAlegreVectorizationModelService,
)
from timpani.conductor.actions.clustering import AlegreClusteringAction
from timpani.raw_store.item import Item
from sqlalchemy import orm

import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()


class DefaultContentItemState(ContentItemState):
    """
    Extend the content item state model with aditional states
    """

    # define additional states for this model
    STATE_VECTORIZED = "vectorized"
    STATE_CLUSTERED = "clustered"
    default_states = [STATE_VECTORIZED, STATE_CLUSTERED]
    default_transitions = {
        ContentItemState.STATE_READY: [
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
        "polymorphic_identity": "DefaultContentItemState",
    }

    def __init__(self):
        super().__init__()
        self.reload_states()

    @orm.reconstructor
    def reload_states(self):
        """
        https://docs.sqlalchemy.org/en/13/orm/constructors.html
        > The SQLAlchemy
        ORM does not call __init__ when recreating objects from database rows.
        The ORM’s process is somewhat akin to the Python standard library’s
        pickle module, invoking the low level __new__ method and then quietly
        restoring attributes directly on the instance rather than calling
        __init__. > If you need to do some setup on database-loaded instances
        before they’re ready to use, there is an event hook known as
        InstanceEvents.load() which can achieve this; it is also available via a
        class-specific decorator called reconstructor().
        """
        super().reload_states()
        self.valid_states, self.valid_transitions = self.update_state_model(
            additional_states=self.default_states,
            additional_transitions=self.default_transitions,
        )


class DefaultWorkflow(Workflow):
    """
    Define a the processing sequence for content items
    that just does vectorization and clustering
    mostly used for testing. Goal is that this a program that
    holds all of the logic for transforming data in a single
    file.
    NOTE: this is expecting content with JSON structure
    corresponding to Junkipedia export, maybe should
    call it DefaultJunkipediaWorkflow?
    """

    def __init__(self, content_store: ContentStoreInterface) -> None:
        # pass the reference to the content store to the super class
        super().__init__(content_store=content_store)

        self.similarity_model = (
            ParaphraseMultilingualAlegreVectorizationModelService()
        )  # MeansTokensAlegreVectorizationModelService()
        self.clustering_action = AlegreClusteringAction(self.content_store)

        # TODO: remove this, now comes from the state model
        self.item_state_sequence = [
            DefaultContentItemState.STATE_READY,
            DefaultContentItemState.STATE_VECTORIZED,
            DefaultContentItemState.STATE_CLUSTERED,
            DefaultContentItemState.STATE_COMPLETED,
        ]

    def get_name(self):
        """
        Return the slug id for the workflow
        """
        return "default_workflow"

    @classmethod
    def get_state_model(cls):
        return DefaultContentItemState()

    def extract_items(self, raw_item: Item, date_id=None) -> ContentItem:
        """
        create a ContentItem from a RawStore Item and return it
        """
        # check what kind of raw item it is (twitter, youtube)
        # extract the appropriate fields from the raw item

        # check that it is a known source that this workflow knows how
        # to extract
        assert raw_item.source_id in ["junkipedia", "faker_testing"]

        items = []

        # if we are unable to  extract the data log, and
        # create the item in a failed state
        try:
            # TODO: there are multple fields we could extract content from
            # create additional items for each? # title, description, transcript
            # title = raw_item.content["attributes"]["title"]
            # concatanation of title and description?
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
                published_at = datetime.strptime(
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
                date_id=date_id,  # TODO: do we need date_id maybe s3 bath is more useful?
                raw_content_id=raw_item.content_id,
                raw_created_at=raw_item.created_at,
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
        Apply the next step in the transformation for the content item.
        This defines the processing sequence each individual item will follow.
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

            case DefaultContentItemState.STATE_READY:
                assert (
                    len(items) == 1
                ), "state transition cannot dispatch batch with multiple items"
                # if it is in ready state, send it to be vectorized
                # by the multilingual means tokens model
                self.content_store.start_transition_to_state(
                    item, DefaultContentItemState.STATE_VECTORIZED
                )
                self.similarity_model.vectorize_content_item(
                    item, target_state=DefaultContentItemState.STATE_VECTORIZED
                )

            # TODO: will eventually need an intermediate VECTOR_STORED state to handle callback of vector model and store it?

            case DefaultContentItemState.STATE_VECTORIZED:
                assert (
                    len(items) == 1
                ), "state transition cannot dispatch batch with multiple items"
                # if vectorized, send it for clustering
                self.content_store.start_transition_to_state(
                    item, DefaultContentItemState.STATE_CLUSTERED
                )
                self.clustering_action.add_item_to_best_cluster(
                    item, DefaultContentItemState.STATE_CLUSTERED
                )

            case DefaultContentItemState.STATE_CLUSTERED:
                assert (
                    len(items) == 1
                ), "state transition cannot dispatch batch with multiple items"
                # our work here is done
                self.content_store.transition_item_state(
                    item, DefaultContentItemState.STATE_COMPLETED
                )
            case _:  # nothing matched
                logging.error(
                    f"Item {item.content_item_id} state {state.current_state} does not match states for workflow {self.get_name}"
                )
                return Workflow.ERROR
        return Workflow.SUCCESS
        # TODO: how do we know it has failed?  because we keep revisiting states, or some kind of timeout from initial creation
