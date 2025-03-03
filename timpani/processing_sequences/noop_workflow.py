from datetime import timedelta
from timpani.processing_sequences.workflow import Workflow
from timpani.content_store.content_item import ContentItem
from timpani.content_store.item_state_model import ContentItemState
from timpani.content_store.content_store_interface import ContentStoreInterface
from timpani.raw_store.item import Item


import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()


class NoopWorkflow(Workflow):
    """
    Does "no-op" (no operation) on any content, used for testing workflow orchestration
    """

    def __init__(self, content_store: ContentStoreInterface) -> None:
        # pass the reference to the content store to the super class
        super().__init__(content_store=content_store)

    def get_name(self):
        """
        Return the slug id for the workflow
        """
        return "nooop_workflow"

    @classmethod
    def get_state_model(cls):
        return ContentItemState()

    def extract_items(self, raw_item: Item, date_id=None) -> ContentItem:
        """
        Since this is no-op, just return an empty array and don't create anything
        """

        items = []
        return items

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

        # this should never find any content, but if it did it wouldn't stop
        # so anything in the ready state, make it completed
        match state_name:
            case ContentItemState.STATE_READY:
                assert (
                    len(items) == 1
                ), "state transition cannot dispatch batch with multiple items"
                # (not doing real operations in this test)
                self.content_store.start_transition_to_state(
                    item, ContentItemState.STATE_COMPLETED
                )
                # our work here is done
                self.content_store.transition_item_state(
                    item, ContentItemState.STATE_COMPLETED
                )
            case _:  # nothing matched
                logging.error(
                    f"Item {item.content_item_id} state {state.current_state} does not match states for workflow {self.get_name}"
                )
        return Workflow.SUCCESS

    def get_item_live_duration(self) -> timedelta:
        """
        Return the duration that items in the workflow will be considered
        active before they are removed. None indicates that records should
        not be removed
        """
        return timedelta(days=1)
