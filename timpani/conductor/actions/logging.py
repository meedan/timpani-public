import logging

# import timpani.content_store.content_store
from timpani.content_store.content_item import ContentItem
from timpani.content_store.item_state_model import ContentItemState
from timpani.content_store.content_store_interface import ContentStoreInterface


class LoggingAction(object):
    """
    Does no action on passed content item, logs it out via the python logger,
    and fires the appropriate state update.
    This action mostly for testing callbacks, state updates, etc
    """

    def log_content_item(
        self,
        item: ContentItem,
        target_state: ContentItemState,
        store: ContentStoreInterface,
    ):
        """
        Writes out the content item into the log, but does not modify it
        and updates state to target state
        """
        logging.info(f"LoggingAction called on item: {item}")
        store.transition_item_state(item, target_state)
