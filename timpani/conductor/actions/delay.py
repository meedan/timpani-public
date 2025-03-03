from typing import List
import time
from timpani.content_store.content_item import ContentItem
from timpani.content_store.item_state_model import ContentItemState
from timpani.content_store.content_store_interface import ContentStoreInterface


class DelayingAction(object):
    """
    Does no action on passed content items, just waits for the specified delay before updating state.
    Used for testing, updates as a batch
    """

    def delay_content_items(
        self,
        content_items: List[ContentItem],
        target_state: ContentItemState,
        store: ContentStoreInterface,
        delay_seconds=1,
    ):
        """
        waits for delay_seconds and then updates the transition
        """
        time.sleep(delay_seconds)
        for item in content_items:
            store.transition_item_state(item, target_state)
