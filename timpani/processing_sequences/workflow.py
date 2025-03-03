import datetime
from datetime import timedelta
from timpani.content_store.content_store_interface import ContentStoreInterface
from timpani.content_store.content_item import ContentItem
from timpani.content_store.item_state_model import ContentItemState
from timpani.raw_store.item import Item


class Workflow(object):
    """ß
    Define the sequence of operations to transform a content item.
    """

    # NOTE: These values can be overridden by subclasess where appropriate
    STATE_TIMEOUT_DURATION = 10  # seconds
    MAX_STATE_UPDATES = 100
    content_store = None

    # constant for representing state status in transition
    SKIPPED = 2
    SUCCESS = 0
    ERROR = 1

    def __init__(self, content_store: ContentStoreInterface) -> None:
        # store a reference to the content store that will be needed for state updates
        self.content_store = content_store

    def get_name(self):
        """
        Return the slug id for the workflow
        """
        raise NotImplementedError

    @classmethod
    def get_state_model(cls) -> ContentItemState:
        """
        Return the ContentItemState used to enforce model tranisions
        """
        raise NotImplementedError

    def extract_items(self, raw_item: Item, date_id=None) -> ContentItem:
        """
        create (possibly multiple)  ContentItems from a RawStore Item
        set up the appropriate state model
        and return in a list
        """
        raise NotImplementedError

    def is_batch_transition_from(self, transition_state_name: str):
        """
        Hack to deal with the fact that processor needs to know if it
        should call all the items corresponding to a transition individually
        (in parallel threads), or in a single batch
        """
        return False

    def next_state(self, items: list[ContentItem], state_name: str) -> int:
        """
        Apply the next step in the transformation for the content item.
        Usually this would be the next step in the item_state_sequence,
        but it is possible that some conditional logic will be applied.
        When multiple items are included, they must all have the same state
        Returns an integer status code
        """
        raise NotImplementedError

    def check_state_timeout(self, state: ContentItemState):
        """
        Returns True if state has recently started a transition within
        the timeout window, indicating that a transition is most likely
        still in process and the next state transition should not start.
        Intended to allow multiple processes to manage state transitions
        """

        # check if transition marked in progress
        if state.transition_start > state.transition_end:
            # check if within window
            duration = datetime.datetime.utcnow() - state.transition_start
            if duration.total_seconds() < self.STATE_TIMEOUT_DURATION:
                return True
        return False

    def check_state_updates_exceeded(self, state: ContentItemState):
        """
        Returns True if state has been updated more than
        MAX_STATE_UPDATE times, indicating that the corresponding
        content item is probably stuck in some kind of loop and
        should be failed. Otherwise returns False
        """

        if state.transition_num > self.MAX_STATE_UPDATES:
            return True
        return False

    def get_item_live_duration(self) -> timedelta:
        """
        Return the duration that items in the workflow will be considered
        active before they are removed. None indicates that records should
        not be removed
        """
        return None

    @staticmethod
    def get_transition_graphviz(state_model: ContentItemState) -> str:
        """
        Return a textual representation of the state transitions
        in the graphviz .dot syntax for rendering a visualization
        """
        gv_txt = "digraph {\n"
        transitions = state_model.valid_transitions
        for source in transitions:
            targets = transitions[source]
            for target in targets:
                gv_txt += f"{source} -> {target};\n"

        gv_txt += "}"
        return gv_txt
