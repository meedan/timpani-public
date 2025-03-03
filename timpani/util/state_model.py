import datetime
from datetime import timezone


class StateModel(object):
    """
    Abstract class for providing common utiliy functions used by state models
    """

    STATE_UNDEFINED = "undefined"  # for abstract class, should use this
    valid_states = [STATE_UNDEFINED]
    valid_transitions = {
        STATE_UNDEFINED: [STATE_UNDEFINED],
    }

    current_state = STATE_UNDEFINED
    transition_timestamp = datetime.datetime.now(timezone.utc)

    def isTransitionAllowed(self, source_state: str, target_state: str):
        """
        Return boolean indicating if transition from source_state to
        target_state is an allowable transition
        """
        if source_state in self.valid_transitions:
            allowable_targests = self.valid_transitions[source_state]
            if target_state in allowable_targests:
                return True
        return False

    def transitionTo(self, target_state: str):
        assert (
            self.current_state in self.valid_states
        ), f"Current state {self.current_state} is not valid"
        assert (
            target_state in self.valid_states
        ), f"Target state '{target_state}' is not a valid state for {type(self)}. valid: {self.valid_states}"
        assert self.isTransitionAllowed(
            self.current_state, target_state
        ), f"State transition from {self.current_state} to {target_state} not allowed"

        self.transition_timestamp = datetime.datetime.now(timezone.utc)
        self.current_state = target_state
        return True

        # NOTE: sub class implements logic of what should happen during state transition

        # TODO: sentry integration to track metrics?
