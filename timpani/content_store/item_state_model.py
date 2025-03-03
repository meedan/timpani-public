import datetime

from timpani.util.state_model import StateModel
from timpani.content_store.content_store_obj import ContentStoreObject
from typing import Optional
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy import String
from sqlalchemy import orm

import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()


class ContentItemState(StateModel, ContentStoreObject):
    """
    Defines a state model for content items that the each piece of content for
    the organization must pass through. In addition to routing, this permits
    async callbacks and retrying/resubmitting when an operation fails.

    *`current_state should indicate the most recently *completed* states, and
    will be used in queries to determine which objects are elligible for the
    next state:

    * `transition_num` should be incremented with every completed
    state transition (if this gets very high, something bad must be happening
    and we should go to an error state)

    * 'transition_start' timestamp will be
    updated when the transition to a new state begins (before current_state is
    updated)

    * `transistion_end` timestamp will be updated when transition
    completes

    * `completed_timestamp` will be set when the process is
    completed (in error or completed state, no more transitions needed)

    If transition_start > transition_end, assume a new transition is in progress

    Note: a workflow may add transitions and states by extending and adapting this
    class

    Theory of operation:

    * Entering a new item in the system
    - New objects arrive, either triggered from incoming batch or individual
      query
    - ContentItem object created with appropriate payload
    - New state object created with states appropriate for workflow, and state
      'unknown'
    - ContentItem and ContentItemState submitted to DB via
      ContentStore.initialize_item()
    which also maps the state id to the content item - state transition to
    'ready' via ContentStore.transitionItemState

    * Determining which items need transitions
    - A worker process repeatedly queries the content store at some appropriate
      `batch_interval`,
    - SELECT ITEM WHERE STATE NOT IN (faild, completed, unknown) AND
      transition_start <= transition_end
    - Consult the processing sequence 'workflow' to determine the next transition
      needed
    - ContentItem detached from ORM and either sent off to be processed, or id
      and relevent payload extracted for callback


    * Starting a transition and submitting a job
    - Call ContentStore.start_transition(item) to update the transition_start
      timestamp
    - Submit payload and id to service for processing

    * Preventing an object from being picked up twice when if the first attempt
      has not yet completed
    (timeout window)

    * Updating job state when content item updated via async callback
    - callback corresponding to specific state transition received from webapp
      with ContentItem object updated
    alternatively, callback has content item id and new field state, item
    queried from db and updated - ContentStore.update_item() called to write
    object changes to db, will be rejected if corresponding state is not
    appropriate - ContentStore.transition_item_state() called to record that new
    state has been achievd and update timestamps

    * Retrying a failed job
    - The job selection query can ignore the transition_start <= transition_end
      clause if the current
    time > transition_start+timeout_window  (1hr?) and resubmit the job, with
    the assumption that the previous job was lost

    * Preventing an object from being updated multiple times for requests from
      the same state transition
    - It is possible that a job is delayed and still sucessful after the timeout
      window.  An update will be ignored
    if the state it would transition to already exists.  Note: in the case of
    out-of-order updates, the first recieved will be recorded and later ones
    will be ignored.

    * Determining when items should no longer be retried
    - Periodically query the database for active items that have number of
    transitions above a threshold or are too old, and put them in the failed state

    * Deleting a content item from the system
    - The content item and associated state should be deleted via
      ContentStore.delete_item()

    * Edge cases not described
    - State *could* be directly updated via a database qeury (for example to
      'rewind' to redo a previous state,
    a not permitted transition) but this should be avoided


    Future notes:
    * we could support a 'deleted' state for soft deletes, ... but
    then system may fill up * if required, we could log a history of state
    changes, but this would create a *lot* of data

    """

    # content item is loaded and ready for first processing tep
    STATE_READY = "ready"

    # something is wrong with this item, don't try it anymore
    STATE_FAILED = "failed"

    # all done, no more transformations needed
    STATE_COMPLETED = "completed"

    # TODO: do we need a STATE_DELETE_PENDING when it is aged out?

    content_item_states = [
        STATE_READY,
        STATE_FAILED,
        STATE_COMPLETED,
    ]

    # default transitions for an item with no transformations
    content_item_transitions = {
        StateModel.STATE_UNDEFINED: [STATE_READY, STATE_FAILED],
        STATE_READY: [STATE_FAILED, STATE_COMPLETED],
        STATE_FAILED: [STATE_READY],  # for restarts
    }

    version = "0.1"

    # --- SQLAlchemy ORM database mappings ---
    __tablename__ = "content_item_state"

    state_id: Mapped[int] = mapped_column(primary_key=True)
    state_model_name: Mapped[str] = mapped_column(index=True)
    current_state: Mapped[str] = mapped_column(String(30), index=True)
    transition_num: Mapped[int]
    transition_start: Mapped[Optional[datetime.datetime]]
    transition_end: Mapped[Optional[datetime.datetime]]
    completed_timestamp: Mapped[Optional[datetime.datetime]]

    # ensure we can instantiate the correct subclass when recreating from orm
    __mapper_args__ = {
        "polymorphic_on": "state_model_name",
        "polymorphic_identity": "ContentItemState",
    }

    def __init__(self):
        super().__init__()
        self.state_model_name = type(self).__name__  # name of class
        self.current_state = StateModel.STATE_UNDEFINED
        self.transition_num = 0
        self.transition_start = datetime.datetime.utcnow()
        self.transition_end = datetime.datetime.utcnow()
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
        self.valid_states, self.valid_transitions = self.update_state_model(
            additional_states=self.content_item_states,
            additional_transitions=self.content_item_transitions,
        )

    def get_processing_state_sequence(self):
        """Return the list of states that should be iterated over for processing.
        Usually all states other than Undefined and Completed"""
        # can also be overridden by subclass
        # need to explicitly remove, because other sub-classes
        # may add their own states
        state_sequence = self.valid_states.copy()
        state_sequence.remove(self.STATE_UNDEFINED)
        state_sequence.remove(self.STATE_COMPLETED)
        state_sequence.remove(self.STATE_FAILED)
        return state_sequence

    def update_state_model(self, additional_states=None, additional_transitions=None):
        # get the states from the default parent model
        valid_states = self.valid_states
        # if there are more states to add
        if additional_states is not None:
            # extend the possible states
            valid_states = list(set(valid_states).union(set(additional_states)))
        valid_transitions = self.valid_transitions.copy()
        if additional_transitions is not None:
            # update the possible transitions overriding previous values
            for key_state, trans_states in additional_transitions.items():
                valid_transitions[key_state] = trans_states
        return valid_states, valid_transitions

    def startTransition(self):
        """
        Just record the timestamp when the transition is starting
        so we can find old/failed jobs
        """
        self.transition_start = datetime.datetime.utcnow()

    def transitionTo(self, target_state: str):
        # call validation code in super class to do the transition
        # NOTE: assuming this is called when operation is complete

        super().transitionTo(target_state=target_state)
        # self.transition_num += 1
        # NOTE: transition_num couter should be incremented previously,
        # in start_transition_to_state()
        self.transition_end = datetime.datetime.utcnow()
        self.current_state = target_state

        if target_state in [self.STATE_FAILED, self.STATE_COMPLETED]:
            self.completed_timestamp = self.transition_end

        logging.debug(
            f"Timpani content item state {self.state_id} transitioned to {self.current_state}"
        )
