import datetime
from datetime import timezone
import uuid
import json
from timpani.util.state_model import StateModel

import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()


class RunState(StateModel):
    """
    Class for tracking and recording the parameters and state transistions associated with a specific
    batch run of a timpani script. This would eventually be backed in a database

    NOTE: default date_id IS FOR THE PREVIOUS DAY. I.e. if no date_id or time range
    args are passed in, the assumption is to fetch content for the previous (UTC) day.
    """

    DATE_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

    # TODO: move all this stuff into a StateMachine class
    STATE_READY = "ready"
    STATE_RUNNING = "running"
    STATE_FAILED = "failed"
    STATE_COMPLETED = "completed"
    valid_states = [STATE_READY, STATE_RUNNING, STATE_FAILED, STATE_COMPLETED]
    valid_transitions = {
        STATE_READY: [STATE_RUNNING],
        STATE_RUNNING: [STATE_FAILED, STATE_COMPLETED],
        STATE_FAILED: [STATE_RUNNING],  # for restarts
    }

    def __init__(self, job_type, date_id: datetime.datetime = None):
        self.job_type = job_type
        self.current_state = self.STATE_READY
        self.run_id = "run_" + uuid.uuid4().hex
        self.attempt_id = None
        self.attempt_num = 0
        self.attempt_start = None
        self.attempt_end = None
        self.transition_timestamp = datetime.datetime.utcnow()

        # these describe the job params
        self.workspace_id = None
        self.source_name = None
        self.query_id = None
        if date_id is None:
            self.date_id = (
                datetime.datetime.now(timezone.utc) - datetime.timedelta(days=1)
            ).strftime("%Y%m%d")
        else:
            self.date_id = date_id
        self.time_range_start = datetime.datetime.strptime(self.date_id, "%Y%m%d")
        self.time_range_end = self.time_range_start + datetime.timedelta(days=1)

    def transitionTo(self, target_state: str):
        # call validation code in super class to do the transition
        super().transitionTo(target_state=target_state)

        self.transition_timestamp = datetime.datetime.utcnow()  # now(timezone.utc)
        self.current_state = target_state

        # TODO: some kind of hooks called on state transitions?
        if target_state == self.STATE_RUNNING:
            self.attempt_num += 1
            self.attempt_id = self.run_id + f":{self.attempt_num}"
            self.attempt_start = self.transition_timestamp

        if target_state in [self.STATE_FAILED, self.STATE_COMPLETED]:
            self.attempt_end = self.transition_timestamp

        logging.info(
            f"Timpani {self.job_type} run attempt {self.attempt_id} transitioned to {self.current_state}"
        )
        # TODO: sentry integration to track metrics?

    def start_run(
        self,
        workspace_id: str,
        source_name: None,
        query_id=None,
        time_range_start=None,
        time_range_end=None,
        date_id=None,
    ):
        """
        Record that a run attempt is starting and track the starting params
        """
        # TODO: day id needs to be tracked here for retries
        self.workspace_id = workspace_id
        self.source_name = source_name
        self.query_id = query_id
        if time_range_start is not None:
            self.time_range_start = time_range_start
        if time_range_end is not None:
            self.time_range_end = time_range_end
        if date_id is not None:
            self.date_id = date_id

        self.transitionTo(self.STATE_RUNNING)

    # TODO: add record_progress() functionality to be able to resume partially completed runs?

    def to_json(self):
        return json.dumps(
            {
                "job_type": self.job_type,
                "current_state": self.current_state,
                "run_id": self.run_id,
                "attempt_id": self.attempt_id,
                "attempt_num": self.attempt_num,
                "attempt_start": self.attempt_start.strftime(self.DATE_FORMAT)
                if self.attempt_start
                else self.attempt_start,
                "attempt_end": self.attempt_end.strftime(self.DATE_FORMAT)
                if self.attempt_end
                else self.attempt_end,
                "transition_timestamp": self.transition_timestamp.strftime(
                    self.DATE_FORMAT
                ),
                "workspace_id": self.workspace_id,
                "source_id": self.source_name,
                "query_id": self.query_id,
                "date_id": self.date_id,
                "time_range_start": self.time_range_start.strftime(self.DATE_FORMAT)
                if self.time_range_start
                else self.time_range_start,
                "time_range_end": self.time_range_end.strftime(self.DATE_FORMAT)
                if self.time_range_end
                else self.time_range_end,
            }
        )
