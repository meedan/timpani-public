import datetime
from timpani.util.run_state import RunState
from timpani.content_store.content_store_obj import ContentStoreObject
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from typing import Optional


class ProcessState(RunState, ContentStoreObject):
    """
    Extends the run state model by adding the ability to log the state into
    the content_store
    """

    # --- SQLAlchemy ORM database mappings ---
    __tablename__ = "process_state"
    job_type: Mapped[str]
    current_state: Mapped[str]
    run_id: Mapped[str] = mapped_column(primary_key=True)
    attempt_id: Mapped[Optional[str]]
    attempt_num: Mapped[int]
    attempt_start: Mapped[Optional[datetime.datetime]]
    attempt_end: Mapped[Optional[datetime.datetime]]
    attempt_transition_timestamp: Mapped[Optional[datetime.datetime]]
    workspace_id: Mapped[Optional[str]]
    source_name: Mapped[Optional[str]]
    query_id: Mapped[Optional[str]]
    date_id: Mapped[Optional[str]]
    # time_range_start
    # time_range_end

    def __init__(self, job_type):
        RunState.__init__(self, job_type)
