"""create process_state table

Revision ID: d64430899783
Revises: 67ad550ac7f5
Create Date: 2024-01-11 16:01:45.827587

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "d64430899783"
down_revision: Union[str, None] = "67ad550ac7f5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """
    Create the table for recording the state of the process_state model
    """
    op.create_table(
        "process_state",
        sa.Column("job_type", sa.String(), nullable=False),
        sa.Column("current_state", sa.String(), nullable=False),
        sa.Column("run_id", sa.String(), nullable=False),
        sa.Column("attempt_id", sa.String(), nullable=True),
        sa.Column("attempt_num", sa.Integer(), nullable=False),
        sa.Column("attempt_start", sa.DateTime(), nullable=True),
        sa.Column("attempt_end", sa.DateTime(), nullable=True),
        sa.Column("attempt_transition_timestamp", sa.DateTime(), nullable=True),
        sa.Column("workspace_id", sa.String(), nullable=True),
        sa.Column("source_name", sa.String(), nullable=True),
        sa.Column("query_id", sa.String(), nullable=True),
        sa.Column("date_id", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("run_id"),
    )


def downgrade() -> None:
    op.drop_table("process_state")
