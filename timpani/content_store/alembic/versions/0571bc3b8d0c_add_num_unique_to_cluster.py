"""add num_unique to cluster

Revision ID: 0571bc3b8d0c
Revises: d64430899783
Create Date: 2024-01-12 14:36:25.198233

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "0571bc3b8d0c"
down_revision: Union[str, None] = "d64430899783"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """
    Add columns to the content cluster for tracking the cumulative number of items and current unique
    """
    # initially added with yout the non null constract
    op.add_column(
        "content_cluster",
        sa.Column("num_items_added", sa.Integer(), nullable=True, default=0),
    )
    op.add_column(
        "content_cluster",
        sa.Column("num_items_unique", sa.Integer(), nullable=True, default=0),
    )

    # set all existing values in the table to 0 to deal with any pre-existing rows
    op.execute(
        "update content_cluster set num_items_added=0 where num_items_added is NULL"
    )
    op.execute(
        "update content_cluster set num_items_unique=0 where num_items_unique is NULL"
    )

    # add the not null contraint
    op.alter_column(
        "content_cluster",
        sa.Column("num_items_added", nullable=False),
    )
    op.alter_column(
        "content_cluster",
        sa.Column("num_items_unique", nullable=False),
    )


def downgrade() -> None:
    op.drop_column("content_cluster", "num_items_unique")
    op.drop_column("content_cluster", "num_items_added")
