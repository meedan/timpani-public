"""add indexes to tables

Revision ID: 67ad550ac7f5
Revises: 19332fce7452
Create Date: 2024-01-02 15:14:08.585314

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "67ad550ac7f5"
down_revision: Union[str, None] = "19332fce7452"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """
    Add indexes to core tables to make lookups more efficient
    (this could be slow if data already in tables)
    """
    op.create_index(
        index_name="ix_content_item_workspace_id",
        table_name="content_item",
        columns=["workspace_id"],
        if_not_exists=True,
    )
    op.create_index(
        index_name="ix_content_item_source_id",
        table_name="content_item",
        columns=["source_id"],
        if_not_exists=True,
    )
    op.create_index(
        index_name="ix_content_item_date_id",
        table_name="content_item",
        columns=["date_id"],
        if_not_exists=True,
    )

    op.create_index(
        index_name="ix_content_item_content_published_date",
        table_name="content_item",
        columns=["content_published_date"],
        if_not_exists=True,
    )

    op.create_index(
        index_name="ix_content_item_content_language_code",
        table_name="content_item",
        columns=["content_language_code"],
        if_not_exists=True,
    )

    op.create_index(
        index_name="ix_content_cluster_workspace_id",
        table_name="content_cluster",
        columns=["workspace_id"],
        if_not_exists=True,
    )

    op.create_index(
        index_name="ix_content_item_state_state_model_name",
        table_name="content_item_state",
        columns=["state_model_name"],
        if_not_exists=True,
    )

    op.create_index(
        index_name="ix_content_item_state_current_state",
        table_name="content_item_state",
        columns=["current_state"],
        if_not_exists=True,
    )


def downgrade() -> None:
    """
    Remove key indexes from tables (could be slow)
    """
    op.drop_index("idx_item_workspace_id", if_exists=True)
    op.drop_index("idx_source_id", if_exists=True)
    op.drop_index("idx_date_id", if_exists=True)
    op.drop_index("idx_published_timestamp", if_exists=True)
    op.drop_index("idx_language_code", if_exists=True)

    op.drop_index("idx_cluster_workspace_id", if_exists=True)

    op.drop_index("idx_state_model", if_exists=True)
    op.drop_index("idx_current_state", if_exists=True)
