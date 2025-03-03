"""create initial table definitions

Revision ID: 19332fce7452
Revises: 16acee8ee5b2
Create Date: 2023-12-21 15:53:46.625419

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "19332fce7452"
down_revision: Union[str, None] = "16acee8ee5b2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "content_item",
        sa.Column("run_id", sa.String(length=40), nullable=False),
        sa.Column("workspace_id", sa.String(length=30), nullable=False),
        sa.Column("source_id", sa.String(length=30), nullable=False),
        sa.Column("query_id", sa.String(length=30), nullable=False),
        sa.Column("date_id", sa.Integer(), nullable=False),
        sa.Column("raw_created_at", sa.DateTime(), nullable=False),
        sa.Column("raw_content_id", sa.String(length=40), nullable=False),
        sa.Column("raw_content", sa.UnicodeText(), nullable=False),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column("content_item_id", sa.Integer(), nullable=False, primary_key=True),
        sa.Column("content_item_state_id", sa.Integer(), nullable=True),
        sa.Column("content_published_date", sa.DateTime(), nullable=True),
        sa.Column("content_published_url", sa.String(), nullable=True),
        sa.Column("content", sa.UnicodeText(), nullable=True),
        sa.Column("content_cluster_id", sa.Integer(), nullable=True),
        sa.Column("content_language_code", sa.String(length=3), nullable=True),
        sa.Column("content_locale_code", sa.String(length=3), nullable=True),
        sa.PrimaryKeyConstraint("content_item_id"),
    )
    op.create_table(
        "content_cluster",
        sa.Column("content_cluster_id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column("exemplar_item_id", sa.Integer(), nullable=True),
        sa.Column("num_items", sa.Integer(), nullable=False),
        sa.Column("workspace_id", sa.String(length=30), nullable=True),
        sa.Column("stress_score", sa.Float(), nullable=False),
        sa.Column("priority_score", sa.Float(), nullable=False),
        sa.PrimaryKeyConstraint("content_cluster_id"),
    )
    op.create_table(
        "content_item_state",
        sa.Column("state_id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("state_model_name", sa.String(), nullable=False),
        sa.Column("current_state", sa.String(length=30), nullable=False),
        sa.Column("transition_num", sa.Integer(), nullable=False),
        sa.Column("transition_start", sa.DateTime(), nullable=True),
        sa.Column("transition_end", sa.DateTime(), nullable=True),
        sa.Column("completed_timestamp", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("state_id"),
    )

    # define foreign key relationships between tables
    op.create_foreign_key(
        constraint_name="fk_content_item_cluster",
        source_table="content_item",
        referent_table="content_cluster",
        local_cols=["content_cluster_id"],
        remote_cols=["content_cluster_id"],
        use_alter=True,
    )
    op.create_foreign_key(
        constraint_name="fk_content_item_state",
        source_table="content_item",
        referent_table="content_item_state",
        local_cols=["content_item_state_id"],
        remote_cols=["state_id"],
        ondelete="CASCADE",
        use_alter=True,
    )
    op.create_foreign_key(
        constraint_name="fk_exemplar_content_item_id",
        source_table="content_cluster",
        referent_table="content_item",
        local_cols=["exemplar_item_id"],
        remote_cols=["content_item_id"],
        use_alter=True,
    )


def downgrade() -> None:
    op.drop_constraint(
        table_name="content_item", constraint_name="fk_content_item_cluster"
    )
    op.drop_constraint(
        table_name="content_item", constraint_name="fk_content_item_state"
    )
    op.drop_constraint(
        table_name="content_cluster", constraint_name="fk_exemplar_content_item_id"
    )

    op.drop_table("content_item_state")
    op.drop_table("content_cluster")
    op.drop_table("content_item")
