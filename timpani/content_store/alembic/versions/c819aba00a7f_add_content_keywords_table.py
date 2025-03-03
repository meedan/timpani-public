"""add content_keywords table

Revision ID: c819aba00a7f
Revises: 09e21c8e7441
Create Date: 2024-03-19 11:03:43.204710

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c819aba00a7f"
down_revision: Union[str, None] = "09e21c8e7441"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "content_keyword",
        sa.Column("content_keyword_id", sa.Integer(), nullable=False),
        sa.Column("workspace_id", sa.String(length=30), nullable=False),
        sa.Column("keyword_model_name", sa.String(), nullable=False),
        sa.Column("content_item_id", sa.Integer(), nullable=False),
        sa.Column("keyword_text", sa.UnicodeText(), nullable=False),
        sa.Column("keyword_score", sa.Float(), nullable=True),
        sa.Column("content_published_date", sa.DateTime(), nullable=True),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False
        ),
        sa.ForeignKeyConstraint(
            ["content_item_id"],
            ["content_item.content_item_id"],
            name="fk_keyword_content_item_id",
            use_alter=True,
        ),
        sa.PrimaryKeyConstraint("content_keyword_id"),
    )
    op.create_index(
        op.f("ix_content_keyword_content_published_date"),
        "content_keyword",
        ["content_published_date"],
        unique=False,
    )
    op.create_index(
        op.f("ix_content_keyword_keyword_model_name"),
        "content_keyword",
        ["keyword_model_name"],
        unique=False,
    )
    op.create_index(
        op.f("ix_content_keyword_keyword_text"),
        "content_keyword",
        ["keyword_text"],
        unique=False,
    )
    op.create_index(
        op.f("ix_content_keyword_workspace_id"),
        "content_keyword",
        ["workspace_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_content_keyword_workspace_id"), table_name="content_keyword")
    op.drop_index(op.f("ix_content_keyword_keyword_text"), table_name="content_keyword")
    op.drop_index(
        op.f("ix_content_keyword_keyword_model_name"), table_name="content_keyword"
    )
    op.drop_index(
        op.f("ix_content_keyword_content_published_date"), table_name="content_keyword"
    )
    op.drop_table("content_keyword")
