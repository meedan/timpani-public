"""widen language code field to five characters

Revision ID: 09e21c8e7441
Revises: cbe455bc57ec
Create Date: 2024-03-04 13:24:49.622889

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "09e21c8e7441"
down_revision: Union[str, None] = "cbe455bc57ec"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        "content_item",
        "content_language_code",
        existing_type=sa.VARCHAR(length=3),
        type_=sa.String(length=5),
        existing_nullable=True,
    )
    op.alter_column(
        "content_item",
        "content_locale_code",
        existing_type=sa.VARCHAR(length=3),
        type_=sa.String(length=5),
        existing_nullable=True,
    )


def downgrade() -> None:
    op.alter_column(
        "content_item",
        "content_locale_code",
        existing_type=sa.String(length=5),
        type_=sa.VARCHAR(length=3),
        existing_nullable=True,
    )
    op.alter_column(
        "content_item",
        "content_language_code",
        existing_type=sa.String(length=5),
        type_=sa.VARCHAR(length=3),
        existing_nullable=True,
    )
