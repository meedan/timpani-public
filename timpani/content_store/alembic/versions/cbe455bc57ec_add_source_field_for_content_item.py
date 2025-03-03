"""add source_field for content_item

Revision ID: cbe455bc57ec
Revises: 0571bc3b8d0c
Create Date: 2024-03-04 10:41:16.106889

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "cbe455bc57ec"
down_revision: Union[str, None] = "0571bc3b8d0c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("content_item", sa.Column("source_field", sa.String(), nullable=True))
    # ### end Alembic commands ###


def downgrade() -> None:
    op.drop_column("content_item", "source_field")

    # ### end Alembic commands ###
