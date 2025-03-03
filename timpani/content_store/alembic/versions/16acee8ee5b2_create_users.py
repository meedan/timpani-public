"""create users

Revision ID: 16acee8ee5b2
Revises:
Create Date: 2023-12-19 15:56:04.265394

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from timpani.app_cfg import TimpaniAppCfg


# revision identifiers, used by Alembic.
revision: str = "16acee8ee5b2"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

cfg = TimpaniAppCfg()


def upgrade() -> None:
    """
    Create the application user that has read/write but not create destroy and later privliages.
    This user will do the normal operations in the db
    """
    conn = op.get_bind()
    conn.execute(
        sa.text(
            f"""
            CREATE ROLE {cfg.content_store_user} with password '{cfg.content_store_pwd}' login;
            GRANT {cfg.content_store_user} TO {cfg.content_store_admin_user};
            GRANT CONNECT ON DATABASE {cfg.content_store_db} to {cfg.content_store_user};
            ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO {cfg.content_store_user};
            ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON SEQUENCES TO {cfg.content_store_user};
            """
        )
    )


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(
        sa.text(
            f"""
            DROP OWNED BY {cfg.content_store_user};
            DROP ROLE {cfg.content_store_user};
            """
        )
    )
