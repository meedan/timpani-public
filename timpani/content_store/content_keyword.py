import datetime

from timpani.content_store.content_store_obj import ContentStoreObject

from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy import UnicodeText
from sqlalchemy import DateTime
from sqlalchemy.sql import func
from sqlalchemy import ForeignKey
from typing import Optional
from sqlalchemy import String
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema


class ContentKeyword(ContentStoreObject):
    """
    Represents the annotation of content items with free-form text keywords
    """

    version = "0.1"

    # --- SQLAlchemy ORM database mappings ---
    __tablename__ = "content_keyword"

    content_keyword_id: Mapped[int] = mapped_column(primary_key=True)
    workspace_id: Mapped[str] = mapped_column(String(30), index=True)
    keyword_model_name: Mapped[str] = mapped_column(index=True)
    content_item_id: Mapped[int] = mapped_column(
        ForeignKey(
            "content_item.content_item_id",
            name="fk_keyword_content_item_id",
            use_alter=True,
        )
    )
    keyword_text: Mapped[str] = mapped_column(UnicodeText(), index=True)
    keyword_score: Mapped[Optional[float]]

    # published date copied from content item
    content_published_date: Mapped[Optional[datetime.datetime]] = mapped_column(
        index=True
    )

    # usually this is the creation date of the keyword
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(), server_default=func.now(), onupdate=func.now()
    )

    def __init__(
        self,
        workspace_id: str,
        keyword_model_name: str,
        content_item_id,
        keyword_text: str,
        content_published_date,
        keyword_score: float = 1.0,
    ):
        self.workspace_id = workspace_id
        self.keyword_model_name = keyword_model_name
        self.content_item_id = content_item_id
        self.keyword_text = keyword_text
        self.content_published_date = content_published_date
        self.keyword_score = keyword_score


class ContentItemSchema(SQLAlchemyAutoSchema):
    class Meta:
        """
        Metadata mapping for marshmallow-sqlalchemy serialization
        """

        model = ContentKeyword
        load_instance = True
