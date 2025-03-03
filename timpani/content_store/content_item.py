import datetime

from typing import Optional
from sqlalchemy import String
from sqlalchemy import UnicodeText
from sqlalchemy import DateTime
from sqlalchemy.sql import func
from sqlalchemy import ForeignKey

from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column

from marshmallow_sqlalchemy import SQLAlchemyAutoSchema

# from marshmallow import fields

from timpani.content_store.content_store_obj import ContentStoreObject


class ContentItem(ContentStoreObject):
    """
    Container for normalized content item.
    Can be serialized and persisted in datbase.

    Must call commit() to persist changes to database

    NOTE: if not explicity defined, getters and setters defined by
    python dataclass https://docs.python.org/3/library/dataclasses.html

    TODO: When running on a DB cluster, any reads related to modification
    *must* occur on the writer instances to avoid race conditions between
    updates

    """

    # TODO: How do we handle permissions? team_id and user_id? This needs
    # to be integrated with database row level permissions

    # TODO: think about equality and hashing (hash serielized representation?)

    # so we can conditionally transform objects if we later change structure
    # needs to be updated if fields added/removed
    version = "0.1"

    # --- SQLAlchemy ORM database mappings ---
    __tablename__ = "content_item"

    #  ---- these are from the raw content item and should not be changed ---
    run_id: Mapped[str] = mapped_column(String(40))
    workspace_id: Mapped[str] = mapped_column(String(30), index=True)
    source_id: Mapped[str] = mapped_column(String(30), index=True)  # i.e. junkipedia
    query_id: Mapped[str] = mapped_column(String(30))  # maps back to search terms
    date_id: Mapped[int] = mapped_column(index=True)
    raw_created_at: Mapped[datetime.datetime]
    raw_content_id: Mapped[str] = mapped_column(String(40))
    raw_content: Mapped[str] = mapped_column(
        UnicodeText()
    )  # original content, should not be modified
    source_field: Mapped[
        Optional[str]
    ]  # name of field content extracted from (disambiguates content from same item)

    #  ----- these are normalized content items -----

    # updated timestamp should be managed by database
    # NOTE: since there is no setter for this, this
    # will be None if the object has not been persisted in DB
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(), server_default=func.now(), onupdate=func.now()
    )

    # primary key, this is an auto-incrment column from database
    content_item_id: Mapped[int] = mapped_column(primary_key=True)
    content_item_state_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(
            "content_item_state.state_id", ondelete="CASCADE"
        )  # remove state when main record removed
    )
    content_published_date: Mapped[Optional[datetime.datetime]] = mapped_column(
        index=True
    )
    content_published_url: Mapped[Optional[str]]
    content: Mapped[Optional[str]] = mapped_column(UnicodeText())

    content_cluster_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("content_cluster.content_cluster_id")
    )

    # probably an  ISO_639 code (we should have a taxonomy for this) and validate against it
    content_language_code: Mapped[Optional[str]] = mapped_column(String(5), index=True)
    content_locale_code: Mapped[Optional[str]] = mapped_column(String(5))
    # script_code   <-- I'm going to leave this of for now until we have use case

    # TODO: https://meedan.atlassian.net/browse/CV2-3289
    # TODO: processing_state   state model indicating what state in the content processing sequence it has reached
    # TODO: processing_model_id  id indicating what the apropriate state model is for content processing

    # TODO: metadata related to the original item? shares? likes? impressions? views?
    # PROPOSAL: ItemEngagement object that tracks states of these for specific date
    # (this would be expected to change over time)
    # content_item_id, enagement_type, engagement_value, datestamp

    # TODO: author information? original source information?

    # TODO: content-to-tag mappings? or those should go in a different field
    # PROPOSAL: ItemTag object holding mappings

    # TODO: taxonomies?
    # ItemTaxonomy that maps to an element of Taxonomy?

    # TODO: sequence of updates/modifications to the content? (which transformations have happend)
    # ItemHistory

    # TODO: id of vector in Index Store corresponding to this item?

    def __init__(
        self,
        # these are from the raw content item -----
        # and should not be changed
        run_id: str,
        workspace_id: str,
        source_id: str,  # i.e. junkipedia
        query_id: str,  # maps back to search terms
        date_id: int,
        raw_created_at: datetime,
        raw_content_id: str,
        raw_content: str,  # original content, should not be modified
        source_field: str = None,
        # these are normalized content items ------
        content=None,
        content_published_date=None,
        content_published_url=None,
        content_language_code=None,
    ):
        self.run_id = run_id
        self.workspace_id = workspace_id
        self.source_id = source_id
        self.query_id = query_id
        self.date_id = date_id
        self.raw_created_at = raw_created_at
        self.raw_content_id = raw_content_id
        self.raw_content = raw_content  # original content, should not be modified
        if content is not None:
            self.content = content
        else:
            self.content = raw_content
        self.source_field = source_field
        self.content_published_date = content_published_date
        self.content_published_url = content_published_url
        self.content_language_code = (
            content_language_code  # TODO: validate to code schema?
        )

    @staticmethod
    def schema():
        return ContentItemSchema()


class ContentItemSchema(SQLAlchemyAutoSchema):
    class Meta:
        """
        Metadata mapping for marshmallow-sqlalchemy serialization
        """

        model = ContentItem
        load_instance = True
        # include_fk = True  # this should be the solution, but then gives error about nulls

        # this doesn't seem to work
        # content_cluster_id = fields.Integer(allow_none=True, partial=True)

    # content_cluster_id = fields.Nested(
    #    "ContentClusterSchema",
    #    only=("content_cluster_id",),
    #    allow_none=True,
    # )

    # content_cluster_id = fields.Nested(
    #    "ContentClusterSchema",
    # )

    # content_cluster_id = fields.Pluck(
    #    "self", field_name="content_cluster_id", allow_none=True
    # )
