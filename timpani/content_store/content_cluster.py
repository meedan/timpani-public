import datetime

from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column

from typing import Optional
from sqlalchemy import DateTime
from sqlalchemy.sql import func
from sqlalchemy import ForeignKey
from sqlalchemy import String

from marshmallow_sqlalchemy import SQLAlchemyAutoSchema

from timpani.content_store.content_store_obj import ContentStoreObject

# from content_store.content_item import ContentItem


class ContentCluster(ContentStoreObject):
    """
    Container for managing groups of content items as cluster
    Can be serialized and persisted in datbase.
    Clusters shouldn't be empty.
    Items should have exactly one cluster at a time.
    """

    version = "0.1"

    # SQLAlchemy ORM database mappings
    __tablename__ = "content_cluster"

    # primary key, hopefully auto-incrment from database
    # TODO: do we want this, or something like a UUID?
    content_cluster_id: Mapped[int] = mapped_column(primary_key=True)

    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(), server_default=func.now()
    )  # TODO: should this be utcnow?

    # updated timestamp should be managed by database
    # NOTE: since there is no setter for this, t
    # will be None if the object has not been persisted in DB
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(), server_default=func.now(), onupdate=func.now()
    )

    exemplar_item_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(
            "content_item.content_item_id",
            name="fk_exemplar_content_item_id",
            use_alter=True,
        )
    )

    # should only be modified when items are updated
    # incrementing/dcrementing will also update updated_at
    num_items: Mapped[int]

    # running total number of items that have ever been added to the cluster
    num_items_added: Mapped[Optional[int]]

    # number of (textually) unique items currently in the cluster
    num_items_unique: Mapped[Optional[int]]

    workspace_id: Mapped[Optional[str]] = mapped_column(String(30), index=True)

    # intended to express how tightly the items in the cluster relate
    stress_score: Mapped[float]

    # intended to express relative urgency for recomputing properties
    # such as stress score and exemplar
    priority_score: Mapped[float]

    def __init__(self):
        self.num_items = 0
        self.num_items_added = 0
        self.num_items_unique = 0
        self.stress_score = 1.0  # empty clusters are very high stress
        self.priority_score = 1.0  # empty clusters should be re-evaluated
        self.workspace_id = None
        self.exemplar_item_id = None

    @staticmethod
    def schema():
        return ContentClusterSchema()


class ContentClusterSchema(SQLAlchemyAutoSchema):
    class Meta:
        """
        Metadata mapping for marshmallow-sqlalchemy serialization
        This will enforce SQLAlchemy definitions.
        """

        model = ContentCluster
        load_instance = True
