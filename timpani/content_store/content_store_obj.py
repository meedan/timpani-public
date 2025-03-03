from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import MappedAsDataclass


class ContentStoreObject(DeclarativeBase, MappedAsDataclass):
    """
    Parent class for things that can be stored in content store and serialzed.

    DeclartiveBase brings in SQLAlchemy database mapping via Annotated Declarative
    Table:s https://docs.sqlalchemy.org/en/20/orm/declarative_tables.html#orm-declarative-mapped-column

    MappedAsDataClass invokes pythons dataclass behavior to create
     default getters and setters https://docs.python.org/3/library/dataclasses.html

    The .schema() function should return a SQLAlchemyAutoSchema subclass for use in
     validation

    This class is also used in database initialization to get the schemas and
    table mappings of all of objects to be stored so that it can construct the
    tables
    """

    @staticmethod
    def schema():
        """
        Returns a SQLAlchemyAutoSchema subclass instance for use in
        validation
        """
        raise NotImplementedError
