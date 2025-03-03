
# Content Store
Primary source of truth for content items, their processing state, cluster membership, and any other state the system needs to track. Implemented as a relational database with an ORM layer. The relational database serves as an integration point for reports and trends, which can extract data using SQL tools or BI dashboard integrations.

**Content Items** are normalized representation of (for now, textual) content that is being processed and stored in the sytem.  They are derrived from original text archived in the raw store (possibly many content items from a single raw store item). They maintain the original content (and a mapping back to original record), as well as transformed version of text for vectorization etc.

Content items are assumed to be backed as database objects via SQLAlchemy ORM. They are also implemented as python dataobjects wich creates some default accessors and repr() functions behind the scene.

They objects must be serializeable so that they can be passed around in http requests or queues and transformations applied to them  -- dictionaries at this point, not JSON because they can easily be converted to JSON. 'Marshmallow' is the library that is doing the serialization and schema enforcement. This also validates the schema on reloading dictionary to object. 

Content items are 'passive' objects data objects, but also enforce schemas and data types

**Content Clsuters** are groups of items that algorithms elsewhere in the system have decided belong together (usually via vector similarity model)

**Content Keywords** are free-form text tags (usually extracted from the content item) that are attached many-to-one to content items. 

The **ContentStore** is like a wrapper for the database.  Ideally, all of the business logic about what happens to the objects should live here.  Deletes and updates should take place here (even tho they are possible elsewhere) because some of them need to update each other. 

# Implementation

Goal is that related changes (i.e. moving an item from one cluster to another and incrementing/decrementing counts) should be in a single database transaction (so failures will rollback appropriately to leave things in a good state).  I'm discovering this means lots of flush() commands to make sure database is updated before next steps.  All of the session management takes place inside methods on ContentStore, other code should not talk directly to the database. 

Database updates are mangaed via alembic migrations, usually deployed by the `content_store_manager.py` see README in /alembic
