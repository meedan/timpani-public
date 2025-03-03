from timpani.content_store.content_store_interface import ContentStoreInterface
from timpani.processing_sequences.classy_workflow import ClassyWorkflow


class ClassyIndiaElectionWorkflow(ClassyWorkflow):
    """
    Implements the classycat workflow, but using the schema defined
    for the india election. All of the work is done by the classyWorkflow
    """

    CLASSY_SCHEMA = "2024 Indian Election"

    def __init__(self, content_store: ContentStoreInterface) -> None:
        # initialize the main classy workflow, but tell it to use the
        # classycat India Elections schema
        super().__init__(content_store, classycat_schema_name=self.CLASSY_SCHEMA)
        # Note: superclass will also try to load the schema

    def get_name(self):
        """
        Return the slug id for the workflow
        """
        return "classy_india_elections"
