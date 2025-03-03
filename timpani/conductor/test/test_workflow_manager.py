import unittest

from timpani.content_store.content_store import ContentStore

from timpani.processing_sequences.workflow_manager import WorkflowManager


class TestWorkflowManager(unittest.TestCase):
    """
    Tests initializing workflows as they are used by conductor
    """

    @classmethod
    def setUpClass(self):

        self.store = ContentStore()
        # create read only connection (no changes expected)
        self.engine = self.store.init_db_engine(
            ro_connect_string=self.store.RO_PG_CONNECT_STR
        )

    def test_workflow_manager_registered_workflows(self):
        """
        confirm registered workflows can be initialized
        """
        # workflow manager instantiates all workflows during init
        manager = WorkflowManager(self.store)

        # confirm we can retrieve some test workflows
        default_workflow = manager.get_workflow("default_workflow")
        # check state model structure
        assert set(default_workflow.get_state_model().valid_states) == set(
            ["undefined", "ready", "failed", "completed", "vectorized", "clustered"]
        )
        test_workflow = manager.get_workflow("test_workflow")
        # should inherit undefined, ready, failed, completed, and add placedholder and delayed
        assert set(test_workflow.get_state_model().valid_states) == set(
            ["undefined", "ready", "failed", "completed", "placeholder", "delayed"]
        )
