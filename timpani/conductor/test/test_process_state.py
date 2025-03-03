import unittest
from timpani.app_cfg import TimpaniAppCfg
from timpani.content_store.content_store import ContentStore
from timpani.content_store.content_store_manager import ContentStoreManager

from timpani.conductor.process_state import ProcessState


class TestProcessState(unittest.TestCase):
    """
    Make sure we can create content items with
    appropriate state model, save to db, and update
    """

    cfg = TimpaniAppCfg()

    @classmethod
    def setUpClass(self):
        """
        Should only be called once during test run because
        it will reset database and cause deadlocks
        """

        # define all the new tables
        # don't run in prod env?
        assert self.cfg.deploy_env_label not in [
            "live",
            "qa",
        ], "Exiting test to avoid modifying live or qa database"
        print("\nDrop and recreating all tables for item state test run\n")
        manager = ContentStoreManager()
        manager.destroy_content_store()
        manager.setup_admin_and_content_store()
        # ContentStoreObject.metadata.drop_all(bind=engine)
        # ContentStoreObject.metadata.create_all(bind=engine)

        # create a content store
        self.store = ContentStore()
        self.store.init_db_engine()

    def test_initialization(self):
        """
        Make sure we can write the process state into the db
        """
        state = ProcessState("test_process")
        self.store.record_process_state(state)
        state2 = ProcessState("test_process")
        self.store.record_process_state(state2)

    def test_state_change(self):
        """
        Make sure we change the states and then save to db, overwriting previous state
        """
        state = ProcessState("test_process")
        self.store.record_process_state(state)
        state.start_run(
            workspace_id="test_workspace",
            source_name="test_source",
            query_id="test_query",
        )
        self.store.record_process_state(state)
        state.transitionTo(state.STATE_COMPLETED)
        self.store.record_process_state(state)
