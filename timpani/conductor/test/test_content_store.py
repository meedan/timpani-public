import unittest
from timpani.content_store.content_store import ContentStore

# from timpani.content_store.content_store_obj import ContentStoreObject
from timpani.content_store.content_store_manager import ContentStoreManager
from timpani.app_cfg import TimpaniAppCfg


class TestContentStore(unittest.TestCase):
    """
    Confirm that we are able to talk to a content_store service,
    assuming backed by PG cluster
    """

    def test_postgres_connection(self):
        """
        Test connection and table creation for posgres content_store db
        """
        cfg = TimpaniAppCfg()
        # 'postgresql://user:user_password@{}:5433/content_store'
        # TODO: this will wipe database when running tests
        # don't run in prod env?
        assert cfg.deploy_env_label not in [
            "live",
            "qa",
        ], "Exiting test to avoid modifying live or qa database"
        manager = ContentStoreManager()
        manager.destroy_content_store()
        manager.setup_admin_and_content_store()

        # TODO: run tests in a seperate parallel schema so they can safely drop and create?
        self.store = ContentStore()
        self.engine = self.store.init_db_engine()

        # this is the old ORM way
        # ContentStoreObject.metadata.drop_all(bind=self.engine)
        # define all the new tables
        # ContentStoreObject.metadata.create_all(bind=self.engine)

        # test that we can call delete workspace
        self.store.erase_workspace(workspace_id="test", source_id="faker_testing")
