import unittest
from datetime import datetime
from timpani.app_cfg import TimpaniAppCfg
from timpani.content_store.content_store_manager import ContentStoreManager

# from timpani.content_store.content_store_obj import ContentStoreObject
from timpani.content_store.content_item import ContentItem
from timpani.content_store.content_store import ContentStore


class TestContentKeyword(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        # create_engine("sqlite+pysqlite:///:memory:", echo=True)
        cfg = TimpaniAppCfg()

        # don't run in prod env?
        assert cfg.deploy_env_label not in [
            "live",
            "qa",
        ], "Exiting test to avoid modifying live or qa database"
        # ContentStoreObject.metadata.drop_all(bind=self.engine)
        # define all the new tables
        # ContentStoreObject.metadata.create_all(bind=self.engine)
        manager = ContentStoreManager()
        manager.destroy_content_store()
        manager.setup_admin_and_content_store()
        self.store = ContentStore()
        self.engine = self.store.init_db_engine()

    def test_keyword_setup_and_delete(self):
        """
        keyword object can be initialized
        """
        content_item = self.store.initialize_item(
            ContentItem(
                date_id=19000101,
                run_id="run_1c43908277e34803ba7eea51b9054219",
                workspace_id="meedan_test",
                source_id="test_source",
                query_id="test_keywords",
                raw_created_at=datetime.strptime(
                    "2023-06-09 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
                ),
                raw_content_id="129839388abc",
                raw_content="This text demonstrates #keywords and #hashtags",
                # skipping auto set fields
                content_published_date=datetime.strptime("2023-06-12", "%Y-%m-%d"),
                content_published_url="http://somesite.com",
            )
        )
        keyword1 = self.store.attach_keyword(
            item=content_item,
            keyword_model_name="keyword_test",
            keyword_text="#keywords",
        )
        assert keyword1.content_item_id == content_item.content_item_id
        assert keyword1.workspace_id == content_item.workspace_id
        assert keyword1.keyword_text == "#keywords"

        keyword2 = self.store.attach_keyword(
            item=content_item,
            keyword_model_name="keyword_test",
            keyword_text="#hashtags",
        )
        assert keyword2.content_item_id == content_item.content_item_id
        assert keyword2.workspace_id == content_item.workspace_id
        assert keyword2.keyword_text == "#hashtags"

        # when item is deleted, keywords should go away
        self.store.delete_item(content_item)
        keyword1 = self.store.refresh_object(keyword1)
        keyword2 = self.store.refresh_object(keyword2)
        assert keyword1 is None
        assert keyword2 is None
