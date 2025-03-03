import unittest
from datetime import datetime
from timpani.trend_viewer.pandas_content_store import PandasContentStore
from timpani.content_store.content_item import ContentItem
from timpani.content_store.content_store_manager import ContentStoreManager

# from timpani.content_store.content_store_obj import ContentStoreObject
from timpani.content_store.content_store import ContentStore
from timpani.app_cfg import TimpaniAppCfg


class TestContentStore(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        cfg = TimpaniAppCfg()
        # don't run in prod env?
        assert cfg.deploy_env_label not in [
            "live",
            "qa",
        ], "Exiting test to avoid modifying live or qa database"
        # ContentStoreObject.metadata.drop_all(bind=self.engine)
        # define all the new tables
        # ContentStoreObject.metadata.create_all(bind=self.engine)
        # create_engine("sqlite+pysqlite:///:memory:", echo=True)
        manager = ContentStoreManager()
        manager.destroy_content_store()
        manager.setup_admin_and_content_store()
        self.store = ContentStore()
        self.engine = self.store.init_db_engine()

    def test_get_items_as_rows(self):
        """
        check that we can return data rows for viewer etc
        """
        num_items = 49
        test_items = []
        test_workspace_id = "test_item_rows"
        # create the test content
        for i in range(num_items):
            item = ContentItem(
                date_id=19000101,
                run_id="run_1c43908277e34803ba7eea51b9054219",
                workspace_id=test_workspace_id,
                source_id="test_source",
                query_id="test_query_id",
                raw_created_at=datetime.strptime(
                    "2023-06-09 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
                ),
                raw_content_id=f"test_get_items_as_rows_{i}",
                raw_content="testing the items as rows function",
            )
            item = self.store.initialize_item(item)
            test_items.append(item)

        pd_content_store = PandasContentStore(content_store=self.store)
        item_rows_df = pd_content_store.get_content_item_rows(
            workspace_id=test_workspace_id
        )
        assert len(item_rows_df) == num_items, f"length of items{len(item_rows_df)}"

        expected_columns = [
            "run_id",
            "workspace_id",
            "source_id",
            "query_id",
            "date_id",
            "raw_created_at",
            "raw_content_id",
            "raw_content",
            "updated_at",
            "content_item_id",
            "content_item_state_id",
            "content_published_date",
            "content_published_url",
            "content",
            "content_cluster_id",
            "content_language_code",
            "content_locale_code",
            "source_field",
            "current_state",
            "completed_timestamp",
        ]
        found_cols = list(item_rows_df.columns.values)
        assert (
            found_cols == expected_columns
        ), f"expected columns {expected_columns} do not match found {found_cols}"

    # TODO: need test for cluster rows

    # TODO: most of these functions (keywords, date ranges, etc) do not have tests!!
