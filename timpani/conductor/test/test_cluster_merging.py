import unittest
from datetime import datetime
from timpani.app_cfg import TimpaniAppCfg
from timpani.content_store.content_store_manager import ContentStoreManager

# from timpani.content_store.content_store_obj import ContentStoreObject
from timpani.content_store.content_store import ContentStore
from timpani.content_store.content_item import ContentItem
from timpani.workspace_config.test_workspace_cfg import TestWorkspaceConfig
from timpani.conductor.process import ContentProcessor
from timpani.conductor.orchestrator import Orchestrator
from timpani.vector_store.alegre_store_wrapper import AlegreVectorStoreService


class TestClusterMerge(unittest.TestCase):
    """
    NOTE: Full tests will not run in CI because no alegre or means tokens model.
    When starting Alegre locally to run tests, need
    to run alegre and the means tokens model FROM THE CHECK DOCKER COMPOSE and
    specifically start mean tokens
    cd /check  `docker compose up alegre xlm_r_bert_base_nli_stsb_mean_tokens`
    or
    `docker compose up alegre paraphrase_multilingual_mpnet_base_v2`
    """

    workspace_cfg = TestWorkspaceConfig()
    app_cfg = TimpaniAppCfg()

    MINIO_S3_TEST_STORE_LOCATION = "http://minio:9002"

    @classmethod
    def setUpClass(self):
        cfg = TimpaniAppCfg()

        # don't run in prod env?
        assert cfg.deploy_env_label not in [
            "live",
            "qa",
        ], "Exiting test to avoid modifying live or qa database"

        # TODO: before dropping tables, get a list of all the content
        # items previously created in alegre by this test, and delete them
        manager = ContentStoreManager()
        manager.destroy_content_store()
        manager.setup_admin_and_content_store()

        # create_engine("sqlite+pysqlite:///:memory:", echo=True)
        self.store = ContentStore()
        self.engine = self.store.init_db_engine()

        # ContentStoreObject.metadata.drop_all(bind=self.engine)
        # define all the new tables
        # ContentStoreObject.metadata.create_all(bind=self.engine)

        # clean out any previous records for this test before starting
        if TimpaniAppCfg().deploy_env_label not in ["test", "live"]:
            self.vector_store = AlegreVectorStoreService()
            self.vector_store.discard_workspace("meedan_clustering_test")
            self.orchestrator = Orchestrator(self.store, self.vector_store)

    @unittest.skipIf(
        TimpaniAppCfg().deploy_env_label in ["test", "live"],
        "integration tests not run in CI test environment because Alegre service not avalible",
    )
    def test_near_dupes_cluster_merging(self):
        """Processor is able to find and merge similar clusters"""
        # do this is a seperate work space so won't match with other tests
        test_workspace_id = "test_clust_items"

        # clean out any previous records for this test before starting
        self.vector_store.discard_workspace(test_workspace_id)

        test_items_a = [
            # first two items identical content
            ContentItem(
                date_id=19000101,
                run_id="run_1c43908277e34803ba7eea51b9054219",
                workspace_id=test_workspace_id,
                source_id="test_source",
                query_id="test_query_id",
                raw_created_at=datetime.strptime(
                    "2023-06-09 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
                ),
                raw_content_id="129839388abc",
                raw_content="This belongs in cluster A",
            ),
            ContentItem(
                date_id=19000101,
                run_id="run_1c43908277e34803ba7eea51b9054219",
                workspace_id=test_workspace_id,
                source_id="test_source",
                query_id="test_query_id",
                raw_created_at=datetime.strptime(
                    "2023-06-09 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
                ),
                raw_content_id="129839388def",
                raw_content="This belongs in cluster A",
            ),
        ]
        test_items_b = [
            # second two items nearly identical content
            ContentItem(
                date_id=19000101,
                run_id="run_1c43908277e34803ba7eea51b9054219",
                workspace_id=test_workspace_id,
                source_id="test_source",
                query_id="test_query_id",
                raw_created_at=datetime.strptime(
                    "2023-06-09 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
                ),
                raw_content_id="129839388ghi",
                raw_content="This is in B but belongs in cluster A",
            ),
            ContentItem(
                date_id=19000101,
                run_id="run_1c43908277e34803ba7eea51b9054219",
                workspace_id=test_workspace_id,
                source_id="test_source",
                query_id="test_query_id",
                raw_created_at=datetime.strptime(
                    "2023-06-09 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
                ),
                raw_content_id="129839388jkl",
                raw_content="This is in B but belongs in cluster A",
            ),
        ]

        # first store all the items
        for item in test_items_a:
            item = self.store.initialize_item(item)
            self.vector_store.store_vector_for_content_item(item)
        cluster_a = self.store.cluster_items(test_items_a[0], test_items_a[1])

        for item in test_items_b:
            item = self.store.initialize_item(item)
            self.vector_store.store_vector_for_content_item(item)
        cluster_b = self.store.cluster_items(test_items_b[0], test_items_b[1])

        processor = ContentProcessor()
        processor.process_clusters(workspace_id=test_workspace_id)

        # we don't know which cluster they got merged into
        result_cluster = self.store.refresh_object(cluster_a)
        if result_cluster is None:
            result_cluster = self.store.refresh_object(cluster_b)
        assert result_cluster is not None
        assert (
            result_cluster.num_items == 4
        ), f"cluster has {result_cluster.num_items} items"

        # clean up
        for item in test_items_a + test_items_b:
            self.vector_store.discard_vector_for_content_item(item)
            self.store.delete_item(item)

    def test_merge_clusters(self):
        """confirm that clusters combined as expected"""
        # NOTE: this doesn't involve any similarity logic
        # do this is a seperate work space so won't match with other tests
        test_workspace_id = "test_clust_merge"

        test_items_a = [
            # first two items identical content
            ContentItem(
                date_id=19000101,
                run_id="run_1c43908277e34803ba7eea51b9054219",
                workspace_id=test_workspace_id,
                source_id="test_source",
                query_id="test_query_id",
                raw_created_at=datetime.strptime(
                    "2023-06-09 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
                ),
                raw_content_id="129839388abc",
                raw_content="This belongs in cluster A",
            ),
            ContentItem(
                date_id=19000101,
                run_id="run_1c43908277e34803ba7eea51b9054219",
                workspace_id=test_workspace_id,
                source_id="test_source",
                query_id="test_query_id",
                raw_created_at=datetime.strptime(
                    "2023-06-09 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
                ),
                raw_content_id="129839388def",
                raw_content="This belongs in cluster A",
            ),
        ]
        test_items_b = [
            # second two items nearly identical content
            ContentItem(
                date_id=19000101,
                run_id="run_1c43908277e34803ba7eea51b9054219",
                workspace_id=test_workspace_id,
                source_id="test_source",
                query_id="test_query_id",
                raw_created_at=datetime.strptime(
                    "2023-06-09 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
                ),
                raw_content_id="129839388ghi",
                raw_content="This is in B but belongs in cluster A",
            ),
            ContentItem(
                date_id=19000101,
                run_id="run_1c43908277e34803ba7eea51b9054219",
                workspace_id=test_workspace_id,
                source_id="test_source",
                query_id="test_query_id",
                raw_created_at=datetime.strptime(
                    "2023-06-09 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
                ),
                raw_content_id="129839388jkl",
                raw_content="This is in B but belongs in cluster A",
            ),
        ]

        # first store all the items
        for item in test_items_a:
            item = self.store.initialize_item(item)
        cluster_a = self.store.cluster_items(test_items_a[0], test_items_a[1])

        for item in test_items_b:
            item = self.store.initialize_item(item)
        cluster_b = self.store.cluster_items(test_items_b[0], test_items_b[1])

        self.store.merge_clusters(
            cluster_b.content_cluster_id,
            cluster_a.content_cluster_id,
        )

        # expecting that this results in B getting merged into A
        cluster_a = self.store.refresh_object(cluster_a)
        assert cluster_a.num_items == 4, f"cluster has {cluster_b.num_items} items"
        # expected cluster b deleted
        cluster_b = self.store.refresh_object(cluster_b)
        assert cluster_b is None

        # clean up
        for item in test_items_a + test_items_b:
            self.store.delete_item(item)
