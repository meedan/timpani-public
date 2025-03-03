import unittest
from datetime import datetime
from timpani.app_cfg import TimpaniAppCfg
from timpani.content_store.content_store_manager import ContentStoreManager

# from timpani.content_store.content_store_obj import ContentStoreObject
from timpani.content_store.content_store import ContentStore
from timpani.content_store.content_item import ContentItem
from timpani.raw_store.store import Store
from timpani.raw_store.cloud_store import CloudStore
from timpani.content_sources.faker_test_content_source import FakerTestingContentSource
from timpani.workspace_config.test_workspace_cfg import TestWorkspaceConfig
from timpani.util.run_state import RunState
from timpani.conductor.process import ContentProcessor
from timpani.conductor.actions.clustering import AlegreClusteringAction
from timpani.vector_store.alegre_store_wrapper import AlegreVectorStoreService


class TestContentProcessor(unittest.TestCase):
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
            vector_store = AlegreVectorStoreService()
            vector_store.discard_workspace("meedan_clustering_test")

    @unittest.skipIf(
        TimpaniAppCfg().deploy_env_label in ["test", "live"],
        "integration tests not run in CI test environment because Alegre service not avalible",
    )
    def test_processing_vectorization_and_clustering(self):
        """
        confirm we can process content from raw store into content store with vectorization and clustering
        This runs with the DefaultWorkflow which includes clustering.
        TODO: this *really* needs a way to clear out state in Alegre, otherwise tests interfere with eachother
        because old items are return for clustering
        """
        # test_size = 1000
        test_size = 12
        # test_size = self.workspace_cfg.get_queries(
        #    FakerTestingContentSource.get_source_name()
        # )

        fake_source = FakerTestingContentSource(
            total_items=test_size, page_size=min(test_size, 100)
        )

        run_state = RunState(
            "test_acquire_2",
        )

        # NOTE: this is configured for MINIO,
        raw_store = CloudStore(store_location=self.MINIO_S3_TEST_STORE_LOCATION)
        raw_store.login_and_validate(
            access_key=self.app_cfg.minio_user,
            secret_key=self.app_cfg.minio_password,
        )

        partition = Store.Partition(
            self.workspace_cfg.get_workspace_slug(),
            fake_source.get_source_name(),
            run_state.date_id,
        )

        content_store = ContentStore()
        content_store.init_db_engine()

        # need to delete content from previous run in content_store
        content_store.dangerously_run_sql(
            f"delete from content_item where source_id='{fake_source.get_source_name()}'"
        )
        content_store.dangerously_run_sql(
            # delete the state records that do not join anymore
            """delete from content_item_state where not exists
                (
                    select state_id from content_item_state
                    join content_item on content_item_state_id = state_id
                )
            """
        )
        # TODO: could use query id to make content specific to this test to avoid colisions
        # between tests

        # need to delete content from previous run in raw_store
        raw_store.delete_partition(partition)

        # load a bunch of fake data into raw store
        fake_source.acquire_new_content(
            self.workspace_cfg, raw_store, run_state=run_state
        )

        processor = ContentProcessor()
        # we are processing with the 'wrong workflow'
        processor.process_raw_content(partition, workflow_id="default_workflow")

        # the number of "in progress" items (not failed or undefined)
        in_progress = list(content_store.get_items_in_progress())
        assert (
            len(in_progress) == test_size
        ), f"expected {test_size} in progress items, not {len(in_progress)}"

        # check the expected summary of states
        states = processor.process_summary()
        assert states["ready"] == test_size, f"states are {states}"

        # all of the items should appear in "in progress" query for processing
        in_progress = list(content_store.get_items_in_progress())
        assert (
            len(in_progress) == test_size
        ), f"expected {test_size} in progress items, not {len(in_progress)}"

        # TODO: is this going to put things in a funny state by processing with a different workflow
        # TODO: make sure workspace configs support multiple workflows
        processor.batch_process_workflows(
            workspace_id=self.workspace_cfg.get_workspace_slug(),
            workflow_id="default_workflow",
        )

        # after processing, all of the items should be done, none in progress
        in_progress = list(content_store.get_items_in_progress())
        assert (
            len(in_progress) == 0
        ), f"expected 0 in progress items, not {len(in_progress)}"

        # check the expected summary of states
        states = processor.process_summary()

        assert states["completed"] == test_size, f"states are {states}"

    @unittest.skipIf(
        TimpaniAppCfg().deploy_env_label in ["test", "live"],
        "integration tests not run in CI test environment because Alegre service not avalible",
    )
    def test_clustering_threshold(self):
        """Confirm that adjusting the cluster threshold changes results returned in cluster"""
        vector_store = AlegreVectorStoreService()

        item1 = ContentItem(
            date_id=19000101,
            run_id="run_1c43908277e34803ba7eea51b9054219",
            workspace_id="meedan_clustering_test",
            source_id="test_source",
            query_id="test_query_id",
            raw_created_at=datetime.strptime(
                "2023-06-09 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
            ),
            raw_content_id="129839388abc",
            raw_content="This is a test of cluster changes from adjusting the Alegre similarity threshold",
        )

        item2 = ContentItem(
            date_id=19000101,
            run_id="run_1c43908277e34803ba7eea51b9054219",
            workspace_id="meedan_clustering_test",
            source_id="test_source",
            query_id="test_query_id",
            raw_created_at=datetime.strptime(
                "2023-06-09 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
            ),
            raw_content_id="129839388def",
            # raw_content="This is a test of Alegre clustering similarity",
            raw_content="This is another test of cluster changes from adjusting the Alegre similarity threshold",
        )
        item1 = self.store.initialize_item(item1)
        item2 = self.store.initialize_item(item2)
        # self.store.transition_item_state(item1, ContentItemState.STATE_READY)
        # self.store.transition_item_state(item2, ContentItemState.STATE_READY)

        vector_store.store_vector_for_content_item(item1)
        vector_store.store_vector_for_content_item(item2)

        # at 0.5 threshold, expected to match
        clusterer = AlegreClusteringAction(self.store, similarity_threshold=0.1)
        assert clusterer.SIMILARITY_THRESHOLD == 0.1
        cluster1 = clusterer.add_item_to_best_cluster(item2, target_state=None)
        cluster1items = list(self.store.get_cluster_items(cluster1))
        # need to refresh states of items to check equality
        item1 = self.store.refresh_object(item1)
        item2 = self.store.refresh_object(item2)
        assert item1 in cluster1items, f"cluster1items: {cluster1items}"
        assert item2 in cluster1items, f"cluster1items: {cluster1items}"

        # threshold of 1.0, expected to not match
        clusterer = AlegreClusteringAction(self.store, similarity_threshold=1.0)
        assert clusterer.SIMILARITY_THRESHOLD == 1.0
        cluster2 = clusterer.add_item_to_best_cluster(
            item2,
            target_state=None,
        )
        cluster2items = list(self.store.get_cluster_items(cluster2))
        item1 = self.store.refresh_object(item1)
        item2 = self.store.refresh_object(item2)
        assert item1 not in cluster2items, f"cluster2items: {cluster2items}"
        assert item2 in cluster2items, f"cluster2items: {cluster2items}"

        # TODO: confirm non-matching across workspaces?

        # clean up from the test
        vector_store.discard_vector_for_content_item(item1)
        vector_store.discard_vector_for_content_item(item2)

    @unittest.skipIf(
        TimpaniAppCfg().deploy_env_label in ["test", "live"],
        "integration tests not run in CI test environment because Alegre service not avalible",
    )
    def test_several_items(self):
        """try to expose the off by one bug num items in cluster"""
        # do this is a seperate work space so won't match with other tests
        test_workspace_id = "test_several_items"
        vector_store = AlegreVectorStoreService()

        # clean out any previous records for this test before starting
        vector_store.discard_workspace(test_workspace_id)

        test_items = [
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
                raw_content="This is a test of repeatedly adding items and clustering them with Alegre",
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
                # raw_content="This is a test of Alegre clustering similarity",
                raw_content="This is a test of repeatedly adding items and clustering them.",
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
                raw_content_id="129839388ghi",
                # raw_content="This is a test of Alegre clustering similarity",
                raw_content="This is a test of repeatedly adding items!",
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
                # raw_content="This is a test of Alegre clustering similarity",
                raw_content="This is a test",
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
                raw_content_id="129839388mno",
                # raw_content="This is a test of Alegre clustering similarity",
                raw_content="This is a test of adding lots of items",
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
                raw_content_id="129839388pqr",
                # raw_content="This is a test of Alegre clustering similarity",
                raw_content="repeatedly adding items and clustering them with Alegre",
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
                raw_content_id="129839388stu",
                # raw_content="This is a test of Alegre clustering similarity",
                raw_content="repeatedly adding items and clustering them with Alegre",
            ),
        ]

        expected_cluster_size = 0
        # low threshold, so most things expected to match
        clusterer = AlegreClusteringAction(self.store, similarity_threshold=0.1)
        for item in test_items:
            item = self.store.initialize_item(item)
            vector_store.store_vector_for_content_item(item)
            cluster = clusterer.add_item_to_best_cluster(item, target_state=None)
            expected_cluster_size += 1
            cluster1items = list(self.store.get_cluster_items(cluster))
            assert (
                len(cluster1items) == cluster.num_items
            ), f"number of items in cluster ({len(cluster1items)}) did not match cluster size ({cluster.num_items})"
            assert (
                cluster.num_items == expected_cluster_size
            ), f"number of items in cluster ({len(cluster1items)}) did not match expected cluster size ({expected_cluster_size})"
            # need to refresh states of items to check equality
            item = self.store.refresh_object(item)
            assert item.content_cluster_id == cluster.content_cluster_id
            assert item in cluster1items, f"cluster1items: {cluster1items}"

        # clean up from the test
        for item in test_items:
            vector_store.discard_vector_for_content_item(item)

    @unittest.skipIf(
        TimpaniAppCfg().deploy_env_label in ["test", "live"],
        "integration tests not run in CI test environment because Alegre service not avalible",
    )
    def test_cluster_examples(self):
        """sort order should not impact clustering of identical items"""
        num_items = 5
        # num_items = 2000
        # do this is a seperate work space so won't match with other tests
        test_workspace_id = "identical_examples"

        vector_store = AlegreVectorStoreService()
        # make sure there are no pre-existing vectors in this workspace to mess thigns up
        vector_store.discard_workspace(test_workspace_id)
        content_id = 0
        test_items = []

        # do a bunch of vectorization first
        while content_id < num_items:
            item = ContentItem(
                date_id=19000101,
                run_id="run_1c43908277e34803ba7eea51b9054219",
                workspace_id=test_workspace_id,
                source_id="test_source",
                query_id="test_cluster_examples",
                raw_created_at=datetime.strptime(
                    "2023-06-09 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
                ),
                raw_content_id=f"content_id_{content_id}",
                raw_content="segurança das urnas",
            )
            item = self.store.initialize_item(item)
            vector_store.store_vector_for_content_item(item)
            test_items.append(item)
            content_id += 1

        # low threshold, so most things expected to match
        # now do the clustering in a second pass
        # exepecting them to all land in the same cluster
        clusterer = AlegreClusteringAction(self.store, similarity_threshold=0.875)
        cluster_ids = []
        # put the last two items (that would probably get sorted last)
        # into a cluster together
        end_cluster = self.store.cluster_items(test_items[-1], test_items[-2])
        cluster_ids.append(end_cluster.content_cluster_id)
        # now group all the rest in id order
        for item in test_items:
            # make sure the object is up to date before clustering
            # .. as it may have been put in a cluster earlier
            item = self.store.refresh_object(item)
            if item.content_cluster_id is None:
                cluster = clusterer.add_item_to_best_cluster(item, target_state=None)
                # print(
                #    f"item {item.content_item_id} added to cluster {cluster.content_cluster_id}"
                # )
                if cluster.content_cluster_id not in cluster_ids:
                    cluster_ids.append(cluster.content_cluster_id)

                cluster1items = list(self.store.get_cluster_items(cluster))
                assert (
                    len(cluster1items) == cluster.num_items
                ), f"number of items in cluster ({len(cluster1items)}) did not match cluster size ({cluster.num_items})"

                # need to refresh states of items to check equality
                item = self.store.refresh_object(item)
                assert item.content_cluster_id == cluster.content_cluster_id
                assert item in cluster1items, f"cluster1items: {cluster1items}"

        # after all this, we should only have one cluster
        assert len(cluster_ids) == 1, f"found {len(cluster_ids)} clusters"
        assert cluster.num_items == num_items

        # clean up from the test
        for item in test_items:
            vector_store.discard_vector_for_content_item(item)

    @unittest.skipIf(
        TimpaniAppCfg().deploy_env_label in ["test", "live"],
        "integration tests not run in CI test environment because Alegre service not avalible",
    )
    def test_multilingual_example(self):
        """test for content matching with paraphrase but not means tokens"""
        # do this is a seperate work space so won't match with other tests
        test_workspace_id = "test_multilingual_items"
        vector_store = AlegreVectorStoreService()
        # make sure there are no pre-existing vectors in this workspace to mess thigns up
        vector_store.discard_workspace(test_workspace_id)

        test_items = [
            ContentItem(
                date_id=19000101,
                run_id="run_1c43908277e34803ba7eea51b9054219",
                workspace_id=test_workspace_id,
                source_id="test_source",
                query_id="test_query_id",
                raw_created_at=datetime.strptime(
                    "2023-06-09 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
                ),
                raw_content_id="content_id_001",
                raw_content=(
                    "صورة يدعون تظهر واحدًا من أندر"
                    + " أنواع القطط في العالم يسمى ” قط الثعبان”، تم اكتشافه بالصدفة عام 2020 في أعماق غابات الأمازون،"
                ),
                # (translation: A picture claiming one of the rarest types of cats in the world called a “snake cat”,
                # it was discovered by chance in 2020 in the depths of the Amazon forest,)
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
                raw_content_id="content_id_002",
                raw_content="هل يوجد قط يدعى قط الثعبان؟",
                # (translation: Is there a cat called the snake cat?)
            ),
        ]
        # NOTE: do not confuse with cat snake (which is real?)
        for item in test_items:
            item = self.store.initialize_item(item)
            vector_store.store_vector_for_content_item(item)

        clusterer = AlegreClusteringAction(self.store, similarity_threshold=0.70)
        # expecting that the second item must match with the first because
        # exepected match score is 0.7188 for paraphrase while only 0.5969 for means-tokens
        cluster = clusterer.add_item_to_best_cluster(test_items[1], target_state=None)
        assert cluster.num_items == 2, f"cluster values {cluster}"

        # clean up from the test
        for item in test_items:
            vector_store.discard_vector_for_content_item(item)

    @unittest.skipIf(
        TimpaniAppCfg().deploy_env_label in ["test", "live"],
        "integration tests not run in CI test environment because Alegre service not avalible",
    )
    def test_no_cross_workspace_match(self):
        """Confirm that similarity query will not match items from another workspace"""
        # do this is a seperate work space so won't match with other tests
        test_workspace_id_a = "test_no_cross_wrksp_a"
        test_workspace_id_b = "test_no_cross_wrksp_b"
        vector_store = AlegreVectorStoreService()

        # clean out any previous records for this test before starting
        vector_store.discard_workspace(test_workspace_id_a)
        vector_store.discard_workspace(test_workspace_id_b)

        test_items = [
            ContentItem(
                date_id=19000101,
                run_id="run_1c43908277e34803ba7eea51b9054219",
                workspace_id=test_workspace_id_a,  # NOTE: different from 2nd item
                source_id="test_source",
                query_id="test_query_id",
                raw_created_at=datetime.strptime(
                    "2023-06-09 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
                ),
                raw_content_id="129839388abc",
                raw_content="This is a test to confirm identical items with different workspaces are not matched by Alegre",
            ),
            ContentItem(
                date_id=19000101,
                run_id="run_1c43908277e34803ba7eea51b9054219",
                workspace_id=test_workspace_id_b,
                source_id="test_source",
                query_id="test_query_id",
                raw_created_at=datetime.strptime(
                    "2023-06-09 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
                ),
                raw_content_id="129839388def",
                raw_content="This is a test to confirm identical items with different workspaces are not matched by Alegre",
            ),
        ]

        # low threshold, so most things expected to match
        # but these two items should NOT match even tho they are idientical
        clusterer = AlegreClusteringAction(self.store, similarity_threshold=0.1)
        for item in test_items:
            item = self.store.initialize_item(item)
            vector_store.store_vector_for_content_item(item)
            cluster = clusterer.add_item_to_best_cluster(item, target_state=None)
            cluster1items = list(self.store.get_cluster_items(cluster))
            assert (
                len(cluster1items) == cluster.num_items
            ), f"number of items in cluster ({len(cluster1items)}) did not match cluster size ({cluster.num_items})"
            assert (
                cluster.num_items == 1
            ), f"number of items in cluster ({len(cluster1items)}) was more than 1)"
            # need to refresh states of items to check equality
            item = self.store.refresh_object(item)
            assert item.content_cluster_id == cluster.content_cluster_id
            assert item in cluster1items, f"cluster1items: {cluster1items}"

        # clean up from the test
        for item in test_items:
            vector_store.discard_vector_for_content_item(item)

    @unittest.skipIf(
        TimpaniAppCfg().deploy_env_label in ["test", "live"],
        "integration tests not run in CI test environment because Alegre service not avalible",
    )
    def test_near_dupes(self):
        """near dupes should cluster together, not only with exact dupes"""
        # do this is a seperate work space so won't match with other tests
        test_workspace_id = "test_dupe_items"
        vector_store = AlegreVectorStoreService()

        # clean out any previous records for this test before starting
        vector_store.discard_workspace(test_workspace_id)

        test_items = [
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
                raw_content="Segurança das urnas",
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
                raw_content="Segurança das urnas",
            ),
            # second two items nearly identical content (off by case)
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
                raw_content="segurança das urnas",
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
                raw_content="segurança das urnas",
            ),
        ]

        # low threshold, so most things expected to match
        clusterer = AlegreClusteringAction(self.store)
        # first store all the items
        for item in test_items:
            item = self.store.initialize_item(item)
            vector_store.store_vector_for_content_item(item)

        clusters = []
        # now cluster them together
        for item in test_items:
            item = self.store.refresh_object(item)
            cluster = clusterer.add_item_to_best_cluster(item, target_state=None)
            if cluster.content_cluster_id not in clusters:
                clusters.append(cluster.content_cluster_id)
            assert (
                len(clusters) < 2
            ), f"number of items in cluster ({len(clusters)}) not < 2"

        # clean up from the test
        for item in test_items:
            vector_store.discard_vector_for_content_item(item)
