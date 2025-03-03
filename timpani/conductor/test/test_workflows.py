import unittest
import json
from timpani.app_cfg import TimpaniAppCfg
from timpani.raw_store.item import Item
from timpani.content_store.content_store_manager import ContentStoreManager

# from timpani.content_store.content_store_obj import ContentStoreObject
from timpani.content_store.content_store import ContentStore
from timpani.content_store.item_state_model import ContentItemState
from timpani.raw_store.store import Store
from timpani.vector_store.alegre_store_wrapper import AlegreVectorStoreService
from timpani.processing_sequences.workflow import Workflow
from timpani.processing_sequences.default_workflow import DefaultWorkflow
from timpani.processing_sequences.meedan_workflow import MeedanWorkflow
from timpani.processing_sequences.meedan_tse_workflow import MeedanTSEWorkflow
from timpani.processing_sequences.meedan_aapi_workflow import MeedanAAPIWorkflow
from timpani.raw_store.cloud_store import CloudStore
from timpani.workspace_config.test_workspace_cfg import TestWorkspaceConfig
from timpani.util.run_state import RunState
from timpani.conductor.process import ContentProcessor


class TestWorkflows(unittest.TestCase):
    """
    Tests extracting and running a single content item example through each workflow
    NOTE: Full tests will not run in CI because services needed by some workflows will not be
    availible
    * THIS REQUIRES THAT THE CONDUCTOR IS RUNNING
    TODO: get the set of workflows to test from WorkflowManager REGISTRED_WORKFLOWS
    """

    app_cfg = TimpaniAppCfg()

    MINIO_S3_TEST_STORE_LOCATION = "http://minio:9002"

    # test content for Default workflow
    with open("timpani/booker/test/test_item_contents_1.json") as json_file_1:
        default_workflow_test_content_1 = json.load(
            json_file_1,
            strict=False,  # because there are \n and \t
        )

    # test content for junkipedia workflows
    # TODO: refactor these to process with Default or test
    with open("timpani/booker/test/test_item_contents_Telegram.json") as json_file_2:
        workflow_test_content_1 = json.load(
            json_file_2,
            strict=False,  # because there are \n and \t
        )
    with open("timpani/booker/test/test_item_contents_Twitter.json") as json_file_3:
        workflow_test_content_2 = json.load(
            json_file_3,
            strict=False,  # because there are \n and \t
        )
    with open("timpani/booker/test/test_item_contents_YT.json") as json_file_4:
        workflow_test_content_3 = json.load(
            json_file_4,
            strict=False,  # because there are \n and \t
        )
    with open("timpani/booker/test/test_item_contents_TikTok.json") as json_file_5:
        workflow_test_content_4 = json.load(
            json_file_5,
            strict=False,  # because there are \n and \t
        )

    workspace_cfg = TestWorkspaceConfig()
    test_item_1 = Item(
        run_id="testrun",
        workspace_id=workspace_cfg.get_workspace_slug(),
        source_id="junkipedia",
        query_id="testquery",
        page_id=None,
        content_id="118550062",
        content=default_workflow_test_content_1,
    )

    # test content item for meedan workflow
    test_item_2 = Item(
        run_id="testrun",
        workspace_id="meedan",
        source_id="junkipedia",
        query_id="testquery",
        page_id=None,
        content_id="118550062",
        content=default_workflow_test_content_1,
    )

    # test content for meedan_tse workflow

    test_item_3 = Item(
        run_id="testrun",
        workspace_id="meedan_tse",
        source_id="s3_csv_tse_tipline",
        query_id="tse.csv",
        page_id=26,
        content_id="282873",
        content={
            "id": "282873",
            "feed_id": "3",
            "request_type": "text",
            "content": "Seguran\u00e7a das urnas ",
            "created_at": "2022-10-30T03:31:27.267607",
            "updated_at": "2022-12-09T06:19:07.156842",
            "request_id": "118",
            "media_id": "1388023",
            "medias_count": "1",
            "requests_count": "1",
            "last_submitted_at": "2022-10-30T03:31:27.267607",
            "webhook_url": "",
            "last_called_webhook_at": "",
            "subscriptions_count": "0",
            "fact_checked_by_count": "0",
            "project_medias_count": "0",
            "quote": "seguran\u00e7a das urnas",
            "type": "Claim",
            "file": "",
        },
    )

    # test content for meedan_aapi workflow

    test_item_4 = Item(
        run_id="testrun",
        workspace_id="meedan_aapi",
        source_id="s3_csv_aapi_tweets",
        query_id="tweets3.csv",
        page_id=39,
        content_id="1640931355096670212",
        content={
            "author_id": "864521635260506112",
            "conversation_id": "1640931355096670212",
            "created_at": "2023-03-29T04:18:16.000Z",
            "edit_history_tweet_ids": "{1640931355096670212}",
            "id": "1640931355096670212",
            "lang": "en",
            "possibly_sensitive": "f",
            "referenced_tweets": '[{"id": "1640711566839672835", "type": "retweeted"}]',
            "text": "RT @AnnaApp91838450: WTF Was Dominion\nServers doing in Germany "
            + "\u2753\ufe0f\n2020 ELECTION FRAUD \ud83d\udcaf\ud83d\udcaf\ud83d\udcaf https:/\u2026",
            "entities.urls": "NaN",
            "public_metrics.impression_count": "0",
            "public_metrics.like_count": "0",
            "public_metrics.quote_count": "0",
            "public_metrics.reply_count": "0",
            "public_metrics.retweet_count": "2212",
            "entities.mentions": '[{"end": 19, "id": "734168909553766405", "start": 3, "username": "AnnaApp91838450"}]',
            "attachments.media_keys": "",
            "in_reply_to_user_id": "",
            "entities.annotations": '[{"end": 103, "normalized_text": "Germany", "probability": 0.9298, "start": 97, "type": "Place"}]',
            "entities.hashtags": "NaN",
            "attachments.poll_ids": "",
            "geo.place_id": "",
            "cluster_id": "22892",
            "cluster_size": "2",
            "is_retweet": "f",  # setting this to false or it will be skipped
            "dataset": "AAPI",
        },
    )

    # test content for meedan
    test_item_5 = Item(
        run_id="testrun",
        workspace_id="meedan",
        source_id="junkipedia",
        query_id="testquery",
        page_id=None,
        content_id="118550062",
        content=default_workflow_test_content_1,
    )

    # telegram test content for meedan
    test_item_6 = Item(
        run_id="testrun",
        workspace_id="meedan",
        source_id="junkipedia",
        query_id="telegram",
        page_id=None,
        content_id="20450777",
        content=workflow_test_content_1,
    )
    test_item_7 = Item(
        run_id="testrun",
        workspace_id="meedan",
        source_id="junkipedia",
        query_id="twitter",
        page_id=None,
        content_id="8284845",
        content=workflow_test_content_2,
    )
    test_item_8 = Item(
        run_id="testrun",
        workspace_id="meedan",
        source_id="junkipedia",
        query_id="youtube",
        page_id=None,
        content_id="200998077",
        content=workflow_test_content_3,
    )
    test_item_9 = Item(
        run_id="testrun",
        workspace_id="meedan",
        source_id="junkipedia",
        query_id="tiktok",
        page_id=None,
        content_id="316199845",
        content=workflow_test_content_4,
    )

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
        manager = ContentStoreManager()
        manager.destroy_content_store()
        manager.setup_admin_and_content_store()

        self.store = ContentStore()
        self.engine = self.store.init_db_engine()

        # need to reset vector database
        self.vector_store = AlegreVectorStoreService()

    def test_default_workflow(self):
        """
        Test processing single item in DefaultWorkflow
        """
        workflow = DefaultWorkflow(self.store)

        test_content = self.test_item_1
        self._test_get_state_model(workflow)
        self._test_extract_transform(workflow, test_content)
        self._test_processing_state_transitions([test_content])
        expected_states = set(
            ["undefined", "ready", "vectorized", "clustered", "completed", "failed"]
        )
        valid_states = set(workflow.get_state_model().valid_states)

        assert valid_states == expected_states, f"workflow states were {valid_states}"

    @unittest.skipIf(
        TimpaniAppCfg().deploy_env_label in ["test", "live"],
        "integration tests not run in CI test environment",
    )
    def test_meedan_workflow(self):
        """
        Test processing single item in MeedanWorkflow
        """
        workflow = MeedanWorkflow(self.store)
        test_content = self.test_item_2

        expected_states = set(
            [
                "undefined",
                "ready",
                "vectorized",
                "clustered",
                "completed",
                "failed",
                "keyworded",
                "hashtaged",
            ]
        )
        valid_states = set(workflow.get_state_model().valid_states)
        assert (
            valid_states == expected_states
        ), f"meedan workflow states were {valid_states}"

        # check the transitions
        expected_transitions = {
            "undefined": ["ready", "failed"],
            "ready": ["keyworded", "failed"],
            "failed": ["ready"],
            "vectorized": ["clustered", "failed"],
            "clustered": ["hashtaged", "failed"],
            "keyworded": ["vectorized", "failed"],
            "hashtaged": ["completed", "failed"],
        }
        transitions = workflow.get_state_model().valid_transitions
        assert (
            expected_transitions == transitions
        ), f"meedan workflow transitions {transitions}"

        self._test_get_state_model(workflow)
        self._test_extract_transform(workflow, test_content)
        self._test_processing_state_transitions([test_content])

    @unittest.skipIf(
        TimpaniAppCfg().deploy_env_label in ["test", "live"],
        "integration tests not run in CI test environment",
    )
    def test_meedan_tse_workflow(self):
        """
        Test processing single item in MeedanTseWorkflow
        """
        workflow = MeedanTSEWorkflow(self.store)
        test_content = self.test_item_3
        # NOTE: this will wipe the workspace
        self.vector_store.discard_workspace(test_content.workspace_id)
        self._test_get_state_model(workflow)
        self._test_extract_transform(workflow, test_content)
        self._test_processing_state_transitions([test_content])

    @unittest.skipIf(
        TimpaniAppCfg().deploy_env_label in ["test", "live"],
        "integration tests not run in CI test environment",
    )
    def test_meedan_AAPI_workflow(self):
        """
        Test processing single item in MeedanAAPIWorkflow
        """
        workflow = MeedanAAPIWorkflow(self.store)
        test_content = self.test_item_4
        # NOTE: this will wipe the workspace
        self.vector_store.discard_workspace(test_content.workspace_id)
        self._test_get_state_model(workflow)
        self._test_extract_transform(workflow, test_content)
        self._test_processing_state_transitions([test_content])

    @unittest.skipIf(
        TimpaniAppCfg().deploy_env_label in ["test", "live"],
        "integration tests not run in CI test environment",
    )
    def test_workflow(self):
        """
        Test processing single item in MeedanWorkflow
        """
        workflow = MeedanWorkflow(self.store)
        test_content = self.test_item_5
        # NOTE: this will wipe the workspace
        self.vector_store.discard_workspace(test_content.workspace_id)

        self._test_get_state_model(workflow)
        items = [
            self.test_item_5,
            self.test_item_6,
            self.test_item_7,
            self.test_item_8,
            self.test_item_9,
        ]
        for test_content in items:
            self._test_extract_transform(workflow, test_content)

        # specifically, we are expecting two items back from the YT test
        yt_items = self._test_extract_transform(workflow, self.test_item_8)
        assert len(yt_items) == 3, f"yt items {yt_items}"

        self._test_processing_state_transitions(items)

    # functions that excercise parts of the workflow

    def _test_extract_transform(self, workflow: Workflow, test_content):
        """
        test that can create ContentItem from raw store items
        """
        items = workflow.extract_items(test_content)
        assert items[0] is not None, f"no items were extracted from {workflow}"
        # check that some content was extracted
        for item in items:
            assert item.content != "", f"extracted blank content item {item}"
        return items

    def _test_get_state_model(self, workflow: Workflow):
        model = workflow.get_state_model()
        assert isinstance(model, ContentItemState)

    def _test_processing_state_transitions(self, test_raw_items):
        """
        confirm we can send a piece of example content through the states
        """

        raw_store = CloudStore(store_location=self.MINIO_S3_TEST_STORE_LOCATION)
        raw_store.login_and_validate(
            access_key=self.app_cfg.minio_user,
            secret_key=self.app_cfg.minio_password,
        )

        run_state = RunState(
            "test_workflow",
        )

        content_store = ContentStore()
        content_store.init_db_engine()

        # create a partition specific to this test
        partition = Store.Partition(
            workspace_id=test_raw_items[0].workspace_id,
            source_id=test_raw_items[0].source_id,
            date_id=run_state.date_id,
        )

        # need to delete content from previous run in raw_store
        raw_store.delete_partition(partition)

        # store the test item(s) into the partition in raw store
        raw_store.append_chunk(partition, test_raw_items)

        processor = ContentProcessor(content_store=content_store)

        # extract from raw store to content store
        processor.process_raw_content(partition)

        # all of the items should appear in "in progress" query for processing
        in_progress = list(
            content_store.get_items_in_progress(workspace_id=partition.workspace_id)
        )
        assert (
            len(in_progress) > 0
        ), f"expected some in progress items, not {len(in_progress)}"

        # this runs simple workflow that doesn't use any services that might fail in CI
        processor.batch_process_workflows(
            workspace_id=partition.workspace_id, max_iterations=200
        )
        # all of the items should be done, none in progress
        in_progress = list(
            content_store.get_items_in_progress(
                workspace_id=partition.workspace_id,
            )
        )
        assert (
            len(in_progress) == 0
        ), f"expected 0 in progress items, not {len(in_progress)}"

        # check the expected summary of states
        states = processor.process_summary(workspace_id=partition.workspace_id)

        assert states["completed"] >= 1, f"states are {states}"
