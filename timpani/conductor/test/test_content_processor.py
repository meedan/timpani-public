import unittest
from unittest.mock import patch
import json
import subprocess
from datetime import datetime
from timpani.app_cfg import TimpaniAppCfg
from timpani.raw_store.item import Item
from timpani.content_store.content_store_manager import ContentStoreManager

# from timpani.content_store.content_store_obj import ContentStoreObject
from timpani.content_store.content_store import ContentStore
from timpani.content_store.content_item import ContentItem
from timpani.raw_store.store import Store
from timpani.processing_sequences.default_workflow import DefaultWorkflow
from timpani.processing_sequences.test_workflow import (
    TestWorkflow,
    TestContentItemState,
)
from timpani.raw_store.cloud_store import CloudStore
from timpani.content_sources.faker_test_content_source import FakerTestingContentSource
from timpani.workspace_config.test_workspace_cfg import TestWorkspaceConfig
from timpani.util.run_state import RunState
from timpani.conductor.process import ContentProcessor
from timpani.conductor.actions.delay import DelayingAction
from timpani.conductor.actions.logging import LoggingAction


class TestContentProcessor(unittest.TestCase):
    """
    NOTE: Full tests will not run in CI
    """

    with open("timpani/booker/test/test_item_contents_1.json") as json_file_1:
        test_content_1 = json.load(
            json_file_1,
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
        content=test_content_1,  # TODO: will this come as text or json?
    )
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
        # ContentStoreObject.metadata.drop_all(bind=self.engine)
        # define all the new tables
        # ContentStoreObject.metadata.create_all(bind=self.engine)
        manager = ContentStoreManager()
        manager.destroy_content_store()
        manager.setup_admin_and_content_store()

        self.store = ContentStore()
        self.engine = self.store.init_db_engine()

    def test_extract_transform_happy(self):
        """
        test that fields parse nicely
        """
        workflow = DefaultWorkflow(self.store)
        items = workflow.extract_items(self.test_item_1)
        assert items[0] is not None

    def test_extract_transform_sad(self):
        """
        test that errors with data structures are handled
        TODO: assert warning generated
        """
        workflow = DefaultWorkflow(self.store)
        bad_item = self.test_item_1
        # remove one of the keys it is expecting
        bad_item.content["attributes"]["search_data_fields"].pop("all_text")
        items = workflow.extract_items(bad_item)
        assert items[0] is not None
        assert items[0].content == ""

    def test_processing_minimal_states(self):
        """
        confirm we can process content from raw store into content store
        """
        test_size = 110
        # test_size = 10
        # test_size = self.workspace_cfg.get_queries(
        #    FakerTestingContentSource.get_source_name()
        # )

        fake_source = FakerTestingContentSource(
            total_items=test_size, page_size=min(test_size, 100)
        )

        run_state = RunState(
            "test_acquire",
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

        self.store.erase_workspace(partition.workspace_id, partition.source_id)
        # TODO: could use query id to make content specific to this test to avoid colisions
        # between tests

        # need to delete content from previous run in raw_store
        raw_store.delete_partition(partition)

        # load a bunch of fake data into raw store
        fake_source.acquire_new_content(
            self.workspace_cfg, raw_store, run_state=run_state
        )

        processor = ContentProcessor(content_store=self.store)
        processor.process_raw_content(partition)

        # the number of "in progress" items (not failed or undefined)
        in_progress = list(
            self.store.get_items_in_progress(workspace_id=partition.workspace_id)
        )
        assert (
            len(in_progress) == test_size
        ), f"expected {test_size} in progress items, not {len(in_progress)}"

        # check the expected summary of states
        states = processor.process_summary(workspace_id=partition.workspace_id)
        assert states["ready"] == test_size, f"states are {states}"

        # all of the items should appear in "in progress" query for processing
        in_progress = list(
            self.store.get_items_in_progress(workspace_id=partition.workspace_id)
        )
        assert (
            len(in_progress) == test_size
        ), f"expected {test_size} in progress items, not {len(in_progress)}"

        # this runs simple workflow that doesn't use any services that might fail in CI
        processor.batch_process_workflows(self.workspace_cfg.get_workspace_slug())

        # all of the items should be done, none in progress
        in_progress = list(
            self.store.get_items_in_progress(workspace_id=partition.workspace_id)
        )
        assert (
            len(in_progress) == 0
        ), f"expected 0 in progress items, not {len(in_progress)}"

        # check the expected summary of states
        states = processor.process_summary(workspace_id=partition.workspace_id)

        assert states["completed"] == test_size, f"states are {states}"

    def test_reload_content_ignored(self):
        """
        Confirm that loading in content with a duplicate id does NOT
        overwrite the previous content UNLESSS it is in failed state
        """
        ITEM_ID = "1111111111"
        CORRECTLY_PARSED = "representing correctly parsed content"
        INCORRECTLY_PARSED = "this will represent content that didn't parse correctly"
        UPDATED = "this will represent content that was updated"

        test_item_2a = ContentItem(
            run_id="testrun",
            workspace_id=self.workspace_cfg.get_workspace_slug(),
            source_id="junkipedia",
            query_id="testquery",
            date_id=20000101,
            raw_content_id=ITEM_ID,
            raw_content=INCORRECTLY_PARSED,
            raw_created_at="2022-02-25T00:00:16.156Z",
            content="",  # indicates parsed in error
        )

        test_item_2b = ContentItem(
            run_id="testrun",
            workspace_id=self.workspace_cfg.get_workspace_slug(),
            source_id="junkipedia",
            query_id="testquery",
            date_id=20000101,
            raw_content_id=ITEM_ID,
            raw_created_at="2022-02-25T00:00:16.156Z",
            raw_content=CORRECTLY_PARSED,
        )

        test_item_2c = ContentItem(
            run_id="testrun",
            workspace_id=self.workspace_cfg.get_workspace_slug(),
            source_id="junkipedia",
            query_id="testquery",
            date_id=20000101,
            raw_content_id=ITEM_ID,
            raw_created_at="2022-02-25T00:00:16.156Z",
            raw_content=UPDATED,
        )
        content_store = ContentStore()
        content_store.init_db_engine()
        workflow = DefaultWorkflow(content_store=content_store)
        processor = ContentProcessor(content_store=content_store)

        # ---- insert the first item (expected to error because blank content)
        item_ready = processor._insert_content_item(
            test_item_2a, workflow.get_state_model()
        )
        assert item_ready is False
        # refresh the item so we can get the db id
        test_item_2a = content_store.refresh_object(test_item_2a)
        db_id = test_item_2a.content_item_id
        assert db_id is not None

        # ---- insert the 2nd item that is expected overwrite
        item_ready = processor._insert_content_item(
            test_item_2b, workflow.get_state_model()
        )

        # it should return True because it overwrote
        assert item_ready is True

        # refresh the object to get its db id and check database state
        test_item_2b = content_store.refresh_object(test_item_2b)
        assert test_item_2b.content == CORRECTLY_PARSED
        # cannot refresh 2a, because it is diconnected and has old db_id
        # which should have been deleted
        assert content_store.get_item(db_id) is None

        # ---- test that correctly parsed content will NOT be overwritten

        item_ready = processor._insert_content_item(
            test_item_2c, workflow.get_state_model()
        )
        # it should return false because din't overwrite
        assert item_ready is False

        # refreshing object should return none,  because it never made it into the db

        test_item_2c_refreshed = content_store.refresh_object(test_item_2c)
        assert test_item_2c_refreshed is None

        # confirm object unchanged
        test_item_2b = content_store.refresh_object(test_item_2b)
        assert test_item_2b.content == CORRECTLY_PARSED

        # --- now test with force overwrite behavior
        item_ready = processor._insert_content_item(
            test_item_2c, workflow.get_state_model(), force_overwrite=True
        )
        # it should return false because didn't overwrite
        assert item_ready is True

        # cannot refresh the object, because it never made it into the db
        test_item_2c = content_store.refresh_object(test_item_2c)
        assert test_item_2c.content == UPDATED

    def test_dispatch_state(self):
        """
        Check the function called by dispatch threads works
        """
        workflow = TestWorkflow(self.store)
        processor = ContentProcessor()

        # check that an ok item returns status code 0
        item_ok = ContentItem(
            date_id=19000101,
            run_id="run_1c43908277e34803ba7eea51b9054219",
            workspace_id="meedan_test",
            source_id="test_source",
            query_id="test_query_id",
            raw_created_at=datetime.strptime(
                "2023-06-09 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
            ),
            raw_content_id="1298_good_item",  # need a unique value to avoid update ignore behavior
            raw_content="This content is expected to process OK",
        )
        item_ok = self.store.initialize_item(item_ok, state=workflow.get_state_model())

        # confirm not in timed out state
        assert (
            workflow.check_state_timeout(
                self.store.get_item_state(item_ok.content_item_id)
            )
            is False
        )
        self.store.start_transition_to_state(
            item_ok, workflow.get_state_model().STATE_READY
        )
        # confirm is "in transition"
        assert (
            workflow.check_state_timeout(
                self.store.get_item_state(item_ok.content_item_id)
            )
            is True
        )
        self.store.transition_item_state(
            item_ok, workflow.get_state_model().STATE_READY
        )
        # confirm not in timed out state
        assert (
            workflow.check_state_timeout(
                self.store.get_item_state(item_ok.content_item_id)
            )
            is False
        )

        workflow_pair = (item_ok, workflow)
        status = processor._dispatch_state(workflow_pair)
        assert status == 0, f"dispatch status was {status}"

        # check that bad item returns status code 1
        item_bad = ContentItem(
            date_id=19000101,
            run_id="run_1c43908277e34803ba7eea51b9054219",
            workspace_id="meedan_test",
            source_id="test_source",
            query_id="test_query_id",
            raw_created_at=datetime.strptime(
                "2023-06-09 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
            ),
            raw_content_id="1298_bad_item",  # need a unique value to avoid update ignore behavior
            raw_content="",
        )
        item_bad = self.store.initialize_item(item_bad)
        self.store.transition_item_state(
            item_bad, workflow.get_state_model().STATE_READY
        )
        workflow_pair = (item_bad, workflow)
        status = processor._dispatch_state(workflow_pair)
        assert status == 1, f"_dispatch status was {status}"

        # that in progress item will return skipped status
        # NOTE: this no longer applies because skipping is handled before dispatching
        # item_skipped = ContentItem(
        #     date_id=19000101,
        #     run_id="run_1c43908277e34803ba7eea51b9054219",
        #     workspace_id="meedan_test",
        #     source_id="test_source",
        #     query_id="test_query_id",
        #     raw_created_at=datetime.strptime(
        #         "2023-06-09 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
        #     ),
        #     raw_content_id="1298_skip_item",  # need a unique value to avoid update ignore behavior
        #     raw_content="This content item will be skipped",
        # )
        # item_skipped = self.store.initialize_item(item_skipped)
        # self.store.transition_item_state(
        #     item_skipped, workflow.get_state_model().STATE_READY
        # )
        # self.store.start_transition_to_state(
        #     item_skipped, workflow.get_state_model().STATE_PLACEHOLDER
        # )
        # workflow_pair = (item_skipped, workflow)
        # status = processor._dispatch_state(workflow_pair)
        # assert status == 2, f"_dispatch status was {status}"

        # set the states to failed to not mess other tests
        self.store.transition_item_state(
            item_bad, workflow.get_state_model().STATE_FAILED
        )
        self.store.transition_item_state(
            item_ok, workflow.get_state_model().STATE_FAILED
        )
        # self.store.transition_item_state(
        #     item_skipped, workflow.get_state_model().STATE_FAILED
        # )

    # patch with a side effect of the real action so tath
    @patch(
        "timpani.conductor.actions.delay.DelayingAction.delay_content_items",
        wraps=DelayingAction().delay_content_items,
    )
    @patch(
        "timpani.conductor.actions.logging.LoggingAction.log_content_item",
        wraps=LoggingAction().log_content_item,
    )
    def test_batch_next_dispatch(
        self,
        mock_logging,
        mock_delay,
    ):
        """
        Confirm that when a workflow indicates a state should be called with a batch,
        it is. Used mocked calls to expected operations measure
        """
        test_size = 10  # this needs to be smaller than batch size to work

        fake_source = FakerTestingContentSource(
            total_items=test_size, page_size=min(test_size, 10)
        )

        run_state = RunState(
            "test_batch",
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

        # need to delete content from previous run in content_store
        self.store.erase_workspace(
            workspace_id=partition.workspace_id, source_id=partition.source_id
        )
        raw_store.delete_partition(partition)

        # load a bunch of fake data into raw store
        fake_source.acquire_new_content(
            self.workspace_cfg, raw_store, run_state=run_state
        )

        processor = ContentProcessor(content_store=self.store)
        processor.process_raw_content(partition)

        # check the number of "read" items (
        in_progress = list(
            self.store.get_items_in_progress(
                workspace_id=partition.workspace_id,
                batch_state=TestContentItemState.STATE_READY,
            )
        )
        assert (
            len(in_progress) == test_size
        ), f"expected {test_size} ready items, not {len(in_progress)}"

        # this runs workflow, but actions are wrapped in patch with side_effect so we can observe
        processor.batch_process_workflows(
            workspace_id=self.workspace_cfg.get_workspace_slug(), max_iterations=15
        )

        # check that action that is not in batch will be call batch_size times with one item
        mock_logging.assert_called()
        assert (
            len(mock_logging.call_args_list) == test_size
        ), f"logging action was called {len(mock_logging.call_args_list)} times"

        # check that action that is supposed to be called in batch will be called with the right number of items
        mock_delay.assert_called()
        assert (
            len(mock_delay.call_args_list[0][1]["content_items"]) == test_size
        ), f"delay action was called with {mock_delay.call_args}"

    def test_cli_args(self):
        """
        Make sure we can run the processing command from the CLI
        (these are also used by application processes)
        TODO: this test will conflict with other test
        """

        # test import via CLI
        try:
            subprocess.run(
                [
                    "python3",
                    "timpani/conductor/process.py",
                    "raw",
                    "--workspace_id=test",
                    "--date_id=20230724",
                    "--source_id=faker_testing",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as error:
            print(error.stdout)
            print(error.stderr)
            raise error

        # test data processing from cli
        try:
            subprocess.run(
                [
                    "python3",
                    "timpani/conductor/process.py",
                    "workflows",
                    "--workspace_id=test",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as error:
            print(error.stdout)
            print(error.stderr)
            raise error

        # summary of content states
        try:
            subprocess.run(
                [
                    "python3",
                    "timpani/conductor/process.py",
                    "summary",
                    "--workspace_id=test",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as error:
            print(error.stdout)
            print(error.stderr)
            raise error

        # test data cleanup from cli
        # (setting date in future to force deletes)
        # NOTE: in this example, the content was published 2023-10-03
        # NOT the same date as the date_id used for import
        try:
            subprocess.run(
                [
                    "python3",
                    "timpani/conductor/process.py",
                    "expired",
                    "--workspace_id=test",
                    "--date_id=20231005",  # expecting all data removed
                ],
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as error:
            print(error.stdout)
            print(error.stderr)
            raise error

        try:
            subprocess.run(
                [
                    "python3",
                    "timpani/conductor/process.py",
                    "clusters",
                    "--workspace_id=test",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as error:
            print(error.stdout)
            print(error.stderr)
            raise error


# TODO: what is the update model if we re-import the same content?
# TODO: test returning multiple content items from single raw item
