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
from timpani.processing_sequences.classy_india_election_workflow import (
    ClassyIndiaElectionWorkflow,
)
from timpani.raw_store.cloud_store import CloudStore
from timpani.util.run_state import RunState
from timpani.conductor.process import ContentProcessor

from timpani.model_service.classycat_wrapper import ClassycatWrapper

import logging

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)


class TestWorkflows(unittest.TestCase):
    """
    Tests extracting and running a single content item example through each workflow
    NOTE:  tests will not run in CI because no classycat service availible
    * THESE TESTS REQUIRE THAT THE CONDUCTOR IS RUNNING
    * THESE TESTS REQUIRE THAT THE CLASSYCAT SERVICE IS RUNNING

    """

    app_cfg = TimpaniAppCfg()

    MINIO_S3_TEST_STORE_LOCATION = "http://minio:9002"

    # TODO: need a real location to store this, private org github?
    INDIA_ELECTION_SCHEMA_FILE = "/usr/src/app/timpani/model_service/classycat_schemas/indian_election_schema.json"

    # test content for junkipedia workflows
    # TODO: need to change this to a test file that has appropriate election content
    with open("timpani/booker/test/test_item_contents_Telegram.json") as json_file_2:
        workflow_test_content_1 = json.load(
            json_file_2,
            strict=False,  # because there are \n and \t
        )

    # telegram test content for meedan_india_election
    test_item_6 = Item(
        run_id="testrun",
        workspace_id="meedan_india_election",
        source_id="junkipedia",
        query_id="telegram",
        page_id=None,
        content_id="20450777",
        content=workflow_test_content_1,
    )

    @classmethod
    def setUpClass(self):
        cfg = TimpaniAppCfg()

        # don't run in prod env?
        assert cfg.deploy_env_label not in [
            "live",
            "qa",
        ], "Exiting test to avoid modifying live or qa database"
        manager = ContentStoreManager()
        manager.destroy_content_store()
        manager.setup_admin_and_content_store()

        self.store = ContentStore()
        self.engine = self.store.init_db_engine()

        # need to reset vector database
        self.vector_store = AlegreVectorStoreService()

    @unittest.skipIf(
        TimpaniAppCfg().deploy_env_label in ["test", "live"],
        "integration tests not run in CI test environment",
    )
    def test_india_election_workflow(self):
        """
        Test processing single item in IndiaElection workflow with classycat
        """
        classycat = ClassycatWrapper()

        # have to initialize the appropriate schema or else
        # "message": "Schema name 2024 Indian Election does not exist"
        schema_id = classycat.get_schema_id(
            schema_name=ClassyIndiaElectionWorkflow.CLASSY_SCHEMA
        )
        if schema_id is None:
            print(
                f"Loading schema file for test from {self.INDIA_ELECTION_SCHEMA_FILE}"
            )
            with open(self.INDIA_ELECTION_SCHEMA_FILE) as schema_file:
                india_schema = json.load(
                    schema_file,
                    strict=False,  # because there are \n and \t
                )
                schema_id = classycat.create_schema(india_schema)

        workflow = ClassyIndiaElectionWorkflow(self.store)
        # TODO: update with election content
        test_content = self.test_item_6
        # NOTE: this will wipe the workspace
        self.vector_store.discard_workspace(test_content.workspace_id)

        self._test_get_state_model(workflow)
        election_items = [
            self.test_item_6,
        ]
        for test_content in election_items:
            self._test_extract_transform(workflow, test_content)

        self._test_processing_state_transitions(election_items)

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
            content_store.get_items_in_progress(
                workspace_id=test_raw_items[0].workspace_id
            )
        )
        assert (
            len(in_progress) > 0
        ), f"expected some in progress items, not {len(in_progress)}"

        # this runs simple workflow that doesn't use any services that might fail in CI
        processor.batch_process_workflows(workspace_id=test_raw_items[0].workspace_id)

        # all of the items should be done, none in progress
        in_progress = list(
            content_store.get_items_in_progress(
                workspace_id=test_raw_items[0].workspace_id
            )
        )
        assert (
            len(in_progress) == 0
        ), f"expected 0 in progress items, not {len(in_progress)}"

        # check the expected summary of states
        states = processor.process_summary(workspace_id=test_raw_items[0].workspace_id)

        # check that keyword classifications were generated

        assert states["completed"] >= 1, f"states are {states}"
