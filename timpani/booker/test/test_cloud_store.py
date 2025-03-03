import unittest
import json
from datetime import datetime
from timpani.app_cfg import TimpaniAppCfg
from timpani.raw_store.store import Store
from timpani.raw_store.cloud_store import CloudStore
from timpani.raw_store.item import Item
from timpani.util.run_state import RunState


class TestCloudStore(unittest.TestCase):
    # use this instead of the normal S3_STORE_LOCATION from environment
    AWS_S3_TEST_STORE_LOCATION = "s3.amazonaws.com"

    # NOTE: not actually using the minio config because API slightly different
    MINIO_S3_TEST_STORE_LOCATION = "http://minio:9002"

    @classmethod
    def setUpClass(cls):
        cls.app_cfg = TimpaniAppCfg()

        # load in a large example json blob from test file
        with open("timpani/booker/test/test_item_contents_1.json") as json_file_1:
            cls.test_content_1 = json.load(
                json_file_1,
                strict=False,  # because there are \n and \t
            )

        cls.test_content_2 = json.loads(
            """
            {
                "data": "I'm a json blob with no ids, add strange things to me to break parsers"
            }
            """,
            strict=False,  # because there are \n and \t
        )

        cls.test_partition_id = Store.Partition("testteam", "testsource", "20230501")

    def _minio_local_login(self):
        mi = CloudStore(store_location=self.MINIO_S3_TEST_STORE_LOCATION)
        mi.login_and_validate(
            access_key=self.app_cfg.minio_user,
            secret_key=self.app_cfg.minio_password,
        )
        return mi

    # don't run these tests in the CI environment because it is
    # fire walled from AWS resources
    @unittest.skipIf(
        TimpaniAppCfg().deploy_env_label in ["test", "live"],
        "integration tests not run in CI test environment",
    )
    def test_aws_s3_local_login(self):
        # TODO: remove env hardcoding if other aws buckets created
        mi = CloudStore(self.AWS_S3_TEST_STORE_LOCATION, "timpani-raw-store-qa")
        mi.login_and_validate()

    def test_minio_s3_local_login(self):
        self._minio_local_login()

    def _local_append_and_fetch_chunk(self, store: Store):
        """
        Called with multiple CloudStore configurations to check
        that they support the same operations
        """
        test_item_1 = Item(
            run_id="testrun",
            workspace_id="testteam",
            source_id="testsource",
            query_id="testquery",
            content_id="118550062",
            page_id=0,
            content=self.test_content_1,
        )
        test_item_2 = Item(
            run_id="testrun",
            workspace_id="testteam",
            source_id="testsource",
            query_id="testquery",
            page_id=None,
            content_id=None,  # missing
            content=self.test_content_2,
        )
        payload = [test_item_1, test_item_2]
        obj_name = store.append_chunk(
            partition_id=self.test_partition_id, payload=payload
        )
        assert obj_name is not None

        # try to fetch it back and confirm text unchanged
        raw_obj = store.fetch_chunk(object_name=obj_name)
        # parse json lines
        rows = raw_obj.split("\n")

        assert rows[0] == test_item_1.toJSON()
        # parse content and validate that the string representation
        # and encodings are correct
        parsed_obj_1 = json.loads(rows[0], strict=False)
        assert parsed_obj_1["content"] == self.test_content_1
        # check that the metadata is there
        assert parsed_obj_1["workspace_id"] == "testteam"

        parsed_obj_2 = json.loads(rows[1], strict=False)
        assert parsed_obj_2["content"] == self.test_content_2
        # check that the metadata is there
        assert parsed_obj_2["workspace_id"] == "testteam"
        # check that we put a uuid in for missing content id
        assert parsed_obj_2["content_id"] is not None

        # check that creation time stamp format ok for Hive timestamp
        # https://docs.aws.amazon.com/athena/latest/ug/data-types.html
        created = datetime.strptime(parsed_obj_2["created_at"], "%Y-%m-%d %H:%M:%S.%f")
        assert type(created) is datetime

        # check reconstruting an item
        test_item_1b = Item.fromJSON(rows[0])
        assert test_item_1.content_id == test_item_1b.content_id
        assert test_item_1.created_at == test_item_1b.created_at
        test_item_2b = Item.fromJSON(rows[1])
        assert test_item_2.content_id == test_item_2b.content_id
        assert test_item_2.created_at == test_item_2b.created_at

        # check fetching chunks, expecting one chunk with two items in it
        chunks = list(store.fetch_chunks_in_partition(self.test_partition_id))
        assert len(chunks) == 1, f"expected one chunk in partition found {len(chunks)}"
        assert len(chunks[0].splitlines()) == 2

    def test_minio_local_append_and_fetch_chunk(self):
        """
        Test object store and retrevial for local minio S3 configuration
        Minio client must have necessary buckets, created by docker compose
        """
        # TODO: test should run in different minio bucket and probably should cleanup
        # afterwards
        cs = self._minio_local_login()
        # need to delete any content from previous test run in raw_store
        cs.delete_partition(self.test_partition_id)
        self._local_append_and_fetch_chunk(cs)

    # don't run these tests in the CI environment because it is
    # fire walled from AWS resources
    @unittest.skipIf(
        TimpaniAppCfg().deploy_env_label in ["test", "live"],
        "integration tests not run in CI test environment",
    )
    def test_aws_s3_append_and_fetch_chunk(self):
        """
        Test object store and retrevial for AWS S3 configuration
        """
        # TODO: change to test bucket after created
        cs = CloudStore(self.AWS_S3_TEST_STORE_LOCATION, "timpani-raw-store-qa")
        cs.login_and_validate()
        # need to delete any content from previous test run in raw_store
        cs.delete_partition(self.test_partition_id)
        self._local_append_and_fetch_chunk(cs)

    # don't run these tests in the CI environment because it is
    # fire walled from AWS resources
    @unittest.skipIf(
        TimpaniAppCfg().deploy_env_label in ["test", "live"],
        "integration tests not run in CI test environment",
    )
    def test_aws_s3_record_partition_state(self):
        """
        make sure partition state writes out correctly
        """
        cs = CloudStore(self.AWS_S3_TEST_STORE_LOCATION, "timpani-raw-store-qa")
        cs.login_and_validate()
        # TODO: need to delete any content from previous test run in raw_store?
        state = RunState("test_run")
        cs.record_partition_run_state(state, self.test_partition_id)
        state.transitionTo(state.STATE_RUNNING)
        cs.record_partition_run_state(state, self.test_partition_id)
        state.transitionTo(state.STATE_COMPLETED)
        cs.record_partition_run_state(state, self.test_partition_id)
