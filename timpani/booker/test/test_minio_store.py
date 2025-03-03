import unittest
import json
from timpani.app_cfg import TimpaniAppCfg
from timpani.raw_store.store import Store
from timpani.raw_store.minio_store import MinioStore
from timpani.raw_store.item import Item
from timpani.util.run_state import RunState


class TestMinioStore(unittest.TestCase):
    # load in a large example json blob from test file
    with open("timpani/booker/test/test_item_contents_1.json") as json_file_1:
        test_content_1 = json.load(
            json_file_1,
            strict=False,  # because there are \n and \t
        )
    app_cfg = TimpaniAppCfg()

    test_content_2 = json.loads(
        """
        {
            "data": "I'm a json blob with no ids, add strange things to me to break parsers"
        }
        """,
        strict=False,  # because there are \n and \t
    )

    test_partition_id = Store.Partition("testteam", "testsource", "20230501")

    def test_minio_local_login(self):
        mi = MinioStore()
        mi.login_and_validate(
            access_key=self.app_cfg.minio_user,
            secret_key=self.app_cfg.minio_password,
        )

    def test_minio_local_append_and_fetch_chunk(self):
        # TODO: test should run in different minio bucket and probably should cleanup
        # afterwards
        mi = MinioStore()
        mi.login_and_validate(
            access_key=self.app_cfg.minio_user,
            secret_key=self.app_cfg.minio_password,
        )
        test_item_1 = Item(
            run_id="testrun",
            workspace_id="testteam",
            source_id="testsource",
            query_id="testquery",
            page_id=None,
            content_id="118550062",
            content=self.test_content_1,
        )
        test_item_2 = Item(
            run_id="testrun",
            workspace_id="testteam",
            source_id="testsource",
            query_id="testquery",
            page_id=0,
            content_id=None,  # missing
            content=self.test_content_2,
        )
        payload = [test_item_1, test_item_2]
        obj_name = mi.append_chunk(partition_id=self.test_partition_id, payload=payload)
        assert obj_name is not None

        # try to fetch it back and confirm text unchanged
        raw_obj = mi.fetch_chunk(object_name=obj_name)
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

    def test_record_partition_state(self):
        """
        make sure partition state writes out correctly
        """
        mi = MinioStore()
        mi.login_and_validate(
            access_key=self.app_cfg.minio_user,
            secret_key=self.app_cfg.minio_password,
        )
        state = RunState("test_run")
        mi.record_partition_run_state(state, self.test_partition_id)
        state.transitionTo(state.STATE_RUNNING)
        mi.record_partition_run_state(state, self.test_partition_id)
        state.transitionTo(state.STATE_COMPLETED)
        mi.record_partition_run_state(state, self.test_partition_id)
