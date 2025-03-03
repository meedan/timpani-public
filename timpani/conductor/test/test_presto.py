import unittest
from unittest.mock import patch
from datetime import datetime


from timpani.content_store.content_item import ContentItem
from timpani.content_store.content_store import ContentStore
from timpani.content_store.content_store_manager import ContentStoreManager

from timpani.model_service.presto_wrapper_service import PrestoWrapperService
from timpani.model_service.yake_presto_service import YakePrestoService

from timpani.app_cfg import TimpaniAppCfg


class TestPrestoWrappers(unittest.TestCase):
    """
    NOTE:  this is testing with Yake, which is a real model.  Feel like this should use a dummy testing model
    TODO: When starting Presto locally to run tests, need to run the presto app and yake model from the presto docker compose.yml
        cd /check/presto  docker compose up app yake
    """

    cfg = TimpaniAppCfg()

    @classmethod
    def setUpClass(self):
        # define all the new tables
        # don't run in prod env?
        assert self.cfg.deploy_env_label not in [
            "live",
            "qa",
        ], "Exiting test to avoid modifying live or qa database"
        manager = ContentStoreManager()
        manager.destroy_content_store()
        manager.setup_admin_and_content_store()

        # create a content store
        self.content_store = ContentStore()
        self.content_store.init_db_engine()

        # create a test item and cache it to the db
        self.item1 = ContentItem(
            date_id=19000101,
            run_id="run_1c43908277e34803ba7eea51b9054219",
            workspace_id="meedan_test_presto_wrapper",
            source_id="test_source",
            query_id="test_query_id",
            raw_created_at=datetime.strptime(
                "2023-06-09 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
            ),
            raw_content_id="129839388abc",
            raw_content="This is a test of the presto wrapper for model access",
        )
        self.item1 = self.content_store.initialize_item(self.item1)

    # don't run these tests in the CI environment because it doesn't talk
    # to full service
    @unittest.skipIf(
        TimpaniAppCfg().deploy_env_label in ["test", "live"],
        "integration tests not run in CI test environment",
    )
    def test_presto_endpoint_submit_real(self):
        """
        Confirm submitting request to presto (for yake)
        """
        presto = YakePrestoService()
        presto.submit_to_presto_model(self.item1, target_state="ready")

        #  query to check that keywords created as expect
        keywords = self.content_store.get_keywords_for_item(self.item1)
        for keyword in keywords:
            assert keyword.keyword_text in [
                "presto wrapper",
                "model access",
                "test",
                "wrapper for model",
            ]

    # TODO: how can we test error response callback? need to be able to trigger model error

    @patch("requests.post")
    def test_presto_endpoint_submit_mock(self, mock_post):
        """
        Confirm submitting request to presto (for yake) validate structure with mock
        """
        expected_request = {
            "id": "timpani_1",
            "content_hash": None,
            "callback_url": PrestoWrapperService.CALLBACK_URL,
            "url": None,
            "text": "This is a test of the presto wrapper for model access",
            "raw": {
                "content_item_id": 1,
                "target_state": "ready",
                "workspace_id": "meedan_test_presto_wrapper",
            },
            "parameters": {},
        }
        presto = YakePrestoService()
        presto.submit_to_presto_model([self.item1], target_state="ready")
        mock_post.assert_called_once()
        assert (
            mock_post.call_args.kwargs["json"] == expected_request
        ), f"call args json was {mock_post.call_args.kwargs['json']}"

    @patch("requests.post")
    def test_presto_size_error(self, mock_post):
        """
        Confirm submitting request to presto (for yake) validate structure with mock
        """
        big_item = ContentItem(
            date_id=19000101,
            run_id="run_1c43908277e34803ba7eea51b9054219",
            workspace_id="meedan_test_presto_wrapper",
            source_id="test_source",
            query_id="test_query_id",
            raw_created_at=datetime.strptime(
                "2023-06-09 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
            ),
            raw_content_id="999abc",
            # NOTE: this next should be bigger than max allowable size
            raw_content="x" * PrestoWrapperService.MAX_SIZE_BYTES,
        )
        big_item = self.content_store.initialize_item(big_item)

        presto = YakePrestoService()
        self.assertRaises(
            AssertionError,
            presto.submit_to_presto_model,
            content_items=[big_item],
            target_state="ready",
        )

    def test_presto_callback(self):
        """
        test with expected response structure
        """
        expected_response = {
            "body": {
                "id": "timpani_1",
                "content_hash": None,
                "callback_url": "http://host.docker.internal:3101/presto_model",
                "url": None,
                "text": "This is a test of the presto wrapper for model access",
                "raw": {
                    "content_item_id": 1,
                    "target_state": "ready",
                    "workspace_id": "meedan_test_presto_wrapper",
                },
                "parameters": {},
                "result": {
                    "keywords": [
                        ["presto wrapper", 0.0020211625251083634],
                        ["model access", 0.0020211625251083634],
                        ["test", 0.04491197687864554],
                        ["wrapper for model", 0.04702391380834952],
                    ]
                },
            },
            "model_name": "yake_keywords.Model",
            "retry_count": 0,
        }

        expected_model_result = {
            "keywords": [
                ["presto wrapper", 0.0020211625251083634],
                ["model access", 0.0020211625251083634],
                ["test", 0.04491197687864554],
                ["wrapper for model", 0.04702391380834952],
            ]
        }
        # call the function the processes the response
        (
            presto_model_name,
            workspace_id,
            content_item_id,
            target_state,
            result_payload,
        ) = PrestoWrapperService.parse_presto_response(expected_response)
        assert presto_model_name == "yake_keywords.Model"
        assert workspace_id == self.item1.workspace_id
        assert content_item_id == self.item1.content_item_id
        assert target_state == "ready"
        assert result_payload == expected_model_result

    def test_presto_error_callback(self):
        """
        test expected response structure for async error repy
        NOTE: DOES NOT CHECK CONTRACT WITH PRESTO
        """
        presto = PrestoWrapperService()
        expected_error_payload = {
            "error": "what sort of error goes here",
            "error_details": "it was a very bad error",
            "error_code": 500,
        }
        expected_response = {
            "body": {
                "id": "timpani_1",
                "content_hash": None,
                "callback_url": "http://host.docker.internal:3101/presto_model",
                "url": None,
                "text": "This is a test of the presto wrapper for model access",
                "raw": {
                    "content_item_id": 1,
                    "target_state": "ready",
                    "workspace_id": "meedan_test_presto_wrapper",
                },
                "parameters": {},
                "result": expected_error_payload,
            },
            "model_name": "yake_keywords.Model",
            "retry_count": 0,
        }

        # call the function the processes the response
        (
            presto_model_name,
            workspace_id,
            content_item_id,
            target_state,
            result_payload,
        ) = PrestoWrapperService.parse_presto_response(expected_response)
        assert presto_model_name == "yake_keywords.Model"
        assert workspace_id == self.item1.workspace_id
        assert content_item_id == self.item1.content_item_id
        assert target_state == "ready"
        assert result_payload == expected_error_payload

        error_code, error, error_details = presto.parse_presto_error_response(
            result_payload
        )
        assert error_code == expected_error_payload["error_code"]
        assert error == expected_error_payload["error"]
        assert error_details == expected_error_payload["error_details"]
