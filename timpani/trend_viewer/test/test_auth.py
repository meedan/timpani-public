import unittest
from collections import namedtuple
import json
from datetime import datetime
from unittest.mock import patch

from timpani.app_cfg import TimpaniAppCfg
from timpani.content_store.content_store_manager import ContentStoreManager
from timpani.content_store.content_item import ContentItem
from timpani.content_store.content_store import ContentStore
from timpani.trend_viewer.data_model import ViewerDataModel


class TestAuth(unittest.TestCase):
    """
    Confirm that Check authentication and permissions work
    TODO: this isn't able to check that auth works, but at least can check that blocking access works
    """

    app_cfg = TimpaniAppCfg()
    model = ViewerDataModel()

    # named tuple for use in mocking responses
    Response = namedtuple("Response", "ok text")

    @classmethod
    def setUpClass(self):
        # Need to set up content store to inject an item in the DB
        # cfg = TimpaniAppCfg()

        # don't run in prod env?
        # assert cfg.deploy_env_label not in [
        #    "live",
        #    "qa",
        # ], "Exiting test to avoid modifying live or qa database"
        # ContentStoreObject.metadata.drop_all(bind=self.engine)
        # define all the new tables
        # ContentStoreObject.metadata.create_all(bind=self.engine)
        manager = ContentStoreManager()
        manager.destroy_content_store()
        manager.setup_admin_and_content_store()
        self.store = ContentStore()
        self.engine = self.store.init_db_engine()

    def test_public_workspaces_reachable_no_auth(self):
        """
        Should always be able to see public workspace
        """
        # meedan is a public workspaces

        assert "meedan" in self.model.get_acessible_workspaces()

    def test_private_workspaces_not_reachable_no_auth(self):
        """
        Should not be able to see private workspace with no auth
        """
        # need to add an item of content to a workspace to know if it is reachable

        # 'test' workspace is not marked as dev-public
        authed = self.model.get_acessible_workspaces()
        assert "test" not in authed, f"authed workspaces: {authed}"

    @patch("requests.post")
    def test_private_workspaces_reachable_good_auth(self, mock_response):
        """
        Should be able to see private workspace with apropriately secret
        """
        # mocked the response we would expect from the check-api
        # if auth was sucessful

        response = {
            "data": {
                "me": {
                    "teams": {
                        "edges": [
                            {
                                "node": {
                                    "id": "VGVhbS81",
                                    "dbid": 5,
                                    "name": "Check Demo (On-Prem)",
                                    "slug": "check-testing",
                                }
                            },
                        ]
                    }
                }
            },
        }
        reponse_text = json.dumps(response)

        mock_response.return_value = self.Response(True, reponse_text)

        # 'test' workspace is not marked as dev-public
        # 'dev' is a default secret login key in dev
        authed = self.model.get_acessible_workspaces(auth_session_secret="dev")
        assert "test" in authed, f"authed workspaces: {authed}"

    def test_private_workspaces_not_reachable_bad_auth(self):
        """
        Shoud not be able to see a private worksapce with wrong secret
        """
        # need to add an item of content to a workspace to know if it is reachable

        # 'test' workspace is not marked as dev-public
        # 'dev' is a default secret login key in dev
        authed = self.model.get_acessible_workspaces(
            auth_session_secret="incorrect_secret"
        )
        assert "test" not in authed, f"authed workspaces: {authed}"

    def test_set_workspace_unathorize_fails(self):
        """
        set workspace to unauthorized workspace should fail
        """
        # should raise an exception
        self.assertRaises(AssertionError, self.model.set_workspace, "test")

    def test_set_workspace_unathorize_bad_secret_fails(self):
        """
        set workspace to unauthorized workspace should fail
        """
        # should raise an exception
        self.assertRaises(
            AssertionError,
            self.model.set_workspace,
            "test",
            auth_session_secret="incorrect_secret",
        )

    @patch("requests.post")
    def test_set_workspace_unathorize_good_secret_works(self, mock_response):
        """
        setting workspace to protected value with secret should work
        """
        # mocked the response we would expect from the check-api
        # if auth was sucessful

        response = {
            "data": {
                "me": {
                    "teams": {
                        "edges": [
                            {
                                "node": {
                                    "id": "VGVhbS81",
                                    "dbid": 5,
                                    "name": "Check Demo (On-Prem)",
                                    "slug": "check-testing",
                                }
                            },
                        ]
                    }
                }
            },
        }
        reponse_text = json.dumps(response)

        mock_response.return_value = self.Response(True, reponse_text)
        self.model.set_workspace("test", auth_session_secret="dev")

    def test_set_workspace_authorized_works(self):
        """
        set workspace to authorized workspace should work
        """
        self.model.set_workspace("meedan")

    @patch("timpani.app_cfg.TimpaniAppCfg.deploy_env_label", "live")
    def test_dev_public_workspaces_not_reachable_in_live(self):
        """
        Should be unable to see dev_public workspace when running in 'live'
        """
        # 'meedan_tse' workspace is marked as dev_public
        authed = self.model.get_acessible_workspaces()
        assert "meedan_tse" not in authed, f"authed workspaces: {authed}"

    @patch("timpani.app_cfg.TimpaniAppCfg.deploy_env_label", "dev")
    def test_dev_public_workspaces_reachable_in_dev(self):
        """
        Should be able to see dev_public workspace when running in dev
        """
        authed = self.model.get_acessible_workspaces()
        assert "meedan_tse" in authed, f"authed workspaces: {authed}"

    @patch("timpani.app_cfg.TimpaniAppCfg.timpani_auth_mode", None)
    def test_disable_auth(self):
        """
        Disabling auth should just return workspacs in db: only test data
        """
        # need to create an item in the db with a workspace
        self.store.initialize_item(
            ContentItem(
                date_id=19000101,
                run_id="run_1c43908277e34803ba7eea51b9054219",
                workspace_id="meedan_test",  # <-- this is the id to search for
                source_id="test_source",
                query_id="test_keywords",
                raw_created_at=datetime.strptime(
                    "2023-06-09 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
                ),
                raw_content_id="129839388abc",
                raw_content="Noise is the new censorship",
                # skipping auto set fields
                content_published_date=datetime.strptime("2023-06-12", "%Y-%m-%d"),
                content_published_url="http://somesite.com",
            )
        )

        authed = self.model.get_acessible_workspaces()
        assert "meedan_test" in authed, f"authed workspaces: {authed}"
