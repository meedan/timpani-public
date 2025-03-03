import unittest
import datetime
import requests

from timpani.workspace_config.test_workspace_cfg import TestWorkspaceConfig
from timpani.content_sources.junkipedia_content_source import JunkipediaContentSource
from timpani.raw_store.debugging_file_store import DebuggingFileStore
from timpani.util.run_state import RunState
from timpani.app_cfg import TimpaniAppCfg


class TestJunkipedia(unittest.TestCase):
    """
    *Live* tests of junkipedia API. These are really more like
    integration tests than unit tests
    """

    # don't run these tests in the CI environment because it doesn't talk
    # to full aws resources
    @unittest.skipIf(
        TimpaniAppCfg().deploy_env_label in ["test", "live"],
        "integration tests not run in CI test environment",
    )
    def test_junkipedia_api_call(self):
        """
        Issues a live call to junkipedia API to make sure
        it is up
        """
        response = requests.get(JunkipediaContentSource.JUNKIPEDIA_API_BASE_URL)
        assert response.ok

    @unittest.skipIf(
        TimpaniAppCfg().deploy_env_label in ["test", "live"],
        "integration tests not run in CI test environment",
    )
    def test_junkipedia_acquistion(self):
        jnk = JunkipediaContentSource()
        workspace_cfg = TestWorkspaceConfig()
        store = DebuggingFileStore()
        jnk.acquire_new_content(workspace_cfg, store, RunState("test_acquire"))
        # TODO: confirm that content written to store

    @unittest.skipIf(
        TimpaniAppCfg().deploy_env_label in ["test", "live"],
        "integration tests not run in CI test environment",
    )
    def test_junkipedia_daterange_acquistion(self):
        """
        Attempts to get content only published in last two days
        NOTE: tests that the daterange paramters don't fail,
        but does not prove that they work
        """
        now = datetime.datetime.now()
        two_days_ago = now - datetime.timedelta(days=2)
        jnk = JunkipediaContentSource()
        workspace_cfg = TestWorkspaceConfig()
        store = DebuggingFileStore()
        run_state = RunState("test_acquire")
        run_state.time_range_start = two_days_ago
        run_state.time_range_end = now
        jnk.acquire_new_content(workspace_cfg, store, run_state)
        # TODO: confirm that content written to store has appropriate range

    @unittest.skipIf(
        TimpaniAppCfg().deploy_env_label in ["test", "live"],
        "integration tests not run in CI test environment",
    )
    def test_junkipedia_bad_auth_key(self):
        """
        Confirms that getting a 403 will raise exception instead of writing data
        """
        test_url = "https://www.junkipedia.org/api/v1/posts?lists=5863&per_page=1&published_at_from=1684074147&published_at_to=1684246947"
        jnk = JunkipediaContentSource()
        # we din't include api key in header, so expecting
        # expecting 403 Client Error: Forbidden for url:
        with self.assertRaises(requests.exceptions.HTTPError):
            list(  # for un-yielding
                jnk.process_junkipedia_query_url(
                    workspace_id="test",
                    query_id=None,
                    query_url=test_url,
                    api_secret_key="too_many_secrets",
                )
            )


if __name__ == "__main__":
    unittest.main()
