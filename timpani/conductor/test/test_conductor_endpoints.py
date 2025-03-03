import unittest
import requests
from timpani.app_cfg import TimpaniAppCfg


class TestConductorEndpoints(unittest.TestCase):
    """
    Confirm that the coductor app's endpoints can be *called*.
    For the most part, it is not checking that appropriate processes
    are run and results are correct.
    NOTE: this test requires that the conductor service is running
    TODO: actually validate return structure or effects, multiple arguments
    """

    app_cfg = TimpaniAppCfg()
    CONDUCTOR_BASE_URL = app_cfg.timpani_conductor_api_endpoint

    def test_base_route(self):
        """
        app is running and returning  ~200 / endpoint
        """
        test_url = self.CONDUCTOR_BASE_URL
        print(f"testing connection to {test_url}")
        resp = requests.get(test_url)
        assert resp.ok is True, f"response to {test_url} was {resp}"

    def test_healthcheck(self):
        """
        app is running and returning a ~200 on /healthcheck
        """
        test_url = self.CONDUCTOR_BASE_URL + "/healthcheck"
        print(f"testing connection to {test_url}")
        resp = requests.get(test_url)
        assert resp.ok is True, f"response to {test_url} was {resp}"

    def test_status_summary(self):
        """
        can return an array which would contain status states
        """
        test_url = self.CONDUCTOR_BASE_URL + "/status_summary"
        print(f"testing connection to {test_url}")
        resp = requests.get(test_url)
        payload = resp.json()
        print(payload)
        assert resp.ok is True, f"response to {test_url} was {resp}"

    def test_process_summary(self):
        """
        can return a dict with status and pid of running processes
        """
        test_url = self.CONDUCTOR_BASE_URL + "/running_processes"
        print(f"testing connection to {test_url}")
        resp = requests.get(test_url)
        payload = resp.text
        print(payload)
        assert resp.ok is True, f"response to {test_url} was {resp}"

    def test_start_workflow_processing(self):
        """
        can respond appropriately to (invalid) workflow id
        """
        workspace_id = "test"
        test_url = self.CONDUCTOR_BASE_URL + f"/start_workflow/{workspace_id}"
        print(f"testing connection to {test_url}")
        resp = requests.get(test_url)
        payload = resp.text
        print(payload)
        assert resp.ok is True, f"response to {test_url} was {resp}"

    def test_stop_workflow_processing(self):
        """
        can respond appropriately to (invalid) workflow id stop command
        """
        workspace_id = "test"
        test_url = self.CONDUCTOR_BASE_URL + f"/stop_workflow/{workspace_id}"
        print(f"testing connection to {test_url}")
        resp = requests.get(test_url)
        payload = resp.text
        print(payload)
        assert resp.ok is True, f"response to {test_url} was {resp}"

    def test_start_expiration_processing(self):
        """
        can respond appropriately to (invalid) expiration id
        """
        workspace_id = "test"
        test_url = self.CONDUCTOR_BASE_URL + f"/start_expiration/{workspace_id}"
        print(f"testing connection to {test_url}")
        resp = requests.get(test_url)
        payload = resp.text
        print(payload)
        assert resp.ok is True, f"response to {test_url} was {resp}"

    def test_stop_expiration_processing(self):
        """
        can respond appropriately to (invalid) expiration id stop command
        """
        workspace_id = "test"
        test_url = self.CONDUCTOR_BASE_URL + f"/stop_expiration/{workspace_id}"
        print(f"testing connection to {test_url}")
        resp = requests.get(test_url)
        payload = resp.text
        print(payload)
        assert resp.ok is True, f"response to {test_url} was {resp}"

    def test_start_import_content_processing(self):
        """
        can respond appropriately to workspace import command with args
        """
        workspace_id = "test"
        source_id = "faker_testing"
        test_url = (
            self.CONDUCTOR_BASE_URL
            + f"/import_content/{workspace_id}/{source_id}/20210101"
        )
        print(f"testing connection to {test_url}")
        resp = requests.get(test_url)
        payload = resp.text
        print(payload)
        assert resp.ok is True, f"response to {test_url} was {resp}"

    def test_clustering_action_callback(self):
        """
        can respond appropriately to workspace import command with args
        """
        item_id = "2"
        action_id = "alegre_clustering"
        target_state = "failed"  # this would be "clustered", but that will make it fail
        # example payload from Alegre
        payload = {
            "result": [
                {
                    "_index": "alegre_similarity",
                    "_type": "_doc",
                    "id": "2",
                    "score": 2.0,
                    "_source": {
                        "context": {
                            "type": "timpani_content_item_text",
                            "workspace_id": "meedan_test",
                        },
                        "content": "This is another test item of the Alegre wrapper for vectorization",
                        "created_at": "2023-10-10T10:39:32.296733",
                        "contexts": [
                            {
                                "type": "timpani_content_item_text",
                                "workspace_id": "meedan_test",
                            }
                        ],
                        "model": "paraphrase-multilingual-mpnet-base-v2",
                        "model_paraphrase-multilingual-mpnet-base-v2": 1,
                    },
                },
                {
                    "_index": "alegre_similarity",
                    "_type": "_doc",
                    "id": "1",
                    "score": 1.9058058,
                    "_source": {
                        "context": {
                            "type": "timpani_content_item_text",
                            "workspace_id": "meedan_test",
                        },
                        "content": "This is a test of the Alegre wrapper for vectorization",
                        "created_at": "2023-10-10T10:39:29.998854",
                        "contexts": [
                            {
                                "type": "timpani_content_item_text",
                                "workspace_id": "meedan_test",
                            }
                        ],
                        "model": "paraphrase-multilingual-mpnet-base-v2",
                        "model_paraphrase-multilingual-mpnet-base-v2": 1,
                    },
                },
            ]
        }
        test_url = (
            self.CONDUCTOR_BASE_URL
            + f"/cluster_item/{item_id}/{action_id}/{target_state}"
        )
        print(f"testing connection to {test_url}")
        resp = requests.post(test_url, json=payload)
        result = resp.text
        print(result)
        assert resp.ok is True, f"response to {test_url} was {resp}"

    def test_backfill_processing(self):
        """
        Can trigger backfill update process
        """
        workspace_id = "test"
        test_url = (
            self.CONDUCTOR_BASE_URL
            + f"/start_backfill/{workspace_id}?start_date_id=20240228&end_date_id=20240228"
        )
        print(f"testing connection to {test_url}")
        resp = requests.get(test_url)
        payload = resp.text
        print(payload)
        assert resp.ok is True, f"response to {test_url} was {resp}"

    def test_start_cluster_updates(self):
        """
        Can trigger clustering updating process
        """
        workspace_id = "test"
        test_url = self.CONDUCTOR_BASE_URL + f"/start_cluster_updates/{workspace_id}"
        print(f"testing connection to {test_url}")
        resp = requests.get(test_url)
        payload = resp.text
        print(payload)
        assert resp.ok is True, f"response to {test_url} was {resp}"
