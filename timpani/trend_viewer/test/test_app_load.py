import unittest
import requests
import datetime
import random
from timpani.app_cfg import TimpaniAppCfg
from timpani.content_sources.faker_test_content_source import FakerTestingContentSource
from timpani.content_store.content_item import ContentItem
from timpani.content_store.content_store import ContentStore
from timpani.content_store.content_store_manager import ContentStoreManager

# from timpani.content_store.content_store_obj import ContentStoreObject

from streamlit.testing.v1 import AppTest


class TestTrendViewer(unittest.TestCase):
    """
    Confirm that the coductor app's endpoints can be *called*.
    For the most part, it is not checking that appropriate processes
    are run and results are correct.
    NOTE: this test requires that the conductor service is running
    TODO: actually validate return structure or effects, multiple arguments
    TODO: we could validate the UI stuff using a seperate selenium server like Check Web
    """

    app_cfg = TimpaniAppCfg()
    VIEWER_BASE_URL = app_cfg.timpani_trend_viewer_endpoint
    test_items = []
    test_clusters = []
    content_store = None
    NUM_TEST_ITEMS = 1111
    NUM_TEST_CLUSTERS = 25
    SAMPLE_TWEETS = [
        "https://twitter.com/meedan/status/1728036602172035167",
        "https://twitter.com/meedan/status/1724110769443213664",
        "https://twitter.com/PENamerica/status/1724423825301914014",
        "https://twitter.com/meedan/status/1727674739320057958",
    ]

    @classmethod
    def setUpClass(self) -> None:
        assert self.app_cfg.deploy_env_label not in [
            "live",
            "qa",
        ], "Exiting test to avoid modifying live or qa database"
        # ContentStoreObject.metadata.drop_all(bind=engine)
        # ContentStoreObject.metadata.create_all(bind=engine)
        manager = ContentStoreManager()
        manager.destroy_content_store()
        manager.setup_admin_and_content_store()

        self.content_store = ContentStore()
        self.content_store.init_db_engine()

        fakeContent = FakerTestingContentSource(total_items=self.NUM_TEST_ITEMS)
        for i in range(self.NUM_TEST_ITEMS):
            item = ContentItem(
                date_id=19000101,
                run_id="run_1c43908277e34803ba7eea51b9054210",
                workspace_id="test_trend_viewer",
                source_id="faker_testing",
                query_id=f"{self.NUM_TEST_ITEMS}_fake_content_items",
                raw_created_at=datetime.datetime.utcnow(),
                raw_content_id=f"viewer_test_{i}",
                # some random text in multiple languages
                raw_content=fakeContent.get_fake_text(),
                # pretend one published per day
                content_published_date=datetime.datetime.utcnow()
                + datetime.timedelta(days=random.randint(0, 14))  # two week time window
                + datetime.timedelta(hours=random.randint(0, 23)),  # random hours
                content_published_url=random.choice(self.SAMPLE_TWEETS),
                # random language (doesn't match text)
                content_language_code=random.choice(
                    ["es", "en", "zh", "ja", "pt", "or", "hi"]
                ),
            )
            item = self.content_store.initialize_item(item)
            self.test_items.append(item)
        # put items in (nonsensical) clusters
        item_count = 0
        for item in self.test_items:
            # put the first items
            if item_count < self.NUM_TEST_CLUSTERS:
                cluster = self.content_store.cluster_items(item)
                self.test_clusters.append(cluster)
            else:
                # remaining items go in one of the previously created clusters
                # (attempting to make a really skewed distribution)
                self.content_store.cluster_items(
                    item,
                    # self.test_items[item_count % self.NUM_TEST_CLUSTERS],
                    self.test_items[
                        int(random.betavariate(2, 2) * self.NUM_TEST_CLUSTERS)
                    ],
                )
            item_count += 1

    def test_base_route(self):
        """
        app is running and returning  ~200 / endpoint
        """
        test_url = self.VIEWER_BASE_URL
        print(f"testing connection to {test_url}")
        resp = requests.get(test_url)
        assert resp.ok is True, f"response to {test_url} was {resp}"

    def test_healthcheck(self):
        """
        app is running and returning a ~200 on healthcheck
        """
        test_url = self.VIEWER_BASE_URL + "/_stcore/health"
        print(f"testing connection to {test_url}")
        resp = requests.get(test_url)
        assert resp.ok is True, f"response to {test_url} was {resp}"

    def test_status(self):
        """
        app is running and returning a ~200 on /workspace_status
        """
        test_url = self.VIEWER_BASE_URL + "/workspace_status"
        print(f"testing connection to {test_url}")
        resp = requests.get(test_url)
        assert resp.ok is True, f"response to {test_url} was {resp}"

    def test_pages_load(self):
        """
        app is running and returning a ~200 on core pages
        """
        page_urls = ["/explore_content", "/explore_clusters", "/explore_keywords"]
        for page in page_urls:
            test_url = self.VIEWER_BASE_URL + page
            print(f"testing connection to {test_url}")
            resp = requests.get(test_url)
            assert resp.ok is True, f"response to {test_url} was {resp}"

    def test_app_structure(self):
        """
        Test app structure using streamlit utils
        https://docs.streamlit.io/library/api-reference/app-testing
        """
        at = AppTest.from_file("../trend_viewer_app.py")
        at.run()
        assert not at.exception, f"Error loading trend_viewer_app.py {at.exception}"

        assert at.get(element_type="dataframe") is not None
        assert at.get(element_type="bar_chart") is not None
