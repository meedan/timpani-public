import unittest
from datetime import datetime

from timpani.conductor.actions.hashtag_keywords import BasicKeywordsExtrator, ScoredTag
from timpani.app_cfg import TimpaniAppCfg
from timpani.content_store.content_store_manager import ContentStoreManager

from timpani.content_store.content_item import ContentItem
from timpani.content_store.item_state_model import ContentItemState
from timpani.content_store.content_store import ContentStore


class TestHashtags(unittest.TestCase):
    """
    Test functions of text transformation classes
    """

    @classmethod
    def setUpClass(self):
        # create_engine("sqlite+pysqlite:///:memory:", echo=True)
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

    def test_get_hashtags(self):
        """
        make sure we can pull #hashtags from text
        """
        test_cases = {
            "最近在Telegram和X上出现了虚假声明，但他是#American，无论如何他都会留在祖国": [
                # twitter doesn't recognize if no whitespace seperating
                # ScoredTag("#American", 1.0)
            ],
            # "RT @yogrishiramdev: मैडम तुसाद अट्रैक्शन, न्यूयॉर्क प्रथम भारतीय सन्यासी #स्वामीरामदेव_मैडमतुसाद": [
            #    ScoredTag("#स्वामीरामदेव_मैडमतुसाद", 1.0)
            # ], # whitespace regex spliting between charaters incorrectly https://bugs.python.org/issue12731
            "RT @ANI: #WATCH": [ScoredTag("#WATCH", 1.0)],
            "#youtubeshorts": [ScoredTag("#youtubeshorts", 1.0)],
            "Looks like the time has come for #BTS member #Jhope to finally enter the military": [
                ScoredTag("#BTS", 1.0),
                ScoredTag("#Jhope", 1.0),
            ],
            "This doesn't have any ^&@#@! hashtags!": [],
            "#عاجل | المجلس النرويجي للاجئين للعربي: حرمان الأونروا من التمويل": [
                ScoredTag("#عاجل", 1.0)
            ],
            "#42": [],  # raw numbers should not create hashtags
            "# ": [],  # lonly hashes are not hashtags
            "https://urm.wwu.edu/how-create-anchor-jump-link#anchor": [],  # anchor link not a hashtag
        }

        extractor = BasicKeywordsExtrator()
        for text in test_cases:
            tags = extractor.get_hashtags(text)
            expected = test_cases[text]
            assert set(tags) == set(
                expected
            ), f"extracted tags did not match for '{text}', found: {tags} expected: {expected}"

    # this test runs fine locally, but consistently fails in CI with:
    #  psycopg2.OperationalError: server closed the connection unexpectedly
    #  This probably means the server terminated abnormally
    #  before or while processing the request.
    @unittest.skipIf(
        TimpaniAppCfg().deploy_env_label in ["test", "live"],
        "integration tests not run in CI test environment due to mysterious postgres failure",
    )
    def test_extraction_workflow(self):
        """
        Test that an item can be successfully annotated with keywords corresponding to extracted hashtags via callback
        """
        # create test content item
        content_item = self.store.initialize_item(
            ContentItem(
                date_id=19000101,
                run_id="run_1c43908277e34803ba7eea51b9054219",
                workspace_id="meedan_test",
                source_id="test_source",
                query_id="test_hashtags",
                raw_created_at=datetime.strptime(
                    "2023-06-09 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
                ),
                raw_content_id="129821388abc",
                raw_content="This text demonstrates extracting keywords and hashtags like #BTS and #MeToo",
                # skipping auto set fields
                content_published_date=datetime.strptime("2023-06-11", "%Y-%m-%d"),
                content_published_url="http://somesite.com",
            ),
        )
        # self.store.transition_item_state(content_item, ContentItemState.STATE_READY)

        extractor = BasicKeywordsExtrator()
        # ask the extractor to update the item and state (via a callback, which is a bit silly)
        extractor.add_keywords_to_item(
            content_item, target_state=ContentItemState.STATE_READY
        )

        #  query to check that keywords created
        keywords = self.store.get_keywords_for_item(content_item)
        for keyword in keywords:
            assert keyword.keyword_text in ["#BTS", "#MeToo"]

    @unittest.skipIf(
        TimpaniAppCfg().deploy_env_label in ["test", "live"],
        "integration tests not run in CI test environment due to mysterious postgres failure",
    )
    def test_multiple_tag_types(self):
        """
        Test annotating with multiple tag types
        """
        # create test content item
        content_item = self.store.initialize_item(
            ContentItem(
                date_id=19000101,
                run_id="run_1c43908277e34803ba7eea51b9054219",
                workspace_id="meedan_test",
                source_id="test_source",
                query_id="test_hashtags",
                raw_created_at=datetime.strptime(
                    "2023-06-10 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
                ),
                raw_content_id="129821388def",
                raw_content="This text also demonstrates extracting keywords and hashtags like #BTS and #MeToo",
                # skipping auto set fields
                content_published_date=datetime.strptime("2023-06-12", "%Y-%m-%d"),
                content_published_url="http://somesite.com",
            ),
        )

        # directly attach keywords to items via content store
        self.store.attach_keyword(
            content_item, keyword_model_name="hashtags", keyword_text="#BTS"
        )
        self.store.attach_keyword(
            content_item, keyword_model_name="topics", keyword_text="extracting"
        )
        self.store.attach_keyword(
            content_item, keyword_model_name="topics", keyword_text="demo"
        )

        #  query to check that keywords created
        keywords = self.store.get_keywords_for_item(content_item)
        for keyword in keywords:
            assert keyword.keyword_text in ["#BTS", "extracting", "demo"]
