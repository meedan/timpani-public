import unittest
from datetime import datetime
from time import sleep

from timpani.app_cfg import TimpaniAppCfg
from timpani.model_service.classycat_wrapper import ClassycatWrapper
from timpani.content_store.content_store_manager import ContentStoreManager

from timpani.content_store.content_item import ContentItem
from timpani.content_store.item_state_model import ContentItemState
from timpani.content_store.content_store import ContentStore


class TestclassycatWrappers(unittest.TestCase):
    """
    NOTE: THESE TESTS NO LONGER WORK BECAUSE QA RESPONSE STRUCTURE DOES NOT MATCH LOCAL DEV
    TODO: These tests cannot be run in CI because classycat not there
    NOTE: These tests will only pass if the OPENROUTER_API_KEY env var is set
    This needs classycat system running https://github.com/meedan/classycat
    `docker compose up classycat`
    """

    # Either the id or the name of the schema needs to remain as a static,
    # knowable reference outside classycat so that depending code can know that
    # it is talking to ‘the same’ schema (even if updated over time)
    # Proposal, currently it is enforcing both name and schema, lets make ‘name’
    # be the unique static identifier, and id refers to a specific version?
    TEST_SCHEMA_NAME = "test_AB_schema"
    TEST_CLASSY_SCHEMA = {
        "event_type": "create_schema",
        "schema_name": "test_AB_schema",
        "topics": [
            {
                "topic": "A",
                "description": "This topic includes texts that have more As (upper or lowercase) than Bs (upper or lowercase)",
            },
            {
                "topic": "B",
                "description": "This topic includes texts that have more Bs (upper or lowercase) than As (upper or lowercase)",
            },
        ],
        "examples": [
            {
                "text": "This text has lots of a characters. AAAAAAA, A AAAA",
                "labels": [
                    "A",
                ],
            },
            {"text": "This text has lots of BBBB characters.  BBBBB", "labels": ["B"]},
            {"text": "This text has both AAAAAA and BBBBBB", "labels": ["A", "B"]},
        ],
        "languages": ["English"],
    }

    @classmethod
    def setUpClass(self):
        # create_engine("sqlite+pysqlite:///:memory:", echo=True)
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

    # don't run these tests in the CI environment because it doesn't talk
    # to full aws resources
    @unittest.skipIf(
        TimpaniAppCfg().deploy_env_label in ["test", "live"],
        "integration tests not run in CI test environment",
    )
    def test_classycat_connection(self):
        """
        Test that we can initialize the class and it can connect to classycat
        """
        cat = ClassycatWrapper()
        assert cat.healthcheck()

    # don't run these tests in the CI environment because it doesn't talk
    # to full aws resources
    @unittest.skipIf(
        TimpaniAppCfg().deploy_env_label in ["test", "live"],
        "integration tests not run in CI test environment",
    )
    def test_classycat_create_shema(self):
        """
        Test submitting schema json and getting back id
        """
        cat = ClassycatWrapper()
        # TODO: much better if we could just delete this
        oneoffschema = self.TEST_CLASSY_SCHEMA.copy()
        # change the name so it won't colide with existing schema
        oneoffschema["schema_name"] = oneoffschema["schema_name"] + f"{datetime.now()}"
        schema_id = cat.create_schema(oneoffschema)
        assert schema_id is not None

    # don't run these tests in the CI environment because it doesn't talk
    # to full aws resources
    @unittest.skipIf(
        TimpaniAppCfg().deploy_env_label in ["test", "live"],
        "integration tests not run in CI test environment because no classycat service",
    )
    def test_classycat_get_schema_id(self):
        """
        Test submitting schema json and getting back id
        """
        cat = ClassycatWrapper()
        assert cat.get_schema_id("bad schema name") is None

        schema_id = cat.get_schema_id(self.TEST_SCHEMA_NAME)
        assert schema_id is not None

    @unittest.skipIf(
        TimpaniAppCfg().deploy_env_label in ["test", "live"],
        "integration tests not run in CI test environment because no classycat service",
    )
    def test_batch_classify(self):
        """
        Test that an item can be successfully annotated with keywords corresponding to classified categories via callback
        """
        # create test content item
        content_item1 = self.store.initialize_item(
            ContentItem(
                date_id=19000101,
                run_id="run_1c43908277e34803ba7eea51b9054219",
                workspace_id="meedan_test",
                source_id="test_source",
                query_id="test_classycat",
                raw_created_at=datetime.strptime(
                    "2023-06-09 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
                ),
                raw_content_id="129821388abc",
                raw_content="This text has many B characters BB BBBB",
                # skipping auto set fields
                content_published_date=datetime.strptime("2023-06-12", "%Y-%m-%d"),
                content_published_url="http://somesite.com",
            ),
        )
        content_item2 = self.store.initialize_item(
            ContentItem(
                date_id=19000101,
                run_id="run_1c43908277e34803ba7eea51b9054219",
                workspace_id="meedan_test",
                source_id="test_source",
                query_id="test_classycat",
                raw_created_at=datetime.strptime(
                    "2023-06-09 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
                ),
                raw_content_id="129821388def",
                raw_content="This a text that has lots of copies of the letter A. A AAA AAAA",
                # skipping auto set fields
                content_published_date=datetime.strptime("2023-06-12", "%Y-%m-%d"),
                content_published_url="http://somesite.com",
            ),
        )
        # self.store.transition_item_state(content_item, ContentItemState.STATE_READY)
        test_batch = [content_item1, content_item2]

        classycat = ClassycatWrapper(self.TEST_SCHEMA_NAME)
        # check if schema exists
        schema_id = classycat.get_schema_id(schema_name=self.TEST_SCHEMA_NAME)
        if schema_id is None:
            schema_id = classycat.create_schema(self.TEST_CLASSY_SCHEMA)
        print(f"running batch with schema_id: {schema_id}")

        classycat.batch_classify(
            content_items=test_batch,
            schema_id=schema_id,
            target_state=ContentItemState.STATE_READY,
        )
        #  query to check that keywords created
        # first one should be B
        keywords1 = self.store.get_keywords_for_item(test_batch[0])
        for keyword in keywords1:
            assert keyword.keyword_text in ["B"]
        # second should be A
        keywords2 = self.store.get_keywords_for_item(test_batch[1])
        for keyword in keywords2:
            assert keyword.keyword_text in ["A"]

    @unittest.skipIf(
        TimpaniAppCfg().deploy_env_label in ["test", "live"],
        "integration tests not run in CI test environment because no classycat service",
    )
    def test_add_to_batch(self):
        """
        Make sure we can submit lots of individual items and they will go through batch
        """
        # TODO: need to test with more than one schema and return state
        num_examples = 27
        examples = []
        for n in range(num_examples):
            content_item = self.store.initialize_item(
                ContentItem(
                    date_id=19000101,
                    run_id="run_1c43908277e34803ba7eea51b9054219",
                    workspace_id="meedan_test",
                    source_id="test_source",
                    query_id="test_classycat",
                    raw_created_at=datetime.strptime(
                        "2023-06-09 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    raw_content_id=f"batch_test_{n}",
                    raw_content=f"Classifying content, example {n}: AAAAAA",
                    # skipping auto set fields
                    content_published_date=datetime.strptime("2023-06-12", "%Y-%m-%d"),
                    content_published_url="http://somesite.com",
                ),
            )
            examples.append(content_item)

        classycat = ClassycatWrapper(self.TEST_SCHEMA_NAME)
        # check if schema exists
        schema_id = classycat.get_schema_id(schema_name=self.TEST_SCHEMA_NAME)
        if schema_id is None:
            schema_id = classycat.create_schema(self.TEST_CLASSY_SCHEMA)
        print(f"running batch with schema_id: {schema_id}")

        for item in examples:
            classycat.add_item_to_batch(
                item,
                schema_id=schema_id,
                target_state=ContentItemState.STATE_READY,
            )

        # TODO: wait until after timeout to make sure everything gets sent back
        #  query to check that keywords created
        # TODO: this actually shouldn't be passing since there are more than MAX_ITEMS
        # so second batch shouldn't have submitted..
        sleep(10)
        for content_item in examples:
            keywords = self.store.get_keywords_for_item(content_item)
            for keyword in keywords:
                assert keyword.keyword_text in [
                    "A"
                ], f"item {content_item.content_item_id} was given keyword {keyword}"
