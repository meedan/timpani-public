import unittest
from datetime import datetime
from marshmallow.exceptions import ValidationError
from timpani.app_cfg import TimpaniAppCfg
from timpani.content_store.content_store_manager import ContentStoreManager

# from timpani.content_store.content_store_obj import ContentStoreObject
from timpani.content_store.content_item import ContentItem, ContentItemSchema
from timpani.content_store.content_store import ContentStore


class TestContentItem(unittest.TestCase):
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

    test_data = {
        "date_id": 19000101,
        "run_id": "run_1c43908277e34803ba7eea51b9054219",
        "workspace_id": "meedan_test",
        "source_id": "test_source",
        "query_id": "test_query_id",
        "raw_created_at": datetime.strptime(
            "2023-06-09 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
        ),
        "raw_content_id": "129839388abc",
        "raw_content": "Nếu có $ 1,200 tiếp theo share để Bảo vệ tiền thuế support to America Great",
        # skipping auto set fields
        "content_published_date": datetime.strptime("2023-06-12", "%Y-%m-%d"),
        "content_published_url": "http://somesite.com",
        "content": "Nếu có $ 1,200 tiếp theo share để Bảo vệ tiền thuế support to America Great",
    }

    expected_json_serialization1 = {
        "run_id": "run_1c43908277e34803ba7eea51b9054219",
        "content_published_date": "2023-06-12T00:00:00",
        "raw_content_id": "129839388abc",
        "workspace_id": "meedan_test",
        "source_id": "test_source",
        "content": "Nếu có $ 1,200 tiếp theo share để Bảo vệ tiền thuế support to America Great",
        "content_item_id": None,
        "raw_content": "Nếu có $ 1,200 tiếp theo share để Bảo vệ tiền thuế support to America Great",
        "raw_created_at": "2023-06-09T10:45:34.715998",
        "query_id": "test_query_id",
        "content_published_url": "http://somesite.com",
        "date_id": 19000101,
        "updated_at": None,
        "content_locale_code": None,
        "content_language_code": None,
        "source_field": None,
    }

    def test_item_initialization_to_db(self):
        item = ContentItem(
            date_id=19000101,
            run_id="run_1c43908277e34803ba7eea51b9054219",
            workspace_id="meedan_test",
            source_id="test_source",
            query_id="test_query_id",
            raw_created_at=datetime.strptime(
                "2023-06-09 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
            ),
            raw_content_id="129839388xyz",  # need a unique value to avoid update ignore behavior
            raw_content="Nếu có $ 1,200 tiếp theo share để Bảo vệ tiền thuế support to America Great",
        )
        self.store.initialize_item(item)
        # NOTE: IRL this should be associate with a state model

        # check if primary key was created
        assert item.content_item_id is not None
        # check updated at was set
        assert item.updated_at is not None

    def test_field_accessors(self):
        """
        Confirm object can be created and appropriate fields
        read and writen too even if not connected to database
        """

        item = ContentItem(
            date_id=self.test_data["date_id"],
            run_id=self.test_data["run_id"],
            workspace_id=self.test_data["workspace_id"],
            source_id=self.test_data["source_id"],
            query_id=self.test_data["query_id"],
            raw_created_at=self.test_data["raw_created_at"],
            raw_content_id=self.test_data["raw_content_id"],
            raw_content=self.test_data["raw_content"],
            content_published_date=self.test_data["content_published_date"],
            content_published_url=self.test_data["content_published_url"],
            content=self.test_data["content"],
        )
        assert item.date_id == self.test_data["date_id"]
        assert item.run_id == self.test_data["run_id"]
        assert item.workspace_id == self.test_data["workspace_id"]
        assert item.source_id == self.test_data["source_id"]
        assert item.query_id == self.test_data["query_id"]
        assert item.raw_created_at == self.test_data["raw_created_at"]
        assert item.raw_content_id == self.test_data["raw_content_id"]
        assert item.raw_content == self.test_data["raw_content"]
        assert item.content_published_date == self.test_data["content_published_date"]
        assert item.content_published_url == self.test_data["content_published_url"]
        assert item.content == self.test_data["content"]

        # not expecting these to be set yet since not in db
        assert item.content_item_id is None
        assert item.updated_at is None

    def test_serialization(self):
        """
        Content item can be dumped to json
        NOTE: Serialization of foreign keys isn't working, so cluster id etc are not included
        """
        # this item is not part of a db session, so will have no
        # conent_item_id or updated_at
        item1 = ContentItem(
            date_id=self.test_data["date_id"],
            run_id=self.test_data["run_id"],
            workspace_id=self.test_data["workspace_id"],
            source_id=self.test_data["source_id"],
            query_id=self.test_data["query_id"],
            raw_created_at=self.test_data["raw_created_at"],
            raw_content_id=self.test_data["raw_content_id"],
            raw_content=self.test_data["raw_content"],
            content_published_date=self.test_data["content_published_date"],
            content_published_url=self.test_data["content_published_url"],
        )

        # serialize
        dump_content = ContentItemSchema().dump(item1)
        assert (
            dump_content == self.expected_json_serialization1
        ), f"Serialization:\n{dump_content}\n not equal to expected:\n{self.expected_json_serialization1}"

    def test_deserialization(self):
        """
        Content item can be loaded from json and read back in,
        schema validated (invalid fields throw errors)
        """
        dump_content = None

        item1 = ContentItem(
            date_id=self.test_data["date_id"],
            run_id=self.test_data["run_id"],
            workspace_id=self.test_data["workspace_id"],
            source_id=self.test_data["source_id"],
            query_id=self.test_data["query_id"],
            raw_created_at=self.test_data["raw_created_at"],
            raw_content_id=self.test_data["raw_content_id"],
            raw_content=self.test_data["raw_content"],
        )

        # need to create and reload
        # becaues of content_item_id and updated_at
        # fields set in database
        item1 = self.store.update_item(item1)
        # serialize
        dump_content = self.store.serialize_object(item1)

        # deserialize
        item2 = self.store.deserialize_object(dump=dump_content, cls=ContentItem)
        assert item1 == item2

    def test_validation(self):
        """
        Trying to load empty data or data with missing fields should raise execption
        """
        # empty dict
        self.assertRaises(
            ValidationError, ContentItemSchema(load_instance=False).load, {}
        )

        # missing database ids and created at
        # marshmallow.exceptions.ValidationError:
        #   {'content_item_id': ['Field may not be null.'], 'updated_at': ['Field may not be null.']}
        self.assertRaises(
            ValidationError,
            ContentItemSchema(load_instance=False).load,
            self.expected_json_serialization1,
        )

    def test_deserialize_and_update(self):
        """
        Content item can be serialized to json, json updated
         to reflect a new state, deserialized, and appropriate
         update performed in database
        """
        # create item
        dump_content = None
        content_id = None
        item1 = ContentItem(
            date_id=self.test_data["date_id"],
            run_id=self.test_data["run_id"],
            workspace_id=self.test_data["workspace_id"],
            source_id=self.test_data["source_id"],
            query_id=self.test_data["query_id"],
            raw_created_at=self.test_data["raw_created_at"],
            raw_content_id=self.test_data["raw_content_id"],
            raw_content=self.test_data["raw_content"],
        )
        item1 = self.store.initialize_item(item1)
        content_id = item1.content_item_id
        dump_content = self.store.serialize_object(item1)

        # update the json with the translation
        translated_content = "If you have the next $1,200 share to protect your tax money support to America Great"
        dump_content["content"] = translated_content

        # reload the object

        # deserialize
        item2 = self.store.deserialize_object(dump_content, ContentItem)
        assert item2.content == translated_content

        # confirm modification happend in db as well
        item3 = self.store.get_item(content_id)
        assert item3.content == translated_content

    def test_delete(self):
        """
        Content item can be deleted from database
        """
        item1 = ContentItem(
            date_id=self.test_data["date_id"],
            run_id=self.test_data["run_id"],
            workspace_id=self.test_data["workspace_id"],
            source_id=self.test_data["source_id"],
            query_id=self.test_data["query_id"],
            raw_created_at=self.test_data["raw_created_at"],
            raw_content_id=self.test_data["raw_content_id"],
            raw_content=self.test_data["raw_content"],
        )
        self.store.initialize_item(item1)

        id = item1.content_item_id
        # confirm it is there
        self.assertIsNotNone(self.store.get_item(id))

        # now delete it
        self.store.delete_item(item1)

        # confirm it is not there
        self.assertIsNone(self.store.get_item(id))

    def test_get_item(self):
        """
        Test fetching a content item by id
        TODO: add support for fetching by (workspace_id,raw_content_id)
        """
        # test when id doesn't match
        item0 = self.store.get_item(9999999999)
        assert item0 is None

        # test getting back the item
        item1 = ContentItem(
            date_id=self.test_data["date_id"],
            run_id=self.test_data["run_id"],
            workspace_id=self.test_data["workspace_id"],
            source_id=self.test_data["source_id"],
            query_id=self.test_data["query_id"],
            raw_created_at=self.test_data["raw_created_at"],
            raw_content_id="129839388def",  # this must be different to avoid overwrite behavior
            raw_content=self.test_data["raw_content"],
        )
        item1 = self.store.initialize_item(item1)
        content_id = item1.content_item_id

        item1a = self.store.get_item(content_id)
        assert item1.content_item_id == item1a.content_item_id
        assert item1 == item1a
