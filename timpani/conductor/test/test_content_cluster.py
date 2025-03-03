import unittest
from datetime import datetime
from sqlalchemy.orm import Session

from timpani.app_cfg import TimpaniAppCfg
from timpani.content_store.content_store_manager import ContentStoreManager

# from timpani.content_store.content_store_obj import ContentStoreObject
from timpani.content_store.content_item import ContentItem
from timpani.content_store.content_store import ContentStore
from timpani.content_store.content_cluster import ContentCluster


class TestContentCluster(unittest.TestCase):
    cfg = TimpaniAppCfg()

    @classmethod
    def setUpClass(self) -> None:
        # define all the new tables
        # don't run in prod env?
        assert self.cfg.deploy_env_label not in [
            "live",
            "qa",
        ], "Exiting test to avoid modifying live or qa database"
        print("\nDrop and recreating all tables for content cluster test run\n")
        manager = ContentStoreManager()
        manager.destroy_content_store()
        manager.setup_admin_and_content_store()

        # create a content store
        self.store = ContentStore()
        self.engine = self.store.init_db_engine()
        # ContentStoreObject.metadata.drop_all(bind=self.engine)
        # ContentStoreObject.metadata.create_all(bind=self.engine)

        self.item1_test_data = {
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
            "content_published_date": datetime.strptime("2023-06-12", "%Y-%m-%d"),
            "content_published_url": "http://somesite.com",
            "content": "Nếu có $ 1,200 tiếp theo share để Bảo vệ tiền thuế support to America Great",
        }

        self.item2_test_data = {
            "date_id": 19000101,
            "run_id": "run_1c43908277e34803ba7eea51b9054219",
            "workspace_id": "meedan_test",
            "source_id": "test_source",
            "query_id": "test_query_id",
            "raw_created_at": datetime.strptime(
                "2023-06-09 10:45:35.716000", "%Y-%m-%d %H:%M:%S.%f"
            ),
            "raw_content_id": "129839388def",
            "raw_content": "If you have the next $1,200 share to protect your tax money support to America Great",
            "content_published_date": datetime.strptime("2023-06-12", "%Y-%m-%d"),
            "content_published_url": "http://somesite.com",
            "content": "If you have the next $1,200 share to protect your tax money support to America Great",
        }

        self.expected_serialization1 = {
            "created_at": None,  # "2023-06-15T13:32:42",
            "content_cluster_id": 9,
            "updated_at": None,  # "2023-06-15T13:32:42",
            "num_items": 1,
            "num_items_added": 1,
            "num_items_unique": 1,
            "stress_score": 0.0,
            "priority_score": 0.0,
            "workspace_id": "meedan_test",
        }

    def test_cluster_initialization_without_db(self):
        """
        confirm object works without db connection
        """
        cluster = ContentCluster()

        # check primary key not created
        assert cluster.content_cluster_id is None
        # check updated at was not set
        assert cluster.updated_at is None

    def test_add_item_to_cluster(self):
        """
        Add a content item to a cluster
        """
        item1a = ContentItem(
            date_id=self.item1_test_data["date_id"],
            run_id=self.item1_test_data["run_id"],
            workspace_id=self.item1_test_data["workspace_id"],
            source_id=self.item1_test_data["source_id"],
            query_id=self.item1_test_data["query_id"],
            raw_created_at=self.item1_test_data["raw_created_at"],
            raw_content_id=self.item1_test_data["raw_content_id"],
            raw_content=self.item1_test_data["raw_content"],
            content_published_date=self.item1_test_data["content_published_date"],
            content_published_url=self.item1_test_data["content_published_url"],
        )

        item1a = self.store.initialize_item(item1a)
        assert item1a is not None
        assert item1a.workspace_id is not None
        cluster = self.store.cluster_items(item1a)
        assert cluster is not None

        # yikes, this changed the state of item1
        # internally in the db, but local copy is out of date
        item1a = self.store.refresh_object(item1a)
        assert item1a.content_cluster_id is not None
        assert cluster.updated_at is not None
        assert item1a.content_cluster_id == cluster.content_cluster_id
        assert cluster.num_items == 1
        assert cluster.num_items_unique == 1
        assert cluster.num_items_added == 1
        # should have been set as exemplar
        assert cluster.exemplar_item_id == item1a.content_item_id

    def test_add_multiple_and_get(self):
        """
        Try adding more than one item to a cluster
        """
        item1b = ContentItem(
            date_id=self.item1_test_data["date_id"],
            run_id=self.item1_test_data["run_id"],
            workspace_id=self.item1_test_data["workspace_id"],
            source_id=self.item1_test_data["source_id"],
            query_id=self.item1_test_data["query_id"],
            raw_created_at=self.item1_test_data["raw_created_at"],
            raw_content_id="229839388abc",
            raw_content=self.item1_test_data["raw_content"],
            content_published_date=self.item1_test_data["content_published_date"],
            content_published_url=self.item1_test_data["content_published_url"],
        )

        item1b = self.store.initialize_item(item1b)
        cluster = self.store.cluster_items(item1b)
        item1b = self.store.refresh_object(item1b)

        assert item1b.content_cluster_id is not None
        assert item1b.content_cluster_id == cluster.content_cluster_id
        assert cluster.num_items == 1

        item2 = ContentItem(
            # NOTE: slightly different than item1
            date_id=self.item2_test_data["date_id"],
            run_id=self.item2_test_data["run_id"],
            workspace_id=self.item2_test_data["workspace_id"],
            source_id=self.item2_test_data["source_id"],
            query_id=self.item2_test_data["query_id"],
            raw_created_at=self.item2_test_data["raw_created_at"],
            raw_content_id=self.item2_test_data["raw_content_id"],
            raw_content=self.item2_test_data["raw_content"],
            content_published_date=self.item2_test_data["content_published_date"],
            content_published_url=self.item2_test_data["content_published_url"],
        )
        item2 = self.store.initialize_item(item2)
        cluster = self.store.cluster_items(item2, item1b)
        item2 = self.store.refresh_object(item2)

        assert item2.content_cluster_id is not None
        assert item2.content_cluster_id == cluster.content_cluster_id
        assert cluster.num_items == 2
        assert cluster.num_items_unique == 2
        assert cluster.num_items_added == 2
        cluster_items = list(self.store.get_cluster_items(cluster))
        assert cluster_items == [item1b, item2], f"cluster items were {cluster_items}"

        # check we can modify property of cluster
        assert cluster.stress_score == 0.0
        cluster.stress_score = 1.0
        cluster = self.store.update_cluster(cluster)
        assert cluster.stress_score == 1.0

    def test_cluster_pair(self):
        """
        Try adding more than one item to a cluster at the same time
        (code path when neither item had a pre-existing cluster)
        """
        item1c = ContentItem(
            date_id=self.item1_test_data["date_id"],
            run_id=self.item1_test_data["run_id"],
            workspace_id=self.item1_test_data["workspace_id"],
            source_id=self.item1_test_data["source_id"],
            query_id=self.item1_test_data["query_id"],
            raw_created_at=self.item1_test_data["raw_created_at"],
            raw_content_id="329839388abc",
            raw_content=self.item1_test_data["raw_content"],
            content_published_date=self.item1_test_data["content_published_date"],
            content_published_url=self.item1_test_data["content_published_url"],
        )

        item1c = self.store.initialize_item(item1c)

        item2c = ContentItem(
            # NOTE: slightly different than item1
            date_id=self.item2_test_data["date_id"],
            run_id=self.item2_test_data["run_id"],
            workspace_id=self.item2_test_data["workspace_id"],
            source_id=self.item2_test_data["source_id"],
            query_id=self.item2_test_data["query_id"],
            raw_created_at=self.item2_test_data["raw_created_at"],
            raw_content_id="329839388def",
            raw_content=self.item2_test_data["raw_content"],
            content_published_date=self.item2_test_data["content_published_date"],
            content_published_url=self.item2_test_data["content_published_url"],
        )
        item2c = self.store.initialize_item(item2c)

        cluster = self.store.cluster_items(item1c, item2c)

        item1c = self.store.refresh_object(item1c)
        item2c = self.store.refresh_object(item2c)

        assert item1c.content_cluster_id is not None
        assert item1c.content_cluster_id == cluster.content_cluster_id
        assert cluster.num_items == 2

        assert item2c.content_cluster_id is not None
        assert item2c.content_cluster_id == cluster.content_cluster_id
        assert cluster.num_items == 2
        cluster_items = list(self.store.get_cluster_items(cluster))
        assert cluster_items == [item1c, item2c], f"cluster items were {cluster_items}"

    def test_delete_item(self):
        """
        Try removing an item from a cluster
        """
        item1d = ContentItem(
            date_id=self.item1_test_data["date_id"],
            run_id=self.item1_test_data["run_id"],
            workspace_id=self.item1_test_data["workspace_id"],
            source_id=self.item1_test_data["source_id"],
            query_id=self.item1_test_data["query_id"],
            raw_created_at=self.item1_test_data["raw_created_at"],
            raw_content_id="429839388abc",
            raw_content=self.item1_test_data["raw_content"],
            content_published_date=self.item1_test_data["content_published_date"],
            content_published_url=self.item1_test_data["content_published_url"],
        )

        item1d = self.store.initialize_item(item1d)
        cluster_id = None
        cluster = self.store.cluster_items(item1d)
        # refresh so it will have its cluster id
        item1d = self.store.refresh_object(item1d)
        assert item1d.content_cluster_id is not None
        cluster_id = cluster.content_cluster_id

        self.store.delete_item(item1d)

        # this is using direct access to check for delete because so far
        # we don't otherwise need to support getting cluster by id
        with Session(self.engine) as session:
            cluster = session.get(ContentCluster, cluster_id)
            assert cluster is None
            session.close()

    def test_move_item_cluster(self):
        """
        test moving an item from one cluster to another
        """
        item1e = ContentItem(
            date_id=self.item1_test_data["date_id"],
            run_id=self.item1_test_data["run_id"],
            workspace_id=self.item1_test_data["workspace_id"],
            source_id=self.item1_test_data["source_id"],
            query_id=self.item1_test_data["query_id"],
            raw_created_at=self.item1_test_data["raw_created_at"],
            raw_content_id="529839388abc",
            raw_content=self.item1_test_data["raw_content"],
            content_published_date=self.item1_test_data["content_published_date"],
            content_published_url=self.item1_test_data["content_published_url"],
        )
        item1e = self.store.initialize_item(item1e)

        item2d = ContentItem(
            # NOTE: slightly different than item1
            date_id=self.item2_test_data["date_id"],
            run_id=self.item2_test_data["run_id"],
            workspace_id=self.item2_test_data["workspace_id"],
            source_id=self.item2_test_data["source_id"],
            query_id=self.item2_test_data["query_id"],
            raw_created_at=self.item2_test_data["raw_created_at"],
            raw_content_id="429839388def",
            raw_content=self.item2_test_data["raw_content"],
            content_published_date=self.item2_test_data["content_published_date"],
            content_published_url=self.item2_test_data["content_published_url"],
        )
        item2d = self.store.initialize_item(item2d)

        cluster1 = self.store.cluster_items(item1e)
        item1e = self.store.refresh_object(item1e)
        assert item1e.content_cluster_id == cluster1.content_cluster_id
        assert cluster1.num_items == 1
        assert cluster1.num_items_unique == 1
        assert cluster1.num_items_added == 1

        cluster2 = self.store.cluster_items(item2d)
        assert cluster2.num_items == 1
        assert cluster2.num_items_unique == 1
        assert cluster2.num_items_added == 1

        # move item 1 to item 2's cluster, expecting
        # first cluster to be deleted
        cluster2 = self.store.cluster_items(item1e, item2d)
        item1e = self.store.refresh_object(item1e)
        assert item1e.content_cluster_id == cluster2.content_cluster_id

        assert cluster2.num_items == 2
        assert cluster2.num_items_unique == 2
        assert cluster2.num_items_added == 2

    def test_serialization(self):
        """
        Content cluster can be dumped to json
        """

        # don't make clusters this way, just for testing
        # cluster = ContentCluster()
        # cluster.content_cluster_id = 5  # never set this directly, primary key
        # cluster.num_items += 1  # never set this directly
        # cluster.exemplar_item_id = 1

        # this is the right way to make a cluster:
        item = ContentItem(
            date_id=self.item1_test_data["date_id"],
            run_id=self.item1_test_data["run_id"],
            workspace_id=self.item1_test_data["workspace_id"],
            source_id=self.item1_test_data["source_id"],
            query_id=self.item1_test_data["query_id"],
            raw_created_at=self.item1_test_data["raw_created_at"],
            raw_content_id="129839388xyz",
            raw_content=self.item1_test_data["raw_content"],
            content_published_date=self.item1_test_data["content_published_date"],
            content_published_url=self.item1_test_data["content_published_url"],
        )
        item = self.store.initialize_item(item)

        cluster = self.store.cluster_items(item)
        assert cluster.content_cluster_id is not None

        # serialize
        dump_content = self.store.serialize_object(cluster)
        # clobber the times since they will be different
        assert dump_content["created_at"] is not None
        assert dump_content["updated_at"] is not None
        dump_content["created_at"] = None
        dump_content["updated_at"] = None

        assert (
            dump_content == self.expected_serialization1
        ), f"Serialization:\n{dump_content}\n not equal to expected:\n{self.expected_serialization1}"

    def test_deserialization(self):
        """
        Content item can be loaded from json and read back in,
        schema validated (invalid fields throw errors)
        """
        dump_content = None

        # need to create and reload
        # becaues of content_item_id and updated_at
        # fields set in database
        item1 = ContentItem(
            date_id=self.item1_test_data["date_id"],
            run_id=self.item1_test_data["run_id"],
            workspace_id=self.item1_test_data["workspace_id"],
            source_id=self.item1_test_data["source_id"],
            query_id=self.item1_test_data["query_id"],
            raw_created_at=self.item1_test_data["raw_created_at"],
            raw_content_id="129839388tuv",
            raw_content=self.item1_test_data["raw_content"],
            content_published_date=self.item1_test_data["content_published_date"],
            content_published_url=self.item1_test_data["content_published_url"],
        )
        item1 = self.store.initialize_item(item1)

        cluster = None
        cluster = self.store.cluster_items(item1)
        assert cluster.content_cluster_id is not None

        dump_content = self.store.serialize_object(cluster)

        cluster2 = self.store.deserialize_object(dump_content, ContentCluster)
        assert cluster2 is not None
        assert cluster2.content_cluster_id == cluster.content_cluster_id

    def test_delete_exemplar_item(self):
        """
        Try removing an item from a cluster when it is the exemplar
        """
        item1f = ContentItem(
            date_id=self.item1_test_data["date_id"],
            run_id=self.item1_test_data["run_id"],
            workspace_id=self.item1_test_data["workspace_id"],
            source_id=self.item1_test_data["source_id"],
            query_id=self.item1_test_data["query_id"],
            raw_created_at=self.item1_test_data["raw_created_at"],
            raw_content_id="429839388fff",
            raw_content=self.item1_test_data["raw_content"],
            content_published_date=self.item1_test_data["content_published_date"],
            content_published_url=self.item1_test_data["content_published_url"],
        )

        item2d = ContentItem(
            # NOTE: slightly different than item1
            date_id=self.item2_test_data["date_id"],
            run_id=self.item2_test_data["run_id"],
            workspace_id=self.item2_test_data["workspace_id"],
            source_id=self.item2_test_data["source_id"],
            query_id=self.item2_test_data["query_id"],
            raw_created_at=self.item2_test_data["raw_created_at"],
            raw_content_id="329839388ddd",
            raw_content=self.item2_test_data["raw_content"],
            content_published_date=self.item2_test_data["content_published_date"],
            content_published_url=self.item2_test_data["content_published_url"],
        )

        item1f = self.store.initialize_item(item1f)
        item2d = self.store.initialize_item(item2d)

        # put both items in the cluster
        cluster = self.store.cluster_items(first_item=item1f, second_item=item2d)
        cluster_id = cluster.content_cluster_id

        # refresh so it will have its cluster id
        item1f = self.store.refresh_object(item1f)
        item2d = self.store.refresh_object(item2d)
        assert item1f.content_cluster_id == cluster.content_cluster_id
        assert item2d.content_cluster_id == cluster.content_cluster_id

        # expecting that item1f is exemplar
        assert cluster.exemplar_item_id == item1f.content_item_id
        assert cluster.num_items == 2

        # delete the exemplar
        self.store.delete_item(item1f)
        cluster = self.store.refresh_object(cluster)
        # expecting item2 is now exemplar
        assert cluster.exemplar_item_id == item2d.content_item_id
        assert cluster.num_items == 1

        # delete the 2nd item, expecting cluster to be removed
        self.store.delete_item(item2d)

        # this is using direct access to check for delete because so far
        # we don't otherwise need to support getting cluster by id
        with Session(self.engine) as session:
            cluster = session.get(ContentCluster, cluster_id)
            assert cluster is None
            session.close()

    def test_unique_item_count(self):
        """
        Try removing an item from a cluster when it is the exemplar
        """
        item1h = ContentItem(
            date_id=self.item1_test_data["date_id"],
            run_id=self.item1_test_data["run_id"],
            workspace_id=self.item1_test_data["workspace_id"],
            source_id=self.item1_test_data["source_id"],
            query_id=self.item1_test_data["query_id"],
            raw_created_at=self.item1_test_data["raw_created_at"],
            raw_content_id="429839388hhh",
            raw_content=self.item1_test_data["raw_content"],
            content_published_date=self.item1_test_data["content_published_date"],
            content_published_url=self.item1_test_data["content_published_url"],
        )

        # this content should be identical to item 1h
        item1i = ContentItem(
            date_id=self.item1_test_data["date_id"],
            run_id=self.item1_test_data["run_id"],
            workspace_id=self.item1_test_data["workspace_id"],
            source_id=self.item1_test_data["source_id"],
            query_id=self.item1_test_data["query_id"],
            raw_created_at=self.item1_test_data["raw_created_at"],
            raw_content_id="429839388iii",
            raw_content=self.item1_test_data["raw_content"],
            content_published_date=self.item1_test_data["content_published_date"],
            content_published_url=self.item1_test_data["content_published_url"],
        )

        item2e = ContentItem(
            # NOTE: slightly different than item1
            date_id=self.item2_test_data["date_id"],
            run_id=self.item2_test_data["run_id"],
            workspace_id=self.item2_test_data["workspace_id"],
            source_id=self.item2_test_data["source_id"],
            query_id=self.item2_test_data["query_id"],
            raw_created_at=self.item2_test_data["raw_created_at"],
            raw_content_id="329839388eee",
            raw_content=self.item2_test_data["raw_content"],
            content_published_date=self.item2_test_data["content_published_date"],
            content_published_url=self.item2_test_data["content_published_url"],
        )

        item1h = self.store.initialize_item(item1h)
        item1i = self.store.initialize_item(item1i)
        item2e = self.store.initialize_item(item2e)

        # put both items in the cluster
        cluster = self.store.cluster_items(first_item=item1h, second_item=item2e)
        assert cluster.num_items == 2
        assert cluster.num_items_added == 2
        assert cluster.num_items_unique == 2

        # add the 3rd identical item
        cluster = self.store.cluster_items(first_item=item1i, second_item=item1h)

        assert cluster.num_items == 3
        assert cluster.num_items_added == 3
        assert (
            cluster.num_items_unique == 2
        )  # SHOULD ONLY BE 2 BECAUSE IT WAS DUPE CONTENT

        # now remove one of the dupe items
        self.store.delete_item(item1i)
        cluster = self.store.refresh_object(cluster)

        assert cluster.num_items == 2, f"num_items is {cluster.num_items}"
        assert cluster.num_items_added == 3
        assert cluster.num_items_unique == 2

        # TODO: this fails with FK violation if delete exemplar
        # remove another
        # now remove one of the dupe items
        self.store.delete_item(item1h)
        cluster = self.store.refresh_object(cluster)
        assert cluster.num_items == 1, f"num_items is {cluster.num_items}"
        assert cluster.num_items_added == 3
        assert cluster.num_items_unique == 1
