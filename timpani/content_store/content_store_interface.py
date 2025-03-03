from typing import List
from timpani.content_store.content_cluster import ContentCluster
from timpani.content_store.content_item import ContentItem
from timpani.content_store.item_state_model import ContentItemState
from timpani.content_store.content_store_obj import ContentStoreObject


class ContentStoreInterface(object):
    """
    abstract class defining ContentStore access functions to
    avoid full instantiation and circular dependencies
    """

    def init_db_engine(self, connect_string=None, debug=False):
        raise NotImplementedError

    # --- operations on clusters and items --

    def initialize_item(self, item: ContentItem, state=None, force_overwrite=False):
        raise NotImplementedError

    def transition_item_state(self, item: ContentItem, state: str) -> ContentItemState:
        raise NotImplementedError

    def start_transition_to_state(self, item: ContentItem, state: str):
        raise NotImplementedError

    def validate_transition(self, item: ContentItem, state: str):
        raise NotImplementedError

    def update_item(self, item: ContentItem, state=None):
        raise NotImplementedError

    def refresh_object(self, object: ContentStoreObject):
        raise NotImplementedError

    def cluster_items(
        self,
        first_item: ContentItem,
        second_item: ContentItem = None,
    ) -> ContentCluster:
        raise NotImplementedError

    def update_cluster(
        self,
        cluster: ContentCluster,
    ):
        raise NotImplementedError

    def get_priority_clusters(self, batch_size=1000):
        raise NotImplementedError

    def get_item(self, content_item_id: int) -> ContentItem:
        raise NotImplementedError

    def get_item_state(self, content_item_state_id: int) -> ContentItemState:
        raise NotImplementedError

    def get_item_state_summary(self, workspace_id=None):
        raise NotImplementedError

    def get_cluster_items(self, cluster: ContentCluster) -> List[ContentItem]:
        raise NotImplementedError

    def delete_item(self, item: ContentItem):
        raise NotImplementedError

    def get_items_older_than(self):
        raise NotImplementedError

    def get_items_in_progress(self, workspace_id=None):
        raise NotImplementedError

    def serialize_object(self, obj: ContentStoreObject):
        raise NotImplementedError

    def deserialize_object(self, dump: dict, cls: ContentStoreObject):
        raise NotImplementedError

    def dangerously_run_sql(self, sql: str):
        raise NotImplementedError
