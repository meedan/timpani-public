from timpani.raw_store.item import Item
from typing import List
from collections import namedtuple
from timpani.util.run_state import RunState


class Store(object):
    """
    Abstract superclass for representing the storage operations supported by
    the Raw Store and handling connections to the external service.

    Subclasses should support append only object-store style operations
    """

    # data structure for storing the partition ids
    Partition = namedtuple("Partition", "workspace_id source_id date_id")

    def validate(self, payload: List[Item]):
        """
        This should check that the payload array is made of the expected items
        and the items are responsible for enforcing any shema
        TODO: are we validating content is json or allowing raw text also?
        TODO: is faster to handle these checks at the schema level?
        """
        assert all(isinstance(item, Item) for item in payload)

    def append_chunk(self, partition_id: Partition, payload: List[Item]):
        """
        Append the data into the store in the indicated partition.
        Payload is an array of dicts
        """
        raise NotImplementedError

    def fetch_chunk(self, object_name: str):
        """
        Return data correspoinding to object_name (includes partition path)
        """
        raise NotImplementedError

    def get_partition_path(self, partition_id: Partition):
        """
        Constructs the path string with the appropriate keys and seperators
        for data store type
        """
        raise NotImplementedError

    def fetch_chunks_in_partition(self, partition_id: Partition):
        """
        Yields a series of lists of Items, each list corresponding to a
        chunk stored in the partition
        """
        raise NotImplementedError

    def get_state_model(self):
        """
        Returns the State model for describing this process
        """
        raise NotImplementedError

    def record_partition_run_state(self, run_state: RunState, partition_id: Partition):
        """
        Record a state status into the raw store that can be used to start or resume jobs
        """
        raise NotImplementedError
