import tempfile
from pathlib import Path
from typing import List
from timpani.raw_store.item import Item
from timpani.raw_store.store import Store

import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()


class DebuggingFileStore(Store):
    """
    Implements Raw Store operations in the local filesystem
    to facilitate tests and debugging. Not intended for production use
    """

    def __init__(self):
        self.base_path = tempfile.gettempdir()
        logging.info(f"DebuggingFileStore will save data to {self.base_path}")

    def get_partition_path(self, partition_id: Store.Partition):
        partition = (
            self.base_path
            + "/"
            + partition_id.workspace_id
            + "/"
            + partition_id.source_id
            + "/"
            + partition_id.date_id
        )
        return partition

    def append_chunk(self, partition_id: Store.Partition, payload: List[Item]):
        super(DebuggingFileStore, self).validate(payload)

        path = self.get_partition_path(partition_id)
        Path(path).mkdir(parents=True, exist_ok=True)
        file_path = path + "/rawstore.jsonl"

        # open the file in append mode and close when done
        with open(file_path, "a", encoding="utf-8") as f:
            for item in payload:
                f.write(item.toJSON() + "\n")
        logging.debug("Wrote DebuggingFileStore data to {}".format(file_path))
        # return the path so tests can do things with it
        return file_path
