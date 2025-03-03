import faker

from timpani.workspace_config.workspace_config import WorkspaceConfig
from timpani.raw_store.store import Store
from timpani.util.run_state import RunState
from timpani.raw_store.item import Item

import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()


class FakerTestingContentSource(object):
    """
    A 'data source' for creating large volumes of random content for testing throughput
    ignoring all input queries and time ranges from a small language model
    TODO: this was from an old branch, need to refactor
    """

    locales = ["es", "en_US", "zh_CN", "ja_JP", "pt", "or_IN", "hi_IN"]
    # TODO: Arabic? "pt_PR"
    faker_source = faker.Faker(locales)

    def __init__(self, total_items=100, page_size=100) -> None:
        self.total_items = total_items  # how many synthetic items will be returned
        self.page_size = page_size

    @staticmethod
    def get_source_name():
        """
        Unique string id that can be used as a key corresponding to this source
        """
        return "faker_testing"

    def acquire_new_content(
        self,
        workspace_cfg: WorkspaceConfig,
        store_location: Store,
        run_state: RunState,
        partition_id: Store.Partition = None,
        limit_downloads: bool = False,
    ):
        if limit_downloads:
            logging.warning(
                "limit_downloads flag not supported for faker content, ignored"
            )
        # work out partition ids and batch job ids
        if partition_id is None:
            partition_id = Store.Partition(
                workspace_cfg.get_workspace_slug(),
                self.get_source_name(),
                run_state.date_id,
            )

        logging.info(
            f"Acquiring randomized fake content for workspace_id {workspace_cfg.get_workspace_slug()} into partition_id {partition_id}"
        )

        num_pages = 0
        num_items = 0

        while num_items < self.total_items:
            # the page size may return too many items
            this_page_size = self.page_size
            if num_items + self.page_size > self.total_items:
                # only request enough to finish the batch
                this_page_size = self.total_items - num_items
            payload = self.get_fake_content(
                this_page_size,
                num_items,
                workspace_cfg.get_workspace_slug(),
                page_id=num_pages,
                run_id=run_state.run_id,
            )
            store_location.append_chunk(partition_id, payload)
            num_pages += 1
            num_items += self.page_size

        logging.info(
            "Completed creading fake content for workspace_id {0}".format(
                workspace_cfg.get_workspace_slug()
            )
        )

    def get_fake_content(
        self,
        page_size: int,
        num_items: int,
        workspace_id: str,
        page_id: int,
        run_id: str,
    ):
        """
        Create synthetic author and title content items in multiple
        languages and scripts.
        TODO: none of these are expected to cluster, should we permute
        or translate so some of them will?
        """
        payload = []

        for i in range(page_size):
            text = self.get_fake_text()
            id = f"fake_item_{num_items+i}"
            item = Item(
                run_id=run_id,
                workspace_id=workspace_id,
                source_id=self.get_source_name(),
                query_id="fake_content",
                page_id=page_id,
                content_id=id,
                content={
                    "author": self.faker_source.name(),
                    "title": text,
                    "id": id,
                },
            )
            payload.append(item)

        return payload

    def get_fake_text(self):
        """
        Get a fake text string from the appropriate
        locale distribution
        """
        return self.faker_source.text()
