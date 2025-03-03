import boto3
import tempfile
import csv
from urllib.parse import urlparse

from timpani.workspace_config.workspace_config import WorkspaceConfig
from timpani.content_sources.content_source import ContentSource
from timpani.raw_store.store import Store

# from timpani.raw_store.item import Item
from timpani.util.run_state import RunState

import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()


class AWSS3CSVContentSource(ContentSource):
    """
    Shared functions to Request content from a large csv file stored in s3 and break it into chunks for
    storage in raw store.  Needs a sub class implemented to determine
    URI, key name, and data mapping

    """

    # TODO: logging config

    # can't find much info on optimal chunk size, 64k seems reasonable start
    CSV_CHUNK_SIZE_BYTES = 1024 * 10
    CSV_CHUNK_SIZE_LINES = 1000

    def get_source_name(self):
        """
        Needs to be implemented in specific datasource subclass
        """
        raise NotImplementedError

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
                "limit_downloads flag yet not supported for AWS content, ignored"
            )
        """
        * TODO: Determine if there is new content since last acquistion
        """
        self.run_state = run_state
        # unpack some variables stored in the run state
        single_query_id = run_state.query_id

        # work out partition ids and batch job ids
        if partition_id is None:
            partition_id = Store.Partition(
                workspace_cfg.get_workspace_slug(),
                self.get_source_name(),
                run_state.date_id,
            )

        logging.info(
            "Acquiring content for workspace_id {0} into partition_id {1}".format(
                workspace_cfg.get_workspace_slug(), partition_id
            )
        )

        # The appropriate S3 access privliages are inherited from environment
        # NOTE: no cross-workspace or cross-team acess controls
        self.s3 = boto3.resource("s3")

        # get the queries with the appropriate types from the workspace
        # for now, we are assuming these are basically list ids
        # that have been defined in junkipedia and populated
        # with accounts to follow
        queries = workspace_cfg.get_queries(self.get_source_name())
        if queries is None:
            logging.error(
                "No queries of type {0} found with query_id {1}".format(
                    self.get_source_name, single_query_id
                )
            )
            return
        if single_query_id is not None:
            logging.info(
                "Restricting workspace acquisition to query_id {}".format(
                    single_query_id
                )
            )
        workspace_total_items = 0
        # TODO: pool of workers to process each query indepenently
        for query_id, query in queries.items():
            # if we are only doing a single query, check if it is the one we are looking for
            if single_query_id is not None:
                if query_id != single_query_id:
                    # skip this query
                    break

            # construct the query url appropriate for API and query
            total_items = 0
            query_uri = query
            s3_url = urlparse(query_uri)
            bucket_name = s3_url.netloc
            key = s3_url.path

            # we are using tempfile so that it will be cleaned up from hosting container
            # if there is a shutdown
            with tempfile.NamedTemporaryFile() as temp_file:
                logging.info(
                    f"Downloading csv content for query_id {query_id} from {query_uri} to {temp_file.name}"
                )
                self.s3.meta.client.download_fileobj(
                    Bucket=bucket_name,
                    Key=key.lstrip("/"),
                    Fileobj=temp_file,
                    Callback=self._report_download_progress,
                )
                temp_file.flush()
                # confirm how much data was downloaded
                bytes_downloaded = temp_file.tell()
                logging.info(
                    f"Completed download of {bytes_downloaded} bytes to {temp_file.name}"
                )

                # loop the payload iterator as it yields data
                for payload in self.process_csv_file(
                    temp_file=temp_file,
                    workspace_id=workspace_cfg.get_workspace_slug(),
                    query_id=query_id,
                ):
                    # TODO: Need to translate encoding?
                    # Seeing \u043f\u043e\u0434\u0434 in response instead of raw utf8 ucharachters

                    # cache the data to the Raw Store
                    store_location.append_chunk(partition_id, payload)
                    total_items += len(payload)

            # TODO: track success/failure state per query id
            logging.info(
                f"Acquired {total_items} raw items from csv for query_id {query_id}"
            )
            workspace_total_items += total_items

        logging.info(
            f"Completed csv content aquistion for workspace_id {workspace_cfg.get_workspace_slug()} loaded {workspace_total_items} items"
        )
        # TODO: report sucess failure rate

    def process_csv_file(self, temp_file, workspace_id: str, query_id: str):
        """
        Manage processing of single CSV file that has be cached locally (by downloading from S3)
        """
        num_pages = 0
        num_items = 0

        logging.info(
            f"Parsing {temp_file.tell()} bytes from {temp_file.name} as csv file"
        )
        # for some reason, need to explicitly open another connection in read mode
        # ... or maybe just needed to seek back to the beginning?
        with open(temp_file.name) as read_file:
            reader = csv.reader(read_file)
            chunk = []
            i = 0
            for line in reader:
                # skip header
                if i > 0:
                    chunk.append(line)

                    if i % self.CSV_CHUNK_SIZE_LINES == 0:
                        payload = self.process_csv_chunk(
                            chunk=chunk,
                            workspace_id=workspace_id,
                            query_id=query_id,
                            page_id=num_pages,
                        )

                        num_pages += 1
                        num_items += len(payload)
                        logging.info(
                            f"Processed query csv file chunk {num_pages} stored {num_items} items.."
                        )
                        # empty the chunk array
                        chunk = []
                        yield payload
                i += 1

            # this is ugly but need to ensure the last incomplete chunk is written

            payload = self.process_csv_chunk(
                chunk=chunk,
                workspace_id=workspace_id,
                query_id=query_id,
                page_id=num_pages,
            )
            logging.debug(f".. flushing remaining {len(payload)} items from last chunk")

            num_pages += 1
            num_items += len(payload)
            yield payload

        logging.info(
            f"Processed query csv file chunk {num_pages} stored {num_items} items.."
        )

    def _report_download_progress(self, bytes_transfered):
        """
        Callback for S3 object download to periodically report on progress for large files
        """
        logging.debug(f"transfered {bytes_transfered} bytes to tempfile ...")

    def process_s3_csv_object(self, s3_object, workspace_id: str, query_id: str):
        """
        Manage the processing of a processing a single (possible large) s3 object
        mapping to a CSV file at an s3 URI (pagination, chunking, storage)
        https://stackoverflow.com/questions/51085539/streaming-in-chunking-csvs-from-s3-to-python

        Seems have to do this by paging through the file in bytes, which don't always map to lines
        (Abandonded this infavor of downloading full file and parsing)

        NOTE: Assumes header file
        NOTE: Assumes uncompressed
        NOTE: Assumes no embeded newlines!
        """

        # TODO: strip header?

        body = s3_object["Body"]

        # split on newlines)
        newline = "\n".encode()
        partial_chunk = b""

        num_pages = 0
        num_items = 0
        while True:
            chunk = partial_chunk + body.read(self.CSV_CHUNK_SIZE_BYTES)

            # If nothing was read there is nothing to process
            if chunk == b"":
                break

            last_newline = chunk.rindex(newline)
            print(f"last_newline: {last_newline}")

            # write to result buffer
            result = chunk[0:last_newline].decode("utf-8")

            if num_pages == 0:
                # we need to strip the first header line
                result = result.split("\r\n")[2:]
                # TODO: hang onto the headers for column names and use for error debugging

            # keep the partial line we have read here to prepend on the next loop
            chunk_boundry = last_newline + 1
            partial_chunk = chunk[chunk_boundry:]
            print(f"partial chunk: {partial_chunk}")

            # chunk it up by lines and convert to list of raw store items
            payload = self.process_csv_chunk(
                chunk=result,
                workspace_id=workspace_id,
                query_id=query_id,
                page_id=num_pages,
            )

            num_pages += 1
            num_items += len(payload)
            logging.info(
                f"Processed query csv file chunk {num_pages} stored {num_items} items.."
            )

            yield payload

    def process_csv_chunk(self, chunk, workspace_id, query_id, page_id):
        """
        Must be implemented by subclass.
        Assumes input is the page result extracted from csv
        and ready to be split by lines.
        Parse each item of the 'data' array into a Raw Store
        and return as a payload array. Trying to delay parsing
        until later in the pipeline, but do need id fields.

        """
        raise NotImplementedError
