import json
import requests
from urllib3.exceptions import ProtocolError
import math
import sentry_sdk

from timpani.workspace_config.workspace_config import WorkspaceConfig
from timpani.content_sources.content_source import ContentSource
from timpani.util.ssm import AccessSSM
from timpani.raw_store.store import Store
from timpani.raw_store.item import Item
from timpani.util.run_state import RunState

import timpani.util.timpani_logger
from timpani.util.metrics_exporter import TelemetryMeterExporter

logging = timpani.util.timpani_logger.get_logger()


class JunkipediaContentSource(ContentSource):
    """
    Request content from the junkipedia API and cache it to raw store

    https://docs.junkipedia.org/reference-material/api/using-your-api-key

    NOTE: Junkipedia has a 10k limit in the number of records that it
    can page through, due to Elastic Search defaults

    """

    telemetry = TelemetryMeterExporter(service_name="timpani-booker")
    request_count_metric = telemetry.get_counter(
        # NOTE: this is *not* the number of items requested
        "content.requests",
        "number of individual requests made to the content source",
    )

    # this is the suffix of key used to store the junkipedia api key
    JUNKIPEDIA_API_TOKEN_KEY = "junkipedia_access_token_secret"

    # this also determines API version
    JUNKIPEDIA_API_BASE_URL = "https://www.junkipedia.org/api/v1"

    JUNKIPEDIA_API_PAGE_SIZE = 100  # could reduce/tune this if content big

    # hard limit on how far we will keep paging into API
    MAX_API_PAGE_REQUESTS = 5000

    # daily requests will broken into sub queries to try to avoid hitting
    # junkipeidia limits
    NUM_SUB_TIME_BINS = 1  # do it all in one bin

    def get_source_name(self):
        return "junkipedia"

    def acquire_new_content(
        self,
        workspace_cfg: WorkspaceConfig,
        store_location: Store,
        run_state: RunState,
        partition_id: Store.Partition = None,
        limit_downloads: bool = False,
    ):
        """
        * TODO: Determine if there is new content since last acquistion
        * Time filters to limit query to appropriate range
        https://docs.junkipedia.org/reference-material/api/query-string-parameters/time-filters
        Unix epoch time stamps
        > Omit both of these parameters to accept a system default time range
        > of recent posts. As of November 2022, the system default time range covers the last
        > seven days of recent posts.
        For now, the default way of specifying the time interval is by the date_id
        which is a YYYYMMDD string used as a storage key that implies a UTC day,
        with a corresponding time range for publishing timestamps to fetch

        * Pull queries from the workspace config
        * Pull API key from workspace config (actually AWS SSM secret)
        * Execute query (TODO: retry?)
        * Crude validation of response
        * Page through API results
        * break open result chunks and write to object store with meta data
        * Count sucesses and failures
        * TODO: restart failed query
        * TODO: track state https://meedan.atlassian.net/browse/CV2-3009
        * TODO: ratelimit

        NOTE: junkipedia doesn't seem to be tracking query state, so could get
        inconsistant results if list content changes while paging or if
        retrying the query.
        """
        self.run_state = run_state
        # unpack some variables stored in the run state
        single_query_id = run_state.query_id
        time_range_start = run_state.time_range_start
        time_range_end = run_state.time_range_end

        # work out partition ids and batch job ids
        # TODO: partitioning should be more strictly managed?

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

        # decrypt the appropriate api key for workspace and environment
        # NOTE: each workspace may have a different API key, these are
        # fetched at call time not deploy
        ssm = AccessSSM()
        api_secret_key = ssm.get_secret_for_workspace(
            self.JUNKIPEDIA_API_TOKEN_KEY, workspace_cfg
        )
        assert api_secret_key is not None

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

        # TODO: pool of workers to process each query indepenently
        for query_id, query in queries.items():
            # if we are only doing a single query, check if it is the one we are looking for
            if single_query_id is not None:
                if query_id != single_query_id:
                    # skip this query
                    break

            # default is do the time range in 1 bin
            num_sub_bins = self.NUM_SUB_TIME_BINS
            # run a test query to check how many items will need to process
            test_query_url = self.construct_junkipedia_url(
                query, time_range_start, time_range_end
            )
            with requests.get(
                test_query_url,
                headers={
                    "Authorization": "Bearer {}".format(api_secret_key),
                    "User-Agent": "Meedan Timpani/0.1 (Booker)",  # TODO: cfg should know version
                },
            ) as r:
                # raise execption for https status codes 404, etc
                if r.status_code >= 400:
                    logging.error(r.text)
                r.raise_for_status()
                test_obj = json.loads(r.text)
                total_items = self.get_progress_info(test_obj)[
                    0
                ]  # total items is included in a meta field
                if total_items > 2000:
                    num_sub_bins = math.ceil(total_items / 1000.0)

            # default is do the time range in 1 bin
            # num_sub_bins = workspace_cfg.get_num_query_bins()
            bin_duration = (time_range_end - time_range_start) / num_sub_bins
            bin_start = time_range_start
            bin_end = time_range_start + bin_duration

            logging.info(
                f"query is expected to return {total_items} items, will fetch in {num_sub_bins} sub intervals of duration {bin_duration}"
            )

            while bin_end <= time_range_end:
                # construct the query url appropriate for API and query
                query_url = self.construct_junkipedia_url(query, bin_start, bin_end)

                logging.info(
                    "Downloading junkipedia content for query_id {0} from {1}".format(
                        query_id, query_url
                    )
                )
                self.request_count_metric.add(
                    1,
                    attributes={
                        "workspace_id": partition_id.workspace_id,
                        "source_id": partition_id.source_id,
                    },
                )

                # loop the payload iterator as it yields data
                for payload in self.process_junkipedia_query_url(
                    workspace_id=workspace_cfg.get_workspace_slug(),
                    query_id=query_id,
                    query_url=query_url,
                    api_secret_key=api_secret_key,
                    limit_downloads=limit_downloads,
                ):
                    # TODO: Need to translate encoding?
                    # Seeing \u043f\u043e\u0434\u0434 in response instead of raw utf8 ucharachters

                    # cache the data to the Raw Store
                    store_location.append_chunk(partition_id, payload)

                # start the text time bin
                bin_start = bin_end
                bin_end += bin_duration

            # TODO: track success/failure state per query id
            logging.info(
                "Acquired junkipedia content for query_id {0}".format(query_id)
            )

        logging.info(
            "Completed junkipedia content aquistion for workspace_id {0}".format(
                workspace_cfg.get_workspace_slug()
            )
        )
        # TODO: report sucess failure rate

    def construct_junkipedia_url(
        self, query: str, time_range_start=None, time_range_end=None
    ):
        """
        Construct the appropriate junkipedia url syntax
        """
        query_timerange = ""
        # convert the datetime.date arguments into the format this API understands
        # ?published_at_from=1641038400&published_at_to=1641124800
        if time_range_start is not None:
            start_sec_from_epoch = time_range_start.strftime("%s")
            query_timerange = f"&published_at_from={start_sec_from_epoch}"
            if time_range_end is not None:
                end_sec_from_epoch = time_range_end.strftime("%s")
                query_timerange += f"&published_at_to={end_sec_from_epoch}"
        logging.info(
            f"Querying for content published between {time_range_start} and {time_range_end}"
        )

        # construct the query url appropriate for API and query
        # https://www.junkipedia.org/apidocs#tag/Lists/operation/findPosts
        # including increasing default pagination from 10 for more throughput
        # i.e. https://www.junkipedia.org/api/v1//posts?lists=1911,4224?per_page=100
        query_url = (
            self.JUNKIPEDIA_API_BASE_URL
            + query
            + "&per_page={}".format(self.JUNKIPEDIA_API_PAGE_SIZE)
            + query_timerange
        )

        return query_url

    def process_junkipedia_query_url(
        self,
        workspace_id: str,
        query_id: str,
        query_url: str,
        api_secret_key: str,
        limit_downloads: bool = False,
    ):
        """
        Manage the processing of a single API call url (pagination, chunking, storage)
        goal is to be able to use this to re-run a failed download

        https://docs.junkipedia.org/reference-material/api/query-string-parameters/pagination
        """
        # set request headers with API secret: Authorization: Bearer Xyz123ApiKey
        headers = {
            "Authorization": "Bearer {}".format(api_secret_key),
            "User-Agent": "Meedan Timpani/0.1 (Booker)",  # TODO: cfg should know version
        }

        # start with original url and then loop pagination using next urls if there are any
        next_query = query_url
        num_pages = 0
        num_items = 0
        max_retries = 3

        while next_query is not None:
            # so we don't get stuck in loop if something wrong or pull down the entire DB
            assert num_pages < self.MAX_API_PAGE_REQUESTS
            # if we are limiting the number of requests for testings purposes, only download first page
            if limit_downloads and num_pages >= 1:
                logging.warning(
                    "Aborting download after first page because limit_downloads is set"
                )
                break

            # reset the retry country
            retries = 0
            while retries < max_retries:
                try:
                    with requests.get(next_query, headers=headers, timeout=60) as r:
                        # raise execption for https status codes 404, etc
                        r.raise_for_status()

                        # assume that each request is limited in size by API pagniation
                        # so we don't need to stream=True buffer bytes and can load into memory
                        page_obj = json.loads(r.text)
                        # check if there is an error or other issue so we don't store that
                        # will raise exception if problems
                        self.check_response_status(page_obj)

                        # figure out if there will be another page
                        next_query = self.get_next_query_url(page_obj)
                        # chunk it up and convert to raw store items
                        payload = self.process_response_page(
                            page_obj, workspace_id, query_id, num_pages
                        )
                        progress = self.get_progress_info(page_obj)
                        # TODO: validate num items extract against what API claims it delivered
                        num_pages += 1
                        num_items += len(payload)
                        logging.info(
                            "Processed query page {0} stored {1} of {2} items..".format(
                                num_pages, num_items, progress[0]
                            )
                        )
                        # exit retry block
                        retries = max_retries + 1
                        yield payload
                except ProtocolError as e:
                    # TODO: we could add other specific exceptions here
                    logging.error(e)
                    logging.warning(f"Retrying query {next_query}")

                    retries += 1
                    if retries > max_retries:
                        err = f"Execeeded {max_retries} for query {next_query}"
                        logging.error(err)
                        sentry_sdk.capture_exception(e)

                        # break the query loop
                        next_query = None
                        break

    def process_response_page(self, page_obj, workspace_id, query_id, page_id):
        """
        Assumes input is the page result parsed as json.
        Parse each item of the 'data' array into a Raw Store
        and return as a payload array. Trying to delay parsing
        until later in the pipeline, but do need id fields.

        ```
        {
        "data": [
            {
                "id": "118550062",
                "type": "posts",
                "attributes": {
                ...
        ```
        """
        payload = []
        for raw_item in page_obj["data"]:
            # try to find the junkipedia id in the data
            # TODO: are these ids unique? where is the spec?
            content_id = raw_item["id"]
            item = Item(
                run_id=self.run_state.run_id,
                workspace_id=workspace_id,
                source_id=self.get_source_name(),
                query_id=query_id,
                page_id=page_id,
                content_id=content_id,
                content=raw_item,
            )
            payload.append(item)
        return payload

    def get_progress_info(self, page_obj):
        """
        posts_count is index of last post
        ```
           "meta": {
                "total_posts": 125,
                "posts_count": 10
            },
            ```
        """
        total_items = page_obj["meta"]["total_posts"]
        last_index = page_obj["meta"]["posts_count"]
        return [total_items, last_index]

    def get_next_query_url(self, page_obj):
        """
        Parse out the url for the next query if there is any.
        ```
        {
            ...
            "links": {
                "prev": null,
                "next": "https://www.junkipedia.org/api/v1/posts?list_ids%5B%5D=all-junkipedia\u0026lists=1911%2C4224\u0026page=2"
            },
            ...
        ```

        """
        # TODO: also parse the x of y info to indicate progress
        next_query = page_obj["links"]["next"]
        return next_query

    def check_response_status(self, response_json):
        """
        Look for valid payloads that may indicate errors. i.e.
         {"errors":[{"code":"forbidden","status":"403",
         "title":"Error","detail":"Your request must include an API Key.","meta":{}}]}
         (however 400 and 500 error codes are caught at request level so harder to check)
        """
        if "errors" in response_json:
            msg = f"Unable to store content due to error with junkipedia request: {response_json['errors']}"
            logging.error(msg)
            raise ValueError(msg)
        # spec is here, but not easily parseble
        # https://www.junkipedia.org/apidocs#tag/Issues/operation/findIssues
        # NOTE: 'included' element seems to be optional
        if "data" not in response_json:
            msg = "Junkipedia API response missing 'data' payload"
            logging.error(msg)
            raise ValueError(msg)

        if "links" not in response_json:
            msg = "Junkipedia API response missing 'links' pagination element "
            logging.error(msg)
            raise ValueError(msg)
        return True
