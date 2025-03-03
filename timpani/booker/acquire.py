import argparse
import datetime
from datetime import timezone
import requests
import sentry_sdk
from timpani.app_cfg import TimpaniAppCfg
from timpani.workspace_config.workspace_cfg_manager import WorkspaceConfigManager

from timpani.raw_store.store import Store
from timpani.raw_store.minio_store import MinioStore
from timpani.raw_store.debugging_file_store import DebuggingFileStore
from timpani.raw_store.cloud_store import CloudStore
from timpani.content_sources.junkipedia_content_source import JunkipediaContentSource
from timpani.content_sources.aws_s3_csv_aapi_tweets_source import (
    AWSS3CSVAAPITweetsContentSource,
)
from timpani.content_sources.aws_s3_csv_tse_tipline_source import (
    AWSS3CSVTSETiplineContentSource,
)
from timpani.content_sources.faker_test_content_source import FakerTestingContentSource
from timpani.util.run_state import RunState

import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()


class AcquisitionOrchestrator(object):
    """
    Eventually this will be a service to 'book' and archive content for workspaces,
    but right now it is just a script

    execute with :

    ```
    python3 -m  scripts.acquire --workspace_id=meedan
    ```
    """

    app_cfg = TimpaniAppCfg()

    REGISTERD_CONTENT_SOURCES = [
        JunkipediaContentSource,
        AWSS3CSVAAPITweetsContentSource,
        AWSS3CSVTSETiplineContentSource,
        AWSS3CSVStopAAPIHateContentSource,
        FakerTestingContentSource,
    ]
    REGISTERED_RAW_STORES = [MinioStore, DebuggingFileStore, CloudStore]
    JOB_NAME = "booker_acquire"

    content_sources = {}
    workspace_cfgs = WorkspaceConfigManager()
    # list of workspaces (tests and oneoff) that should be ignored when running 'all' workspaces
    SKIP_WHEN_RUNNING_ALL = [
        "meedan-lite",  # for testing
        "meedan_tse",  # one-off import
        "meedan_aapi",  # one-off import
        "junkipedia_public",  # permissions problems on junkipedia side, not working yet
        "test",  # yup, its a test
        "meedan_classy_india_election",  # temporarily disabled to use less LLM credits
        "meedan_nawa_gaza",
        "meedan_india_election",  # election completed
        "meedan_us2024",  # election completed
    ]

    # list of workspaces that can run in test and QA, but disabled from running in live
    # kind of like a crude feature flag
    # TODO: these should become properties of the workspaces, not listed here
    SKIP_IN_LIVE = [
        "meedan_classy_india_election",  # Still being tested out, not ready for Live
        "meedan_classy_us2024",  # Still being tested out, not ready for Live
        "meedan_nawa_gaza",  # disabled because large result set causing heap errors in open search
    ]
    runs = []

    def load_workspace_configs_and_sources(self):
        # also load in the content sources
        # TODO: replace this with a content source manageer
        for cls in self.REGISTERD_CONTENT_SOURCES:
            source = cls()
            name = source.get_source_name()

            # make sure we don't have two sources with same id
            assert name not in self.content_sources

            self.content_sources[name] = source

    def acquire_all(
        self,
        time_range_start=None,
        time_range_end=None,
        date_id=None,
        trigger_ingest=False,
        limit_downloads=False,
    ):
        """
        Get the releveant updates for all the workspaces, datasources, and registred queries
        Defaulting to last full day (yesterday)
        """
        for workspace_id in self.workspace_cfgs.get_all_workspace_ids():
            if workspace_id in self.SKIP_WHEN_RUNNING_ALL:
                logging.info(f"Skipping workspace {workspace_id}")
            elif (
                self.app_cfg.deploy_env_label == "live"
                and workspace_id in self.SKIP_IN_LIVE
            ):
                logging.info(
                    f"Skipping workspace {workspace_id} due to SKIP_IN_LIVE list"
                )
            else:
                try:
                    # handle exception so error in one workspaces won't fail others
                    self.acquire_for_workspace(
                        workspace_id,
                        time_range_start=time_range_start,
                        time_range_end=time_range_end,
                        date_id=date_id,
                        trigger_ingest=trigger_ingest,
                        limit_downloads=limit_downloads,
                    )
                except Exception as e:
                    logging.error(
                        f"Exception processing workspace {workspace_id} {date_id}:{e}"
                    )
                    logging.exception(e)

    def acquire_for_workspace(
        self,
        workspace_id: str,
        query_id=None,
        time_range_start=None,
        time_range_end=None,
        date_id=None,
        trigger_ingest=False,
        limit_downloads=False,
    ):
        """
        Get the relavent updates for a workspace, optionally subsetting
        to specific data source and query, and load it into the Raw Store.
        Type of raw store determined by `--env S3_STORE_LOCATION=s3.amazonaws.com`

        """

        workspace_cfg = self.workspace_cfgs.get_config_for_workspace(workspace_id)
        # decide which kind of store based on app config
        # can set this from docker compose via --env S3_STORE_LOCATION=s3.amazonaws.com
        # TODO: replace with version in StoreFactory for consistency

        if self.app_cfg.s3_store_location == ("DebuggingFileStore"):
            store = DebuggingFileStore()

        elif self.app_cfg.s3_store_location.startswith("minio"):
            # This is for debugging with defaults
            store = MinioStore()
            store.login_and_validate(
                self.app_cfg.minio_user, self.app_cfg.minio_password
            )
        else:
            if self.app_cfg.s3_store_location == "s3.amazonaws.com":
                # for AWS S3
                store = CloudStore(store_location=self.app_cfg.s3_store_location)
                store.login_and_validate()
            else:
                # for minio etc, read PWD from env
                store = CloudStore(store_location=self.app_cfg.s3_store_location)
                store.login_and_validate(
                    TimpaniAppCfg.minio_user, TimpaniAppCfg.minio_password
                )

        # TODO: need to decide on approprate model for default acquisition intervals
        # should it just get one day of data? There is probably lag from the source
        # and so data won't be arriving exactly when it is published.
        # also depends on the interval the job will be run.
        # TIMEZONES ARE MESSY, we can make our side UTC, but what about publish dates?
        # Current implementation defaults to fetch for 'yesterday' UTC and
        # stores to an appropriately named date_id partition

        source_types = workspace_cfg.get_content_source_types()
        # loop over the ContentSources configured for the workspace
        # TODO: each of these calls could be passed off to a seperate process
        for source_name in source_types:
            source = self.content_sources[source_name]
            run = RunState(self.JOB_NAME, date_id=date_id)
            partition_id = Store.Partition(
                workspace_cfg.get_workspace_slug(),
                source.get_source_name(),
                run.date_id,
            )
            self.runs.append(run)
            try:
                run.start_run(
                    workspace_id,
                    source_name,
                    query_id,
                    time_range_start,
                    time_range_end,
                    date_id,
                )
                store.record_partition_run_state(run, partition_id)
                source.acquire_new_content(
                    workspace_cfg, store, run, partition_id, limit_downloads
                )
                if trigger_ingest is True:
                    # call the import_content endpoint on conductor to trigger partition loading
                    # NOTE: if trigger fails, fetch will be marked as failed even if data was downloaded
                    self.trigger_import(run)
                run.transitionTo(run.STATE_COMPLETED)
                store.record_partition_run_state(run, partition_id)
            # TODO: handle more specific exceptions and retry if recoverable
            except Exception as e:
                # handle exception so we can close state, still reraise
                run.transitionTo(run.STATE_FAILED)
                store.record_partition_run_state(run, partition_id)
                raise e

    def trigger_import(self, run: RunState):
        """
        call the import_content endpoint on conductor to trigger partition loading
        """
        logging.info(
            f"Requesting ingest of partition {run.workspace_id} {run.source_name} {run.date_id}"
        )
        # "/import_content/<workspace_id>/<source_id>/<date_id>"
        url = f"{self.app_cfg.timpani_conductor_api_endpoint}/import_content/{run.workspace_id}/{run.source_name}/{run.date_id}?trigger_import=True"
        callback_response = requests.get(url)
        assert (
            callback_response.ok
        ), f"Unable to process response from Timpani conductor partition import request {url} {callback_response.text}"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="TimpaniBooker",
        description="Acquires and stores content from external sources",
        epilog="https://github.com/meedan/timpani#readme",
    )
    parser.add_argument(
        "-w",
        "--workspace_id",
        help="workspace_id slug, to run acquisitons for a single workspace",
    )
    parser.add_argument(
        "-q", "--query_id", help="query_id if only one acquisition should be run"
    )
    parser.add_argument(
        "-s",
        "--publish_start_day",
        help="date <YYYMMDD> (inclusive) of earliest published day to fetch",
    )
    parser.add_argument(
        "-e",
        "--publish_end_day",
        help="date <YYYMMDD> (exclusive) of latest published day to fetch until",
    )
    parser.add_argument(
        "-d",
        "--date_id",
        help="UTC date id <YYYMMDD> partition to fetch for",
    )
    parser.add_argument(
        "-t",
        "--trigger_ingest",
        help="signal to the Conductor to ingest the partition when fetched",
        required=False,
        default=False,
        type=bool,
    )
    parser.add_argument(
        "-l",
        "--limit_downloads",
        help="only fetch the first page (~100) records to reduce bandwith in qa and testing",
        required=False,
        default=False,
        type=bool,
    )
    app_cfg = TimpaniAppCfg()

    sentry_sdk.init(
        dsn=app_cfg.sentry_sdk_dsn,
        environment=app_cfg.deploy_env_label,
        traces_sample_rate=1.0,
    )

    # TODO: run_id (for resuming/rerunning failed downloads)
    args = parser.parse_args()
    print("Starting Content Acquisition Orchestrator with args:{}".format(args))

    # TODO: initialize logging and log levels

    # parse any date ranges
    start_day = None
    end_day = None

    if args.date_id is not None:
        start_day = datetime.datetime.strptime(args.date_id, "%Y%m%d").replace(
            tzinfo=timezone.utc
        )
        end_day = start_day + datetime.timedelta(days=1)

    if args.publish_start_day is not None:
        if args.date_id is not None:
            logging.warning("publish_start_day overriding date_id value")
        start_day = datetime.datetime.strptime(args.publish_start_day, "%Y%m%d")
    if args.publish_end_day is not None:
        if args.date_id is not None:
            logging.warning("publish_start_day overriding date_id value")
        end_day = datetime.datetime.strptime(args.publish_end_day, "%Y%m%d")

    booker = AcquisitionOrchestrator()
    booker.load_workspace_configs_and_sources()

    if args.workspace_id is None:
        if args.query_id is not None:
            logging.warning(
                "query_id parameter cannot be used with multiple workspaces, ignoring"
            )
        booker.acquire_all(
            time_range_start=start_day,
            time_range_end=end_day,
            date_id=args.date_id,
            trigger_ingest=args.trigger_ingest,
            limit_downloads=args.limit_downloads,
        )
    else:
        booker.acquire_for_workspace(
            workspace_id=args.workspace_id,
            query_id=args.query_id,
            time_range_start=start_day,
            time_range_end=end_day,
            date_id=args.date_id,
            trigger_ingest=args.trigger_ingest,
            limit_downloads=args.limit_downloads,
        )
