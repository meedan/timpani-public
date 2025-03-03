import argparse
import datetime
from datetime import timezone
import sentry_sdk
from timpani.app_cfg import TimpaniAppCfg
from timpani.booker.acquire import AcquisitionOrchestrator

import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()


class BackfillOrchestrator(object):
    """
    This calls booker's acquire script on a sequence of date ids
    execute with :
    ```
    python3 -m  scripts.backfill --workspace_id=meedan --start_date_id=20240228 --end_date_id=20240301
    ```
    """


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="TimpaniBookerBackfill",
        description="Acquires and stores content from external sources for a set of date_id",
        epilog="https://github.com/meedan/timpani#readme",
    )
    parser.add_argument(
        "-w",
        "--workspace_id",
        help="workspace_id slug, to run acquisitons for a single workspace",
        required=True,
    )
    parser.add_argument(
        "-q", "--query_id", help="query_id if only one acquisition should be run"
    )

    parser.add_argument(
        "-s",
        "--start_date_id",
        help="beginning UTC date id <YYYMMDD> partition to fetch for (inclusive)",
        required=True,
    )
    parser.add_argument(
        "-d",
        "--end_date_id",
        help="ending UTC date id <YYYMMDD> partition to fetch for (inclusive)",
        required=True,
    )
    parser.add_argument(
        "-t",
        "--trigger_ingest",
        help="signal to the Conductor to ingest the partition when fetched (default True)",
        required=False,
        default=True,
        type=bool,
    )
    app_cfg = TimpaniAppCfg()

    sentry_sdk.init(
        dsn=app_cfg.sentry_sdk_dsn,
        environment=app_cfg.deploy_env_label,
        traces_sample_rate=1.0,
    )

    args = parser.parse_args()
    logging.info("Starting Backfill Orchestrator with args:{}".format(args))

    # parse any date ranges

    start_date = datetime.datetime.strptime(args.start_date_id, "%Y%m%d").replace(
        tzinfo=timezone.utc
    )

    end_date = datetime.datetime.strptime(args.end_date_id, "%Y%m%d").replace(
        tzinfo=timezone.utc
    )

    # generate the sequence of date ids we will need to import for
    # need to use datetime classes because datemath are hard
    date_ids = []
    for x in range(0, (end_date - start_date).days + 1):
        date_id = start_date + datetime.timedelta(days=x)
        date_ids.append(date_id.strftime("%Y%m%d"))

    booker = AcquisitionOrchestrator()
    booker.load_workspace_configs_and_sources()

    logging.info(f"Beginning backfill process for date_id range {date_ids} ..")
    error_dates = {}
    completed_dates = []
    for date_id in date_ids:
        # form may depend on how we detect partition updates
        logging.info(
            f"Starting backfill acquire raw content process for {args.workspace_id} {args.query_id} {date_id}.."
        )
        start_day = datetime.datetime.strptime(date_id, "%Y%m%d").replace(
            tzinfo=timezone.utc
        )
        end_day = start_day + datetime.timedelta(days=1)
        try:
            if args.workspace_id is None:
                if args.query_id is not None:
                    logging.warning(
                        "query_id parameter cannot be used with multiple workspaces, ignoring"
                    )
                booker.acquire_all(
                    time_range_start=start_day,
                    time_range_end=end_day,
                    date_id=date_id,
                    trigger_ingest=args.trigger_ingest,
                )
            else:
                booker.acquire_for_workspace(
                    workspace_id=args.workspace_id,
                    query_id=args.query_id,
                    time_range_start=start_day,
                    time_range_end=end_day,
                    date_id=date_id,
                    trigger_ingest=args.trigger_ingest,
                )
            completed_dates.append(date_id)
        except Exception as e:
            logging.error(
                f"Unable to run backfill for {args.workspace_id} {args.query_id} {date_id}: {e}"
            )
            error_dates[date_id] = e

    logging.info(
        f"Completed backfill process with {len(completed_dates)} completed dates and {len(error_dates)} errors:{error_dates}"
    )
