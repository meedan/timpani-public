#!/usr/local/bin/python
import argparse
import time
import timpani.util.timpani_logger

from timpani.app_cfg import TimpaniAppCfg
from timpani.raw_store.cloud_store import CloudStore
from timpani.content_store.content_store import ContentStore
from timpani.raw_store.store import Store
from timpani.util.run_state import RunState
from timpani.conductor.process import ContentProcessor

from timpani.workspace_config.workspace_cfg_manager import WorkspaceConfigManager
from timpani.workspace_config.test_workspace_cfg import TestWorkspaceConfig
from timpani.content_sources.faker_test_content_source import FakerTestingContentSource

# from timpani.processing_sequences.default_workflow import DefaultWorkflow

logging = timpani.util.timpani_logger.get_logger()


class ConductorBenchmark(object):
    """
    Script for assessing throughput of various parts of the conductor system.
    This is is intended to be run in QA and live systems, so all operations
    must be non-destructive to data outside the test.
    `python3 -m timpani.conductor.benchmark clustering`

    or
    `docker compose run conductor benchmark clustering`
    """

    # TODO: does the rate change over the course fo the test or as the number of records increase

    app_cfg = TimpaniAppCfg()
    test_cfg = TestWorkspaceConfig()
    store = ContentStore()
    engine = store.init_db_engine()
    cfg_manager = WorkspaceConfigManager()

    def clean_up(
        self,
        content_store: ContentStore,
        raw_store: Store,
        partition: Store.Partition,
        force_delete=False,
    ):
        """
        delete the records created by this test (or previous runs) in the content store
        TODO: cloud store records not deleted
        """

        # refuse to delete 'real' workspaces in live unless forced
        if (
            partition.workspace_id in self.cfg_manager.get_all_workspace_ids()
            and partition.workspace_id != "test"
        ):
            if self.app_cfg.deploy_env_label == "live":
                assert (
                    force_delete is True
                ), f"in Live environment, workspace {partition.workspace_id} can only be deleted with --force_delete argument "

        logging.info("Deleting benchmark records from previous run from content store")

        content_store.erase_workspace(
            workspace_id=partition.workspace_id, source_id=partition.source_id
        )
        # TODO: could use query id to make content specific to this test to avoid colisions
        # between tests
        logging.info(
            f"Deleting benchmark records from previous run from raw store for partition {partition}"
        )
        # need to delete content from previous run in raw_store
        raw_store.delete_partition(partition)
        logging.info("Done cleaning up benchmark data")

    def benchmark_workflows(self, test_size, force_delete=False):
        """
        Load fake raw data and toggle from ready to completed state as quickly as possible
        (no computation performed)
        """

        # create a bunch of fake data
        fake_source = FakerTestingContentSource(
            total_items=test_size, page_size=min(test_size, 100)
        )

        run_state = RunState(
            "benchmark",
        )

        raw_store = CloudStore()
        raw_store.login_and_validate(
            access_key=self.app_cfg.minio_user,
            secret_key=self.app_cfg.minio_password,
        )

        partition = Store.Partition(
            self.test_cfg.get_workspace_slug(),
            fake_source.get_source_name(),
            run_state.date_id,
        )

        # clean up any records from previous test
        self.clean_up(
            content_store=self.store,
            raw_store=raw_store,
            partition=partition,
            force_delete=force_delete,
        )
        logging.info("Starting benchmark")
        benchmark_start = time.time()

        logging.info("loading benchmark data into raw store")
        raw_store_load_start = time.time()
        # load a bunch of fake data into raw store
        fake_source.acquire_new_content(self.test_cfg, raw_store, run_state=run_state)

        processor = ContentProcessor(content_store=self.store)
        logging.info("loading benchmark data into raw store")
        content_store_load_start = time.time()
        processor.process_raw_content(partition)

        workflow_start = time.time()
        processor.process_workflows(
            workspace_id=self.test_cfg.get_workspace_slug(),
            # this runs the DefaultWorkflow that just does clustering
            workflow_id="default_workflow",
        )
        benchmark_end = time.time()

        results = {
            "date": partition.date_id,
            "benchmark_size": test_size,
            "benchmark_duration": benchmark_end - benchmark_start,
            "benchmark_rate": (benchmark_end - benchmark_start) / test_size,
            "raw_store_duration": content_store_load_start - raw_store_load_start,
            "raw_store_rate": (content_store_load_start - raw_store_load_start)
            / test_size,
            "content_store_duration": workflow_start - content_store_load_start,
            "content_store_rate": (workflow_start - content_store_load_start)
            / test_size,
            "workflow_duration": benchmark_end - workflow_start,
            "workfow_rate": (benchmark_end - workflow_start) / test_size,
        }

        logging.info(f"benchmark completed. (partition {partition})")
        logging.info(f"Benchmark results:{results}")

    def benchmark_clustering(self, test_size, force_delete=False):
        """
        Load fake raw data and go through clustering as quickly as possible.
        (fake data is not expected to cluster well tho, so probably very small clsuter sizes)
        """

        # create a bunch of fake data
        fake_source = FakerTestingContentSource(
            total_items=test_size, page_size=min(test_size, 100)
        )

        run_state = RunState(
            "benchmark",
        )

        raw_store = CloudStore()
        # TODO: this will use minio in QA?
        raw_store.login_and_validate(
            access_key=self.app_cfg.minio_user,
            secret_key=self.app_cfg.minio_password,
        )

        partition = Store.Partition(
            self.test_cfg.get_workspace_slug(),
            fake_source.get_source_name(),
            run_state.date_id,
        )

        # clean up any records from previous test
        self.clean_up(
            content_store=self.store,
            raw_store=raw_store,
            partition=partition,
            force_delete=force_delete,
        )
        logging.info("Starting benchmark")
        benchmark_start = time.time()

        logging.info("loading benchmark data into raw store")
        raw_store_load_start = time.time()
        # load a bunch of fake data into raw store
        fake_source.acquire_new_content(self.test_cfg, raw_store, run_state=run_state)

        processor = ContentProcessor(content_store=self.store)
        logging.info("loading benchmark data into raw store")
        content_store_load_start = time.time()
        processor.process_raw_content(partition, workflow_id="default_workflow")
        workflow_start = time.time()
        # run with the Default workflow which just does clustering
        processor.process_workflows(
            workspace_id=self.test_cfg.get_workspace_slug(),
            workflow_id="default_workflow",
        )
        benchmark_end = time.time()

        results = {
            "date": partition.date_id,
            "benchmark_size": test_size,
            "benchmark_duration": benchmark_end - benchmark_start,
            "benchmark_rate": (benchmark_end - benchmark_start) / test_size,
            "raw_store_duration": content_store_load_start - raw_store_load_start,
            "raw_store_rate": (content_store_load_start - raw_store_load_start)
            / test_size,
            "content_store_duration": workflow_start - content_store_load_start,
            "content_store_rate": (workflow_start - content_store_load_start)
            / test_size,
            "workflow_duration": benchmark_end - workflow_start,
            "workfow_rate": (benchmark_end - workflow_start) / test_size,
        }

        logging.info(f"benchmark completed. (partition {partition})")
        logging.info(f"Benchmark results:{results}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="TimpaniConductorBenchmark",
        description="Process data through various components of the Timpani system to understand performance limits.\n"
        + "'workflows' - create fake data, ingest into content store, and move from ready to completed-no action",
        epilog="more details at https://github.com/meedan/timpani#readme",
    )
    parser.add_argument(
        "command",
        metavar="<command> [workflows, clustering, clean_up]",
        help="the benchmark command to run",
    )
    parser.add_argument(
        "-t",
        "--test_size",
        help="when true, signal to the Conductor to start workspace workflow processing when raw ingest complete",
        required=False,
        default=100,
    )
    parser.add_argument(
        "-f",
        "--force_delete",
        help="active workspaces (other than 'test') can only be deleted in Live if this is true",
        required=False,
        default=False,
    )
    args = parser.parse_args()
    logging.info("Starting Benchmark Processing with args:{}".format(args))
    logging.info(f"processing command: {args.command}")
    benchmark = ConductorBenchmark()
    if args.command == "workflows":
        benchmark.benchmark_workflows(
            test_size=int(args.test_size), force_delete=args.force_delete
        )
    elif args.command == "clustering":
        benchmark.benchmark_clustering(
            test_size=int(args.test_size), force_delete=args.force_delete
        )
    elif args.command == "clean_up":
        benchmark.clean_up(force_delete=args.force_delete)
    else:
        print(f"Unknown command: {args.command}")
