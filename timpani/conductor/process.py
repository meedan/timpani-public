#!/usr/local/bin/python
import argparse
import json
import requests
import time
import datetime
import sentry_sdk
from datetime import timezone
from concurrent.futures import ThreadPoolExecutor
from timpani.app_cfg import TimpaniAppCfg
from timpani.raw_store.store_factory import StoreFactory
from timpani.raw_store.store import Store
from timpani.raw_store.item import Item
from timpani.content_store.content_store import ContentStore
from timpani.content_store.content_item import ContentItem
from timpani.content_store.item_state_model import ContentItemState
from timpani.processing_sequences.workflow_manager import WorkflowManager
from timpani.processing_sequences.workflow import Workflow
from timpani.workspace_config.workspace_cfg_manager import WorkspaceConfigManager
from timpani.util.exceptions import UnuseableContentException
from timpani.vector_store.alegre_store_wrapper import (
    AlegreVectorStoreService,
)
from timpani.conductor.actions.clustering import AlegreClusteringAction

from timpani.conductor.process_state import ProcessState

from timpani.util.metrics_exporter import TelemetryMeterExporter
import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()


class ContentProcessor(object):
    """
    Script for managing the processing of content items and clusters.
    This actions are normally triggered via the orchestra web app,
    but should also be callable from commandline and docker container for
    debugging, backfill, etc.

    Theory of operation: Each of the functions are intended to run as independent
    processes (possibly concurrently). The underlying data state is expected to
    maintain consistency across failures/restarts due to the associated ItemStateModel
    (see ItemStateModel docs).

    For examble, when a workflow is being processed, a newly ingested item in the 'ready'
    state may be sent to be vectorized.  The last step of the vectorization process
    is to update the state to 'vectorized'.  If the system crashes (or vectorization
    service is down) the item will still be in the 'ready' state, so will be picked up
    again and retried on the next processor iteration.

    `process_raw_content()` -  import data and establish state model in 'ready' state. This will be
    triggered externally as new data arrives. Optionally triggers an indirect call to process_workflows()

    `batch_process_workflows()` - iteratively try to route content items to the destination
    until items are in completed/faild states or max iterations are exceeded. Should
    be called by an orchestrator process on a regular cadence. Content is processed by looping
    over the set of (unordered) states defined in the workflow's state model and querying
    for the items in batches corresponding to the state.  The workflow is checked to
    determine if the action corresponding to the state accepts a batch of items. If so, the
    items are  dispatched in a single call, otherwise in parallel set of individual calls.


    `process_clusters()` - maintain overall cluster consistency by comparing and
    reevaluating clusters acording to cluster priority scores.


    `process_summary()` - status summary of state counts in the system. Can be called
    any time for diagnostics.

    `process_expired_items()` - Removes data in completed state that has aged
    past the retention window. Intended to be called orchestrator process (daily?)



    TODO: Split into multiple files based on the type of operation to simplify
    CLI args?
    """

    """
    maintain a mapping of workspace_id's to content processing workflows
    TODO: this should probably live in workflow config
    """

    app_cfg = TimpaniAppCfg()
    telemetry = TelemetryMeterExporter(service_name="timpani-conductor")
    workspace_workflows = None
    workspace_cfgs = WorkspaceConfigManager()
    content_store = None
    raw_store = None
    vector_store = None
    clustering_action = None

    def __init__(
        self,
        content_store: ContentStore = None,
        raw_store: Store = None,
        vector_store=None,
    ):
        if content_store is None:
            # get a connection to the content store db
            # which will default to appropriate endpoints in env
            self.content_store = ContentStore()
            self.content_store.init_db_engine()
        else:
            self.content_store = content_store

        if raw_store is None:
            # get a connection to the raw content store
            self.raw_store = StoreFactory.get_store(self.app_cfg)
        else:
            self.raw_store = raw_store

        if vector_store is None:
            # TODO: eventually need a factory pattern for these
            self.vector_store = AlegreVectorStoreService()
        else:
            self.vector_store = vector_store

        self.workspace_workflows = WorkflowManager(content_store=self.content_store)

        # initialize the sentry error tracking integration
        sentry_sdk.init(
            dsn=self.app_cfg.sentry_sdk_dsn,
            environment=self.app_cfg.deploy_env_label,
            traces_sample_rate=1.0,
        )

        # initialize the metrics
        self.content_items_loaded_metric = self.telemetry.get_counter(
            "items.loaded",
            "number of individual content items extracted into content_store",
        )
        self.content_items_load_errors_metric = self.telemetry.get_counter(
            "items.errors",
            "number of individual content items in error state from extraction",
        )

        self.states_dispatched_metric = self.telemetry.get_counter(
            "states.dispatched",
            "number of times next transition called on content item states",
        )
        self.states_errors_metric = self.telemetry.get_counter(
            "states.error", "number of content items transition to error state"
        )
        self.content_items_removed_metric = self.telemetry.get_counter(
            "items.removed",
            "number of expired content items removed content_store",
        )

    def process_raw_content(
        self,
        partition_id: Store.Partition,
        workflow_id=None,
        run=None,
        force_overwrite=False,
        trigger_workflow=False,
    ):
        """
        Get the chunks of data from a single partition in the raw_store
        create content_item(s) for each raw store item
        according to the processing sequence defined
        in that corresponding workspace's workflow and write it
        into the database, ready to be picked up for processing.

        Content that fails to parse will still be loaded into
        the database but in a failed state
        """
        if run is None:
            run = ProcessState("process_partition")

        # TODO: state model for this process
        workspace_id = partition_id.workspace_id
        chunks_read = 0
        raw_items_read = 0
        content_items_inserted = 0
        content_items_errors = 0

        # get the workspace id corresponding to the content
        if workflow_id is None:
            # get the workflow for the workspace id from the config and instantiate
            workflow = self._workflow_from_workspace_id(workspace_id)
            workflow_id = workflow.get_name()
        else:
            workflow = self.workspace_workflows.get_workflow(workflow_id)

        # loop over each chunk in the bucket
        # TODO: what if we don't want to include source_id and just parse all?
        run.start_run(
            workspace_id=workspace_id,
            source_name=partition_id.source_id,
            query_id=f"{partition_id}",
            date_id=partition_id.date_id,
        )
        self.content_store.record_process_state(run)
        logging.info(
            f"Loading chunks of raw content items from partition {partition_id}"
        )
        try:
            for chunk in self.raw_store.fetch_chunks_in_partition(partition_id):
                chunks_read += 1
                # s3://timpani-raw-store-qa/content_items/date_id=20230704/
                # update state
                # TODO: parallelize this
                # loop over each raw_content item in the chunk
                for row in chunk.splitlines():
                    raw_item = Item.fromJSON(row)
                    raw_items_read += 1
                    assert raw_item.workspace_id == workspace_id
                    # ask the workflow how to extract that content
                    # (may return multiple items)
                    content_items = workflow.extract_items(
                        raw_item, partition_id.date_id
                    )
                    # TODO: store a reference to the original object path?
                    for item in content_items:
                        # initialize a state model with appropriate states
                        state = workflow.get_state_model()
                        item_ready = self._insert_content_item(
                            item, state, force_overwrite
                        )
                        if not item_ready:
                            content_items_errors += 1
                            # record error metrics for system health
                            self.content_items_load_errors_metric.add(
                                1,
                                attributes={
                                    "workspace_id": partition_id.workspace_id,
                                    "source_id": partition_id.source_id,
                                    "workflow_id": workflow_id,
                                },
                            )
                        content_items_inserted += 1
                        # record metrics for system health
                        self.content_items_loaded_metric.add(
                            1,
                            attributes={
                                "workspace_id": partition_id.workspace_id,
                                "source_id": partition_id.source_id,
                                "workflow_id": workflow_id,
                            },
                        )

            # case where partition was empty or misspecified
            if chunks_read == 0:
                logging.warning(f"No chunks were read from {partition_id}")
                run.transitionTo(run.STATE_FAILED)
                self.content_store.record_process_state(run)
            else:
                logging.info(
                    f"Loaded {chunks_read} chunks,  {raw_items_read} raw items, yielding {content_items_inserted}"
                    + f" content items and {content_items_errors} errors"
                )
                if trigger_workflow is True:
                    # Ask the conductor service to kick off an ingest for this workspace
                    # This will run as a seperate process (if not already running)
                    # and will indirectly call process_workflows
                    logging.info(f"Requesting processing of workspace {workspace_id}")
                    # /start_workflow/<workspace_id>
                    url = f"{self.app_cfg.timpani_conductor_api_endpoint}/start_workflow/{workspace_id}"
                    callback_response = requests.get(url)
                    if callback_response.ok is not True:
                        logging.warning(
                            f"Unable to process response from Timpani conductor workflow trigger {url} {callback_response.text}"
                        )

                run.transitionTo(run.STATE_COMPLETED)
                self.content_store.record_process_state(run)

        # TODO: handle more specific exceptions and retry if recoverable
        except Exception as e:
            # handle exception so we can close state, still reraise
            msg = f"Error loading partition {partition_id}:{e}"
            logging.error(msg)
            run.transitionTo(run.STATE_FAILED)
            self.content_store.record_process_state(run)
            raise e

    def _insert_content_item(
        self, content_item: ContentItem, state: ContentItemState, force_overwrite=False
    ):
        """
        Internal function for managing states around insert,
        usually called from process_raw_content()
        """
        # ask the store to save it
        content_item = self.content_store.initialize_item(
            content_item, state, force_overwrite
        )

        if content_item is None:
            # there was probably a conflict with an existing item
            return False

        # check if it should be marked as ready or failed
        if content_item.content == "":
            # either no content, or parsing failure
            self.content_store.transition_item_state(content_item, state.STATE_FAILED)
            return False
        else:
            # ready to be picked up for processing!
            self.content_store.transition_item_state(content_item, state.STATE_READY)
        return True

    def batch_process_workflows(
        self, workspace_id, workflow_id=None, run=None, max_iterations=10000
    ):
        """
        Get the sequence of states from the workflow, then for each state
        query the content store for content items that are not
        in the completed state, and call next operation on them
        as indicated by the workflow.

        Each item's state is checked, and will be removed from batch if too
        many transitions or it is within the state timeout window (i.e. already in process)

        Operations that can be called in batch will pass a list of items in to next_state(),
        operations that don't support batch will be called in parallel with a list of 1 items each.

        If no items are availible for a batch query state, processing will slow down and back off.

        If multiple iterations don't find any content to process, it will decide that it is done.

        It will also error out if the error rate in an iteration is too high, or if it hits the
        limit on total number of iterations.
        """
        if run is None:
            run = ProcessState("batch_process_workflows")

        # get the workspace id corresponding to the content
        workspace_cfg = self.workspace_cfgs.get_config_for_workspace(workspace_id)
        if workflow_id is None:
            # get the workflow for the workspace_id from the config and instantiate
            workflow_id = (
                workspace_cfg.get_workflow_id()
            )  # TODO: workspace ought to be able to have multiple workflows

        workflow = self.workspace_workflows.get_workflow(workflow_id)
        assert workflow is not None

        # get the set of states from the workflow
        state_sequence = workflow.get_state_model().get_processing_state_sequence()
        logging.debug(
            f"processing workflow {workflow.get_name()} with state sequence {state_sequence}"
        )

        batch_size = 25
        log_interval = batch_size * 10
        max_state_error_rate = 0.25  # tolerate up to 25 percent errors
        max_state_errors = 9999  # but if more than 10k errors fail
        iteration_num = 0
        max_empty_iterations = 3
        empty_iterations = 0
        num_items_processed = 0
        state_errors = 0
        iteration_state_errors = 0

        run.start_run(
            workspace_id=workspace_id,
            source_name="all",
            query_id=f"workflow_id:{workflow_id}",
        )  # TODO; add date_id etc for logging, and to use for cluster history?
        self.content_store.record_process_state(run)
        pool = ThreadPoolExecutor(max_workers=batch_size)

        # NOTE: although we loop over states in sequence, it is expected that an item
        # may be updated async, so may not be transitioned to the next state until following iteration
        try:
            while iteration_num <= max_iterations:
                # each "iteration" is one loop through the states
                # this is limited make sure process doesn't run away (but may stop too early)
                # each batch state is a fresh call to get a list of items in appropriate state
                num_items_iteratation = 0
                num_items_skipped_iteration = 0
                state_errors += iteration_state_errors
                iteration_state_errors = 0  # reset the state errors counter for the
                # TODO: need to work with running multiple batches in parallel (currently two batch queries can reutrn the same items)
                assert (
                    iteration_num < max_iterations
                ), f"workflow processing for workspace {workspace_id} workflow {workflow_id} stopped for exceeding {max_iterations} iterations"
                iteration_num += 1
                for batch_state in state_sequence:

                    # get the set of all the in-progress items for the workspace
                    # that are in the indicated current batch state
                    logging.debug(
                        f"requesting batch of {batch_size} items in state '{batch_state}' for workflow {workflow_id}"
                    )

                    batch = list(
                        self.content_store.get_items_in_progress(
                            workspace_id=workspace_id,
                            batch_state=batch_state,  # only get items in this state
                            chunk_size=batch_size,
                        )
                    )
                    logging.debug(
                        f"batch found {len(batch)} items for state {batch_state}"
                    )

                    # remove items from batch if they are in transition, have timed out or too many attempts
                    # NOTE: this is here instead of in selection query as timeout values can very per state per workflow
                    for item in batch:
                        state = self.content_store.get_item_state(item.content_item_id)
                        # if there are too many attempts, fail the item so we don't retry indefinitly
                        if workflow.check_state_updates_exceeded(state) is True:
                            batch.remove(item)
                            num_items_skipped_iteration += 1
                            logging.warning(
                                f"State updates exceeded {workflow.MAX_STATE_UPDATES} for item {item.content_item_id}, transitioning to failed"
                            )
                            self.content_store.transition_item_state(
                                item, ContentItemState.STATE_FAILED
                            )
                        if workflow.check_state_timeout(state) is True:
                            batch.remove(item)
                            num_items_skipped_iteration += 1
                            logging.debug(
                                f"Skipping next state for {item.content_item_id}, transition in progress or timed out"
                            )

                        # TODO: could also put items in faild state if the transition has been in progress for more than a few days

                    if len(batch) > 0:
                        # check if transitions from this state can be processed in batch
                        if workflow.is_batch_transition_from(batch_state):
                            # call with batch syntax and get back the states per item
                            try:
                                workflow_status = workflow.next_state(
                                    batch, batch_state
                                )
                                item_result = [workflow_status] * len(batch)
                                # record metrics for system health
                                self.states_dispatched_metric.add(
                                    len(batch),
                                    attributes={
                                        "workflow_id": workflow.get_name(),
                                        "from_state": batch_state,
                                    },
                                )
                            except UnuseableContentException as e:
                                # this represent content that cannot be processed futher, but may not be an error
                                # ... but we don't know which item in the batch caused it
                                logging.warning(
                                    f"Unable to process content item batch: {e}"
                                )
                                # TODO: can't put things into an error state, because maybe only one item failed?
                                self.states_errors_metric.add(
                                    len(batch),
                                    attributes={
                                        "workflow_id": workflow.get_name(),
                                        "from_state": batch_state,
                                    },
                                )
                                item_result = [Workflow.ERROR] * len(batch)

                            except Exception as e:
                                # TODO : incrementing the transition num even if prohibited transition
                                # so will eventually fail out if in a bad state - tricky because of exceptions
                                msg = f"Error in state transtion for item batch: {e}"
                                logging.warning(msg)
                                logging.exception(e)
                                item_result = [Workflow.ERROR] * len(batch)

                        # IF NOT IN BATCH, PROCESS ITEMS IN PARALLEL
                        else:
                            items_batch = []

                            for content_item in batch:
                                # batch up the items with workflow reference so we can process in parallel
                                # tuple is ugly hack to pass in two args
                                items_batch.append((content_item, workflow))
                            # pass the batch to pool to be executed
                            item_result = pool.map(
                                self._dispatch_state,
                                items_batch,
                            )
                            items_batch = []

                        # INDIVIDUALLY PROCESS RESULT STATUS FROM BATCH FOR ACCOUNTING
                        for result_code in item_result:
                            num_items_iteratation += 1

                            # record the number off errors that happend (result ==1 means error, 0 means OK)
                            if result_code == Workflow.ERROR:
                                iteration_state_errors += 1
                            elif result_code == Workflow.SKIPPED:
                                # NOTE: if there are few items left to process, and they are skipping because of timeout
                                # the loops will go too fast and will hit iteration limit before slow process completes.
                                # To prevent this we count skips. if the number of skips is equal
                                # to the size of the batch, wait a few seconds before starting the next iteration
                                num_items_skipped_iteration += 1
                            else:
                                num_items_processed += 1

                            # log progress periodcially
                            if num_items_iteratation % log_interval == 0:
                                logging.info(
                                    f"Workflow processing for workspace {workspace_id} workflow {workflow_id} has dispatched {num_items_processed}"
                                    + f" item states ({num_items_iteratation} in iteration {iteration_num}) and skipped {num_items_skipped_iteration}"
                                )

                            # check against the absolute error count
                            assert iteration_state_errors < max_state_errors, (
                                f"Workflow processing for workspace {workspace_id} workflow {workflow_id}"
                                + f"stopped for exceeding {max_state_errors} transition errors per iteration"
                            )
                            # check against the error percentage rate (but only if more than 100 so can compute)
                            if num_items_iteratation > 100:
                                error_rate = (
                                    float(iteration_state_errors)
                                    / num_items_iteratation
                                )
                                assert error_rate < max_state_error_rate, (
                                    f"Workflow processing for workspace {workspace_id} workflow {workflow_id} "
                                    + f"stopped for transition error rate {(iteration_state_errors / num_items_iteratation)} > {max_state_error_rate}"
                                )
                    # if we skipped all of the items (probably because they are waiting)
                    # (or there were no items)
                    # take a breath before starting next batch
                    if (
                        num_items_skipped_iteration > 0
                        and num_items_skipped_iteration == num_items_iteratation
                    ):
                        logging.debug(
                            f"Skipped {num_items_skipped_iteration} items in iteration {iteration_num}, pausing before next iteration"
                        )
                        time.sleep(1)

                # THIS IS THE CASE of empty iteration. We are probably done if there are lots of these
                if num_items_iteratation == 0:
                    if empty_iterations > max_empty_iterations:
                        run_duration = datetime.datetime.utcnow() - run.attempt_start
                        rate = (
                            float(num_items_processed)
                        ) / run_duration.total_seconds()
                        logging.info(
                            f"Workflow processing for workspace {workspace_id} workflow {workflow_id} finished after "
                            + f"dispatching {num_items_processed} item states with {state_errors} errors in {run_duration}"
                            + f" ({rate} items states/sec)"
                        )
                        run.transitionTo(run.STATE_COMPLETED)
                        self.content_store.record_process_state(run)
                        # assume we are done, so exit state and iteration loop
                        iteration_num = max_iterations + 1
                        break
                    empty_iterations += 1
                    # no items were found, pause before starting next iteration
                    # because more items may have returned from processing
                    # wait longer each time to back off
                    delay = empty_iterations * 1
                    logging.debug(
                        f"empty iteration, delaying {delay} seconds before next"
                    )
                    time.sleep(delay)
                else:
                    # iteration was not empty, so reset the empty iteration counter
                    empty_iterations = 0

        except Exception as e:
            # handle exception so we can close state, still reraise
            msg = f"Error processing workflow for workspace_id {workspace_id}: {e}"
            logging.error(msg)
            run.transitionTo(run.STATE_FAILED)
            self.content_store.record_process_state(run)
            raise e

    def _dispatch_state(self, content_item_workflow):
        # NOTE: if this is a blocking operation, we are stuck here until
        # the operation returns
        content_item = content_item_workflow[0]
        workflow = content_item_workflow[1]
        status = 1  # default to error
        try:
            # execute the action or call needed to transition
            # to the next state as defined by the workflow
            status = workflow.next_state([content_item])
            # record metrics for system health
            self.states_dispatched_metric.add(
                1,
                attributes={
                    "workflow_id": workflow.get_name(),
                },
            )
        except UnuseableContentException as e:
            # this represent content that cannot be processed futher, but may not be an error
            logging.warning(
                f"Unable to process content item {content_item.content_item_id}: {e}"
            )
            # put it into a failed state so we don't try to keep processing
            # TODO: or maybe we should implement an INOPERABLE state?
            self.content_store.transition_item_state(
                content_item, ContentItemState.STATE_FAILED
            )
            self.states_errors_metric.add(
                1,
                attributes={
                    "workflow_id": workflow.get_name(),
                },
            )
            status = 1

        except Exception as e:
            msg = (
                f"Error in state transtion for item {content_item.content_item_id}: {e}"
            )
            logging.warning(msg)
            logging.exception(e)
            sentry_sdk.capture_exception(e)

            status = 1
        # everything ok, so return 0
        return status

    def process_expired_items(
        self, workspace_id, window_end=None, force_window_end=False
    ):
        """
        Query the content store for content items that are not
        in the completed state, check if they are too old,
        or have had too many transitions, and put them in the
        failed state. The timedelta defining the TTL for data
        is specific to each workspace.
        NOTE: it is possible that the item may not exist in all of
        the services delete is requested from
        """

        # lookup the workspace to determine the TTL
        # get the workspace id corresponding to the content
        workspace_cfg = self.workspace_cfgs.get_config_for_workspace(workspace_id)
        # get the workflow for the workspace_id from the config and instantiate
        workflow_id = (
            workspace_cfg.get_workflow_id()
        )  # TODO: workspace ought to be able to have multiple workflows

        workflow = self.workspace_workflows.get_workflow(workflow_id)
        assert workflow is not None
        if window_end is None:
            window_end = datetime.datetime.utcnow()
        if force_window_end is True:
            # ignore the live duration and just use window end as earliest time bound
            earliest = window_end
            logging.warning(
                f"Ignoring live duration and forcing deletion of items older than {window_end}"
            )
        else:
            live_duration = workflow.get_item_live_duration()
            if live_duration is None:
                logging.warning(
                    f"No content items deleted because workflow {workflow_id} has a live duration of None"
                )
                return None
            earliest = window_end - live_duration
            logging.info(
                f"Live duration of {live_duration} found for workflow {workflow_id}. Using live window end of {window_end}"
            )

        run = ProcessState("process_expired")
        run.start_run(
            workspace_id=workspace_id,
            source_name="all",
            query_id=f"workflow_id:{workflow_id}",
        )
        self.content_store.record_process_state(run)
        try:
            logging.info(
                f"Starting removal of content items older than {earliest} in workspace {workspace_id}"
            )
            max_errors = 1000
            error_count = 0
            max_iteration_count = 100
            total_deleted_count = 0
            iteration_count = 0
            # get_items_before is limited to 10k chunk size for memory safety, so need to track an outer loop here
            while iteration_count < max_iteration_count:
                iteration_deleted_count = 0  # how man
                for item_to_delete in self.content_store.get_items_before(
                    workspace_id=workspace_id, earliest=earliest
                ):
                    item_id = item_to_delete.content_item_id
                    logging.debug(
                        f"deleting expired item_id {item_id} from workspace_id {workspace_id}"
                    )
                    # ask the vector store to delete the item
                    # NOTE: this does not validate workspace id or context
                    try:
                        self.vector_store.discard_vector_for_content_item(
                            item_to_delete
                        )
                        # delete the item from the content store
                        self.content_store.delete_item(item_to_delete)
                        iteration_deleted_count += 1
                        # record metrics for system health
                        self.content_items_removed_metric.add(
                            1,
                            attributes={
                                "workspace_id": workspace_id,
                            },
                        )
                    except Exception as e:
                        msg = f"Error deleting expired content item {item_to_delete.content_item_id}: {e}"
                        logging.error(msg)
                        sentry_sdk.capture_message(msg)
                        error_count += 1

                    assert (
                        error_count < max_errors
                    ), f"Encountered more than {max_errors} errors deleting expired content"

                if (
                    iteration_deleted_count == 0
                ):  # nothing was deleted in the last loop (or too many errors)
                    break
                else:
                    # get another loop of expired items
                    logging.info(
                        f"deleted {iteration_deleted_count} expired items from workspace {workspace_id} in iteration {iteration_count}"
                    )
                    iteration_count += 1
                    total_deleted_count += iteration_deleted_count
            assert (
                iteration_count < max_iteration_count
            ), f"exceeded maximum number of iterations ({max_iteration_count}) for processing expired items"
            logging.info(
                f"Completed deletion of {total_deleted_count} items from workspace {workspace_id} with {error_count} errors"
            )
            run.transitionTo(run.STATE_COMPLETED)
            self.content_store.record_process_state(run)
        except Exception as e:
            # handle exception so we can close state, still reraise
            msg = (
                f"Error processing expired content for workspace_id {workspace_id}: {e}"
            )
            logging.error(msg)
            run.transitionTo(run.STATE_FAILED)
            self.content_store.record_process_state(run)
            raise e

    def process_clusters(self, workspace_id):
        """
        Query the content store for clusters with high priority
        and do bookeeping operations such as computing stress,
        splitting high stress clusters, merging clusters, etc
        TODO: this only runs batch_size updates, what do we want for stopping criteria?
        Note: priority score is incremented in AlegreClusteringAction
        TODO: seems like this logic belongs inside the clustering action??
        """
        # get the clustering action/threshold for the the workspace
        workflow = self._workflow_from_workspace_id(workspace_id)
        # mot all workspaces define a threshold
        threshold = getattr(workflow, "SIMILARITY_THRESHOLD", None)
        clusterer = AlegreClusteringAction(
            self.content_store, similarity_threshold=threshold
        )

        # get the list of clusters likely needing updates
        logging.info(
            f"Starting reprocessing clusters for workspace {workspace_id} with similarity threshold {threshold}"
        )
        to_check = self.content_store.get_priority_clusters(workspace_id=workspace_id)
        # TODO: add a metric for these stats?
        num_clusters_checked = 0
        num_merges = 0
        for cluster in to_check:
            # get the exemplar
            exemplar = self.content_store.get_item(cluster.exemplar_item_id)
            # TODO: probably we should check for splits before merges?
            # get the set if items more similar than the threshold

            scored_items = self.vector_store.request_similar_content_item_ids(
                exemplar, threshold=threshold
            )
            # enforce sorting by score using the clusterer's sorting logic
            # so it will work the same way sort by score,size,id
            sorted_scored_items = clusterer.get_sorted_scored_items(scored_items)
            # check if any land in another cluster
            merged = False
            for scored_item in sorted_scored_items:
                item_id = scored_item[1]
                item = self.content_store.get_item(item_id)
                # check if it is in the same cluster
                if item.content_cluster_id != exemplar.content_cluster_id:
                    logging.info(
                        f"exemplar {exemplar.content_item_id} has similar item in cluster {item.content_cluster_id} with score {scored_item[0]}."
                        + f"Cluster {cluster.content_cluster_id} will be merged into cluster {item.content_cluster_id}"
                    )
                    self.content_store.merge_clusters(
                        cluster.content_cluster_id, item.content_cluster_id
                    )
                    merged = True
                    num_merges += 1
                    break
            if not merged:
                # if cluster has been checked, don't check again for a while
                # (if it was merged, the cluster probably deleted)
                cluster.priority_score = 0.0
                self.content_store.update_item(cluster)
            num_clusters_checked += 1
        logging.info(
            f"Cluster processing completed for {workspace_id}, checked {num_clusters_checked} clusters resulting {num_merges} merges"
        )

    def process_summary(self, workspace_id=None):
        """
        Query the content store to return a summary of states
        and counts of content items in each state
        """
        results = self.content_store.get_item_state_summary(workspace_id=workspace_id)
        results_dict = {}
        for result in results:
            results_dict[result[0]] = result[1]
        results_json = json.dumps(results_dict)
        logging.info(f"content store state summary:\n{results_json}")
        return results_dict

    def _workflow_from_workspace_id(self, workspace_id):
        # get the workspace id corresponding to the content
        workspace_cfg = self.workspace_cfgs.get_config_for_workspace(workspace_id)
        workflow_id = workspace_cfg.get_workflow_id()
        workflow = self.workspace_workflows.get_workflow(workflow_id)
        return workflow


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="TimpaniProcessor",
        description=" 'raw' - processes content from partitions in RawStore and extracts and transforms it into ContentStore\n"
        + "'workflows' - iteratively process the content for a specifc workspace according to the sequence in workspace's workflow\n"
        + "'expired' - interatively remove content older than the live duration defined in a workspace workflow",
        epilog="more details at https://github.com/meedan/timpani#readme",
    )
    parser.add_argument(
        "command",
        metavar="<command> [raw, workflows, expired, clusters, summary]",
        help="the processing command to run: import 'raw' data, start 'workflows', remove 'expired' items, evaluate and update 'clusters'",
    )
    parser.add_argument(
        "-w",
        "--workspace_id",
        help="workspace_id slug component of partition id or workflow to process for",
        required=True,
    )
    parser.add_argument(
        "-d",
        "--date_id",
        help="UTC date id <YYYMMDD> partition to process data from for",
        required=False,
    )
    parser.add_argument(
        "-s",
        "--source_id",
        help="content source id component of partition to process data from for",
        required=False,
    )
    parser.add_argument(
        "-f",
        "--force_overwrite",
        help="when True, during raw import items with matching raw_content_id will be overwritten instead of ignored.\n"
        + "When processing expired items, date_id or current time will be forced as time window and all older content removed",
        required=False,
        default=False,
        type=bool,
    )
    parser.add_argument(
        "-t",
        "--trigger_workflow",
        help="when true, signal to the Conductor to start workspace workflow processing when raw ingest complete",
        required=False,
        default=False,
        type=bool,
    )

    args = parser.parse_args()
    logging.info("Starting Content Processing with args:{}".format(args))
    logging.info(f"processing command: {args.command}")

    processor = ContentProcessor()
    if args.command == "raw":
        assert (
            args.date_id is not None
        ), "partition date_id is required for processing raw content"
        if args.source_id is not None:
            source_ids = [args.source_id]
        else:
            # look up the list of source_ids that the workspace imports
            cfg = processor.workspace_cfgs.get_config_for_workspace(args.workspace_id)
            source_ids = cfg.get_content_source_types()

        # loop over the source_ids and try to pull from that partition
        for source_id in source_ids:
            # form may depend on how we detect partition updates
            partition = Store.Partition(args.workspace_id, source_id, args.date_id)
            processor.process_raw_content(
                partition,
                force_overwrite=args.force_overwrite,
                trigger_workflow=args.trigger_workflow,
            )
        # report on the number of items ready
    elif args.command == "workflows":
        assert args.workspace_id is not None
        processor.batch_process_workflows(workspace_id=args.workspace_id)
    elif args.command == "summary":
        processor.process_summary(workspace_id=args.workspace_id)
    elif args.command == "clusters":
        processor.process_clusters(workspace_id=args.workspace_id)
    elif args.command == "expired":
        window_end = None
        if args.date_id is not None:
            window_end = datetime.datetime.strptime(args.date_id, "%Y%m%d").replace(
                tzinfo=timezone.utc
            )
        # because force overwrite could massively delete data, require interactive confirmation
        ignore_workspace_settings_and_force_delete = False
        if args.force_overwrite:
            env = processor.app_cfg.deploy_env_label
            print(
                f"\nCONFIRMATION: DELETE ALL CONTENT older than {window_end} for workspace {args.workspace_id} in the {env} environment? (yes/no)"
            )
            choice = input()
            if choice.lower() == "yes":
                ignore_workspace_settings_and_force_delete = True
            else:
                assert False, "Force delete of old content canceled."

        processor.process_expired_items(
            workspace_id=args.workspace_id,
            window_end=window_end,
            force_window_end=ignore_workspace_settings_and_force_delete,
        )
    else:
        assert False, f"Processing command {args.command} is not yet supported"
