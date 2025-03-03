import subprocess
import time
import datetime
from threading import Thread
from timpani.content_store.content_store import ContentStore

# from timpani.content_store.content_item import ContentItem
# from timpani.content_store.item_state_model import ContentItemState
from timpani.conductor.actions.clustering import AlegreClusteringAction
from timpani.vector_store.alegre_store_wrapper import AlegreVectorStoreService
from timpani.model_service.yake_presto_service import YakePrestoService
from timpani.model_service.presto_wrapper_service import PrestoWrapperService

import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()


class Orchestrator(object):
    """
    This implements the actions requested by the the web app, including directly dispatching appropriate
    commands to the various stores and services as well starting and stopping data processing state
    dispatch tasks via timpani.conductor.process.
    The seperation from the app is it make it possible to test logic and run processes
    without starting the web app (i.e. from future batch scripts).

    The orchestrator does spin off independent processes in the host environment so that they
    can be executed concurrently. These are intended to be lightweight processes that are managing
    callbacks and state updats (maybe network intensive, but not compute tasks)

    The process_mapping is intented to provide a way to keep track of running processes so
    that they can be queried for status, or stopped.

    TODO: start an instance of ProcessConductor that can trigger operations that need to happen on a schedule
    TODO: create and maintain instances of content stores, database connection pools, etc
    TODO: how do we make sure there are no previous processes running?
    TODO: restarting incomplete jobs should be handled somewhere?
    TODO: Orchestrator should just return json objects.  Any explanation text should be added by app
    TODO: the update actions seem different than the processing actions, do they belong here?
    TODO: Use celery or some other library instead of creating our own process_mapping?
    """

    content_store = None
    vector_store = None
    process_mapping = {}
    MAX_PROCESSES = 100

    def __init__(self, content_store=None, vector_store=None) -> None:
        """
        Defaults to postgres connection
        """
        if content_store is None:
            self.content_store = ContentStore()
            self.content_store.init_db_engine()
        else:
            self.content_store = content_store

        if vector_store is None:
            self.vector_store = AlegreVectorStoreService()
        else:
            self.vector_store = vector_store

        # TODO: need factory function to initialize all the actions it knows about
        # or maybe a better design is to grab a reference to the workspace
        # and use that
        self.clustering_action = AlegreClusteringAction(self.content_store)

        # TODO: factory function to initialize models/services?
        self.yake_model = YakePrestoService()

    def start_conductor(self):
        """
        TODO: start an instance of ProcessConductor that can trigger operations that need to happen on a schedule
        This is also reponsible for restarting in-progress jobs that failed due to system restart
        """
        raise NotImplementedError

    def stop_conductor(self):
        """
        TODO: stop the instance of the conductor
        """
        raise NotImplementedError

    def update_content_item_state(self, content_item_id, state):
        """
        Called by web callbacks from services to indicate that a state (such as vectorization)
        has completed for a content item
        TODO: batch
        """
        # pull a reference to the content item from the store
        int_id = int(content_item_id)
        item = self.content_store.get_item(content_item_id=int_id)
        assert (
            item is not None
        ), f"unable to find content_item in content store with id {int_id}"
        # try to update it to the requested state
        state = self.content_store.transition_item_state(item, state)
        # TODO: how do we know the appropriate state model class to use for the item?
        return state

    def update_content_item_property(
        self, content_item_id, property_name, property_value
    ):
        """
        Called from a model to set a property on the content item (i.e. language id, updading the content text, etc)
        """
        raise NotImplementedError

    def start_cluster_updates(self, workspace_id):
        """
        Triggers a process that will check the quality of clusters to determine if some should be merged or split
        """
        key = workspace_id + "_clusters"
        if key in self.process_mapping:
            # check if the process is running, and if so don't start another
            p = self.process_mapping[key]
            status = p.poll()
            if status is None:
                return (
                    f"Cannot start new cluster processing for workspace {workspace_id} "
                    + f"because a process is already running (pid {p.pid})"
                )

        # kick off a subprocess to run the item state processor
        subp = subprocess.Popen(
            [
                "python3",
                "-m",
                "timpani.conductor.process",
                "clusters",
                f"--workspace={workspace_id}",
            ]
        )
        self.process_mapping[key] = subp
        # TODO: should we wait a second and poll before returning to know if it started?
        return f"Started cluster processing on workspace {workspace_id} with pid {subp.pid}"

    def cluster_items(
        self, content_item_id: str, action_id: str, target_state: str, payload
    ):
        """
        Once we are getting callbacks, this will take the list of
        items considered for clustering, call the clustering
        action to store them
        TODO: create an 'action' interface and make this action_items?
        """
        # TODO: in theory, this might come from something other than Alegre,
        # would need to look up the appropriate action by id..
        assert (
            action_id == AlegreClusteringAction.get_name()
        ), f"Action id {action_id} did not match a known action"

        # TODO: how to configure the action appropriately for the workflow
        # maybe needs to get the workflow id from the payload document context
        #  and ask it for the reference?
        item = self.content_store.get_item(content_item_id)
        items = self.vector_store.extract_scored_item_ids_from_response(payload)
        if item is None:
            # TODO: raise exception to api will return error code?
            msg = f"Unable to locate content item id {content_item_id} in content store for clustering"
            logging.error(msg)
            return msg

        cluster = self.clustering_action.add_item_to_best_cluster(
            item=item,
            target_state=target_state,
            scored_item_ids=items,
        )
        return f"Added item {content_item_id} to cluster {cluster}"

    def start_workflow_processing(self, workspace_id, workflow_id=None):
        """
        start the process that will keep checking items and advancing them to the next state
        keep track of running workflow processes
        TODO: enable workflow per workspace
        TODO: check if it is a valid workspace id?
        """
        key = workspace_id + "_workflow"
        if key in self.process_mapping:
            # check if the process is running, and if so don't start another
            p = self.process_mapping[key]
            status = p.poll()
            if status is None:
                return (
                    f"Cannot start new workflow processing for workspace {workspace_id} "
                    + f"because a process is already running (pid {p.pid})"
                )

        # kick off a subprocess to run the item state processor
        subp = subprocess.Popen(
            [
                "python3",
                "-m",
                "timpani.conductor.process",
                "workflows",
                f"--workspace={workspace_id}",
            ]
        )
        self.process_mapping[key] = subp
        # TODO: should we wait a second and poll before returning to know if it started?
        return f"Started workflow processing on workspace {workspace_id} with pid {subp.pid}"

    def stop_workflow_processing(self, workspace_id, workflow_id=None):
        """
        terminate the process that will keep checking items and advancing them to the next state
        keep track of running workflow processes
        NOTE: this will not be able to find and stop processes started by previous orchestrator instance
        """
        key = workspace_id + "_workflow"
        assert (
            key in self.process_mapping
        ), f"No known workflow processing for workspace {workspace_id}"
        # check if the process is running, and if so don't start another
        p = self.process_mapping[key]
        status = p.poll()
        if status is None:
            p.terminate()
            return f"Stopped workflow processing for workspace {workspace_id}"
        else:
            return f"Workflow processing for workspace {workspace_id} previously stopped with status {status}"

    def start_expiration_processing(self, workspace_id):
        """
        start the process that will query for items in a workspace that have been
        around longer than the workflow's live duration window and delete them
        TODO: this is going to run with today's default date, should this be an argument?
        TODO: check if it is a valid workspace id?
        """
        key = workspace_id + "_expiration"
        if key in self.process_mapping:
            # check if the process is running, and if so don't start another
            p = self.process_mapping[key]
            status = p.poll()
            if status is None:
                return (
                    f"Cannot start new expiration processing for workspace {workspace_id} "
                    + f"because a process is already running (pid {p.pid})"
                )

        # kick off a subprocess to run the item state processor
        subp = subprocess.Popen(
            [
                "python3",
                "-m",
                "timpani.conductor.process",
                "expired",
                f"--workspace={workspace_id}",
            ]
        )
        self.process_mapping[key] = subp
        # TODO: should we wait a second and poll before returning to know if it started?
        return f"Started expiration processing on worksapce {workspace_id} with pid {subp.pid}"

    def stop_expiration_processing(self, workspace_id):
        """
        Terminate the process that will query for items in a workspace that have been
        around longer than the workflow's live duration window and delete them
        NOTE: this will not be able to find and stop processes started by previous orchestrator instance
        """
        key = workspace_id + "_expiration"
        assert (
            key in self.process_mapping
        ), f"No known expiration processing for workspace {workspace_id}"
        # check if the process is running, and if so don't start another
        p = self.process_mapping[key]
        status = p.poll()
        if status is None:
            p.terminate()
            return f"Stopped expiration processing for workspace {workspace_id}"
        else:
            return f"Expiration processing for workspace {workspace_id} previously stopped with status {status}"

    def get_processing_status(self):
        """
        return information and ids of running processes
        """
        # TODO: filter on workspace id
        # TODO: should use celery or something for this vs inventing a process management sytem
        status_map = {}
        for process_key in self.process_mapping:
            p = self.process_mapping[process_key]
            status_map[process_key] = {"pid": p.pid, "status": p.poll()}
        return status_map

    def is_new_process_allowed(self):
        """
        Returns if the number of active processes are below the MAX_PROCESSES limit
        """
        num_active = 0
        status_map = self.get_processing_status()
        # loop over all of the processes and record how many are still active
        for process_key in status_map:
            if status_map[process_key]["status"] is None:
                num_active += 1
        if num_active < self.MAX_PROCESSES:
            return True
        return False

    def start_import_processing(
        self, workspace_id, source_id=None, date_id=None, trigger=None
    ):
        """
        Start the data import process for content in a raw store partition,
        (expressed via workspace,source,date) as a sub process
        """

        key = f"{workspace_id}_{source_id}_{date_id}_import"

        if key in self.process_mapping:
            # check if an idential process is running, and if so don't start another
            p = self.process_mapping[key]
            status = p.poll()
            if status is None:
                return (
                    f"Cannot start new raw import process for partition {key} "
                    + f"because a process is already running (pid {p.pid})"
                )
        # TODO: check how many imports already running?
        # kick off a subprocess to run the raw item import processor
        cmd_with_args = [
            "python3",
            "-m",
            "timpani.conductor.process",
            "raw",
            f"--workspace={workspace_id}",
        ]
        if source_id is not None:
            cmd_with_args.append(f"--source_id={source_id}")
        if date_id is not None:
            cmd_with_args.append(f"--date_id={date_id}")
        if trigger is not None:
            cmd_with_args.append(f"--trigger_workflow={trigger}")

        subp = subprocess.Popen(cmd_with_args)
        self.process_mapping[key] = subp
        # TODO: should we wait a second and poll before returning to know if it started?

        return {
            "workspace_id": workspace_id,
            "source_id": source_id,
            "date_id": date_id,
            "pid": subp.pid,
            "key": key,
        }

    def get_status_summary(self, workspace_id=None):
        """
        return some top level stats about what is going on with various processes
        """
        return self.content_store.get_item_state_summary(workspace_id=workspace_id)

    # TODO: start/stop the old content removing process and the cluster updating processes

    def start_workflow_backfill(
        self, workspace_id=None, start_date_id=None, end_date_id=None
    ):
        """
        Start the backfill process manager
        TODO: It should restart when server restarts right?
        """
        # generate the sequence of date ids we will need to import for
        # need to use datetime classes because datemath are hard
        start_date = datetime.datetime.strptime(start_date_id, "%Y%m%d")
        end_date = datetime.datetime.strptime(end_date_id, "%Y%m%d")
        date_ids = []
        for x in range(0, (end_date - start_date).days + 1):
            date_id = start_date + datetime.timedelta(days=x)
            date_ids.append(date_id.strftime("%Y%m%d"))

        logging.info(
            f"Beginning backfill import process for {workspace_id} date_id range {date_ids}"
        )
        # probably confusing that this spins up a thread, where other stuff spins up a process
        Thread(
            target=self.run_workflow_backfill,
            kwargs={"workspace_id": workspace_id, "date_ids": date_ids},
        ).start()
        # self.run_workflow_backfill(workspace_id=workspace_id, date_ids=date_ids)

        return date_ids

    def run_workflow_backfill(self, workspace_id=None, date_ids=None):
        """
        Manages triggering import/reimport processes of content for specified
        workspace and array of date_ids in sequence, polling the running processes
        to know when to start the next date_id
        """
        # TODO: this needs its own entry in the process map
        # TODO: overwrite flag
        error_dates = []
        completed_dates = []
        for date_id in date_ids:
            # loop check how many running jobs there every 5 minutes
            # TODO failsafe stopping exit criteria?
            process_key = f"{date_id}_not_started"
            while True:
                # keep checking  on the status of the job until it completes or errors
                process_dict = self.get_processing_status()
                updated_status = process_dict.get(
                    process_key
                )  # this will be None the first time
                if updated_status is not None:
                    # check if it has any non-None status code (completed or error)
                    status_code = updated_status["status"]
                    if status_code is not None:
                        if status_code == 0:
                            completed_dates.append(date_id)
                        else:
                            error_dates.append(date_id)
                        logging.info(
                            f"Status of backfill {process_key} for date_id {date_id} is {status_code} starting next process"
                        )
                        break
                else:
                    # start a backfill process for this date_id
                    # # check how many processes are already running
                    if self.is_new_process_allowed():
                        # start an import process and record the process key
                        # start the import and tell it to trigger the workflow
                        # TODO: what happens when multiple processes try to run the workflow?
                        logging.info(
                            f"Starting backfill import process with workflow trigger for {workspace_id} {date_id} "
                        )
                        process_key = self.start_import_processing(
                            workspace_id=workspace_id, date_id=date_id, trigger=True
                        )["key"]

                    else:
                        logging.info(
                            f"Waiting to start backfill import for {workspace_id} {date_id}, already {self.MAX_PROCESSES} processes running"
                        )
                time.sleep(60)  # sleep 1 minute before checking again

        logging.info(
            f"Completed backfill process with {len(completed_dates)} completed dates and {len(error_dates)} errors:{error_dates}"
        )

    def add_content_item_keywords(
        self, workspace_id, content_item_id, model_name: str, keywords, state
    ):
        """
        Called by web callbacks from services to attach a set of keywords to content
        keywords expected to be a list of  text,score tuples
        """
        # TODO: auth permissions to access workspace
        # pull a reference to the content item from the store
        int_id = int(content_item_id)
        item = self.content_store.get_item(content_item_id=int_id)
        assert (
            item is not None
        ), f"unable to find content_item in content store with id {int_id} to add keywords"
        assert item.workspace_id == workspace_id

        for keyword in keywords:
            self.content_store.attach_keyword(
                item,
                keyword_model_name=model_name,
                keyword_text=keyword[0],
                score=keyword[1],
            )
        # try to update it to the requested state
        state = self.content_store.transition_item_state(item, state)
        return len(keywords)

    def dispatch_presto_keyword_response(self, response):
        """
        NOTE: this only works for keyword models like yake and classycat.
        Other Presto models with need different dispatches
        """
        # parse the expected presto structure from response
        (
            presto_model_name,
            workspace_id,
            content_item_id,
            target_state,
            result_payload,
        ) = PrestoWrapperService.parse_presto_response(response)

        # TODO: look up the appropriate presto model wrapper to parse the response
        # (for now hardcoded as Yake)
        assert (
            # NOTE: the model name is *NOT* what was passed in
            # presto_model_name == YakePrestoService.MODEL_KEY
            presto_model_name
            == "yake_keywords.Model"
        ), f"Unkown Presto model name: {presto_model_name}"
        num_added = 0
        # check if the result was an error payload
        # assume that the error codes work like http codes
        if result_payload is not None:
            if result_payload.get("error_code") and result_payload["error_code"] > 200:
                error_code, error, error_details = (
                    self.yake_model.parse_presto_error_response(result_payload)
                )
                logging.error(
                    f"content item {content_item_id} error from Presto model {presto_model_name}: {error_code} : {error} - {error_details}"
                )
            else:
                # get the keywords
                keywords = self.yake_model.parse_presto_response_result(result_payload)

                # TODO: the model name may not be just the Presto model name
                # i.e. multiple classycat schemas
                num_added = self.add_content_item_keywords(
                    workspace_id=workspace_id,
                    content_item_id=content_item_id,
                    model_name=presto_model_name,
                    keywords=keywords,
                    state=target_state,
                )
        return num_added
