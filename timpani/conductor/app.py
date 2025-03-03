import json
from flask import Flask
from flask import request
from flask import abort
from werkzeug import serving
import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration

from timpani.app_cfg import TimpaniAppCfg
from timpani.conductor.orchestrator import Orchestrator

import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()


app = Flask(__name__)

# try to skip logging for healthcheck endpoint because clutters logs
parent_log_request = serving.WSGIRequestHandler.log_request


def log_request(self, *args, **kwargs):
    """
    Logging function that skipps healthcheck endpoint
    """
    if self.path == "/healthcheck":
        return
    parent_log_request(self, *args, **kwargs)


serving.WSGIRequestHandler.log_request = log_request


orchestrator = Orchestrator()

# TODO: app should check for uncompleted tasks on startup (and eventually restart them)

"""
NOTE: These endpoints should not be exposed publically because they don't validate
workspace access permissions before triggering operations
"""


@app.route("/")
def splashpage():
    msg = f"""
    This is the 'Conductor' application for orchestrating data processing
    in Meedan's Timpani 'social listening' project. This application is
    deployed in '{cfg.deploy_env_label}'.
    """
    return msg


@app.route("/healthcheck")
def healthcheck():
    return "the conductor app is up"


@app.route("/status_summary")
def status_summary():
    """
    Return some basic stats about running state of the conductor application
    (number of items in each state?)
    TODO: filter by workspace id
    """
    return orchestrator.get_status_summary()


@app.route("/add_keywords/<workspace_id>/<content_item_id>", methods=["POST"])
def content_item_add_keywords(workspace_id=None, content_item_id=None):
    """
    Called by services to attach keywords to content items
    """
    logging.debug(f"content item add keywords called {request.get_json()}")
    data = request.get_json()
    # TODO: how do we enforce schema on json?
    # TODO: this should align with YAKE/presto return structures
    model_name = data["model_name"]
    keywords = data["keywords"]
    target_state = data["state"]
    try:
        num_added = orchestrator.add_content_item_keywords(
            workspace_id,
            content_item_id,
            model_name=model_name,
            keywords=keywords,
            state=target_state,
        )
        return f"Added {num_added} keywords to content_item_id {content_item_id}"
    except AssertionError as e:
        # return information about the error to client
        # (only appropriate for internal api)
        msg = f"Failed to add kewords content_item_id '{content_item_id}' to state '{target_state}': '{e}'"
        logging.exception(msg)
        sentry_sdk.capture_exception(e)
        abort(
            500,
            description=msg,
        )
        raise e


@app.route("/presto_model", methods=["POST"])
def presto_model_response():
    """
    Process async response from Presto models to the appropriate
    workspace and model
    """
    # TODO: this may be a batch response?
    logging.debug(f"presto_model_response called {request.get_json()}")
    data = request.get_json()

    try:
        num_added = orchestrator.dispatch_presto_keyword_response(response=data)
        return f"Added {num_added} keywords from presto response"
    except AssertionError as e:
        # return information about the error to client
        # (only appropriate for internal api)
        msg = f"Failed Presto model callback response: '{e}'"
        logging.exception(msg)
        abort(
            500,
            description=msg,
        )
        raise e


# TODO: this could be /content_item/<id>/state/
@app.route("/update_item_state", methods=["POST"])
def content_item_update_state():
    """
    Called by services to indicate a process has completed.
    accepts the content item id and the requested target state
    (in future, accept the content item object)
    """
    # get the item id and state from the request
    # ask the orchestrator to update in the content store

    logging.debug(f"content item update called {request.get_json()}")
    data = request.get_json()
    # TODO how do we enforce argument validation?
    content_item_id = data["content_item_id"]
    target_state = data["state"]
    try:
        state = orchestrator.update_content_item_state(content_item_id, target_state)
        return state.current_state
    except AssertionError as e:
        # return information about the error to client
        # (only appropriate for internal api)
        msg = f"Failed to update content_item_id '{content_item_id}' to state '{target_state}': '{e}'"
        logging.exception(msg)
        sentry_sdk.capture_exception(e)
        abort(
            500,
            description=msg,
        )
        raise e


@app.route("/import_content/<workspace_id>/<source_id>/<date_id>")
def import_content(workspace_id: None, source_id=None, date_id=None):
    """
    Called by S3 monitoring or 'Booker' app to indicate new content is
    in s3 and needs to be processed into the system
    TODO: this should be a POST?
    """
    # needs partition id to process
    # calls the processor script to lanuch appropriate task
    # TODO: orchetrator may need to limit how many of these can lanunch at once?
    trigger = False
    if request.args.get("trigger_import"):
        trigger = True
    response_fields = orchestrator.start_import_processing(
        workspace_id, source_id, date_id, trigger
    )
    return (
        f"Started processing content from raw store partition (workspace,source_id,date_id): {workspace_id},"
        + f" {response_fields['source_id']}, {response_fields['date_id']} with pid {response_fields['pid']}"
    )


@app.route("/start_workflow/<workspace_id>")
def start_workflow(workspace_id=None):
    """
    Turns on the process that will keep checking if content is ready to be sent to a new state
    TODO: do we need to ability to enable per workspace / per workflow?
    TODO: return an id for the workflow so it can be stopped later?
    TODO: should this be a POST?
    """
    # TODO: sanitize workspace_id?
    # calls the processor script to lanuch appropriate processing workflow
    return orchestrator.start_workflow_processing(workspace_id)


@app.route("/running_processes")
def running_process_summary():
    return json.dumps(orchestrator.get_processing_status())


@app.route("/stop_workflow/<workspace_id>")
def stop_workflow(workspace_id=None):
    """
    Turns off process that will keep checking if content is ready to be sent to a new state
    TODO: do we need to ability to stop processing per workflow (and leave others running)?
    """
    # calls the processor script to lanuch appropriate workflow task?
    return orchestrator.stop_workflow_processing(workspace_id)


@app.route("/start_cluster_updates/<workspace_id>")
def start_cluster_updates(workspace_id=None):
    """
    Kicks off a process that will update clusters that should be merged or split
    TODO: return an id for the workflow so it can be stopped later?
    TODO: should this be a POST?
    """
    # TODO: sanitize workspace_id?
    # calls the processor script to lanuch appropriate processing workflow
    return orchestrator.start_cluster_updates(workspace_id)


@app.route("/start_expiration/<workspace_id>")
def start_expiration(workspace_id=None):
    """
    Kicks off a process that will look for content older than the workflows live duration and delete it
    TODO: return an id for the workflow so it can be stopped later?
    TODO: should this be a POST?
    """
    # TODO: sanitize workspace_id?
    # calls the processor script to lanuch appropriate processing workflow
    return orchestrator.start_expiration_processing(workspace_id)


@app.route("/stop_expiration/<workspace_id>")
def stop_expiration(workspace_id=None):
    """
    Stops the expired content deletion process corresponding to the workflow
    TODO: should this be a POST?
    """
    # TODO: sanitize workspace_id?
    # calls the processor script to lanuch appropriate processing workflow
    return orchestrator.stop_expiration_processing(workspace_id)


@app.route("/start_backfill/<workspace_id>")
def start_backfill(workspace_id=None):
    """
    Starts the backfill process to import content into the workspace
    for timerange between start_date_id and end_date_id (inclusive)
    TODO: should this be a POST?
    """
    start_date_id = None
    if request.args.get("start_date_id"):
        start_date_id = (
            f'{int(request.args.get("start_date_id"))}'  # it is actually a date 2021231
        )
    end_date_id = None
    if request.args.get("end_date_id"):
        end_date_id = (
            f'{int(request.args.get("end_date_id"))}'  # it is actually a date 2021231
        )
    assert (
        start_date_id is not None
    ), "backfill requires start_date_id in format YYYYMMDD"
    assert end_date_id is not None, "backfill requires end_date_id in format YYYYMMDD"
    # TODO: sanitize workspace_id?
    # calls orchestrator script to lanuch appropriate backfill processing workflows
    response = orchestrator.start_workflow_backfill(
        workspace_id, start_date_id=start_date_id, end_date_id=end_date_id
    )
    return f"Starting backfill processing for date_ids {response}"


@app.route("/cluster_item/<item_id>/<action_id>/<target_state>", methods=["POST"])
def cluster_items(item_id=None, action_id=None, target_state=None):
    """
    Accepts callback from similarity request with item and its possible matches.
    The payload will be a json object with document ids and scores
    """

    # TODO how do we enforce argument validation?

    return orchestrator.cluster_items(
        content_item_id=item_id,
        action_id=action_id,
        target_state=target_state,
        payload=request.get_json(),
    )


if __name__ == "__main__":
    # test that we can import configuration
    cfg = TimpaniAppCfg()
    # set debug based on environment variable
    debug = cfg.deploy_env_label in [
        "local",
        "dev",
    ]  # run in debug, but not in qa, live etc

    # initialize the sentry error tracking integration
    sentry_sdk.init(
        dsn=cfg.sentry_sdk_dsn,
        integrations=[FlaskIntegration()],
        environment=cfg.deploy_env_label,
        traces_sample_rate=1.0,
    )

    app.run(debug=debug, host="0.0.0.0", port=3101)
