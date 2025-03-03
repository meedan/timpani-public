import requests
import json
from dataclasses import dataclass
from timpani.app_cfg import TimpaniAppCfg

import timpani.util.timpani_logger

from timpani.util.metrics_exporter import TelemetryMeterExporter

logging = timpani.util.timpani_logger.get_logger()


class AlegreWrapperService(object):
    """
    Wrapper for the text vectorization model service as implemented in alegre
    TODO: this should be replaced by code that talks to specific model service
    hosted by presto
    """

    app_cfg = TimpaniAppCfg()
    telemetry = TelemetryMeterExporter(service_name="timpani-conductor")
    MODEL_KEY = None  # NOTE: cannot be none, must be overidden by sub class
    CALLBACK_URL = app_cfg.timpani_conductor_api_endpoint + "/update_item_state"

    service_response_metric = telemetry.get_gauge(
        "service.request.duration",
        "duration of blocking request to service",
        unit="seconds",
    )

    def _do_state_callback(self, content_item_id: str, target_state: str):
        """
        Helper function to make sure we do the state update callbacks in the same way
        """
        callback_response = requests.post(
            self.CALLBACK_URL,
            data=json.dumps(
                {"content_item_id": content_item_id, "state": target_state}
            ),
            headers={
                "Content-Type": "application/json",
                "User-Agent": "Meedan Timpani/0.1 (Conductor)",  # TODO: cfg should know version
            },
        )
        assert (
            callback_response.ok
        ), f"Unable to process response from Timpani status update at {self.CALLBACK_URL} {callback_response.text}"

    def healthcheck(self):
        """
        Confirm that we are able to connect to Alegre service
        """
        healthcheck_url = self.app_cfg.alegre_api_endpoint + "/healthcheck"
        response = requests.get(healthcheck_url)
        assert (
            response.ok
        ), f"Unable to process response from Alegre service at {healthcheck_url}"
        return response.ok is True

    def _do_alegre_vectorization(
        self, model_key: str, content_item_id: str, content_text: str, workspace_id: str
    ):
        """
        Internal wrapper that actually calls the Alegre API, but
        doesn't handle callbacks etc
        """
        # construct the json blob that Alegre will expect
        query_blob = AlegreContext(
            model_key=model_key,
            text=content_text,
            doc_id=AlegreContext.format_doc_id(content_item_id),
            workspace_id=workspace_id,
        ).get_context()
        post_url = self.app_cfg.alegre_api_endpoint + "/text/similarity/"
        logging.debug(
            f"requesting vectorization from Alegre {post_url} for content_item_id {content_item_id}"
        )
        response = requests.post(
            post_url,
            json=query_blob,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "Meedan Timpani/0.1 (Conductor)",  # TODO: cfg should know version
            },
        )
        self.service_response_metric.set(
            response.elapsed.total_seconds(),
            attributes={
                "workspace_id": workspace_id,
                "service_name": "alegre vectorization",
                "response_code": response.status_code,
            },
        )
        # TODO: need more detailed response info from the service to know if this specific
        # item is failing or the service is down.
        assert (
            response.ok
        ), f"Unable to process response from Alegre service at {post_url} {response.text}. request: {query_blob}"
        result = json.loads(response.text)
        return result


@dataclass
class AlegreContext(object):
    """
    Template class for restricting the fields
    for the AlegreContext object to ensure consistency
    """

    model_key: str
    text: str
    doc_id: str
    workspace_id: str
    threshold: float = None

    @staticmethod
    def format_doc_id(content_item_id: str):
        """
        Adds an timpani specific prefix to doc ids so that they cannot  collide
        with items stored by other services
        """
        return f"timpani_{content_item_id}"

    @staticmethod
    def unformat_doc_id(doc_id: str):
        return doc_id.removeprefix("timpani_")

    def get_context(self):
        assert self.model_key is not None
        # model key must be included or Alegre will fail oddly
        context = {
            "text": self.text,
            # unique id in timpani database
            "doc_id": f"{self.doc_id}",
            # language : ""  TODO: this is where language would go
            "context": {
                # distingish from other content in alegre
                "type": "timpani_content_item_text",
                "workspace_id": f"{self.workspace_id}",
            },
            "models": [self.model_key],  # model must be included
            # "threshold": self.threshold,
        }
        # Alegre doesn't accept threshold of None, so only add if we have non-default value
        if self.threshold is not None:
            context["threshold"] = self.threshold
        return context
