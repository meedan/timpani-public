import requests
import datetime
import json
from collections import namedtuple

from timpani.app_cfg import TimpaniAppCfg
from timpani.content_store.content_item import ContentItem
from timpani.model_service.alegre_wrapper_service import AlegreContext

# from timpani.model_service.means_tokens_vectorization_alegre_wrapper import (
#    MeansTokensAlegreVectorizationModelService as SimilarityModel,
# )

from timpani.model_service.paraphrase_multilingual_vectorization_alegre_wrapper import (
    ParaphraseMultilingualAlegreVectorizationModelService as SimilarityModel,
)

from timpani.util.metrics_exporter import TelemetryMeterExporter
import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()

# class for associating content items with scores
ScoredId = namedtuple("ScoredItem", "score id")


class AlegreVectorStoreService(object):
    """
    Wrapper for the vector storage and query operations implemented in Alegre.
    Assumption is that operations with the store block until completed? (i.e.
    we don't do callback stuff)
    TODO: this should be replaced by code that talks to a vector store, and probably
    parts abstracted to controllers
    TODO: these should all be converted to batch operations
    """

    app_cfg = TimpaniAppCfg()
    telemetry = TelemetryMeterExporter(service_name="timpani-conductor")

    service_response_metric = telemetry.get_gauge(
        "service.request.duration",
        "duration of blocking request to service",
        unit="seconds",
    )

    CALLBACK_URL = app_cfg.timpani_conductor_api_endpoint + "/update_item_state"

    # NOTE: this is a hardcoded relationship that shouldn't exist when we have real model services
    vector_model = SimilarityModel()

    def healthcheck(self):
        """
        Confirm that we are able to connect to Alegre service
        """
        healthcheck_url = self.app_cfg.alegre_api_endpoint + "/healthcheck"
        response = requests.get(healthcheck_url)
        assert (
            response.ok
        ), f"Unable to process response from Alegre service at {healthcheck_url}: {response} : {response.text}"
        return response.ok is True

    def store_vector_for_content_item(self, item: ContentItem):
        """
        Store the vector.  For Alegre this actually calls the vectorization on text
        TODO: add argument for vector to be stored
        """
        self.vector_model._do_alegre_vectorization(
            model_key=self.vector_model.MODEL_KEY,
            content_item_id=item.content_item_id,
            content_text=item.content,
            workspace_id=item.workspace_id,
        )

    def request_similar_content_item_ids(self, item: ContentItem, threshold=None):
        """
        Find items that have vectors similar to the vector for content item.
        The Alegre layer applies a filter on context type and workspace id
        via its 'context' object. Returns an array of (score,id) tuples
        NOTE: alegre doesn't work this way, so this actually resubmits the
        text of them for vectorization
        NOTE: this blocks until query returns
        """
        # TODO: for presto/batch world, should this trigger a callback to clustering endpoint with responses?
        # TODO: batching
        # construct the json blob that Alegre will expect
        # TODO: is there a way to tell alegre not to save this?
        # TODO: if Alegre/elastic search is not ordering results by score, and it only returns 10 results, how do we know 10 best?
        query_blob = AlegreContext(
            model_key=self.vector_model.MODEL_KEY,
            text=item.content,
            doc_id=AlegreContext.format_doc_id(item.content_item_id),
            workspace_id=item.workspace_id,
            threshold=threshold,
        ).get_context()
        get_url = self.app_cfg.alegre_api_endpoint + "/text/similarity/search/"
        logging.debug(f"requesting similar item from Alegre {get_url}")
        response = requests.post(
            get_url,
            json=query_blob,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "Meedan Timpani/0.1 (Booker)",  # TODO: cfg should know version
            },
        )
        self.service_response_metric.set(
            response.elapsed.total_seconds(),
            attributes={
                "workspace_id": item.workspace_id,
                "service_name": "alegre similarity",
                "response_code": response.status_code,
            },
        )
        assert (
            response.ok
        ), f"Unable to process response from Alegre service at {get_url} {response.text}"
        result = json.loads(response.text)
        logging.debug(f"similarity result from alegre: {result}")
        item_ids = self.extract_scored_item_ids_from_response(result)

        # TODO: should we do a callback here instead of returning? Or we assume this is always fast
        return item_ids

    def extract_scored_item_ids_from_response(self, response_payload):
        """
        Extracts the items ids and corresponding scores from Alegre
        response and returns them as list of (score, id) tuples
        """
        item_ids = []
        for result_item in response_payload["result"]:
            id = AlegreContext.unformat_doc_id(result_item["id"])
            score = result_item["score"]
            item_ids.append(ScoredId(score, id))

        return item_ids

    def request_vector_for_content_item(self, item: ContentItem):
        """
        Ask the alegre server to send back a web request with vector payload for content item
        TODO: does this return the values or that is handled in the callback?
        TODO: if it blocks waiting for return, it can't do batch?
        """
        logging.debug(f"requesting vector for content item {item.content_item_id}")
        # TODO: alegre doesn't return vectors
        # TODO: can alegre lookup by ID, or we need to cache that?
        raise NotImplementedError

    def discard_vector_for_content_item(self, item: ContentItem):
        """
        Ask the alegre server to delete any vectors corresponding to the content item.
        TODO: need ELASTIC SEARCH document id to delete, probably need to update alegre to return?
        .. or we have to push in keys generated in Timpani
        """
        logging.debug(
            f"requesting vector delete for content item {item.content_item_id}"
        )
        delete_url = self.app_cfg.alegre_api_endpoint + "/text/similarity/"
        response = requests.delete(
            delete_url,
            data=json.dumps(
                {
                    "doc_id": f"{AlegreContext.format_doc_id(item.content_item_id)}",
                }
            ),
            headers={
                "Content-Type": "application/json",
                "User-Agent": "Meedan Timpani/0.1 (Conductor)",  # TODO: cfg should know version
            },
        )
        # Alegre will return a 404 if the document does not exist
        # (but could also conflate with other 404 errors)
        if response.status_code == 404:
            logging.warning(
                f"Recieved 404 from Alegre attempting to delete content_item_id {item.content_item_id}:{response.text} "
            )
        else:
            assert (
                response.ok
            ), f"Unable to process response from Alegre service at {delete_url} {response.text}"

    def discard_workspace(self, workspace_id):
        """
        Loop over all the items in an workspace and discard them (probably slow, mostly used for deleting tests)
        """
        query_item = ContentItem(
            date_id=00000000,
            run_id=None,
            workspace_id=workspace_id,
            source_id="deletion query",
            query_id=f"deletion query for {workspace_id}",
            raw_created_at=datetime.datetime.utcnow(),
            raw_content_id="0000000",
            raw_content="",
        )
        query_item.content_item_id = "dummy_id"

        # issue a query with a blank content item and
        items_to_delete = self.request_similar_content_item_ids(
            query_item, threshold=0.0
        )
        while len(items_to_delete) > 0:
            logging.info(
                f"discarding {len(items_to_delete)} for workspace {workspace_id}"
            )
            for item in items_to_delete:
                self.discard_vector_for_content_item(item)
            items_to_delete = self.request_similar_content_item_ids(
                query_item, threshold=0.0
            )
        self.discard_vector_for_content_item(query_item)
