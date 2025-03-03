import requests
import json
from dataclasses import dataclass, field
from timpani.app_cfg import TimpaniAppCfg
from timpani.content_store.content_item import ContentItem

import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()


class PrestoWrapperService(object):
    """
    Wrapper for the model services hosted in Presto
    """

    app_cfg = TimpaniAppCfg()
    MODEL_KEY = None  # NOTE: cannot be none, must be overidden by subclass
    PRESTO_ENDPOINT = app_cfg.presto_endpoint
    CALLBACK_URL = app_cfg.timpani_conductor_api_endpoint + "/presto_model"
    # NOTE: to run tests on localdev, may need to switch to this address
    # because the address will be different inside vs outside the docker network
    # CALLBACK_URL = "http://host.docker.internal:3101" + "/presto_model"
    MAX_SIZE_BYTES = 256000

    def healthcheck(self):
        """
        Confirm that we are able to connect to Presto service
        """
        healthcheck_url = self.PRESTO_ENDPOINT + "/healthcheck"
        response = requests.get(healthcheck_url)
        assert (
            response.ok
        ), f"Unable to process response from Alegre service at {healthcheck_url}"
        return response.ok is True

    def submit_to_presto_model(
        self, content_items: list[ContentItem], target_state=None
    ):
        """
        Submit text from a content item to the Presto service processing queue via async call
        """
        # TODO: refactor for batch
        # NOTE: SOME PRESTO ENDPOINTS SUPPORT BATCH AND SOME DO NOT
        if len(content_items) > 1:
            logging.warning(
                f"Batch of {len(content_items)} content items submitted to presto {self.MODEL_KEY}, only the first item used"
            )
        content_item = content_items[0]

        # expected call and response structure according to
        # # https://github.com/meedan/presto/blob/master/test/lib/test_http.py
        #  test_data = {"id": 1, "callback_url": "http://example.com", "text": "This is a test"}
        request_body = PrestoRequest(
            id=PrestoRequest.format_id(content_item.content_item_id),
            # /presto_model/<workspace_id>/<model_name>
            callback_url=self.CALLBACK_URL,
            text=content_item.content,
            raw={
                "content_item_id": content_item.content_item_id,
                "target_state": target_state,
                "workspace_id": content_item.workspace_id,
            },
        )
        # check that the payload isn't too big for the SQS queue
        payload_size = request_body.size_in_bytes()
        assert (
            payload_size < self.MAX_SIZE_BYTES
        ), f"Could not submit request because payload size {payload_size} was greater than {self.MAX_SIZE_BYTES}"

        request_url = self.PRESTO_ENDPOINT + f"/process_item/{self.MODEL_KEY}__Model"
        response = requests.post(url=request_url, json=request_body.get_dict())
        assert response.ok, f"Error submitting request to Presto model:{response.text}"
        logging.debug(f"presto response:{response}  {response.text}")

    @staticmethod
    def parse_presto_response(response: dict):
        """
        Parse the "response' (incoming request) from Presto service async callback
        to extract the items that would have been added by this wapper
        """
        # NOTE: these structures are enforced by a schema in presto, but not checked by contract test
        # The response (incoming reque) will have body, model_name, and retry_count
        # body should have a message
        # the structure of the message depends on
        # https://github.com/meedan/presto/blob/c8f4a4925464f1be4be2e5500e188052fbfde56d/lib/schemas.py#L24
        # class Message(BaseModel):
        #   body: GenericItem
        #   model_name: str
        #   retry_count: int = 0
        # TODO: check for error
        # BaseModel should have the same structure as PrestoRequest
        # so try to set up that model using the arguments from the dict in body
        # however, it could have a different structure if it is an error

        logging.debug(f"response:{response}")
        body = response["body"]
        response_obj = PrestoRequest(**body)

        model_name = response["model_name"]
        workspace_id = response_obj.raw["workspace_id"]
        content_item_id = response_obj.raw["content_item_id"]
        target_state = response_obj.raw["target_state"]
        result_payload = response_obj.result
        return model_name, workspace_id, content_item_id, target_state, result_payload

    def parse_presto_response_result(self, result: dict):
        """
        Parse the model-specific details from the presto response
        """
        # must be implented by specific model because each model returns different structure
        raise NotImplementedError

    def parse_presto_error_response(self, response: dict):
        """
        log out errors reported by presto
        """
        # may be implemented by specific model to change error behavior
        # class ErrorResponse(BaseModel):
        # error: Optional[str] = None
        # error_details: Optional[Dict] = None
        # error_code: int = 500
        return (
            response["error_code"],
            response.get("error"),
            response.get("error_details"),
        )


@dataclass
class PrestoRequest(object):
    """
    Template class for restricting the fields
    for the Presto request object to ensure consistency
    more detailed schema, including definition of optional params:
    ```
    https://github.com/meedan/presto/blob/c8f4a4925464f1be4be2e5500e188052fbfde56d/lib/schemas.py#L14
    class GenericItem(BaseModel):
        id: Union[str, int, float]
        content_hash: Optional[str] = None
        callback_url: Optional[str] = None
        url: Optional[str] = None
        text: Optional[str] = None
        raw: Optional[Dict] = {}
        parameters: Optional[Dict] = {}
        result: Optional[Union[MediaResponse, VideoResponse, YakeKeywordsResponse]] = None
    ```
    """

    id: str
    content_hash: str = None
    callback_url: str = None
    url: str = (
        None  # optionally, this would point to object or media the model should parse
    )
    text: str = None  # optionally, this contains the text the model should parse
    raw: dict = None
    parameters: dict = field(default_factory=dict)  # needs to be empty dict, not None
    result: dict = None  # TODO: why is this in the request

    @staticmethod
    def format_id(content_item_id: str):
        """
        Adds an timpani specific prefiex to doc ids so that they cannot  collide
        with items stored by other services
        """
        return f"timpani_{content_item_id}"

    @staticmethod
    def unformat_id(doc_id: str):
        return doc_id.removeprefix("timpani_")

    def get_dict(self):
        # model key must be included or Alegre will fail oddly
        request_dict = {
            "id": self.id,
            "content_hash": self.content_hash,
            "callback_url": self.callback_url,
            "url": self.url,
            "text": self.text,
            "raw": self.raw,
            "parameters": self.parameters,
        }
        # presto crashes if this is None
        if self.result is not None:
            request_dict["result"] = self.result
        return request_dict

    def size_in_bytes(self):
        """
        Make sure the payload is not too big for SQS queues
        """
        return len(json.dumps(self.get_dict()).encode())
