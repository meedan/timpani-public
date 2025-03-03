import requests
import json
import uuid
import datetime
from typing import List
from collections import namedtuple

from timpani.app_cfg import TimpaniAppCfg
from timpani.content_store.content_item import ContentItem

# from timpani.conductor.conductor import ProcessConductor

import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()


ScoredTag = namedtuple("ScoredTag", "text score")


class ClassycatWrapper(object):
    """
    Temporary wrapper for timpani to send batches of content to
    classycat service https://github.com/meedan/classycat.

    Usually
    classycat will call an external LLM API, and return sets of
    "keywords" corresponding to labels in a schema

    TODO: if each workflow will have its own instance maybe
    the batch collection will be less efficient? Also, we will
    be hitting the lambda in parallel
    TODO: this doesn't work here, the schedule (and the state it contains)
    should be in the service, not the the wrapper
    """

    app_cfg = TimpaniAppCfg()
    # TODO: load from app cfg
    CLASSYCAT_BASE_URL = app_cfg.classycat_api_endpoint

    CALLBACK_URL = app_cfg.timpani_conductor_api_endpoint + "/add_keywords"

    MAX_BATCH_SIZE = 25
    # BATCH_WAIT_LIMIT_SECONDS = 60
    BATCH_WAIT_LIMIT_SECONDS = 5  # for testing

    REQUEST_HEADERS = {
        "Content-Type": "application/json",
    }

    def __init__(self, default_schema_name=None, batch_wait_limit_seconds=None) -> None:
        if batch_wait_limit_seconds is not None:
            self.BATCH_WAIT_LIMIT_SECONDS = batch_wait_limit_seconds  # for testing
        self.default_schema_name = default_schema_name
        self.schema_id = None

        # initialize the data structure that will collect
        self.schema_collector = {}
        # start the scheduled job to flush incomplete batches if they get too old
        # TODO this should exist elsewhere and isn't starting correctly
        # self.schedular = ProcessConductor()
        # self.schedular.register_scheduled_function(
        #    self.submit_old_batches, interval_seconds=self.BATCH_WAIT_LIMIT_SECONDS
        # )
        # start the scheduling process
        # self.schedular.start_schedular()

    def ensure_default_schema(self, schema_name):
        # check if the schema exists with that name and load the schema id
        # but because the check is slow and expensive, don't do it if
        # there is already a schemas
        # TODO: this needs to be more complicated once multiple schemas
        if self.schema_id is None:
            self.schema_id = self.get_schema_id(schema_name)
            assert (
                self.schema_id is not None
            ), f"Unable to locate schema_id for schema named '{self.default_schema_name}'"
            logging.debug(
                f"located schema_id {self.schema_id} for schema_name '{schema_name}'"
            )
        return self.schema_id

    def get_name(self):
        # TODO: should we append the LLM type?
        return "classycat"

    def healthcheck(self):
        """
        Confirm that we are able to connect to Classycat service
        """
        healthcheck_url = self.CLASSYCAT_BASE_URL
        schema = {}
        response = requests.post(
            url=healthcheck_url, json=schema, headers=self.REQUEST_HEADERS
        )
        assert (
            response.ok
        ), f"Unable to process response from Classycat service at {healthcheck_url}: {response} : {response.text}"
        return response.ok is True

    def get_schema_id(self, schema_name):
        """
        Returns a schema_id if there is a schema with the given name or None if not found
        """
        url = self.CLASSYCAT_BASE_URL

        response = requests.post(
            url, json={"event_type": "get_schema_id", "schema_name": schema_name}
        )
        if response.status_code == 404:
            logging.warning(
                f"requested id for schema name '{schema_name}': {response.json()['message']}"
            )
            # schema does not exist
            return None
        assert (
            response.ok
        ), f"Classycat get schema id schema call to {url} returned {response.text}"
        return response.json()["schema_id"]

    def create_schema(self, schema_dict):
        """
        Sends the classification schema to classycat at gets back an id for it
        (mostly for testing, as real schemas should be loaded externally)
        """
        # NOTE: assuming schema is validated on classycat side?
        url = self.CLASSYCAT_BASE_URL

        response = requests.post(url, json=schema_dict, headers=self.REQUEST_HEADERS)
        # TODO: catch error when schema already exists
        assert (
            response.ok
        ), f"Classycat create schema call to {url} returned {response.text}"

        schema_id = response.json()["schema_id"]
        return schema_id

    def add_item_to_batch(self, item: ContentItem, target_state, schema_id=None):
        """
        Adds a single item to a batch. A batch collects items with the same schema_id
        and target_state.  Batches will be dispatched to the model
        when we reach MAX_ITEMS,
        NOT IMPLEMENTED: or if more than BATCH_WAIT_LIMIT_SECONDS pass.
        NOTE: at shutdown, any items not submitted will *not* be flushed.
        TODO: initially, we will block while waiting for the batch to come back
        from the model
        {
            schema_id_1:{
                state_a:{
                    "last_updated": datetime.datetime.utcnow(),
                    "batch_id": uuid.uuid4().hex,
                    "items": [item1, item2, ...],
                },
                state_b:None,
            },
            schema_id_2:{}
        }
        """
        # TODO: make batch into an object
        # TODO: threading problems?
        # TODO: maybe this batch accumulation should live in classycat instead
        if schema_id is None:
            # maybe one was included by name during class setup
            # if so look
            schema_id = self.ensure_default_schema(self.default_schema_name)

        assert (
            schema_id is not None
        ), "No schema name was included and no default availible"

        if self.schema_collector.get(schema_id) is None:
            # this is the first time we've seen an item for this schema
            self.schema_collector[schema_id] = {target_state: None}

        # is the first time we've seen this target state for the schema?
        if self.schema_collector[schema_id].get(target_state) is None:
            self.schema_collector[schema_id][target_state] = {
                "last_updated": datetime.datetime.utcnow(),
                "batch_id": uuid.uuid4().hex,
                "items": [item],
            }
        else:
            batch = self.schema_collector[schema_id][target_state]
            batch["last_updated"] = datetime.datetime.utcnow()
            batch["items"].append(item)

            # decide if batch is ready to submit
            if len(batch["items"]) >= self.MAX_BATCH_SIZE:
                # remove the batch from the collector
                del self.schema_collector[schema_id][target_state]
                # NOTE: this will block until classycat returns
                self.batch_classify(
                    content_items=batch["items"],
                    schema_id=schema_id,
                    batch_id=batch["batch_id"],
                    target_state=target_state,
                )
        return True

    def submit_old_batches(self, timeout_seconds=BATCH_WAIT_LIMIT_SECONDS):
        """
        Check if any of the batches that are not large enough to
        reach the submission threshold have exceeded timeput and submit them.
        This also flushes last tiems
        """
        logging.debug("checking for expired classycat batches to submit")
        timeout_threshold = datetime.datetime.utcnow()
        for schema_id in self.schema_collector:
            for target_state in self.schema_collector[schema_id]:
                batch = self.schema_collector[schema_id][target_state]
                if (
                    batch["last_updated"] + datetime.timedelta(seconds=timeout_seconds)
                    < timeout_threshold
                ):
                    # remove the batch from the collector
                    del self.schema_collector[schema_id][target_state]
                    logging.debug(
                        f"flushing batch for {schema_id}:{target_state} because older than {self.BATCH_WAIT_LIMIT_SECONDS}"
                    )
                    # NOTE: this will block until batch returns
                    self.batch_classify(
                        content_items=batch["items"],
                        schema_id=schema_id,
                        batch_id=batch["batch_id"],
                        target_state=target_state,
                    )

    def batch_classify(
        self,
        content_items: List[ContentItem],
        schema_id: str,
        target_state,
        batch_id=None,
    ):
        # NOTE: this is for *synchronous* processing
        # get all the text out of the content items and key by id
        # NOTE: there are no ids associated with items, so preserving ordering is crucial
        # TOOD: we should add a metric here so we know how often we hit the $ API (tho better
        # (metric better on classycat side)
        assert len(content_items) <= self.MAX_BATCH_SIZE
        submit_items = []

        for item in content_items:
            # TODO: forbidden content? ("", None, \n)?
            submit_items.append({"id": item.content_item_id, "text": item.content})
        if batch_id is None:
            batch_id = uuid.uuid4().hex

        # TODO: lookup item states to know if some items have been retried a bunch already, and if so
        # break them into smaller batches?

        # try to make a request to classycat for the batch, and block on response
        logging.debug(
            f"Requesting classycat categorization batch {batch_id} with schema {schema_id} for {len(content_items)} content_items "
        )
        url = self.CLASSYCAT_BASE_URL
        response = requests.post(
            url,
            json={
                "items": submit_items,
                "schema_id": schema_id,
                "event_type": "classify",
            },
            headers=self.REQUEST_HEADERS,
        )
        assert response.ok, f"Classycat classify call to {url} returned {response.text}"

        cat_results = response.json()["classification_results"]
        # make a dummy list of results
        # cat_results = ["A"] * len(content_items)

        assert len(cat_results) == len(
            content_items
        ), f"Number of Classycat results {len(cat_results)} does not match size {len(content_items)} for batch {batch_id}"

        # since we are not doing callbacks right now, spin off a bunch of requests to the keywords update endpoint
        for i in range(len(content_items)):
            assert cat_results[i]["id"] == content_items[i].content_item_id
            item_cats = []
            for label in cat_results[i]["labels"]:
                # confirm the ids match up

                item_cats.append(ScoredTag(label, 1.0))
            self._do_keywords_callback(
                schema_name=self.default_schema_name,
                workspace_id=content_items[i].workspace_id,
                content_item_id=content_items[i].content_item_id,
                keywords=item_cats,
                target_state=target_state,
            )
        logging.debug(f"Updated keyword categories for batch {batch_id}")

    def _do_keywords_callback(
        self,
        schema_name: str,
        workspace_id: str,
        content_item_id: str,
        keywords,
        target_state: str,
    ):
        """
        Helper function to make sure we do the keyword update callbacks in the same way
        """
        callback_response = requests.post(
            self.CALLBACK_URL + f"/{workspace_id}/{content_item_id}",
            data=json.dumps(
                {
                    "model_name": self.get_name() + ":" + schema_name,
                    "state": target_state,
                    "keywords": keywords,
                }
            ),
            headers={
                "Content-Type": "application/json",
                "User-Agent": "Meedan Timpani/0.1 (Conductor)",  # TODO: cfg should know version
            },
        )
        assert (
            callback_response.ok
        ), f"Unable to process response from Timpani adding classycat keywords at {self.CALLBACK_URL} : {callback_response.text}"
