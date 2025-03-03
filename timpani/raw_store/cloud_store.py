import boto3
import uuid

from timpani.app_cfg import TimpaniAppCfg
from gzip import compress, decompress

from timpani.util.run_state import RunState
from timpani.raw_store.item import Item
from timpani.raw_store.store import Store
from typing import List

import timpani.util.timpani_logger
from timpani.util.metrics_exporter import TelemetryMeterExporter


logging = timpani.util.timpani_logger.get_logger()


class CloudStore(Store):
    """
    Implements Raw Store operations using Boto3 interface for S3
    to talk to either remote aws S3 services or local minio.

    Storage partitioning is set up to support Hive/Athena and
    objects will be gz compressed.
    TODO: switch to parquet format
    """

    cfg = TimpaniAppCfg()
    telemetry = TelemetryMeterExporter(service_name="timpani-booker")

    STORE_LOCATION = cfg.s3_store_location
    BUCKET_NAME = f"timpani-raw-store-{cfg.deploy_env_label}"
    ITEMS_PATH = "content_items"  # base path for S3 to play nice with Athena
    STATES_PATH = "content_states"
    s3_bucket = None

    def __init__(self, store_location=None, bucket_name=None):
        """
        For setting non-default values instad of fetching from cfg
        (usually for testing)
        """
        if store_location is not None:
            self.STORE_LOCATION = store_location
        if bucket_name is not None:
            self.BUCKET_NAME = bucket_name
        self.records_stored_metric = self.telemetry.get_counter(
            "items.stored", "number of individual items acquired into raw store"
        )

    def login_and_validate(self, access_key=None, secret_key=None):
        """
        Create an S3 client using config in credentials,
        login to make sure server is up,
        check bucket access and create if it doesn't exist.
        AWS S3 doesn't require acces_key and secret_key
        because boto uses AWS roles for auth
        """

        if self.STORE_LOCATION == "s3.amazonaws.com":
            s3 = boto3.resource("s3")
        else:
            s3 = boto3.resource(
                "s3",
                endpoint_url=self.STORE_LOCATION,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
            )
        self.s3_bucket = s3.Bucket(self.BUCKET_NAME)

        logging.info(
            f"CloudStore connecting to {self.STORE_LOCATION} : {self.s3_bucket}"
        )

    def get_partition_path(self, partition_id: Store.Partition, base_path=ITEMS_PATH):
        """
        Constructs the path string with the appropriate keys for data
        store type
        """
        # NOTE: for S3, putting everything in a single date_id partition
        # for now because the other partition names overlap with column
        # names and Athena/hive doesn't like that
        # other partitions part of file name
        partition = (
            base_path
            + "/date_id="
            + partition_id.date_id
            + "/"
            + partition_id.workspace_id
            + "_"
            + partition_id.source_id
        )
        return partition

    def append_chunk(self, partition_id: Store.Partition, payload: List[Item]):
        super(CloudStore, self).validate(payload)
        """
        We are writing out a json compatible format instead of pickling
        because we'd like other things to be able to use the data in the
        object store (like AWS Athena)
        """
        chunk_id = uuid.uuid4().hex
        partition_path = self.get_partition_path(partition_id)
        object_name = partition_path + "_" + chunk_id + ".jsonl.gz"
        payload_str = ""
        num_items = 0
        for item in payload:
            # convert each item to json objects but
            # chunk is json lines https://jsonlines.org/
            payload_str += item.toJSON() + "\n"
            num_items += 1
        compressed_obj = compress(bytes(payload_str, encoding="utf8"))
        self.s3_bucket.put_object(
            Key=object_name,
            Body=compressed_obj,
            ContentType="application/json",
            ContentEncoding="gzip",
        )
        logging.debug("Wrote data to CloudStore object {}".format(object_name))
        self.records_stored_metric.add(
            num_items,
            attributes={
                "workspace_id": partition_id.workspace_id,
                "source_id": partition_id.source_id,
            },
        )

        return object_name

    def fetch_chunk(self, object_name: str):
        raw_bytes = (
            self.s3_bucket.Object(object_name)
            .get(
                ResponseContentType="application/json", ResponseContentEncoding="gzip"
            )["Body"]
            .read()
        )
        raw_obj = decompress(raw_bytes).decode("utf-8")
        # TODO: update to return Item list of item objects instead of dict
        return raw_obj

    def fetch_chunks_in_partition(self, partition_id: Store.Partition):
        bucket_prefix = self.get_partition_path(partition_id)
        logging.debug(f"Fetching objects in bucket {bucket_prefix}")
        for obj in self.s3_bucket.objects.filter(Prefix=bucket_prefix):
            yield self.fetch_chunk(obj.key)

            # NOTE: I confirmed this will return more than 1000 chunks without pagination

    def delete_partition(self, partition_id: Store.Partition):
        """
        Permenently delete all the content stored in a partition.
        Should only be used by test scripts
        """
        bucket_prefix = self.get_partition_path(partition_id)
        for obj in self.s3_bucket.objects.filter(Prefix=bucket_prefix):
            obj.delete()

    def record_partition_run_state(
        self, run_state: RunState, partition_id: Store.Partition
    ):
        """
        Record a state status into the raw store that can be used to start or resume jobs
        Path structure must be identical to partition path structure
        """
        chunk_id = uuid.uuid4().hex
        partition_path = self.get_partition_path(partition_id, self.STATES_PATH)
        object_name = partition_path + "_" + chunk_id + "_state.jsonl"
        payload_str = run_state.to_json()
        self.s3_bucket.put_object(
            Key=object_name,
            Body=payload_str,
            ContentType="application/json",
        )
        logging.debug("Wrote state to CloudStore object {}".format(object_name))
