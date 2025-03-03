import io
import uuid
from minio import Minio
from typing import List
from timpani.app_cfg import TimpaniAppCfg
from timpani.raw_store.item import Item
from timpani.raw_store.store import Store
from timpani.util.run_state import RunState

import timpani.util.timpani_logger
from timpani.util.metrics_exporter import TelemetryMeterExporter

logging = timpani.util.timpani_logger.get_logger()


class MinioStore(Store):
    """
    Implements Raw Store operations using Minio interface
    to talk to either a local Minio object store in a container
    (for local dev)
    TODO: refactor so share as much as possible between
    minio store and botoS3 store so they will work the same way
    """

    cfg = TimpaniAppCfg()
    telemetry = TelemetryMeterExporter(service_name="timpani-booker")
    records_stored_metric = telemetry.get_counter(
        "items.stored", "number of individual items acquired into minio raw store"
    )

    STORE_LOCATION = cfg.s3_store_location
    MINIO_BUCKET_NAME = f"timpani-raw-store-{cfg.deploy_env_label}"
    ITEMS_PATH = "content_items"  # base path for items
    STATES_PATH = "content_states"  # where states will be stored

    minio_client = None

    def login_and_validate(self, access_key: str, secret_key: str):
        """
        Create a Minio client using config in credentials,
        login to make sure server is up,
        check bucket access and create if it doesn't exist.
        """
        # Create a client with the MinIO server playground, its access key
        # and secret key.
        # TODO: pull keys from SSM? or pull from app config
        # TODO: need to set up TLS csert for server to set secure=True
        # https://min.io/docs/minio/linux/operations/network-encryption.html?ref=docs-redirect
        minio_server = "minio:9002"  # minio localdev docker container
        self.minio_client = Minio(
            minio_server,
            access_key=access_key,
            secret_key=secret_key,
            secure=False,
        )

        logging.info(f"MinioStore connected to {minio_server}")
        found = self.minio_client.bucket_exists(self.MINIO_BUCKET_NAME)
        if not found:
            logging.info(
                "Bucket {} did not exist, creating".format(self.MINIO_BUCKET_NAME)
            )
            self.minio_client.make_bucket(self.MINIO_BUCKET_NAME)

    def get_partition_path(self, partition_id: Store.Partition, base_path=ITEMS_PATH):
        partition = (
            base_path
            + "/"
            + partition_id.workspace_id
            + "/"
            + partition_id.source_id
            + "/"
            + partition_id.date_id
        )
        return partition

    def append_chunk(self, partition_id: Store.Partition, payload: List[Item]):
        super(MinioStore, self).validate(payload)
        """
        We are writing out a json compatible format instead of pickling
        because we'd like other things to be able to use the data in the
        object store (like AWS Athena)
        """
        chunk_id = uuid.uuid4().hex
        partition_path = self.get_partition_path(partition_id)
        object_name = partition_path + "/" + chunk_id + ".jsonl"
        payload_str = ""
        num_items = 0
        for item in payload:
            # convert each item to json objects but
            # chunk is json lines https://jsonlines.org/
            payload_str += item.toJSON() + "\n"
            num_items += 1
        # need to encode to bytes
        bytes_object = payload_str.encode()
        # TODO: compression?
        self.minio_client.put_object(
            bucket_name=self.MINIO_BUCKET_NAME,
            object_name=object_name,
            length=len(bytes_object),
            data=io.BytesIO(bytes_object),
        )
        logging.debug("Wrote MinioStore data to partition{}".format(partition_path))
        self.records_stored_metric.add(
            num_items,
            attributes={
                "workspace_id": partition_id.workspace_id,
                "source_id": partition_id.source_id,
            },
        )
        return object_name

    def fetch_chunk(self, object_name: str):
        response = self.minio_client.get_object(
            bucket_name=self.MINIO_BUCKET_NAME, object_name=object_name
        )
        raw_obj = response.data.decode()
        return raw_obj

    def record_partition_run_state(
        self, run_state: RunState, partition_id: Store.Partition
    ):
        """
        Record a state status into the raw store that can be used to start or resume jobs
        Path structure must be idental to partition path structure
        """
        chunk_id = uuid.uuid4().hex
        partition_path = self.get_partition_path(partition_id, self.STATES_PATH)
        object_name = partition_path + "/" + chunk_id + ".jsonl"
        payload_str = run_state.to_json() + "\n"
        # need to encode to bytes
        bytes_object = payload_str.encode()
        self.minio_client.put_object(
            bucket_name=self.MINIO_BUCKET_NAME,
            object_name=object_name,
            length=len(bytes_object),
            data=io.BytesIO(bytes_object),
        )
        logging.debug("Wrote MinioStore state to partition{}".format(partition_path))
