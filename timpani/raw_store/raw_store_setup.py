"""
Script to setup (or validate) environment configurations
and resources such as S3 buckets
TODO: this is now needed by both timpani and booker .. rename to minio setup but where should it live
"""
from timpani.app_cfg import TimpaniAppCfg

import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()

cfg = TimpaniAppCfg()


def create_minio_bucket(env):
    """
    Make sure the buckets we will need exist
    (all the hacks go here)
    """
    logging.info("creating buckets in minio store")
    from timpani.raw_store.minio_store import MinioStore

    # import minio.error.MinioException

    mi = MinioStore()
    mi.login_and_validate(cfg.minio_user, cfg.minio_password)
    bucket_name = f"timpani-raw-store-{env}"

    if mi.minio_client.bucket_exists(bucket_name):
        logging.info(f"bucket {bucket_name} already exists in minio store")
    else:
        mi.minio_client.make_bucket(bucket_name)
        logging.info(f"creating bucket {bucket_name} in minio store")


if __name__ == "__main__":
    logging.info(
        f"The application will be run in the {cfg.deploy_env_label} environment"
    )
    # figure out which environment we are in
    if cfg.deploy_env_label in ["local", "dev", "test"]:
        # create the buckets in minio container (should already exist in aws)
        create_minio_bucket(cfg.deploy_env_label)
    else:
        logging.info(f"no setup required for deploy environment {cfg.deploy_env_label}")
