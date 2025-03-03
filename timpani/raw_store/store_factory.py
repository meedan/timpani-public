from timpani.app_cfg import TimpaniAppCfg
from timpani.raw_store.store import Store


class StoreFactory(object):
    """
    Returns an appropriate configured raw content store depending on
    application and environment.
    Note: reqirements are loaded conditionally so that we don't require
    imports if env does not support
    """

    def get_store(app_cfg: TimpaniAppCfg) -> Store:
        """
        static function
        decide which kind of store based on app config
        can set this from docker compose via --env S3_STORE_LOCATION=s3.amazonaws.com
        """
        if app_cfg.s3_store_location == ("DebuggingFileStore"):
            from timpani.raw_store.debugging_file_store import DebuggingFileStore

            store = DebuggingFileStore()

        elif app_cfg.s3_store_location.startswith("minio"):
            # This is for debugging with defaults
            from timpani.raw_store.minio_store import MinioStore

            store = MinioStore()
            store.login_and_validate(app_cfg.minio_user, app_cfg.minio_password)
        else:
            from timpani.raw_store.cloud_store import CloudStore

            if app_cfg.s3_store_location == "s3.amazonaws.com":
                # for AWS S3
                store = CloudStore(store_location=app_cfg.s3_store_location)
                store.login_and_validate()
            else:
                # for minio etc, read PWD from env
                store = CloudStore(store_location=app_cfg.s3_store_location)
                store.login_and_validate(
                    TimpaniAppCfg.minio_user, TimpaniAppCfg.minio_password
                )
        return store
