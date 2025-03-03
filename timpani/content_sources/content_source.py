import sentry_sdk
from timpani.app_cfg import TimpaniAppCfg
from timpani.workspace_config.workspace_config import WorkspaceConfig
from timpani.raw_store.store import Store
from timpani.util.run_state import RunState


class ContentSource(object):
    app_cfg = TimpaniAppCfg()
    # initialize the sentry error tracking integration
    sentry_sdk.init(
        dsn=app_cfg.sentry_sdk_dsn,
        environment=app_cfg.deploy_env_label,
        traces_sample_rate=1.0,
    )

    def get_source_name(self):
        """
        Unique string id that can be used as a key corresponding to this source
        """
        raise NotImplementedError

    def acquire_new_content(
        self,
        workspace_cfg: WorkspaceConfig,
        store_location: Store,
        run_state: RunState,
        partition_id: Store.Partition = None,
        limit_downloads: bool = False,
    ):
        raise NotImplementedError
