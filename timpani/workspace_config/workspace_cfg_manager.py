from timpani.workspace_config.workspace_config import WorkspaceConfig
from timpani.workspace_config.meedan_cfg import MeedanConfig
from timpani.workspace_config.test_workspace_cfg import TestWorkspaceConfig
from timpani.workspace_config.meedan_lite_cfg import MeedanLiteConfig
from timpani.workspace_config.meedan_tse_cfg import MeedanTSEConfig
from timpani.workspace_config.junkipeida_public_cfg import JunkipediaPublicConfig
from timpani.workspace_config.meedan_nawa_gaza_cfg import MeedanNAWAGazaConfig
from timpani.workspace_config.meedan_india_election_cfg import MeedanIndiaElectionConfig
from timpani.workspace_config.meedan_classy_india_cfg import (
    MeedanClassyIndiaElectionConfig,
)
from timpani.workspace_config.meedan_us2024_cfg import MeedanUS2024Config

import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()


class WorkspaceConfigManager(object):
    """
    Class for managing access to workspace configurations used by teams
    (either from disk, db, or an internal repository)
    """

    # these are all the workspace configurations it knows about
    # when new workspace_config classes are added, they also need to
    # be imported and listed here before they are visable to be called
    # eventually, this could be a database, or would be finding these
    # via introspection
    REGISTRED_WORKSPACE_CONFIGURATIONS = [
        MeedanConfig,
        MeedanLiteConfig,
        TestWorkspaceConfig,
        MeedanTSEConfig,
        JunkipediaPublicConfig,
        MeedanNAWAGazaConfig,
        MeedanIndiaElectionConfig,
        MeedanClassyIndiaElectionConfig,
        MeedanUS2024Config,
        StopAAPIHateConfig,
    ]
    workspace_configs = {}

    """
    Instantiate the configs it knows about (reporting errors)
    and create a lookup dictionary by slug.
    """
    for cls in REGISTRED_WORKSPACE_CONFIGURATIONS:
        try:
            cfg = cls()
            slug = cfg.get_workspace_slug()

            # make sure we don't have two workspace with same id slug
            assert slug not in workspace_configs

            workspace_configs[slug] = cfg
        except Exception as e:
            err = f"Unable to load workspace config {cls} due to error: {e}"
            logging.error(err)

    def get_config_for_workspace(self, workspace_id: str) -> WorkspaceConfig:
        """
        Return workspace config corresponding to workspace_id
        """
        assert (
            workspace_id in self.workspace_configs
        ), f"workspace_id '{workspace_id}' does not match any known workspace_configs."
        return self.workspace_configs[workspace_id]

    def get_all_workspace_ids(self):
        """
        Return the workspace_id slugs for all the workspace configurations it knows about
        """
        return self.workspace_configs.keys()
