"""
Utility functions for interacting with secrets
and paramters stored in AWS SSM
TODO: make this 'fetch_secrets' and support reading from env vars or other secret stores?
"""

import boto3

from timpani.app_cfg import TimpaniAppCfg
from timpani.workspace_config.workspace_config import WorkspaceConfig


class AccessSSM(object):
    """
    Authenticate to AWS by asking boto3 to assume a role
    https://stackoverflow.com/questions/44171849/aws-boto3-assumerole-example-which-includes-role-usage

    Fetches paramters stored in AWS Systems Manager Parameter store
    https://eu-west-1.console.aws.amazon.com/systems-manager/parameters/?region=eu-west-1&tab=Table

    It should be possible for other organizations to share api key secrets via SSM
    https://docs.aws.amazon.com/systems-manager/latest/userguide/documents-ssm-sharing.html

    NOTE: This sometimes produces terminal output like
    `sys:1: ResourceWarning: unclosed <ssl.SSLSocket fd=5, family=2, type=1, proto=6,...>`
    which appears to be a known bug with no impact: https://github.com/boto/boto3/issues/3552

    """

    def __init__(self):
        self.cfg = TimpaniAppCfg()
        # TODO: can we leave this out and just letboto3 inherit from
        # AWS_PROFILE if that env var exists?
        self.session = boto3.session.Session(region_name=self.cfg.aws_region)
        self.ssm = self.session.client("ssm")

    def _format_ssm_key(self, param_key: str, workspace_cfg: WorkspaceConfig):
        """
        Concats the various env, workspace, and paramter components
        to construct an SSM key in a standardized way:
        /DEPLOY_ENV/timpani/workspace_id/<param_key>)
        """
        ssm_path = "/{0}/{1}/{2}/{3}".format(
            self.cfg.deploy_env_label,
            self.cfg.app,
            workspace_cfg.get_workspace_slug(),
            param_key,
        )
        return ssm_path

    def get_parameter_for_workspace(
        self, param_key: str, workspace_cfg: WorkspaceConfig
    ):
        """
        constructs appropriate ssm key path for param and fetches from SSM
        """

        # /dev/timpani/meedan/non_secret_value
        ssm_path = self._format_ssm_key(param_key, workspace_cfg)

        response = self.ssm.get_parameters(Names=[ssm_path])
        for parameter in response["Parameters"]:
            return parameter["Value"]

    def get_secret_for_workspace(self, param_key: str, workspace_cfg: WorkspaceConfig):
        """
        constructs appropriate ssm key path for param and fetches with
        decryption from SSM.
        NOTE: this won't work when testing code offline, maybe we want
        to also support default localdev credentials?
        """

        # /dev/timpani/meedan/junkipedia_access_token_secret
        ssm_path = self._format_ssm_key(param_key, workspace_cfg)

        response = self.ssm.get_parameters(Names=[ssm_path], WithDecryption=True)
        for parameter in response["Parameters"]:
            return parameter["Value"]
