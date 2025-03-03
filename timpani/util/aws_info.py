import boto3
from timpani.app_cfg import TimpaniAppCfg

import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()


class RoleLogger(object):
    """
    Print out aws role information for help with permission debugging
    """

    def __init__(self):
        self.cfg = TimpaniAppCfg()
        # TODO: can we leave this out and just letboto3 inherit from
        # AWS_PROFILE if that env var exists?
        self.session = boto3.session.Session(region_name=self.cfg.aws_region)
        self.sts = self.session.client("sts")

    def log_role_info(self):
        try:
            role_info = self.sts.get_caller_identity()
            del role_info["ResponseMetadata"]
            logging.info(f"Current AWS role info:\n{role_info}")
            print(role_info)
        except Exception as e:
            logging.warning(f"Unable access aws info to log role: {e}")


if __name__ == "__main__":
    logger = RoleLogger()
    logger.log_role_info()
