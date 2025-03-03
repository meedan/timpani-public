import logging
import sys
from pythonjsonlogger import jsonlogger


from timpani.app_cfg import TimpaniAppCfg


LOGGER_NAME = "timpani"

"""
Logging format to be used by all timpani modules
Uses structure json format https://pypi.org/project/python-json-logger/
By default, log level is set by environment, but can be overriden by
the env param `LOG_LEVEL`
"""

# get the log level from the environment (default to INFO)
cfg = TimpaniAppCfg()

# the LOG_LEVEL env variable will override
if cfg.log_level is not None:
    log_level = cfg.log_level
else:
    # determine a default log level by environment
    if cfg.deploy_env_label in ["dev", "local"]:
        log_level = logging.DEBUG
    elif cfg.deploy_env_label == "live":
        log_level = logging.WARNING
        # NOTE: this means various process startup commands will not be recorded
    else:
        # default to info in QA etc
        log_level = logging.INFO

logger = logging.getLogger(LOGGER_NAME)
logger.setLevel(log_level)
logger.propagate = True
logHandler = logging.StreamHandler(sys.stdout)
json_formatter = jsonlogger.JsonFormatter(
    fmt="%(levelname)s %(asctime)s %(message)s %(module)s",
    # Note: We could rename fields, but doing that changes the output order
    # which makes it harder for humans to read
    # rename_fields={
    #    "asctime": "time",
    #    "message": "msg",
    #    "levelname": "level",
    # },
    datefmt="%Y-%m-%d %H:%M:%S",
)
logHandler.setFormatter(json_formatter)
logger.addHandler(logHandler)


@staticmethod
def get_logger():
    return logging.getLogger(LOGGER_NAME)
