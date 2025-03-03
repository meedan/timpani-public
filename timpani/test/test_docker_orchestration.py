import unittest
import sys
import json

# import os.path
import timpani.util.timpani_logger


class TestBasicEnviornment(unittest.TestCase):
    def test_tests_are_run(self):
        print("timpani tests are running")
        assert True

    def test_deps_installed(self):
        # ensure that non-standard libraries in requirements.txt are installed
        import boto3

        boto3

    def test_py_version(self):
        assert sys.version_info[0] >= 3

    # test logging
    def test_json_logging(self):
        """
        Should record logs in json format with specific expected fields:
        i.e '{"asctime": "2024-01-29 14:00:31", "module": "test_docker_orchestration", "levelname": "WARNING", "message": "Warning Here"}'
        """
        # TODO: how do we log in a format that will go to cloudwatch
        logger = timpani.util.timpani_logger.get_logger()
        logger.info("testing logging")
        self.maxDiff = None

        with self.assertLogs(logger, level="DEBUG") as test_log:
            logger.setLevel("DEBUG")
            logger.handlers[0].setFormatter(timpani.util.timpani_logger.json_formatter)
            # logger.debug("safe debug statement")
            # logger.info("Its an information")
            logger.warning("Generates Warning")
            # logger.error("Error occurs i.e when divide by zero")
            # logger.critical("Internet connection is slow")

            warning_json = json.loads(test_log.output[0])
            self.assertIsNotNone(
                warning_json["asctime"]
            )  # don't know what time the test runs
            self.assertEqual(warning_json["module"], "test_docker_orchestration")
            self.assertEqual(warning_json["levelname"], "WARNING")
            self.assertEqual(warning_json["message"], "Generates Warning")

    # TODO: make sure all of the services are availible arnd running
