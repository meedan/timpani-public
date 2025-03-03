import unittest
import subprocess
from timpani.app_cfg import TimpaniAppCfg


class TestBookerAcquire(unittest.TestCase):
    @unittest.skipIf(
        TimpaniAppCfg().deploy_env_label in ["test", "live"],
        "CI test environment does not have access to SSM credentials",
        # TODO: implement tests with faker data source that doesn't need any secrets
    )
    def test_cli_args(self):
        """
        Make sure we can run the booker acquisition command from the CLI
        with standard args
        """
        try:
            subprocess.run(
                [
                    "python3",
                    "timpani/booker/acquire.py",
                    "--workspace_id=test",
                    "--date_id=20230724",
                    "--limit_downloads=True",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as error:
            print(error.stdout)
            print(error.stderr)
            raise error
