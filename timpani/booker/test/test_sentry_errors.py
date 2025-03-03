import unittest
import sentry_sdk
from timpani.app_cfg import TimpaniAppCfg


class TestAlegreWrappers(unittest.TestCase):
    """
    Tests of error message reporting via Sentry.
    These are used to produce preditable error messages in sentry
    https://meedan.sentry.io/issues/?project=4506337520451584
    (DSN can be found there as well, needs to be set in environemnt)
    """

    cfg = TimpaniAppCfg()

    @classmethod
    def setUpClass(self):
        self.cfg = TimpaniAppCfg()

        # initialize the sentry error tracking integration
        sentry_sdk.init(
            dsn=self.cfg.sentry_sdk_dsn,
            # Set traces_sample_rate to 1.0 to capture 100%
            # of transactions for performance monitoring.
            environment=self.cfg.deploy_env_label,
            traces_sample_rate=1.0,
            # Set profiles_sample_rate to 1.0 to profile 100%
            # of sampled transactions.
            # We recommend adjusting this value in production.
            profiles_sample_rate=1.0,
        )

    def test_sentry_message_reporting(self):
        """
        Confirm that messages logged via sentry are reported
        """
        sentry_sdk.capture_message(
            f"Timpani Booker test sentry capture_message in env {self.cfg.deploy_env_label}"
        )

    def test_sentry_report_thrown_error(self):
        """Test that thrown errors are reported"""
        # not sure how to actually throw error, since test framework will catch
        try:
            assert (
                False
            ), f"Timpani Booker test sentry capture_message in env {self.cfg.deploy_env_label}"
        except AssertionError as e:
            sentry_sdk.capture_exception(e)
