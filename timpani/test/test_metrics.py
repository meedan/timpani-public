import unittest
from timpani.util.metrics_exporter import TelemetryMeterExporter
from timpani.app_cfg import TimpaniAppCfg


class TestBasicEnviornment(unittest.TestCase):
    """
    TODO: these tests don't actually validate anything, they just exercise the code.
    Should be possible to write tests using InMemoryMetricReader?
    NOTE: if api key is not located, test will record to console
    ```
    timpani % docker compose run -e HONEYCOMB_API_KEY=<api key> booker test timpani/test/ test_metrics.py
    ```
    """

    @unittest.skipIf(
        TimpaniAppCfg().telemetery_api_key != "",
        "skipped local Honeycomb console test because HONECOMB_API_KEY is provided",
    )
    def test_metrics_reported_console(self):
        """
        Should print metrics to console
        """
        telemetry = TelemetryMeterExporter("timpani-test-service", local_debug=True)
        # https://opentelemetry.io/docs/languages/python/instrumentation/
        counter = telemetry.get_counter("test", "checking if the counter works locally")
        counter.add(1, {"workspace_id": "testing"})

    @unittest.skipIf(
        TimpaniAppCfg().telemetery_api_key == "",
        "skipped Honeycomb integration test because HONECOMB_API_KEY is missing",
    )
    def test_metrics_reported_honeycomb(self):
        """
        Should deliver to honeycomb metrics collecting service
        """
        telemetry = TelemetryMeterExporter("timpani-test-service", local_debug=False)
        counter = telemetry.get_counter(
            "test", "checking if the counter works to honeycomb"
        )
        # record some values
        for n in range(10):
            counter.add(1, {"workspace_id": "testing"})

        # try adding another value
        counter.add(5, {"workspace_id": "testing"})

    def test_more_metrics_reported_honeycomb(self):
        """
        Should deliver to honeycomb metrics collecting service
        (trying to avoid "Overriding of current MeterProvider is not allowed" warning)
        """
        telemetry = TelemetryMeterExporter("timpani-test-service", local_debug=False)
        counter = telemetry.get_counter(
            "another_test", "checking if the second counter works to honeycomb"
        )
        # record some values
        for n in range(10):
            counter.add(1, {"workspace_id": "testing"})
