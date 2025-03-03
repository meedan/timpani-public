from opentelemetry.sdk.resources import SERVICE_NAME, Resource


from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.metrics import NoOpMeterProvider
from opentelemetry.sdk.metrics.export import (
    PeriodicExportingMetricReader,
    ConsoleMetricExporter,
    # InMemoryMetricReader,
)
from timpani.app_cfg import TimpaniAppCfg
import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()


class TelemetryMeterExporter(object):
    """
    Provides a basic implementation of Open Telemetry metrics configured to provide
    simple counters to services so they can log metrics to the Honeycomb service.
    Should work with other metrics service providers with minimal changes
    TODO: the Honeycomb EU endpoint doesn't work with the api key https://api.eu1.honeycomb.io
    I'm not sure if there would be any befinits to logging there instead of in US?
    """

    HONEYCOMB_DATASET = "timpani"
    HONEYCOMB_API_ENDPOINT = "https://api.honeycomb.io"

    cfg = TimpaniAppCfg()

    # NOTE: in Honeycomb, the API key determines which environment that telemetry will appear in
    HONEYCOMB_API_KEY = cfg.telemetery_api_key
    METRICS_REPORTING_INTERVAL = cfg.metrics_reporting_interval

    def __init__(self, service_name: str, local_debug=False) -> None:
        # Service name i.e timpani-booker, timpani-conductor
        # TODO: read from config?
        self.service_name = service_name
        # these should be included whenever metrics are reported
        resource = Resource(
            attributes={
                SERVICE_NAME: self.service_name,
                # this env label makes it easier to disambiguate metrics downstream
                # but the they honycomb env is determined by the API key
                "env.label": self.cfg.deploy_env_label,
            }
        )

        if local_debug:
            # write metrics to console instead of sending them
            reader = PeriodicExportingMetricReader(
                ConsoleMetricExporter(),
                export_interval_millis=self.METRICS_REPORTING_INTERVAL,
            )
            meterProvider = MeterProvider(resource=resource, metric_readers=[reader])
        else:
            if self.HONEYCOMB_API_KEY == "":
                logging.warning(
                    "Metrics telemetery is not enabled because no HONEYCOMB_API_KEY found, running in no-op mode"
                )
                # create a 'no-op' meter provider so that counters can be created but won't do anything
                meterProvider = NoOpMeterProvider()

            else:
                logging.debug(
                    f"Metrics telemetry will be sent to {self.HONEYCOMB_API_ENDPOINT}"
                )
                reader = PeriodicExportingMetricReader(
                    OTLPMetricExporter(
                        endpoint=f"{self.HONEYCOMB_API_ENDPOINT}/v1/metrics",
                        headers={
                            "X-Honeycomb-Team": self.HONEYCOMB_API_KEY,
                            "X-Honeycomb-Dataset": self.HONEYCOMB_DATASET,
                        },
                    ),
                    export_interval_millis=self.METRICS_REPORTING_INTERVAL,
                )
                meterProvider = MeterProvider(
                    resource=resource, metric_readers=[reader]
                )

        # Sets the global default meter provider
        # TODO: if has previously been set in a session will warn
        # "Overriding of current MeterProvider is not allowed" and ignore
        # maybe we could move this into some kind of class-level sigleton
        # so it only gets called once?
        metrics.set_meter_provider(meterProvider)

    def get_counter(self, counter_name: str, description: str):
        """
        Returns a named 'counter' metric that can only be incremented in integer increments
        """
        # Creates a meter from the global meter provider
        meter = metrics.get_meter(f"{self.cfg}.{self.service_name}")
        counter = meter.create_counter(
            f"{counter_name}.counter",
            unit="1",
            description=description,
        )
        return counter

    def get_updown_counter(self, counter_name: str, description: str, unit: str):
        """
        Returns a named 'counter' metric that be incremented or decremented
        """
        # Creates a meter from the global meter provider
        meter = metrics.get_meter(f"{self.cfg}.{self.service_name}")
        counter = meter.create_up_down_counter(
            f"{counter_name}.counter",
            unit=unit,
            description=description,
        )
        return counter

    def get_gauge(self, gauge_name: str, description: str, unit: str):
        """
        Returns a named 'gauge' metric
        """
        # Creates a gauge from the global meter provider
        meter = metrics.get_meter(f"{self.cfg}.{self.service_name}")
        gauge = meter.create_gauge(
            f"{gauge_name}.gauge",
            unit=unit,
            description=description,
        )
        return gauge
