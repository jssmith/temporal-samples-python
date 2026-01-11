"""Shared OTEL tracing setup for financial research agent."""

import os

from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor

DEFAULT_OTLP_ENDPOINT = "http://localhost:4317"


def get_otlp_endpoint() -> str:
    """Get OTLP endpoint from environment or use default."""
    return os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", DEFAULT_OTLP_ENDPOINT)


def create_tracer_provider(service_name: str) -> TracerProvider:
    """Create a TracerProvider configured for OTLP export.

    Args:
        service_name: The service name to use in traces.

    Returns:
        A configured TracerProvider.
    """
    resource = Resource.create(attributes={"service.name": service_name})
    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(
        SimpleSpanProcessor(OTLPSpanExporter(endpoint=get_otlp_endpoint(), insecure=True))
    )
    return tracer_provider
