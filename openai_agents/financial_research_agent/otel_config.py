"""Shared OpenTelemetry configuration for financial research agent.

This module provides common OTEL setup used by both the client and worker.
"""

from __future__ import annotations

import os

from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk import trace as trace_sdk
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace.export import SimpleSpanProcessor

# Default OTLP endpoint - can be overridden via OTEL_EXPORTER_OTLP_ENDPOINT env var
DEFAULT_OTLP_ENDPOINT = "http://localhost:4317"


def get_otlp_endpoint() -> str:
    """Get OTLP endpoint from environment or use default."""
    return os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", DEFAULT_OTLP_ENDPOINT)


def create_tracer_provider(service_name: str) -> trace_sdk.TracerProvider:
    """Create a TracerProvider configured for OTLP export.

    Args:
        service_name: The service name to use in traces.

    Returns:
        Configured TracerProvider ready for use.
    """
    resource = Resource.create(
        attributes={
            "service.name": service_name,
        }
    )
    tracer_provider = trace_sdk.TracerProvider(resource=resource)

    endpoint = get_otlp_endpoint()
    otlp_exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
    processor = SimpleSpanProcessor(otlp_exporter)

    tracer_provider.add_span_processor(processor)

    return tracer_provider
