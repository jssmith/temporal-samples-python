"""Shared fixtures for financial_research_agent tests.

This module provides shared OTEL tracing setup to avoid TracerProvider conflicts
when running multiple test files together.
"""

from __future__ import annotations

import pytest
from openinference.instrumentation.openai_agents import OpenAIAgentsInstrumentor
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter


# Global provider/exporter - only initialized once per process
_provider: TracerProvider | None = None
_exporter: InMemorySpanExporter | None = None
_instrumented: bool = False


def _setup_shared_tracing() -> tuple[TracerProvider, InMemorySpanExporter]:
    """Setup shared OTEL tracing. Only initializes once per process."""
    global _provider, _exporter, _instrumented

    if _provider is None:
        _provider = TracerProvider(
            resource=Resource.create({"service.name": "test-financial-research"})
        )
        _exporter = InMemorySpanExporter()
        _provider.add_span_processor(SimpleSpanProcessor(_exporter))
        trace.set_tracer_provider(_provider)

    if not _instrumented:
        OpenAIAgentsInstrumentor().instrument(tracer_provider=_provider)
        _instrumented = True

    return _provider, _exporter


@pytest.fixture
def otel_exporter():
    """Shared OTEL exporter fixture. Clears spans between tests."""
    _, exporter = _setup_shared_tracing()
    exporter.clear()
    yield exporter
    exporter.clear()


@pytest.fixture
def tracing():
    """Alias for otel_exporter for backward compatibility."""
    _, exporter = _setup_shared_tracing()
    exporter.clear()
    yield exporter
    exporter.clear()
