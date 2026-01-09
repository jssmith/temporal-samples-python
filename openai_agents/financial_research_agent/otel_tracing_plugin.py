"""OTEL Tracing Plugin for Temporal workflows with OpenAI Agents.

This plugin provides clean OTEL context propagation using Temporal SDK's built-in
TracingInterceptor with create_spans=False, which propagates W3C TraceContext
without creating Temporal infrastructure spans.

The magic that makes this work:
1. TemporalAwareContext - A custom OTEL context implementation (set via
   OTEL_PYTHON_CONTEXT=temporal_aware_context) that stores context on both
   contextvars AND workflow.instance(). This survives Temporal's sandbox isolation.
2. TracingInterceptor(create_spans=False) - Propagates OTEL context via headers
   without creating Temporal spans, giving you clean traces with only your
   application spans.

Usage:
    # Option 1: Environment variable (recommended)
    import os
    os.environ["OTEL_PYTHON_CONTEXT"] = "temporal_aware_context"

    # Then create plugin
    plugin = OtelTracingPlugin(tracer_provider)
    client = await Client.connect("localhost:7233", plugins=[openai_plugin, plugin])

    # Option 2: Let the plugin set it for you
    plugin = OtelTracingPlugin(tracer_provider)  # Sets env var automatically
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from opentelemetry import trace as otel_trace
from temporalio.contrib.opentelemetry import TracingInterceptor
from temporalio.plugin import SimplePlugin

if TYPE_CHECKING:
    from opentelemetry.sdk.trace import TracerProvider


def setup_tracing(tracer_provider: TracerProvider) -> None:
    """Set up OpenAI Agents OTEL tracing with Temporal sandbox support.

    This sets up OpenInferenceTracingProcessor which will now work correctly
    inside Temporal workflows thanks to TemporalAwareContext making
    get_current_span() work inside the sandbox.
    """
    from openinference.instrumentation.openai_agents import OpenAIAgentsInstrumentor

    # Set as global tracer provider
    otel_trace.set_tracer_provider(tracer_provider)

    # Instrument OpenAI Agents SDK - this registers OpenInferenceTracingProcessor
    # which converts OpenAI Agents spans to OTEL spans
    OpenAIAgentsInstrumentor().instrument(tracer_provider=tracer_provider)


class OtelTracingPlugin(SimplePlugin):
    """Plugin for OTEL context propagation in Temporal workflows with OpenAI Agents.

    This plugin:
    1. Ensures OTEL_PYTHON_CONTEXT is set to use TemporalAwareContext
    2. Uses TracingInterceptor with create_spans=False for context-only propagation
    3. Sets up OpenInferenceTracingProcessor for OTEL span creation

    The result is clean traces with only your application spans (from OpenAI Agents
    SDK), no Temporal infrastructure spans, and proper parent-child relationships
    even inside sandboxed workflows.

    Args:
        tracer_provider: The OTEL TracerProvider to use for creating spans.
            If provided, setup_tracing() will be called automatically.
    """

    def __init__(self, tracer_provider: TracerProvider | None = None) -> None:
        # Ensure TemporalAwareContext is used for OTEL context
        # This makes get_current_span() work inside Temporal's sandbox
        if "OTEL_PYTHON_CONTEXT" not in os.environ:
            os.environ["OTEL_PYTHON_CONTEXT"] = "temporal_aware_context"

        # Set up tracing if provider given
        if tracer_provider is not None:
            setup_tracing(tracer_provider)

        # Use TracingInterceptor with create_spans=False
        # This propagates OTEL context via W3C TraceContext headers
        # but doesn't create any Temporal spans
        interceptor = TracingInterceptor(create_spans=False)

        super().__init__(
            name="OtelTracingPlugin",
            worker_interceptors=[interceptor],
            client_interceptors=[interceptor],
        )
