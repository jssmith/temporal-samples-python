"""Custom TracingProcessor for Temporal workflows with OTEL trace continuity.

This module provides a Temporal-aware processor that extends the upstream
OpenInferenceTracingProcessor with support for the Temporal workflow sandbox.

The Temporal sandbox isolates contextvars, so standard OTEL context propagation
via get_current_span() doesn't work inside workflows. This processor overrides
_get_parent_context() to also check for parent context stored on the workflow
instance by the interceptor.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, cast

from opentelemetry import trace as otel_trace
from opentelemetry.context import Context
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags, Tracer, set_span_in_context

from openinference.instrumentation.openai_agents._processor import (
    OpenInferenceTracingProcessor,
)

if TYPE_CHECKING:
    from opentelemetry.sdk.trace import TracerProvider


# Attribute name for stored OTEL parent context (must match interceptor)
OTEL_PARENT_CONTEXT_ATTR = "__otel_parent_context"


def _get_workflow_otel_parent_context() -> Context | None:
    """Try to get OTEL parent context from workflow instance.

    Inside a Temporal workflow sandbox, OTEL's get_current_span() doesn't work
    because contextvars are isolated. The interceptor stores the parent OTEL
    context on the workflow instance, which IS accessible from within the sandbox.

    Returns:
        OTEL Context with parent span if found, None otherwise.
    """
    try:
        from temporalio import workflow

        instance = workflow.instance()
        parent_info = getattr(instance, OTEL_PARENT_CONTEXT_ATTR, None)
        if parent_info:
            parent_ctx = SpanContext(
                trace_id=parent_info["trace_id"],
                span_id=parent_info["span_id"],
                is_remote=True,
                trace_flags=TraceFlags(0x01),  # Sampled
            )
            parent_span = NonRecordingSpan(parent_ctx)
            return set_span_in_context(parent_span)
    except Exception:
        pass  # Not in workflow context
    return None


class ParentAwareTracingProcessor(OpenInferenceTracingProcessor):
    """Temporal-aware TracingProcessor that handles sandbox context isolation.

    Extends OpenInferenceTracingProcessor by overriding _get_parent_context()
    to also check for parent context stored on the workflow instance.

    This is necessary because the Temporal sandbox isolates contextvars,
    making standard OTEL context propagation via get_current_span() fail
    inside workflows.
    """

    def _get_parent_context(self) -> Context | None:
        """Get parent OTEL context, with Temporal workflow support.

        Checks two sources for parent context:
        1. Standard OTEL context via get_current_span() (works outside sandbox)
        2. Workflow instance attribute (works inside sandbox)

        Returns:
            OTEL Context with parent span if found, None otherwise.
        """
        # First try standard OTEL context (works outside sandbox, e.g., activities)
        if context := super()._get_parent_context():
            return context

        # Fall back to workflow instance for sandbox context
        return _get_workflow_otel_parent_context()


def setup_tracing(tracer_provider: TracerProvider) -> None:
    """Setup tracing with our Temporal-aware processor.

    This replaces OpenAIAgentsInstrumentor().instrument() with our custom
    processor that handles Temporal sandbox context isolation.

    Args:
        tracer_provider: The OTEL TracerProvider to use for creating spans.
    """
    from agents import set_trace_processors

    from openinference.instrumentation import OITracer, TraceConfig

    tracer = OITracer(
        otel_trace.get_tracer(
            "openinference.openai_agents",
            tracer_provider=tracer_provider,
        ),
        config=TraceConfig(),
    )
    set_trace_processors([ParentAwareTracingProcessor(cast(Tracer, tracer))])
