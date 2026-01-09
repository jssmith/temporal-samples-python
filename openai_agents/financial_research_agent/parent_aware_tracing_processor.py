"""Custom TracingProcessor that respects existing OTEL parent context.

This module provides a fix for OpenInferenceTracingProcessor.on_trace_start()
which creates new OTEL trace IDs instead of continuing existing parent context.
This breaks trace continuity when propagating traces across service boundaries
(e.g., from client to Temporal worker).

The fix: Pass the current OTEL context to start_span() so that new traces
continue the existing OTEL trace ID instead of creating a new one.

For Temporal workflows, the OTEL context is stored on the workflow instance
since the workflow sandbox isolates Python state and contextvars don't propagate.

Additional fix: Enter the OTEL span as the current span so that context
propagation can find it when creating headers.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, cast

from agents.tracing import Trace
from opentelemetry import trace as otel_trace
from opentelemetry.context import attach, detach
from opentelemetry.trace import set_span_in_context, Tracer, SpanContext, TraceFlags, NonRecordingSpan

from openinference.instrumentation.openai_agents._processor import (
    OpenInferenceTracingProcessor,
)
from openinference.semconv.trace import (
    OpenInferenceSpanKindValues,
    SpanAttributes,
)

if TYPE_CHECKING:
    from opentelemetry.sdk.trace import TracerProvider


OPENINFERENCE_SPAN_KIND = SpanAttributes.OPENINFERENCE_SPAN_KIND

# Attribute name for stored OTEL parent context (must match interceptor)
OTEL_PARENT_CONTEXT_ATTR = "__otel_parent_context"


def _get_workflow_otel_parent_context() -> Context | None:
    """Try to get OTEL parent context from workflow instance.

    Inside a Temporal workflow sandbox, OTEL's attach() doesn't work because
    contextvars are isolated. The interceptor stores the parent OTEL context
    on the workflow instance, which IS accessible from within the sandbox.

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
    """TracingProcessor that respects existing OTEL parent context.

    Fixes the issue where OpenInferenceTracingProcessor.on_trace_start()
    creates new OTEL trace IDs instead of continuing parent context.

    When an OTEL parent span exists (e.g., from propagated context), this
    processor will create the new trace span as a child of that parent,
    preserving the OTEL trace ID across service boundaries.
    """

    def on_trace_start(self, trace: Trace) -> None:
        """Called when a trace is started.

        Unlike the base implementation, this checks for existing OTEL parent
        context and passes it to start_span() to maintain trace continuity.

        Checks two sources for parent context:
        1. Current OTEL span (works outside sandbox, e.g., activities)
        2. Workflow instance attribute (works inside sandbox)

        Args:
            trace: The trace that started.
        """
        context = None

        # First, check for current OTEL span context (works outside sandbox)
        current_span = otel_trace.get_current_span()
        parent_ctx = current_span.get_span_context() if current_span else None
        if parent_ctx and parent_ctx.is_valid:
            context = set_span_in_context(current_span)

        # If no OTEL context, try workflow instance (works inside sandbox)
        if context is None:
            context = _get_workflow_otel_parent_context()

        otel_span = self._tracer.start_span(
            name=trace.name,
            context=context,  # Pass parent context to continue trace ID
            attributes={
                OPENINFERENCE_SPAN_KIND: OpenInferenceSpanKindValues.AGENT.value,
            },
        )
        self._root_spans[trace.trace_id] = otel_span

        # Enter the span as current so context propagation can find it
        # Store the token so we can detach in on_trace_end
        span_context = set_span_in_context(otel_span)
        token = attach(span_context)
        if not hasattr(self, "_context_tokens"):
            self._context_tokens = {}
        self._context_tokens[trace.trace_id] = token

    def on_trace_end(self, trace: Trace) -> None:
        """Called when a trace is ended.

        Detaches the OTEL context we attached in on_trace_start.
        """
        # Detach the context token we stored
        if hasattr(self, "_context_tokens") and trace.trace_id in self._context_tokens:
            token = self._context_tokens.pop(trace.trace_id)
            try:
                detach(token)
            except ValueError:
                pass  # Context was created in different context - expected in some cases

        # Call parent implementation to end the span
        super().on_trace_end(trace)


def setup_tracing(tracer_provider: TracerProvider) -> None:
    """Setup tracing with our custom parent-aware processor.

    This replaces OpenAIAgentsInstrumentor().instrument() with our custom
    processor that respects parent OTEL context.

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
