"""OpenAI Agents SDK context propagation without temporal span creation.

This interceptor propagates the native OpenAI Agents SDK trace context
through Temporal workflow/activity boundaries WITHOUT creating any
temporal:* spans. This allows the OpenAIAgentsInstrumentor to export
only the business logic spans (trace(), custom_span()) to OpenTelemetry.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any

from agents import custom_span, get_current_span, trace
from agents.tracing import get_trace_provider
from agents.tracing.spans import NoOpSpan

from opentelemetry import trace as otel_trace
from opentelemetry.context import attach, detach
from opentelemetry.trace import SpanContext, TraceFlags, NonRecordingSpan, set_span_in_context

import temporalio.activity
import temporalio.client
import temporalio.converter
import temporalio.worker
import temporalio.workflow
from temporalio import activity, workflow

HEADER_KEY = "__openai_span"

# Debug flag
_DEBUG = False
def _debug(msg: str) -> None:
    if _DEBUG:
        print(f"[INTERCEPTOR] {msg}")


def _set_header_from_context(
    input: Any, payload_converter: temporalio.converter.PayloadConverter
) -> None:
    """Inject current OpenAI Agents trace/span info AND OTEL context into headers."""
    current = get_current_span()
    if current is None or isinstance(current, NoOpSpan):
        _debug(f"_set_header_from_context: no current span, skipping")
        return
    _debug(f"_set_header_from_context: setting headers from span_id={current.span_id[:8]}...")

    current_trace = get_trace_provider().get_current_trace()

    # Capture OTEL context for trace continuity
    otel_span = otel_trace.get_current_span()
    otel_ctx = otel_span.get_span_context() if otel_span else None

    header_data = {
        "traceName": current_trace.name if current_trace else "Workflow",
        "spanId": current.span_id,
        "traceId": current.trace_id,
    }

    # Add OTEL context if available and valid
    if otel_ctx and otel_ctx.is_valid:
        header_data["otelTraceId"] = format(otel_ctx.trace_id, "032x")
        header_data["otelSpanId"] = format(otel_ctx.span_id, "016x")

    input.headers = {
        **input.headers,
        HEADER_KEY: payload_converter.to_payload(header_data),
    }


@contextmanager
def _attach_context_from_header(
    input: Any, payload_converter: temporalio.converter.PayloadConverter
):
    """Extract and attach OpenAI Agents trace context AND OTEL context from headers.

    Unlike OpenAIAgentsTracingInterceptor.context_from_header(), this does NOT
    create any temporal:* spans - it only attaches the context.

    IMPORTANT: We attach OTEL context FIRST so that when the SDK trace/spans are
    created, the OpenInferenceTracingProcessor will create OTEL spans under the
    correct parent, preserving the trace_id across Temporal boundaries.
    """
    payload = input.headers.get(HEADER_KEY)
    span_info = payload_converter.from_payload(payload) if payload else None

    if span_info is None:
        yield
        return

    workflow_type = (
        activity.info().workflow_type
        if activity.in_activity()
        else workflow.info().workflow_type
    )
    metadata = {
        "temporal:workflowId": activity.info().workflow_id
        if activity.in_activity()
        else workflow.info().workflow_id,
        "temporal:runId": activity.info().workflow_run_id
        if activity.in_activity()
        else workflow.info().run_id,
        "temporal:workflowType": workflow_type,
    }

    # Restore OTEL context FIRST if available
    # This ensures OpenInferenceTracingProcessor creates spans under the correct parent
    otel_token = None
    if "otelTraceId" in span_info and "otelSpanId" in span_info:
        parent_ctx = SpanContext(
            trace_id=int(span_info["otelTraceId"], 16),
            span_id=int(span_info["otelSpanId"], 16),
            is_remote=True,
            trace_flags=TraceFlags(0x01),  # Sampled
        )
        parent_span_carrier = NonRecordingSpan(parent_ctx)
        otel_context = set_span_in_context(parent_span_carrier)
        otel_token = attach(otel_context)

    try:
        # Check if trace already exists (e.g., from previous workflow activation)
        current_trace = get_trace_provider().get_current_trace()
        current_span = get_current_span()
        is_activity = activity.in_activity()
        context_type = "activity" if is_activity else "workflow"
        has_span = current_span is not None and not isinstance(current_span, NoOpSpan)
        _debug(f"_attach_context_from_header ({context_type}): current_trace={current_trace is not None}, current_span={has_span}")

        if current_trace is not None and has_span:
            # Both trace and span exist - just continue
            _debug(f"_attach_context_from_header ({context_type}): trace+span exist, continuing")
            yield
        elif current_trace is not None:
            # Trace exists but no span - need span for header propagation to child activities
            _debug(f"_attach_context_from_header ({context_type}): creating span only")
            with custom_span(name=span_info["traceName"], data=metadata):
                yield
        elif is_activity:
            # Activity: create trace only (activities rarely start child activities)
            # Skip extra custom_span() wrapper to reduce span count
            _debug(f"_attach_context_from_header ({context_type}): creating trace only")
            with trace(span_info["traceName"], trace_id=span_info["traceId"], metadata=metadata):
                yield
        else:
            # Workflow: create trace AND span (span needed for header propagation to activities)
            _debug(f"_attach_context_from_header ({context_type}): creating trace and span")
            with trace(span_info["traceName"], trace_id=span_info["traceId"], metadata=metadata):
                with custom_span(name=span_info["traceName"], data=metadata):
                    yield
    finally:
        # Detach OTEL context
        # Note: detach may fail if workflow sandbox uses different context variables
        # This is expected and safe to ignore - the important thing is that we
        # attached context before creating SDK traces
        if otel_token is not None:
            try:
                detach(otel_token)
            except ValueError:
                pass  # Context was created in different context - expected for workflows


class OpenAIAgentsContextInterceptor(
    temporalio.client.Interceptor, temporalio.worker.Interceptor
):
    """Interceptor that propagates OpenAI Agents context WITHOUT creating temporal spans.

    This interceptor is designed to work with OpenAIAgentsInstrumentor, which converts
    native OpenAI Agents SDK spans to OpenTelemetry spans. By not creating temporal:*
    spans, the exported traces contain only the business logic spans.
    """

    def __init__(
        self,
        payload_converter: temporalio.converter.PayloadConverter = temporalio.converter.default().payload_converter,
    ) -> None:
        self._payload_converter = payload_converter

    def intercept_client(
        self, next: temporalio.client.OutboundInterceptor
    ) -> temporalio.client.OutboundInterceptor:
        return _ClientOutboundInterceptor(next, self._payload_converter)

    def intercept_activity(
        self, next: temporalio.worker.ActivityInboundInterceptor
    ) -> temporalio.worker.ActivityInboundInterceptor:
        return _ActivityInboundInterceptor(next, self._payload_converter)

    def workflow_interceptor_class(
        self, input: temporalio.worker.WorkflowInterceptorClassInput
    ) -> type[_WorkflowInboundInterceptor]:
        return _WorkflowInboundInterceptor


class _ClientOutboundInterceptor(temporalio.client.OutboundInterceptor):
    def __init__(
        self,
        next: temporalio.client.OutboundInterceptor,
        payload_converter: temporalio.converter.PayloadConverter,
    ) -> None:
        super().__init__(next)
        self._payload_converter = payload_converter

    async def start_workflow(
        self, input: temporalio.client.StartWorkflowInput
    ) -> temporalio.client.WorkflowHandle[Any, Any]:
        # Only create trace if none exists - don't duplicate
        current_trace = get_trace_provider().get_current_trace()
        current_span = get_current_span()
        _debug(f"client.start_workflow: current_trace={current_trace is not None}, current_span={current_span is not None and not isinstance(current_span, NoOpSpan)}")

        if current_trace is None:
            # No trace context - create trace AND span (span needed for header propagation)
            _debug(f"client.start_workflow: creating trace and span")
            with trace(input.workflow, group_id=input.id):
                with custom_span(name=input.workflow, data={"workflowId": input.id}):
                    _set_header_from_context(input, self._payload_converter)
                    return await super().start_workflow(input)
        elif current_span is None or isinstance(current_span, NoOpSpan):
            # Trace exists but no span - need span for header propagation
            _debug(f"client.start_workflow: creating span only")
            with custom_span(name=input.workflow, data={"workflowId": input.id}):
                _set_header_from_context(input, self._payload_converter)
                return await super().start_workflow(input)
        else:
            # Both trace and span exist - just propagate context
            _debug(f"client.start_workflow: just propagating")
            _set_header_from_context(input, self._payload_converter)
            return await super().start_workflow(input)

    async def query_workflow(self, input: temporalio.client.QueryWorkflowInput) -> Any:
        _set_header_from_context(input, self._payload_converter)
        return await super().query_workflow(input)

    async def signal_workflow(self, input: temporalio.client.SignalWorkflowInput) -> None:
        _set_header_from_context(input, self._payload_converter)
        return await super().signal_workflow(input)

    async def start_workflow_update(
        self, input: temporalio.client.StartWorkflowUpdateInput
    ) -> temporalio.client.WorkflowUpdateHandle[Any]:
        _set_header_from_context(input, self._payload_converter)
        return await super().start_workflow_update(input)


class _ActivityInboundInterceptor(temporalio.worker.ActivityInboundInterceptor):
    def __init__(
        self,
        next: temporalio.worker.ActivityInboundInterceptor,
        payload_converter: temporalio.converter.PayloadConverter,
    ) -> None:
        super().__init__(next)
        self._payload_converter = payload_converter

    async def execute_activity(
        self, input: temporalio.worker.ExecuteActivityInput
    ) -> Any:
        _debug(f"ActivityInbound.execute_activity called")
        # Attach context but NO temporal:executeActivity span
        with _attach_context_from_header(input, temporalio.activity.payload_converter()):
            return await self.next.execute_activity(input)


_workflow_interceptor_counter = 0

class _WorkflowInboundInterceptor(temporalio.worker.WorkflowInboundInterceptor):
    def __init__(self, next: temporalio.worker.WorkflowInboundInterceptor) -> None:
        super().__init__(next)
        global _workflow_interceptor_counter
        _workflow_interceptor_counter += 1
        self._id = _workflow_interceptor_counter
        _debug(f"WorkflowInboundInterceptor #{self._id} created")

    def init(self, outbound: temporalio.worker.WorkflowOutboundInterceptor) -> None:
        self.next.init(_WorkflowOutboundInterceptor(outbound))

    async def execute_workflow(
        self, input: temporalio.worker.ExecuteWorkflowInput
    ) -> Any:
        _debug(f"WorkflowInbound #{self._id}.execute_workflow called")
        # Attach context but NO temporal:executeWorkflow span
        with _attach_context_from_header(input, temporalio.workflow.payload_converter()):
            return await self.next.execute_workflow(input)

    async def handle_signal(self, input: temporalio.worker.HandleSignalInput) -> None:
        with _attach_context_from_header(input, temporalio.workflow.payload_converter()):
            return await self.next.handle_signal(input)

    async def handle_query(self, input: temporalio.worker.HandleQueryInput) -> Any:
        with _attach_context_from_header(input, temporalio.workflow.payload_converter()):
            return await self.next.handle_query(input)

    def handle_update_validator(self, input: temporalio.worker.HandleUpdateInput) -> None:
        with _attach_context_from_header(input, temporalio.workflow.payload_converter()):
            self.next.handle_update_validator(input)

    async def handle_update_handler(
        self, input: temporalio.worker.HandleUpdateInput
    ) -> Any:
        with _attach_context_from_header(input, temporalio.workflow.payload_converter()):
            return await self.next.handle_update_handler(input)


class _WorkflowOutboundInterceptor(temporalio.worker.WorkflowOutboundInterceptor):
    def start_activity(
        self, input: temporalio.worker.StartActivityInput
    ) -> temporalio.workflow.ActivityHandle:
        # Just propagate OTEL context via headers - no extra spans needed
        _set_header_from_context(input, temporalio.workflow.payload_converter())
        return self.next.start_activity(input)

    async def start_child_workflow(
        self, input: temporalio.worker.StartChildWorkflowInput
    ) -> temporalio.workflow.ChildWorkflowHandle:
        _set_header_from_context(input, temporalio.workflow.payload_converter())
        return await self.next.start_child_workflow(input)

    def start_local_activity(
        self, input: temporalio.worker.StartLocalActivityInput
    ) -> temporalio.workflow.ActivityHandle:
        _set_header_from_context(input, temporalio.workflow.payload_converter())
        return self.next.start_local_activity(input)

    async def signal_child_workflow(
        self, input: temporalio.worker.SignalChildWorkflowInput
    ) -> None:
        _set_header_from_context(input, temporalio.workflow.payload_converter())
        await self.next.signal_child_workflow(input)

    async def signal_external_workflow(
        self, input: temporalio.worker.SignalExternalWorkflowInput
    ) -> None:
        _set_header_from_context(input, temporalio.workflow.payload_converter())
        await self.next.signal_external_workflow(input)


# Plugin that replaces the default tracing interceptor with our context-only version
from typing import Sequence, Union

from temporalio.contrib.openai_agents import OpenAIAgentsPlugin, ModelActivityParameters

try:
    from temporalio.contrib.openai_agents._mcp_server_provider import (
        StatelessMCPServerProvider,
        StatefulMCPServerProvider,
    )
except ImportError:
    StatelessMCPServerProvider = Any
    StatefulMCPServerProvider = Any


class OpenAIAgentsPluginNoTemporalSpans(OpenAIAgentsPlugin):
    """OpenAI Agents plugin that propagates trace context WITHOUT creating temporal:* spans.

    This subclass replaces the default OpenAIAgentsTracingInterceptor with
    OpenAIAgentsContextInterceptor, which only propagates context without
    creating temporal:startActivity, temporal:executeActivity, etc. spans.

    Use this when you want clean traces containing only your business logic spans
    (trace(), custom_span()) exported via OpenAIAgentsInstrumentor.
    """

    def __init__(
        self,
        model_params: ModelActivityParameters | None = None,
        model_provider=None,
        mcp_server_providers: Sequence[Union[StatelessMCPServerProvider, StatefulMCPServerProvider]] = (),
        register_activities: bool = True,
    ) -> None:
        super().__init__(
            model_params=model_params,
            model_provider=model_provider,
            mcp_server_providers=mcp_server_providers,
            register_activities=register_activities,
        )
        # Override with our context-only interceptor (public attribute from SimplePlugin)
        self.worker_interceptors = [OpenAIAgentsContextInterceptor()]
