"""OpenAI Agents SDK context propagation without temporal span creation.

This interceptor propagates the native OpenAI Agents SDK trace context
through Temporal workflow/activity boundaries WITHOUT creating any
temporal:* spans. This allows the OpenAIAgentsInstrumentor to export
only the business logic spans (trace(), custom_span()) to OpenTelemetry.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any

from agents import get_current_span
from agents.tracing import get_trace_provider
from agents.tracing.scope import Scope
from agents.tracing.spans import NoOpSpan
from agents.tracing.traces import Trace

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


class CarrierTrace(Trace):
    """A trace carrier that holds trace_id/name but doesn't emit processor events.

    Used to restore SDK trace context in activities/workflows without creating
    duplicate traces. Allows custom_span() to work by providing the trace_id.
    """

    def __init__(self, name: str, trace_id: str):
        self._name = name
        self._trace_id = trace_id
        self._prev_context_token = None

    @property
    def trace_id(self) -> str:
        return self._trace_id

    @property
    def name(self) -> str:
        return self._name

    def __enter__(self) -> Trace:
        self.start(mark_as_current=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finish(reset_current=True)

    def start(self, mark_as_current: bool = False):
        if mark_as_current:
            self._prev_context_token = Scope.set_current_trace(self)

    def finish(self, reset_current: bool = False):
        if reset_current and self._prev_context_token is not None:
            Scope.reset_current_trace(self._prev_context_token)
            self._prev_context_token = None

    def export(self) -> dict[str, Any] | None:
        return None  # Carrier traces don't export


class CarrierSpan:
    """A span carrier that holds span_id/trace_id but doesn't emit processor events.

    Used to restore SDK span context in activities/workflows without creating
    duplicate spans. Allows custom_span() to properly parent under this span.
    """

    def __init__(self, trace_id: str, span_id: str, parent_id: str | None = None):
        self._trace_id = trace_id
        self._span_id = span_id
        self._parent_id = parent_id
        self._prev_span_token = None

    @property
    def trace_id(self) -> str:
        return self._trace_id

    @property
    def span_id(self) -> str:
        return self._span_id

    @property
    def parent_id(self) -> str | None:
        return self._parent_id

    @property
    def span_data(self):
        return None

    def __enter__(self):
        self.start(mark_as_current=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finish(reset_current=True)

    def start(self, mark_as_current: bool = False):
        if mark_as_current:
            self._prev_span_token = Scope.set_current_span(self)

    def finish(self, reset_current: bool = False):
        if reset_current and self._prev_span_token is not None:
            Scope.reset_current_span(self._prev_span_token)
            self._prev_span_token = None

    def set_error(self, error) -> None:
        pass

    @property
    def error(self):
        return None

    def export(self) -> dict[str, Any] | None:
        return None  # Carrier spans don't export

    @property
    def started_at(self) -> str | None:
        return None

    @property
    def ended_at(self) -> str | None:
        return None


# Debug flag - set to True to enable debug output
_DEBUG = False
def _debug(msg: str) -> None:
    if _DEBUG:
        import sys
        print(f"[INTERCEPTOR] {msg}", file=sys.stderr, flush=True)


def _set_header_from_context(
    input: Any, payload_converter: temporalio.converter.PayloadConverter
) -> None:
    """Inject current OpenAI Agents trace/span info AND OTEL context into headers."""
    current_trace = get_trace_provider().get_current_trace()
    current_span = get_current_span()

    # We need at least a trace to propagate context
    # Inside `with agents_trace()`, there's a trace but might not be a span
    if current_trace is None:
        _debug(f"_set_header_from_context: no current trace, skipping")
        return

    # Skip if trace is NoOp-like (check trace_id)
    if current_trace.trace_id == "no-op":
        _debug(f"_set_header_from_context: trace is no-op, skipping")
        return

    # Build header data with trace info (span might be None)
    header_data = {
        "traceName": current_trace.name,
        "traceId": current_trace.trace_id,
    }

    # Add span info if available and not NoOp
    if current_span is not None and not isinstance(current_span, NoOpSpan):
        header_data["spanId"] = current_span.span_id
        _debug(f"_set_header_from_context: setting headers with trace={current_trace.trace_id[:16]}... span={current_span.span_id[:16]}...")
    else:
        _debug(f"_set_header_from_context: setting headers with trace={current_trace.trace_id[:16]}... (no span)")

    # Capture OTEL context for trace continuity
    otel_span = otel_trace.get_current_span()
    otel_ctx = otel_span.get_span_context() if otel_span else None

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
    """Extract and attach context from headers for trace continuity.

    This interceptor restores both OTEL and SDK trace context using carrier
    objects that don't emit processor events. This allows:
    1. OTEL spans to be properly parented
    2. SDK's custom_span() to work without requiring explicit trace() calls
    3. No duplicate traces created by the interceptor

    The carrier objects hold IDs but don't trigger on_trace_start/end or
    on_span_start/end events, avoiding duplication while maintaining context.
    """
    payload = input.headers.get(HEADER_KEY)
    span_info = payload_converter.from_payload(payload) if payload else None

    if span_info is None:
        yield
        return

    is_activity = activity.in_activity()
    context_type = "activity" if is_activity else "workflow"
    _debug(f"_attach_context_from_header ({context_type}): restoring context")

    # Restore OTEL context for proper span parenting in OTEL exporters
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
        _debug(f"_attach_context_from_header ({context_type}): attached OTEL parent context")

    # Restore SDK trace/span context using carrier objects
    # This allows custom_span() to work without requiring explicit trace() calls
    carrier_trace = None
    carrier_span = None

    trace_name = span_info.get("traceName", "Workflow")
    trace_id = span_info.get("traceId")
    span_id = span_info.get("spanId")

    if trace_id:
        carrier_trace = CarrierTrace(name=trace_name, trace_id=trace_id)
        carrier_trace.start(mark_as_current=True)
        _debug(f"_attach_context_from_header ({context_type}): set carrier trace {trace_id[:16]}...")

    if span_id and trace_id:
        carrier_span = CarrierSpan(trace_id=trace_id, span_id=span_id)
        carrier_span.start(mark_as_current=True)
        _debug(f"_attach_context_from_header ({context_type}): set carrier span {span_id[:16]}...")

    try:
        yield
    finally:
        # Clean up SDK context (in reverse order)
        if carrier_span is not None:
            carrier_span.finish(reset_current=True)
        if carrier_trace is not None:
            carrier_trace.finish(reset_current=True)

        # Detach OTEL context
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
        # Just propagate existing context - don't create traces
        # The business code creates its own traces with `with trace(...)`
        _debug(f"client.start_workflow: propagating context")
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

def _ensure_tracing_random() -> None:
    """Ensure the workflow instance has a deterministic random generator for tracing.

    This is required by TemporalTraceProvider.gen_span_id() when custom_span() is called
    inside a workflow context.
    """
    from temporalio.contrib.openai_agents._trace_interceptor import RunIdRandom
    instance = workflow.instance()
    if not hasattr(instance, "__temporal_openai_tracing_random"):
        setattr(instance, "__temporal_openai_tracing_random", RunIdRandom())


# Attribute name for storing OTEL parent context on workflow instance
OTEL_PARENT_CONTEXT_ATTR = "__otel_parent_context"


def _store_otel_parent_context(otel_trace_id: int, otel_span_id: int) -> None:
    """Store OTEL parent context on workflow instance for sandbox propagation.

    The workflow sandbox isolates Python state, so we can't use attach() to
    propagate OTEL context. Instead, store the IDs on the workflow instance
    which IS accessible from inside the sandbox.
    """
    instance = workflow.instance()
    setattr(instance, OTEL_PARENT_CONTEXT_ATTR, {
        "trace_id": otel_trace_id,
        "span_id": otel_span_id,
    })
    _debug(f"_store_otel_parent_context: stored trace_id={format(otel_trace_id, '032x')} span_id={format(otel_span_id, '016x')}")


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
        _ensure_tracing_random()  # Required for TemporalTraceProvider

        # Store OTEL parent context on workflow instance for sandbox propagation
        # This must be done BEFORE entering the context manager because
        # the sandbox isolates Python state
        payload = input.headers.get(HEADER_KEY)
        if payload:
            span_info = temporalio.workflow.payload_converter().from_payload(payload)
            if span_info and "otelTraceId" in span_info and "otelSpanId" in span_info:
                _store_otel_parent_context(
                    otel_trace_id=int(span_info["otelTraceId"], 16),
                    otel_span_id=int(span_info["otelSpanId"], 16),
                )

        # Attach context but NO temporal:executeWorkflow span
        with _attach_context_from_header(input, temporalio.workflow.payload_converter()):
            return await self.next.execute_workflow(input)

    async def handle_signal(self, input: temporalio.worker.HandleSignalInput) -> None:
        _ensure_tracing_random()
        with _attach_context_from_header(input, temporalio.workflow.payload_converter()):
            return await self.next.handle_signal(input)

    async def handle_query(self, input: temporalio.worker.HandleQueryInput) -> Any:
        _ensure_tracing_random()
        with _attach_context_from_header(input, temporalio.workflow.payload_converter()):
            return await self.next.handle_query(input)

    def handle_update_validator(self, input: temporalio.worker.HandleUpdateInput) -> None:
        _ensure_tracing_random()
        with _attach_context_from_header(input, temporalio.workflow.payload_converter()):
            self.next.handle_update_validator(input)

    async def handle_update_handler(
        self, input: temporalio.worker.HandleUpdateInput
    ) -> Any:
        _ensure_tracing_random()
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
