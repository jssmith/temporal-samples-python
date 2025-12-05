"""
OpenTelemetry Context Propagation Interceptor for Temporal.

This interceptor propagates OTEL trace context through Temporal workflows and activities
WITHOUT creating the extra Temporal spans (StartWorkflow, RunWorkflow, StartActivity, RunActivity).

This allows custom spans created in activities to be properly linked to the original trace.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Mapping, Protocol, Type

from temporalio import activity, api, client, converter, worker, workflow

with workflow.unsafe.imports_passed_through():
    from opentelemetry import context as otel_context
    from opentelemetry import propagate

# Header key for OTEL trace context
OTEL_CONTEXT_KEY = "otel-trace-context"

# ContextVar to store the trace context carrier for propagation within workflows
_workflow_trace_carrier: ContextVar[dict[str, str] | None] = ContextVar(
    "_workflow_trace_carrier", default=None
)


class _InputWithHeaders(Protocol):
    headers: Mapping[str, api.common.v1.Payload]


def inject_trace_context(
    input: _InputWithHeaders, payload_converter: converter.PayloadConverter
) -> None:
    """Inject current OTEL trace context into headers using W3C format."""
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    if carrier:
        input.headers = {
            **input.headers,
            OTEL_CONTEXT_KEY: payload_converter.to_payload(carrier),
        }


def inject_trace_context_from_carrier(
    input: _InputWithHeaders,
    payload_converter: converter.PayloadConverter,
    carrier: dict[str, str] | None,
) -> None:
    """Inject trace context from a carrier dict into headers."""
    if carrier:
        input.headers = {
            **input.headers,
            OTEL_CONTEXT_KEY: payload_converter.to_payload(carrier),
        }


@contextmanager
def extract_trace_context(
    input: _InputWithHeaders, payload_converter: converter.PayloadConverter
):
    """Extract OTEL trace context from headers and set as current context."""
    payload = input.headers.get(OTEL_CONTEXT_KEY)
    if payload:
        carrier = payload_converter.from_payload(payload, dict)
        ctx = propagate.extract(carrier)
        token = otel_context.attach(ctx)
        try:
            yield
        finally:
            otel_context.detach(token)
    else:
        yield


def get_carrier_from_headers(
    input: _InputWithHeaders, payload_converter: converter.PayloadConverter
) -> dict[str, str] | None:
    """Extract the trace context carrier from headers without setting context."""
    payload = input.headers.get(OTEL_CONTEXT_KEY)
    if payload:
        return payload_converter.from_payload(payload, dict)
    return None


class OtelContextPropagationInterceptor(client.Interceptor, worker.Interceptor):
    """
    Interceptor that propagates OTEL trace context through Temporal.
    
    Unlike TracingInterceptor, this does NOT create any Temporal-specific spans.
    It only propagates the trace context so that spans created in activities
    are properly linked to the original trace.
    """

    def __init__(
        self,
        payload_converter: converter.PayloadConverter = converter.default().payload_converter,
    ) -> None:
        self._payload_converter = payload_converter

    def intercept_client(
        self, next: client.OutboundInterceptor
    ) -> client.OutboundInterceptor:
        return _OtelClientOutboundInterceptor(next, self._payload_converter)

    def intercept_activity(
        self, next: worker.ActivityInboundInterceptor
    ) -> worker.ActivityInboundInterceptor:
        return _OtelActivityInboundInterceptor(next)

    def workflow_interceptor_class(
        self, input: worker.WorkflowInterceptorClassInput
    ) -> Type[_OtelWorkflowInboundInterceptor]:
        return _OtelWorkflowInboundInterceptor


class _OtelClientOutboundInterceptor(client.OutboundInterceptor):
    """Injects OTEL context when starting workflows from the client."""

    def __init__(
        self,
        next: client.OutboundInterceptor,
        payload_converter: converter.PayloadConverter,
    ) -> None:
        super().__init__(next)
        self._payload_converter = payload_converter

    async def start_workflow(
        self, input: client.StartWorkflowInput
    ) -> client.WorkflowHandle[Any, Any]:
        inject_trace_context(input, self._payload_converter)
        return await super().start_workflow(input)

    async def query_workflow(self, input: client.QueryWorkflowInput) -> Any:
        inject_trace_context(input, self._payload_converter)
        return await super().query_workflow(input)

    async def signal_workflow(self, input: client.SignalWorkflowInput) -> None:
        inject_trace_context(input, self._payload_converter)
        await super().signal_workflow(input)

    async def start_workflow_update(
        self, input: client.StartWorkflowUpdateInput
    ) -> client.WorkflowUpdateHandle[Any]:
        inject_trace_context(input, self._payload_converter)
        return await self.next.start_workflow_update(input)


class _OtelActivityInboundInterceptor(worker.ActivityInboundInterceptor):
    """Extracts OTEL context when executing activities."""

    async def execute_activity(self, input: worker.ExecuteActivityInput) -> Any:
        # Extract trace context and set as current - no span creation here
        with extract_trace_context(input, activity.payload_converter()):
            return await self.next.execute_activity(input)


class _OtelWorkflowInboundInterceptor(worker.WorkflowInboundInterceptor):
    """Extracts OTEL context when executing workflows and propagates to outbound."""

    def init(self, outbound: worker.WorkflowOutboundInterceptor) -> None:
        self.next.init(_OtelWorkflowOutboundInterceptor(outbound))

    async def execute_workflow(self, input: worker.ExecuteWorkflowInput) -> Any:
        # Extract the trace carrier from headers and store it for propagation to activities.
        # We can't set OTEL context in the sandbox, but we can store the carrier
        # and use it when starting activities.
        carrier = get_carrier_from_headers(input, workflow.payload_converter())
        token = _workflow_trace_carrier.set(carrier)
        try:
            return await self.next.execute_workflow(input)
        finally:
            _workflow_trace_carrier.reset(token)

    async def handle_signal(self, input: worker.HandleSignalInput) -> None:
        carrier = get_carrier_from_headers(input, workflow.payload_converter())
        token = _workflow_trace_carrier.set(carrier)
        try:
            return await self.next.handle_signal(input)
        finally:
            _workflow_trace_carrier.reset(token)

    async def handle_query(self, input: worker.HandleQueryInput) -> Any:
        carrier = get_carrier_from_headers(input, workflow.payload_converter())
        token = _workflow_trace_carrier.set(carrier)
        try:
            return await self.next.handle_query(input)
        finally:
            _workflow_trace_carrier.reset(token)

    def handle_update_validator(self, input: worker.HandleUpdateInput) -> None:
        self.next.handle_update_validator(input)

    async def handle_update_handler(self, input: worker.HandleUpdateInput) -> Any:
        carrier = get_carrier_from_headers(input, workflow.payload_converter())
        token = _workflow_trace_carrier.set(carrier)
        try:
            return await self.next.handle_update_handler(input)
        finally:
            _workflow_trace_carrier.reset(token)


class _OtelWorkflowOutboundInterceptor(worker.WorkflowOutboundInterceptor):
    """Injects OTEL context when starting activities from workflows."""

    def start_activity(
        self, input: worker.StartActivityInput
    ) -> workflow.ActivityHandle:
        # Get the stored trace carrier and inject it into activity headers
        carrier = _workflow_trace_carrier.get()
        inject_trace_context_from_carrier(input, workflow.payload_converter(), carrier)
        return self.next.start_activity(input)

    async def start_child_workflow(
        self, input: worker.StartChildWorkflowInput
    ) -> workflow.ChildWorkflowHandle:
        carrier = _workflow_trace_carrier.get()
        inject_trace_context_from_carrier(input, workflow.payload_converter(), carrier)
        return await self.next.start_child_workflow(input)

    def start_local_activity(
        self, input: worker.StartLocalActivityInput
    ) -> workflow.ActivityHandle:
        carrier = _workflow_trace_carrier.get()
        inject_trace_context_from_carrier(input, workflow.payload_converter(), carrier)
        return self.next.start_local_activity(input)

    async def signal_child_workflow(
        self, input: worker.SignalChildWorkflowInput
    ) -> None:
        carrier = _workflow_trace_carrier.get()
        inject_trace_context_from_carrier(input, workflow.payload_converter(), carrier)
        return await self.next.signal_child_workflow(input)

    async def signal_external_workflow(
        self, input: worker.SignalExternalWorkflowInput
    ) -> None:
        carrier = _workflow_trace_carrier.get()
        inject_trace_context_from_carrier(input, workflow.payload_converter(), carrier)
        return await self.next.signal_external_workflow(input)
