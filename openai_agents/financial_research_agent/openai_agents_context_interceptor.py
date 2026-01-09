"""OpenAI Agents SDK context propagation without temporal span creation.

This interceptor propagates the native OpenAI Agents SDK trace context
through Temporal workflow/activity boundaries WITHOUT creating any
temporal:* spans. This allows the OpenAIAgentsInstrumentor to export
only the business logic spans (trace(), custom_span()) to OpenTelemetry.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any

from agents import CustomSpanData, custom_span, get_current_span, trace
from agents.tracing import get_trace_provider
from agents.tracing.scope import Scope
from agents.tracing.spans import NoOpSpan

import temporalio.activity
import temporalio.client
import temporalio.converter
import temporalio.worker
import temporalio.workflow
from temporalio import activity, workflow

HEADER_KEY = "__openai_span"


def _set_header_from_context(
    input: Any, payload_converter: temporalio.converter.PayloadConverter
) -> None:
    """Inject current OpenAI Agents trace/span info into headers."""
    current = get_current_span()
    if current is None or isinstance(current, NoOpSpan):
        return

    current_trace = get_trace_provider().get_current_trace()
    input.headers = {
        **input.headers,
        HEADER_KEY: payload_converter.to_payload(
            {
                "traceName": current_trace.name if current_trace else "Workflow",
                "spanId": current.span_id,
                "traceId": current.trace_id,
            }
        ),
    }


@contextmanager
def _attach_context_from_header(
    input: Any, payload_converter: temporalio.converter.PayloadConverter
):
    """Extract and attach OpenAI Agents trace context from headers.

    Unlike OpenAIAgentsTracingInterceptor.context_from_header(), this does NOT
    create any temporal:* spans - it only attaches the context.
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

    # Enter trace context properly - this creates AND activates the trace
    with trace(span_info["traceName"], trace_id=span_info["traceId"], metadata=metadata):
        # Create a span with the propagated parent ID and START it
        # This establishes the parent relationship for any child spans
        parent_span = get_trace_provider().create_span(
            span_data=CustomSpanData(name="", data={}),
            span_id=span_info["spanId"]
        )
        parent_span.start(mark_as_current=True)
        try:
            yield
        finally:
            parent_span.finish()


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
        # If no trace exists, create one to establish context for propagation
        # We use the workflow name as the trace name (not "temporal:*")
        current_trace = get_trace_provider().get_current_trace()
        current_span = get_current_span()

        if current_trace is None:
            # No trace context - create one with a workflow span
            with trace(input.workflow, group_id=input.id):
                with custom_span(name=input.workflow, data={"workflowId": input.id}):
                    _set_header_from_context(input, self._payload_converter)
                    return await super().start_workflow(input)
        else:
            # Trace exists - just ensure there's a span for context propagation
            if current_span is None or isinstance(current_span, NoOpSpan):
                with custom_span(name=input.workflow, data={"workflowId": input.id}):
                    _set_header_from_context(input, self._payload_converter)
                    return await super().start_workflow(input)
            else:
                # Both trace and span exist - just propagate
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
        # Attach context but NO temporal:executeActivity span
        with _attach_context_from_header(input, temporalio.activity.payload_converter()):
            return await self.next.execute_activity(input)


class _WorkflowInboundInterceptor(temporalio.worker.WorkflowInboundInterceptor):
    def init(self, outbound: temporalio.worker.WorkflowOutboundInterceptor) -> None:
        self.next.init(_WorkflowOutboundInterceptor(outbound))

    async def execute_workflow(
        self, input: temporalio.worker.ExecuteWorkflowInput
    ) -> Any:
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
        # Must create a span to establish context for header injection
        # The original creates "temporal:startActivity" - we use empty name to be invisible
        current_trace = get_trace_provider().get_current_trace()
        span = None
        if current_trace:
            span = custom_span(name="", data={"activity": input.activity})
            span.start(mark_as_current=True)

        _set_header_from_context(input, temporalio.workflow.payload_converter())
        handle = self.next.start_activity(input)

        if span:
            handle.add_done_callback(lambda _: span.finish())
        return handle

    async def start_child_workflow(
        self, input: temporalio.worker.StartChildWorkflowInput
    ) -> temporalio.workflow.ChildWorkflowHandle:
        current_trace = get_trace_provider().get_current_trace()
        span = None
        if current_trace:
            span = custom_span(name="", data={"workflow": input.workflow})
            span.start(mark_as_current=True)

        _set_header_from_context(input, temporalio.workflow.payload_converter())
        handle = await self.next.start_child_workflow(input)

        if span:
            handle.add_done_callback(lambda _: span.finish())
        return handle

    def start_local_activity(
        self, input: temporalio.worker.StartLocalActivityInput
    ) -> temporalio.workflow.ActivityHandle:
        current_trace = get_trace_provider().get_current_trace()
        span = None
        if current_trace:
            span = custom_span(name="", data={"activity": input.activity})
            span.start(mark_as_current=True)

        _set_header_from_context(input, temporalio.workflow.payload_converter())
        handle = self.next.start_local_activity(input)

        if span:
            handle.add_done_callback(lambda _: span.finish())
        return handle

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
