"""
Minimal test for OTEL context propagation through Temporal workflows.

This test verifies that our custom OtelContextPropagationInterceptor
correctly propagates trace context from client -> workflow -> nested spans.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Iterable

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from temporalio import workflow
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import UnsandboxedWorkflowRunner, Worker

from openai_agents.financial_research_agent.otel_interceptor import (
    OtelContextPropagationInterceptor,
)

# Uses shared fixtures from conftest.py (otel_exporter)


@workflow.defn
class SimpleOtelWorkflow:
    """Simple workflow that creates OTEL spans."""

    @workflow.run
    async def run(self, query: str) -> str:
        tracer = trace.get_tracer(__name__)
        with tracer.start_as_current_span("workflow.main") as span:
            span.set_attribute("query", query)
            with tracer.start_as_current_span("workflow.step1"):
                await asyncio.sleep(0.01)
            with tracer.start_as_current_span("workflow.step2"):
                await asyncio.sleep(0.01)
        return "done"


def dump_spans(
    spans: Iterable[ReadableSpan],
    *,
    parent_id: int | None = None,
    indent_depth: int = 0,
) -> list[str]:
    """Recursively dump spans in a tree structure."""
    ret: list[str] = []
    for span in spans:
        span_parent_id = span.parent.span_id if span.parent else None
        if (not span.parent and parent_id is None) or (span_parent_id == parent_id):
            span_str = f"{'  ' * indent_depth}{span.name}"
            ret.append(span_str)
            if span.context:
                ret += dump_spans(
                    spans,
                    parent_id=span.context.span_id,
                    indent_depth=indent_depth + 1,
                )
    return ret


@pytest.mark.asyncio
async def test_otel_context_propagation_basic(otel_exporter: InMemorySpanExporter):
    """Test that OTEL context propagates from client to workflow spans."""
    tracer = trace.get_tracer(__name__)

    async with await WorkflowEnvironment.start_local() as env:
        # Create client with our interceptor
        client_config = env.client.config()
        client_config["interceptors"] = [OtelContextPropagationInterceptor()]
        client = Client(**client_config)

        task_queue = f"test-queue-{uuid.uuid4()}"

        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[SimpleOtelWorkflow],
            workflow_runner=UnsandboxedWorkflowRunner(),
        ):
            # Start a root span on client side
            with tracer.start_as_current_span("client.root") as root_span:
                root_span.set_attribute("test", "value")
                result = await client.execute_workflow(
                    SimpleOtelWorkflow.run,
                    "test query",
                    id=f"test-workflow-{uuid.uuid4()}",
                    task_queue=task_queue,
                )

        assert result == "done"

    # Get all captured spans
    spans = otel_exporter.get_finished_spans()

    print("\n=== Captured Spans ===")
    for span in spans:
        parent_info = f"parent={span.parent.span_id:016x}" if span.parent else "parent=None"
        print(f"  {span.name}: trace={span.context.trace_id:032x}, span={span.context.span_id:016x}, {parent_info}")

    print("\n=== Span Hierarchy ===")
    for line in dump_spans(spans):
        print(line)

    # Check that we have spans
    assert len(spans) > 0, "Expected at least one span"

    # Check all spans have the same trace ID
    trace_ids = set(span.context.trace_id for span in spans)
    print(f"\n=== Trace IDs: {len(trace_ids)} unique ===")
    for tid in trace_ids:
        print(f"  {tid:032x}")

    # This is the key assertion - all spans should be in the same trace
    assert len(trace_ids) == 1, f"Expected 1 trace ID, got {len(trace_ids)}. Context not propagating!"

    # Check hierarchy: workflow spans should descend from client.root
    root_spans = [s for s in spans if s.name == "client.root"]
    assert len(root_spans) == 1, "Expected exactly one client.root span"

    workflow_main_spans = [s for s in spans if s.name == "workflow.main"]
    assert len(workflow_main_spans) == 1, "Expected exactly one workflow.main span"

    # workflow.main should be a descendant of client.root (may have intermediate spans)
    # Build parent chain for workflow.main to verify it connects to root
    root_span_id = root_spans[0].context.span_id
    span_map = {s.context.span_id: s for s in spans}
    current = workflow_main_spans[0]
    found_root = False
    max_depth = 10
    depth = 0
    while current.parent and depth < max_depth:
        if current.parent.span_id == root_span_id:
            found_root = True
            break
        current = span_map.get(current.parent.span_id)
        if current is None:
            break
        depth += 1

    assert found_root, f"workflow.main should descend from client.root"


@pytest.mark.asyncio
async def test_otel_context_propagation_no_client_span(otel_exporter: InMemorySpanExporter):
    """Test workflow spans when there's no client-side span (no context to propagate)."""
    async with await WorkflowEnvironment.start_local() as env:
        # Create client with our interceptor
        client_config = env.client.config()
        client_config["interceptors"] = [OtelContextPropagationInterceptor()]
        client = Client(**client_config)

        task_queue = f"test-queue-{uuid.uuid4()}"

        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[SimpleOtelWorkflow],
            workflow_runner=UnsandboxedWorkflowRunner(),
        ):
            # No root span - just call workflow directly
            result = await client.execute_workflow(
                SimpleOtelWorkflow.run,
                "test query",
                id=f"test-workflow-{uuid.uuid4()}",
                task_queue=task_queue,
            )

        assert result == "done"

    spans = otel_exporter.get_finished_spans()

    print("\n=== Captured Spans (no client span) ===")
    for span in spans:
        parent_info = f"parent={span.parent.span_id:016x}" if span.parent else "parent=None"
        print(f"  {span.name}: trace={span.context.trace_id:032x}, span={span.context.span_id:016x}, {parent_info}")

    print("\n=== Span Hierarchy ===")
    for line in dump_spans(spans):
        print(line)

    # Workflow spans should still be nested correctly relative to each other
    workflow_main_spans = [s for s in spans if s.name == "workflow.main"]
    step1_spans = [s for s in spans if s.name == "workflow.step1"]
    step2_spans = [s for s in spans if s.name == "workflow.step2"]

    if workflow_main_spans and step1_spans:
        # step1 and step2 should be children of workflow.main
        main_span_id = workflow_main_spans[0].context.span_id
        for step_span in step1_spans + step2_spans:
            step_parent = step_span.parent.span_id if step_span.parent else None
            assert step_parent == main_span_id, f"Step span should be child of workflow.main"


def simulate_external_api_call(tracer):
    """Simulate what OpenAI instrumentation does - creates a span for the API call."""
    with tracer.start_as_current_span("external.api_call") as span:
        span.set_attribute("api", "simulated")
        # In reality, this would be the OpenAI API call
        return "api response"


@workflow.defn
class WorkflowWithExternalCall:
    """Workflow that creates spans AND makes simulated external calls."""

    @workflow.run
    async def run(self, query: str) -> str:
        tracer = trace.get_tracer(__name__)
        with tracer.start_as_current_span("workflow.main") as span:
            span.set_attribute("query", query)

            # This simulates what happens when OpenAI instrumentation
            # auto-creates a span for an API call
            with tracer.start_as_current_span("workflow.call_agent"):
                result = simulate_external_api_call(tracer)

        return result


@pytest.mark.asyncio
async def test_otel_with_external_calls(otel_exporter: InMemorySpanExporter):
    """Test that external API call spans are properly nested."""
    tracer = trace.get_tracer(__name__)

    async with await WorkflowEnvironment.start_local() as env:
        client_config = env.client.config()
        client_config["interceptors"] = [OtelContextPropagationInterceptor()]
        client = Client(**client_config)

        task_queue = f"test-queue-{uuid.uuid4()}"

        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[WorkflowWithExternalCall],
            workflow_runner=UnsandboxedWorkflowRunner(),
        ):
            with tracer.start_as_current_span("client.root"):
                result = await client.execute_workflow(
                    WorkflowWithExternalCall.run,
                    "test query",
                    id=f"test-workflow-{uuid.uuid4()}",
                    task_queue=task_queue,
                )

        assert result == "api response"

    spans = otel_exporter.get_finished_spans()

    print("\n=== Captured Spans (with external call) ===")
    for span in spans:
        parent_info = f"parent={span.parent.span_id:016x}" if span.parent else "parent=None"
        print(f"  {span.name}: trace={span.context.trace_id:032x}, {parent_info}")

    print("\n=== Span Hierarchy ===")
    for line in dump_spans(spans):
        print(line)

    # All spans should share the same trace ID
    trace_ids = set(span.context.trace_id for span in spans)
    assert len(trace_ids) == 1, f"Expected 1 trace ID, got {len(trace_ids)}"

    # Verify the external.api_call span is nested inside workflow.call_agent
    api_spans = [s for s in spans if s.name == "external.api_call"]
    call_agent_spans = [s for s in spans if s.name == "workflow.call_agent"]

    assert len(api_spans) == 1, "Expected exactly one external.api_call span"
    assert len(call_agent_spans) == 1, "Expected exactly one workflow.call_agent span"

    call_agent_span_id = call_agent_spans[0].context.span_id
    api_parent_id = api_spans[0].parent.span_id if api_spans[0].parent else None
    assert api_parent_id == call_agent_span_id, "external.api_call should be child of workflow.call_agent"


if __name__ == "__main__":
    from openai_agents.financial_research_agent.conftest import _setup_shared_tracing
    _, exporter = _setup_shared_tracing()
    asyncio.run(test_otel_context_propagation_basic(exporter))
