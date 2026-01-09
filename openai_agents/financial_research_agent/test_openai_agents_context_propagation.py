"""
Minimal test for OpenAI Agents context propagation through Temporal workflows.

Start with the simplest possible test and build up from there.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import timedelta

import pytest
from agents import custom_span, trace as agents_trace
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import UnsandboxedWorkflowRunner, Worker

from openai_agents.financial_research_agent.openai_agents_context_interceptor import (
    OpenAIAgentsContextInterceptor,
)

# Uses shared fixtures from conftest.py (tracing)

# Skip if no API key
pytestmark = pytest.mark.skipif(
    not os.environ.get("OPENAI_API_KEY"),
    reason="OPENAI_API_KEY not set"
)


# Simplest possible activity
@activity.defn
async def simple_activity(data: str) -> str:
    """Activity that just creates a custom span."""
    with custom_span(name="activity_span", data={"input": data}):
        return f"done: {data}"


# Simplest possible workflow
@workflow.defn
class SimpleWorkflow:
    @workflow.run
    async def run(self, data: str) -> str:
        is_replaying = workflow.unsafe.is_replaying()
        print(f"[WORKFLOW] run() called, is_replaying={is_replaying}")
        result = await workflow.execute_activity(
            simple_activity,
            data,
            start_to_close_timeout=timedelta(seconds=30),
        )
        print(f"[WORKFLOW] run() returning, is_replaying={workflow.unsafe.is_replaying()}")
        return result


def dump_spans(spans: list[ReadableSpan]) -> None:
    """Print spans in a flat list with all details."""
    print(f"Total spans: {len(spans)}")
    trace_ids = set(s.context.trace_id for s in spans)
    print(f"Unique trace IDs: {len(trace_ids)}")
    print()

    for s in spans:
        name = s.name if s.name else "(empty)"
        parent_info = f"parent={s.parent.span_id:016x}" if s.parent else "ROOT"
        print(f"  {name}")
        print(f"    span_id={s.context.span_id:016x}, {parent_info}")
        print(f"    trace_id={s.context.trace_id:032x}")


@pytest.mark.asyncio
async def test_minimal_workflow(tracing: InMemorySpanExporter):
    """Test 1: Just run a workflow, verify it completes."""
    async with await WorkflowEnvironment.start_local() as env:
        # Use interceptor directly like production does
        interceptor = OpenAIAgentsContextInterceptor()

        client_config = env.client.config()
        client_config["interceptors"] = [interceptor]
        client = Client(**client_config)

        task_queue = f"test-{uuid.uuid4()}"

        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[SimpleWorkflow],
            activities=[simple_activity],
            workflow_runner=UnsandboxedWorkflowRunner(),
        ):
            result = await client.execute_workflow(
                SimpleWorkflow.run,
                "test",
                id=f"wf-{uuid.uuid4()}",
                task_queue=task_queue,
            )

    assert result == "done: test"

    await asyncio.sleep(0.3)
    spans = tracing.get_finished_spans()
    print("\n=== Spans ===")
    dump_spans(spans)

    # Verify we got activity_span
    names = [s.name for s in spans]
    assert "activity_span" in names, f"Missing activity_span, got: {names}"

    # Verify reduced span count (was 6-8, now should be <=5)
    assert len(spans) <= 5, f"Expected <=5 spans, got {len(spans)}"

    # Verify all spans share the same trace_id
    trace_ids = set(s.context.trace_id for s in spans)
    assert len(trace_ids) == 1, f"Expected 1 trace_id, got {len(trace_ids)}"


@pytest.mark.asyncio
async def test_with_client_trace(tracing: InMemorySpanExporter):
    """Test 2: Add client-side trace, verify activity is connected."""
    async with await WorkflowEnvironment.start_local() as env:
        # Use interceptor directly like production does
        interceptor = OpenAIAgentsContextInterceptor()
        client_config = env.client.config()
        client_config["interceptors"] = [interceptor]
        client = Client(**client_config)

        task_queue = f"test-{uuid.uuid4()}"

        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[SimpleWorkflow],
            activities=[simple_activity],
            workflow_runner=UnsandboxedWorkflowRunner(),
        ):
            # Start with client-side trace
            with agents_trace("client_trace"):
                with custom_span(name="client_span", data={}):
                    result = await client.execute_workflow(
                        SimpleWorkflow.run,
                        "test",
                        id=f"wf-{uuid.uuid4()}",
                        task_queue=task_queue,
                    )

    assert result == "done: test"

    await asyncio.sleep(0.3)
    spans = tracing.get_finished_spans()
    print("\n=== Spans (with client trace) ===")
    dump_spans(spans)

    # Verify activity span exists
    names = [s.name for s in spans]
    assert "activity_span" in names, f"Missing activity_span, got: {names}"

    # Find spans we care about
    activity_spans = [s for s in spans if s.name == "activity_span"]
    # client_trace comes from agents_trace() or client_span from custom_span()
    client_spans = [s for s in spans if "client" in s.name.lower()]

    print(f"\nActivity spans: {len(activity_spans)}")
    print(f"Client spans: {len(client_spans)}")

    # KEY TEST: ALL spans should share the same trace ID
    trace_ids = set(s.context.trace_id for s in spans)
    print(f"\nUnique trace IDs: {len(trace_ids)}")
    for tid in trace_ids:
        print(f"  {tid:032x}")

    assert len(trace_ids) == 1, (
        f"Expected all spans to share one trace ID, but found {len(trace_ids)}: "
        f"{[format(t, '032x') for t in trace_ids]}"
    )

    # Verify reduced span count (with client trace, expect <=7)
    assert len(spans) <= 7, f"Expected <=7 spans with client trace, got {len(spans)}"


@pytest.mark.asyncio
async def test_span_hierarchy(tracing: InMemorySpanExporter):
    """Test 3: Verify activity_span has valid parent chain to root."""
    async with await WorkflowEnvironment.start_local() as env:
        # Use interceptor directly like production does
        interceptor = OpenAIAgentsContextInterceptor()
        client_config = env.client.config()
        client_config["interceptors"] = [interceptor]
        client = Client(**client_config)

        task_queue = f"test-{uuid.uuid4()}"

        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[SimpleWorkflow],
            activities=[simple_activity],
            workflow_runner=UnsandboxedWorkflowRunner(),
        ):
            result = await client.execute_workflow(
                SimpleWorkflow.run,
                "test",
                id=f"wf-{uuid.uuid4()}",
                task_queue=task_queue,
            )

    assert result == "done: test"

    await asyncio.sleep(0.3)
    spans = tracing.get_finished_spans()
    span_map = {s.context.span_id: s for s in spans}

    # Find activity_span
    activity_span = next((s for s in spans if s.name == "activity_span"), None)
    assert activity_span is not None, "activity_span not found"

    # Walk up parent chain within collected spans
    # Note: In distributed tracing, parent might be from client (not in worker's span list)
    current = activity_span
    depth = 0
    local_depth = 0  # Depth within local spans
    while current.parent:
        parent = span_map.get(current.parent.span_id)
        if parent is None:
            # Parent is external (from client) - this is OK in distributed tracing
            print(f"  External parent at depth {depth}: {current.parent.span_id:016x}")
            break
        current = parent
        depth += 1
        local_depth += 1
        assert depth < 10, "Parent chain too deep - possible cycle"

    # Verify we traversed at least some local spans before hitting external parent
    print(f"\nParent chain: {local_depth} local spans, depth {depth} total")
    assert local_depth >= 1, "activity_span should have at least 1 local parent span"

    # All collected spans should share same trace_id
    trace_ids = set(s.context.trace_id for s in spans)
    assert len(trace_ids) == 1, f"Expected 1 trace_id, got {len(trace_ids)}"


if __name__ == "__main__":
    from openai_agents.financial_research_agent.conftest import _setup_shared_tracing
    _, exporter = _setup_shared_tracing()
    asyncio.run(test_minimal_workflow(exporter))
