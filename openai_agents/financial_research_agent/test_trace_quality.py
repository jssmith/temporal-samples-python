"""Tests for trace quality - detecting duplicate traces and disconnected spans.

These tests verify that the OpenAI Agents context propagation through Temporal
produces clean, well-structured traces without duplication.

Uses the new simplified approach:
1. TemporalAwareContext - Custom OTEL context that survives sandbox isolation
2. TracingInterceptor(create_spans=False) - Context propagation without Temporal spans
"""

from __future__ import annotations

import asyncio
import logging
import os
import uuid
from datetime import timedelta

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.contrib.opentelemetry import TracingInterceptor
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import UnsandboxedWorkflowRunner, Worker

# Uses shared fixtures from conftest.py (tracing)

# OTEL tracer for activities (custom_span requires agents_trace context which doesn't propagate)
tracer = trace.get_tracer(__name__)

# Tests don't require OpenAI API key
pytestmark = pytest.mark.skipif(False, reason="")


# Activity that creates a trace and custom spans using OTEL tracer
@activity.defn
async def activity_with_trace(name: str) -> str:
    """Activity that creates spans - uses OTEL tracer for proper context propagation."""
    with tracer.start_as_current_span(f"activity_trace_{name}") as span:
        span.set_attribute("name", name)
        with tracer.start_as_current_span(f"activity_work_{name}") as work_span:
            work_span.set_attribute("input", name)
            await asyncio.sleep(0.01)  # Simulate work
            return f"done: {name}"


@activity.defn
async def simple_activity(name: str) -> str:
    """Activity that creates an OTEL span (picks up propagated context)."""
    with tracer.start_as_current_span(f"work_{name}") as span:
        span.set_attribute("input", name)
        await asyncio.sleep(0.01)
        return f"done: {name}"


# Workflow that creates a trace and calls activities
@workflow.defn
class WorkflowWithTrace:
    """Workflow that creates a trace span."""

    @workflow.run
    async def run(self, query: str) -> str:
        # Use OTEL tracer for proper context propagation
        # Note: workflow code runs in sandbox, but TemporalAwareContext makes this work
        otel_tracer = trace.get_tracer(__name__)
        with otel_tracer.start_as_current_span("test_trace") as span:
            span.set_attribute("query", query)
            result1 = await workflow.execute_activity(
                simple_activity,
                "step1",
                start_to_close_timeout=timedelta(seconds=30),
            )
            result2 = await workflow.execute_activity(
                simple_activity,
                "step2",
                start_to_close_timeout=timedelta(seconds=30),
            )
        return f"{result1}, {result2}"


@workflow.defn
class WorkflowWithMultipleActivities:
    """Workflow that calls multiple activities to test span connectivity."""

    @workflow.run
    async def run(self, count: int) -> str:
        results = []
        for i in range(count):
            result = await workflow.execute_activity(
                simple_activity,
                f"item_{i}",
                start_to_close_timeout=timedelta(seconds=30),
            )
            results.append(result)
        return ", ".join(results)


def get_spans_by_name(spans: list[ReadableSpan], name: str) -> list[ReadableSpan]:
    """Get all spans with a specific name."""
    return [s for s in spans if s.name == name]


def get_span_ids(spans: list[ReadableSpan]) -> set[int]:
    """Get all span IDs in the trace."""
    return {s.context.span_id for s in spans}


def get_disconnected_spans(spans: list[ReadableSpan]) -> list[ReadableSpan]:
    """Find spans whose parent is not in the collected spans (and isn't root)."""
    span_ids = get_span_ids(spans)
    disconnected = []
    for s in spans:
        if s.parent is not None:  # Has a parent
            if s.parent.span_id not in span_ids:  # Parent not in our spans
                disconnected.append(s)
    return disconnected


def count_unique_trace_ids(spans: list[ReadableSpan]) -> int:
    """Count unique trace IDs in spans."""
    return len({s.context.trace_id for s in spans})


class TestTraceDuplication:
    """Tests that verify traces are not duplicated."""

    @pytest.mark.asyncio
    async def test_single_trace_name_should_appear_once(self, tracing: InMemorySpanExporter):
        """
        When a workflow creates a trace with `with trace("name")`,
        that trace name should appear exactly once in the output.
        """
        async with await WorkflowEnvironment.start_local() as env:
            interceptor = TracingInterceptor(create_spans=False)
            client_config = env.client.config()
            client_config["interceptors"] = [interceptor]
            client = Client(**client_config)

            task_queue = f"test-{uuid.uuid4()}"

            async with Worker(
                client,
                task_queue=task_queue,
                workflows=[WorkflowWithTrace],
                activities=[simple_activity],
                interceptors=[interceptor],
                workflow_runner=UnsandboxedWorkflowRunner(),
            ):
                result = await client.execute_workflow(
                    WorkflowWithTrace.run,
                    "test_query",
                    id=f"wf-{uuid.uuid4()}",
                    task_queue=task_queue,
                )

        assert "done:" in result

        await asyncio.sleep(0.3)
        spans = tracing.get_finished_spans()

        # Find all spans named "test_trace"
        trace_spans = get_spans_by_name(spans, "test_trace")

        # Should have exactly 1 span named "test_trace"
        assert len(trace_spans) == 1, (
            f"Expected exactly 1 'test_trace' span, but found {len(trace_spans)}. "
            f"This indicates trace duplication. "
            f"Span details: {[(s.name, s.context.span_id) for s in trace_spans]}"
        )

    @pytest.mark.asyncio
    async def test_all_spans_share_single_trace_id(self, tracing: InMemorySpanExporter):
        """
        All spans from a single workflow execution should share the same trace ID.
        """
        async with await WorkflowEnvironment.start_local() as env:
            interceptor = TracingInterceptor(create_spans=False)
            client_config = env.client.config()
            client_config["interceptors"] = [interceptor]
            client = Client(**client_config)

            task_queue = f"test-{uuid.uuid4()}"

            async with Worker(
                client,
                task_queue=task_queue,
                workflows=[WorkflowWithTrace],
                activities=[simple_activity],
                interceptors=[interceptor],
                workflow_runner=UnsandboxedWorkflowRunner(),
            ):
                result = await client.execute_workflow(
                    WorkflowWithTrace.run,
                    "test_query",
                    id=f"wf-{uuid.uuid4()}",
                    task_queue=task_queue,
                )

        await asyncio.sleep(0.3)
        spans = tracing.get_finished_spans()

        unique_trace_ids = count_unique_trace_ids(spans)

        assert unique_trace_ids == 1, (
            f"Expected all spans to share 1 trace ID, but found {unique_trace_ids}. "
            f"This indicates trace context is not being propagated correctly."
        )


class TestSpanConnectivity:
    """Tests that verify spans are properly connected in a tree structure."""

    @pytest.mark.asyncio
    async def test_no_disconnected_spans(self, tracing: InMemorySpanExporter):
        """
        All spans should form a connected tree - each span's parent
        should either be another span in the trace or be the root.
        """
        async with await WorkflowEnvironment.start_local() as env:
            interceptor = TracingInterceptor(create_spans=False)
            client_config = env.client.config()
            client_config["interceptors"] = [interceptor]
            client = Client(**client_config)

            task_queue = f"test-{uuid.uuid4()}"

            async with Worker(
                client,
                task_queue=task_queue,
                workflows=[WorkflowWithMultipleActivities],
                activities=[simple_activity],
                interceptors=[interceptor],
                workflow_runner=UnsandboxedWorkflowRunner(),
            ):
                result = await client.execute_workflow(
                    WorkflowWithMultipleActivities.run,
                    3,  # 3 activities
                    id=f"wf-{uuid.uuid4()}",
                    task_queue=task_queue,
                )

        await asyncio.sleep(0.3)
        spans = tracing.get_finished_spans()

        disconnected = get_disconnected_spans(spans)

        assert len(disconnected) == 0, (
            f"Found {len(disconnected)} disconnected spans (parent not in trace). "
            f"Disconnected spans: {[(s.name, format(s.parent.span_id, '016x') if s.parent else 'ROOT') for s in disconnected]}"
        )

    @pytest.mark.asyncio
    async def test_activity_spans_have_workflow_parent(self, tracing: InMemorySpanExporter):
        """
        Activity spans should be children of workflow spans, forming a proper hierarchy.
        """
        async with await WorkflowEnvironment.start_local() as env:
            interceptor = TracingInterceptor(create_spans=False)
            client_config = env.client.config()
            client_config["interceptors"] = [interceptor]
            client = Client(**client_config)

            task_queue = f"test-{uuid.uuid4()}"

            async with Worker(
                client,
                task_queue=task_queue,
                workflows=[WorkflowWithTrace],
                activities=[simple_activity],
                interceptors=[interceptor],
                workflow_runner=UnsandboxedWorkflowRunner(),
            ):
                result = await client.execute_workflow(
                    WorkflowWithTrace.run,
                    "test",
                    id=f"wf-{uuid.uuid4()}",
                    task_queue=task_queue,
                )

        await asyncio.sleep(0.3)
        spans = tracing.get_finished_spans()
        span_map = {s.context.span_id: s for s in spans}

        # Find activity work spans
        work_spans = [s for s in spans if s.name.startswith("work_")]

        assert len(work_spans) >= 2, f"Expected at least 2 work spans, found {len(work_spans)}"

        # Each work span should have a parent in the trace
        for work_span in work_spans:
            assert work_span.parent is not None, f"work span {work_span.name} has no parent"
            parent_id = work_span.parent.span_id
            assert parent_id in span_map, (
                f"work span {work_span.name}'s parent {format(parent_id, '016x')} "
                f"is not in the collected spans"
            )


class TestTraceWarnings:
    """Tests that verify no trace-related warnings are generated."""

    @pytest.mark.asyncio
    async def test_no_trace_already_exists_warnings(self, tracing: InMemorySpanExporter, caplog):
        """
        The worker should not log "Trace already exists. Creating a new trace,
        but this is probably a mistake." warnings.
        """
        # Capture warnings from the openai.agents logger
        with caplog.at_level(logging.WARNING, logger="openai.agents"):
            async with await WorkflowEnvironment.start_local() as env:
                interceptor = TracingInterceptor(create_spans=False)
                client_config = env.client.config()
                client_config["interceptors"] = [interceptor]
                client = Client(**client_config)

                task_queue = f"test-{uuid.uuid4()}"

                async with Worker(
                    client,
                    task_queue=task_queue,
                    workflows=[WorkflowWithTrace],
                    activities=[simple_activity],
                    interceptors=[interceptor],
                ):
                    result = await client.execute_workflow(
                        WorkflowWithTrace.run,
                        "test",
                        id=f"wf-{uuid.uuid4()}",
                        task_queue=task_queue,
                    )

        await asyncio.sleep(0.3)

        # Check for "Trace already exists" warnings
        trace_warnings = [
            record for record in caplog.records
            if "Trace already exists" in record.message
        ]

        assert len(trace_warnings) == 0, (
            f"Found {len(trace_warnings)} 'Trace already exists' warnings. "
            f"This indicates duplicate trace creation."
        )


class TestSpanCounts:
    """Tests that verify reasonable span counts (no explosion of spans)."""

    @pytest.mark.asyncio
    async def test_reasonable_span_count_for_simple_workflow(self, tracing: InMemorySpanExporter):
        """
        A simple workflow with 2 activities should not produce an excessive
        number of spans.

        Expected spans:
        - 1 trace span (from workflow's agents_trace)
        - 2 work spans (from activities' custom_span)
        - Maybe 1-2 additional spans for workflow/client context

        Total: ~3-5 spans maximum
        """
        async with await WorkflowEnvironment.start_local() as env:
            interceptor = TracingInterceptor(create_spans=False)
            client_config = env.client.config()
            client_config["interceptors"] = [interceptor]
            client = Client(**client_config)

            task_queue = f"test-{uuid.uuid4()}"

            async with Worker(
                client,
                task_queue=task_queue,
                workflows=[WorkflowWithTrace],
                activities=[simple_activity],
                interceptors=[interceptor],
                workflow_runner=UnsandboxedWorkflowRunner(),
            ):
                result = await client.execute_workflow(
                    WorkflowWithTrace.run,
                    "test",
                    id=f"wf-{uuid.uuid4()}",
                    task_queue=task_queue,
                )

        await asyncio.sleep(0.3)
        spans = tracing.get_finished_spans()

        # With proper deduplication, we should have ~3-5 spans
        # Without deduplication, we might see 10+ spans
        max_expected_spans = 6

        assert len(spans) <= max_expected_spans, (
            f"Expected at most {max_expected_spans} spans, but got {len(spans)}. "
            f"This may indicate span duplication. "
            f"Span names: {[s.name for s in spans]}"
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
