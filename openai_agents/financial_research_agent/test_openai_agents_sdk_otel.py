"""
Tests for OpenAI Agents SDK OTEL span generation.

Verifies that:
1. Agent-only execution generates expected OTEL spans
2. Agent via Temporal generates spans without losing any
3. Temporal may add context spans but doesn't lose agent spans

Uses the new simplified approach:
1. TemporalAwareContext - Custom OTEL context that survives sandbox isolation
2. TracingInterceptor(create_spans=False) - Context propagation without Temporal spans
"""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import timedelta

import pytest
from agents import Agent, Runner, custom_span, trace as agents_trace
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.contrib.opentelemetry import TracingInterceptor
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import UnsandboxedWorkflowRunner, Worker

# Uses shared fixtures from conftest.py (tracing)

# Skip all tests if no API key
pytestmark = pytest.mark.skipif(
    not os.environ.get("OPENAI_API_KEY"),
    reason="OPENAI_API_KEY not set"
)


def create_test_agent() -> Agent:
    """Create a minimal test agent."""
    return Agent(
        name="TestAgent",
        instructions="You are a helpful assistant. Respond with exactly one word.",
        model="gpt-4o-mini",
    )


def dump_spans(spans: list[ReadableSpan], title: str = "Spans") -> None:
    """Print spans for debugging."""
    print(f"\n=== {title} ({len(spans)} total) ===")
    for s in spans:
        parent_info = f"parent={s.parent.span_id:016x}" if s.parent else "ROOT"
        print(f"  {s.name} [{parent_info}]")


# Activity that runs the agent
@activity.defn
async def run_agent_activity(prompt: str) -> str:
    """Activity that runs an agent and returns the result."""
    agent = create_test_agent()
    with custom_span(name="activity_agent_wrapper", data={"prompt": prompt}):
        result = await Runner.run(agent, prompt)
        return result.final_output


# Workflow that calls the activity
@workflow.defn
class AgentWorkflow:
    @workflow.run
    async def run(self, prompt: str) -> str:
        return await workflow.execute_activity(
            run_agent_activity,
            prompt,
            start_to_close_timeout=timedelta(seconds=60),
        )


@pytest.mark.asyncio
async def test_agent_direct_spans(tracing: InMemorySpanExporter):
    """Baseline: Run agent directly, capture all OTEL spans."""
    agent = create_test_agent()

    # Run agent directly with a trace wrapper
    with agents_trace("direct_agent_trace"):
        with custom_span(name="direct_agent_span", data={"test": "direct"}):
            await Runner.run(agent, "Say hello")

    # Allow spans to be exported
    await asyncio.sleep(0.3)

    spans = tracing.get_finished_spans()
    dump_spans(spans, "Direct Agent Spans")

    # Verify we got spans
    assert len(spans) > 0, "Expected some spans from agent execution"

    span_names = [s.name for s in spans]

    # Verify our custom spans exist
    assert "direct_agent_trace" in span_names, f"Missing trace span, got: {span_names}"
    assert "direct_agent_span" in span_names, f"Missing custom span, got: {span_names}"

    # Verify agent-related spans exist (from OpenAIAgentsInstrumentor)
    # The instrumentor should create spans for agent execution
    agent_spans = [n for n in span_names if "agent" in n.lower() or "response" in n.lower()]
    print(f"Agent-related spans: {agent_spans}")

    # All spans should share same trace_id
    trace_ids = set(s.context.trace_id for s in spans)
    assert len(trace_ids) == 1, f"Expected 1 trace_id, got {len(trace_ids)}"

    # Return span names for comparison test
    return set(span_names)


@pytest.mark.asyncio
async def test_agent_temporal_spans(tracing: InMemorySpanExporter):
    """Run agent through Temporal, verify spans are captured."""
    async with await WorkflowEnvironment.start_local() as env:
        interceptor = TracingInterceptor(create_spans=False)
        client_config = env.client.config()
        client_config["interceptors"] = [interceptor]
        client = Client(**client_config)

        task_queue = f"test-agent-{uuid.uuid4()}"

        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[AgentWorkflow],
            activities=[run_agent_activity],
            workflow_runner=UnsandboxedWorkflowRunner(),
        ):
            # Start with client-side trace for context propagation
            with agents_trace("temporal_agent_trace"):
                with custom_span(name="temporal_client_span", data={"test": "temporal"}):
                    await client.execute_workflow(
                        AgentWorkflow.run,
                        "Say hello",
                        id=f"wf-{uuid.uuid4()}",
                        task_queue=task_queue,
                    )

    # Allow spans to be exported
    await asyncio.sleep(0.3)

    spans = tracing.get_finished_spans()
    dump_spans(spans, "Temporal Agent Spans")

    # Verify we got spans
    assert len(spans) > 0, "Expected some spans from Temporal agent execution"

    span_names = [s.name for s in spans]

    # Verify our trace context spans exist
    assert "temporal_agent_trace" in span_names, f"Missing trace span, got: {span_names}"
    assert "temporal_client_span" in span_names, f"Missing client span, got: {span_names}"
    assert "activity_agent_wrapper" in span_names, f"Missing activity wrapper span, got: {span_names}"

    # Verify all spans share same trace_id (context propagation worked)
    trace_ids = set(s.context.trace_id for s in spans)
    assert len(trace_ids) == 1, f"Expected 1 trace_id for connected trace, got {len(trace_ids)}"

    # Return span names for comparison test
    return set(span_names)


@pytest.mark.asyncio
async def test_temporal_preserves_agent_spans(tracing: InMemorySpanExporter):
    """Verify Temporal doesn't lose any agent spans compared to direct execution."""

    # Run agent directly
    agent = create_test_agent()
    with agents_trace("compare_direct_trace"):
        with custom_span(name="compare_direct_span", data={}):
            await Runner.run(agent, "Say yes")

    await asyncio.sleep(0.3)
    direct_spans = tracing.get_finished_spans()
    direct_names = set(s.name for s in direct_spans)
    dump_spans(direct_spans, "Direct Execution Spans")

    # Clear for Temporal run
    tracing.clear()

    # Run via Temporal
    async with await WorkflowEnvironment.start_local() as env:
        interceptor = TracingInterceptor(create_spans=False)
        client_config = env.client.config()
        client_config["interceptors"] = [interceptor]
        client = Client(**client_config)

        task_queue = f"test-compare-{uuid.uuid4()}"

        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[AgentWorkflow],
            activities=[run_agent_activity],
            workflow_runner=UnsandboxedWorkflowRunner(),
        ):
            with agents_trace("compare_temporal_trace"):
                with custom_span(name="compare_temporal_span", data={}):
                    await client.execute_workflow(
                        AgentWorkflow.run,
                        "Say yes",
                        id=f"wf-{uuid.uuid4()}",
                        task_queue=task_queue,
                    )

    await asyncio.sleep(0.3)
    temporal_spans = tracing.get_finished_spans()
    temporal_names = set(s.name for s in temporal_spans)
    dump_spans(temporal_spans, "Temporal Execution Spans")

    # Filter to agent-related spans (exclude our test wrapper spans)
    def is_agent_span(name: str) -> bool:
        """Check if span is from agent execution (not our test wrappers)."""
        test_wrappers = {
            "compare_direct_trace", "compare_direct_span",
            "compare_temporal_trace", "compare_temporal_span",
            "activity_agent_wrapper"
        }
        return name not in test_wrappers

    direct_agent_spans = {n for n in direct_names if is_agent_span(n)}
    temporal_agent_spans = {n for n in temporal_names if is_agent_span(n)}

    print(f"\nDirect agent spans: {sorted(direct_agent_spans)}")
    print(f"Temporal agent spans: {sorted(temporal_agent_spans)}")

    # Check that Temporal doesn't lose any agent spans
    missing = direct_agent_spans - temporal_agent_spans
    if missing:
        print(f"\nMissing spans in Temporal: {missing}")

    # Allow Temporal to have additional context spans, but not lose agent spans
    assert not missing, (
        f"Temporal execution lost these agent spans: {missing}\n"
        f"Direct: {sorted(direct_agent_spans)}\n"
        f"Temporal: {sorted(temporal_agent_spans)}"
    )


if __name__ == "__main__":
    from openai_agents.financial_research_agent.conftest import _setup_shared_tracing
    _, exporter = _setup_shared_tracing()
    asyncio.run(test_agent_direct_spans(exporter))
