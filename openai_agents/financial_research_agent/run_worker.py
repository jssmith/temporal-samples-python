#!/usr/bin/env python3
"""Run the financial research worker with OpenTelemetry tracing."""

import asyncio

from temporalio.client import Client
from temporalio.worker import Worker
from temporalio.contrib.openai_agents import OpenAIAgentsPlugin, setup_tracing
from temporalio.contrib.opentelemetry import OtelTracingPlugin

from openai_agents.financial_research_agent.tracing_setup import (
    create_tracer_provider,
    get_otlp_endpoint,
)
from openai_agents.financial_research_agent.workflows.financial_research_workflow import (
    FinancialResearchWorkflow,
)


async def main():
    tracer_provider = create_tracer_provider("financial-research-agent-worker")

    # Set up OpenAI Agents tracing with OpenInference
    setup_tracing(tracer_provider)

    # Two-plugin architecture:
    # 1. OpenAIAgentsPlugin - activities, data converter, sandbox, model params
    # 2. OtelTracingPlugin - OTEL context propagation and replay filtering
    openai_plugin = OpenAIAgentsPlugin(create_spans=False)
    otel_plugin = OtelTracingPlugin(tracer_provider=tracer_provider)

    client = await Client.connect(
        "localhost:7233",
        plugins=[openai_plugin, otel_plugin],
    )

    # OtelTracingPlugin automatically configures sandbox passthrough
    worker = Worker(
        client,
        task_queue="financial-research-task-queue",
        workflows=[FinancialResearchWorkflow],
    )

    print("Starting financial research worker with OpenTelemetry tracing...")
    print(f"Traces exported to: {get_otlp_endpoint()}")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
