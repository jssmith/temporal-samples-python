#!/usr/bin/env python3

import asyncio

from agents import trace as agents_trace

from temporalio.client import Client
from temporalio.contrib.openai_agents import OpenAIAgentsPlugin

from openai_agents.financial_research_agent.otel_config import create_tracer_provider
from openai_agents.financial_research_agent.otel_tracing_plugin import OtelTracingPlugin
from openai_agents.financial_research_agent.workflows.financial_research_workflow import (
    FinancialResearchWorkflow,
)


async def main():
    tracer_provider = create_tracer_provider("financial-research-agent-client")

    # Get the query from user input
    query = input("Enter a financial research query: ")
    if not query.strip():
        query = "Write up an analysis of Apple Inc.'s most recent quarter."
        print(f"Using default query: {query}")

    # Two-plugin architecture:
    # 1. OpenAIAgentsPlugin - data converter (client side), create_spans=False
    # 2. OtelTracingPlugin - OTEL context propagation
    openai_plugin = OpenAIAgentsPlugin(create_spans=False)
    otel_plugin = OtelTracingPlugin(tracer_provider=tracer_provider)

    client = await Client.connect(
        "localhost:7233",
        plugins=[openai_plugin, otel_plugin],
    )

    print(f"Starting financial research for: {query}")
    print("This may take several minutes to complete...\n")

    # Wrap in trace() to establish context that propagates through Temporal
    with agents_trace("Financial research trace"):
        result = await client.execute_workflow(
            FinancialResearchWorkflow.run,
            query,
            id=f"financial-research-{hash(query)}",
            task_queue="financial-research-task-queue",
        )

    print(result)


if __name__ == "__main__":
    asyncio.run(main())
