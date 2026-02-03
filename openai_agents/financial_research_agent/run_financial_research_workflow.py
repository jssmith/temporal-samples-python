#!/usr/bin/env python3

import asyncio
from datetime import timedelta

from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

from temporalio.client import Client
from temporalio.contrib.openai_agents import ModelActivityParameters, OpenAIAgentsPlugin

from openai_agents.financial_research_agent.workflows.financial_research_workflow import (
    FinancialResearchWorkflow,
)


async def main():
    # Create OTLP exporter for Jaeger
    exporter = OTLPSpanExporter(endpoint="http://localhost:4317", insecure=True)

    # Get the query from user input
    query = input("Enter a financial research query: ")
    if not query.strip():
        query = "Write up an analysis of Apple Inc.'s most recent quarter."
        print(f"Using default query: {query}")

    client = await Client.connect(
        "localhost:7233",
        plugins=[
            OpenAIAgentsPlugin(
                model_params=ModelActivityParameters(
                    start_to_close_timeout=timedelta(seconds=60)
                ),
                otel_exporters=[exporter],
                add_temporal_spans=False,
            ),
        ],
    )

    print(f"Starting financial research for: {query}")
    print("This may take several minutes to complete...\n")

    result = await client.execute_workflow(
        FinancialResearchWorkflow.run,
        query,
        id=f"financial-research-{hash(query)}",
        task_queue="financial-research-task-queue",
    )

    print(result)


if __name__ == "__main__":
    asyncio.run(main())
