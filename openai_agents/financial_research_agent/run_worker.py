#!/usr/bin/env python3

import asyncio
from datetime import timedelta

from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

from temporalio.client import Client
from temporalio.contrib.openai_agents import ModelActivityParameters, OpenAIAgentsPlugin
from temporalio.worker import Worker

from openai_agents.financial_research_agent.workflows.financial_research_workflow import (
    FinancialResearchWorkflow,
)


async def main():
    # Create OTLP exporter for Jaeger
    exporter = OTLPSpanExporter(endpoint="http://localhost:4317", insecure=True)

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

    worker = Worker(
        client,
        task_queue="financial-research-task-queue",
        workflows=[FinancialResearchWorkflow],
    )

    print("Starting financial research worker with OpenTelemetry tracing...")
    print("Traces exported to: http://localhost:4317 (Jaeger)")
    print("View traces at: http://localhost:16686/")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
