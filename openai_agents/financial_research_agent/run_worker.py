#!/usr/bin/env python3

import asyncio

from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk import trace as trace_sdk
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.resources import Resource

from temporalio.client import Client
from temporalio.worker import Worker
from temporalio.contrib.openai_agents import OpenAIAgentsPlugin

from openai_agents.financial_research_agent.otel_tracing_plugin import OtelTracingPlugin
from openai_agents.financial_research_agent.workflows.financial_research_workflow import (
    FinancialResearchWorkflow,
)


def setup_otel_tracing() -> trace_sdk.TracerProvider:
    """Setup OpenTelemetry tracing with OTLP exporter."""
    resource = Resource.create(
        attributes={
            "service.name": "financial-research-agent-worker",
        }
    )
    tracer_provider = trace_sdk.TracerProvider(resource=resource)
    otlp_exporter = OTLPSpanExporter(endpoint="http://localhost:4317", insecure=True)
    tracer_provider.add_span_processor(SimpleSpanProcessor(otlp_exporter))
    return tracer_provider


async def main():
    tracer_provider = setup_otel_tracing()

    # Two-plugin architecture:
    # 1. OpenAIAgentsPlugin - activities, data converter, sandbox, model params
    # 2. OtelTracingPlugin - OTEL context propagation (replaces default interceptors)
    openai_plugin = OpenAIAgentsPlugin()
    otel_plugin = OtelTracingPlugin(tracer_provider=tracer_provider)

    client = await Client.connect(
        "localhost:7233",
        plugins=[openai_plugin, otel_plugin],  # Order matters: otel_plugin replaces interceptors
    )

    worker = Worker(
        client,
        task_queue="financial-research-task-queue",
        workflows=[FinancialResearchWorkflow],
    )

    print("Starting financial research worker with OpenTelemetry tracing...")
    print("Traces exported to: http://localhost:4317 (OpenAI Agents spans only)")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
