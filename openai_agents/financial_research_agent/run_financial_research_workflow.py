#!/usr/bin/env python3
"""Run the financial research workflow client with OpenTelemetry tracing."""

import asyncio
import os

from agents import trace as agents_trace

from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor

from temporalio.client import Client
from temporalio.contrib.openai_agents import OpenAIAgentsPlugin
from temporalio.contrib.opentelemetry import OtelTracingPlugin

from openai_agents.financial_research_agent.workflows.financial_research_workflow import (
    FinancialResearchWorkflow,
)

# Default OTLP endpoint - can be overridden via OTEL_EXPORTER_OTLP_ENDPOINT
DEFAULT_OTLP_ENDPOINT = "http://localhost:4317"


def get_otlp_endpoint() -> str:
    """Get OTLP endpoint from environment or use default."""
    return os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", DEFAULT_OTLP_ENDPOINT)


def create_tracer_provider(service_name: str) -> TracerProvider:
    """Create a TracerProvider configured for OTLP export."""
    resource = Resource.create(attributes={"service.name": service_name})
    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(
        SimpleSpanProcessor(OTLPSpanExporter(endpoint=get_otlp_endpoint(), insecure=True))
    )
    return tracer_provider


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
