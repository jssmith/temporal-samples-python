#!/usr/bin/env python3
"""
Starter for the financial research workflow.

IMPORTANT: OpenTelemetry setup MUST happen before importing any code that uses OpenAI,
otherwise the instrumentation won't capture those calls.
"""

from __future__ import annotations

# =============================================================================
# OTEL SETUP - Must happen FIRST, before any other application imports
# =============================================================================
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# Set up tracer provider at module load time
_resource = Resource.create(
    {
        "service.name": "FINANCIAL_RESEARCH_AGENT",
        "service_version": "0.1.0",
        "deployment.environment": "development",
    }
)
_provider = TracerProvider(resource=_resource)
_otlp_exporter = OTLPSpanExporter(endpoint="http://localhost:4317", insecure=True)
_provider.add_span_processor(BatchSpanProcessor(_otlp_exporter))
trace.set_tracer_provider(_provider)

print("OpenTelemetry tracer provider configured for workflow starter")

# =============================================================================
# Now safe to import application code
# =============================================================================
import asyncio

from temporalio.client import Client
from temporalio.contrib.openai_agents import OpenAIAgentsPlugin

from openai_agents.financial_research_agent.workflows.financial_research_workflow import (
    FinancialResearchWorkflow,
)
from openai_agents.financial_research_agent.otel_interceptor import (
    OtelContextPropagationInterceptor,
)

# Get a tracer for creating the root span
_tracer = trace.get_tracer(__name__)


async def main():
    # Get the query from user input.
    query = input("Enter a financial research query: ")
    if not query.strip():
        query = "Write up an analysis of Apple Inc.'s most recent quarter."
        print(f"Using default query: {query}")

    # Use our custom OTEL context propagation interceptor
    client = await Client.connect(
        "localhost:7233",
        plugins=[
            OpenAIAgentsPlugin(),
        ],
        interceptors=[OtelContextPropagationInterceptor()],
    )

    print(f"Starting financial research for: {query}")
    print("This may take several minutes to complete...\n")

    # The interceptor will inject the current trace context into the workflow
    result = await client.execute_workflow(
        FinancialResearchWorkflow.run,
        query,
        id=f"financial-research-{hash(query)}",
        task_queue="financial-research-task-queue",
    )

    print(result)


if __name__ == "__main__":
    asyncio.run(main())
