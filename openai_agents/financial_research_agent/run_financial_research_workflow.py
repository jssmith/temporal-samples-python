#!/usr/bin/env python3

from __future__ import annotations

import asyncio
from typing import List

from temporalio.client import Client, Interceptor
from temporalio.contrib.openai_agents import OpenAIAgentsPlugin
from temporalio.contrib.opentelemetry import TracingInterceptor

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
)
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

from openai_agents.financial_research_agent.workflows.financial_research_workflow import (
    FinancialResearchWorkflow,
)


def get_interceptors() -> List[Interceptor]:
    interceptors = []
    try:
        tracing_interceptor = TracingInterceptor()
        interceptors.append(tracing_interceptor)
        print("OpenTelemetry TracingInterceptor added successfully")
    except Exception as e:
        print(f"Failed to create OpenTelemetry TracingInterceptor: {e}")

    return interceptors


def setup_tracer_provider(service_name: str, otlp_endpoint: str):
    resource = Resource.create(
        {
            "service.name": service_name,
            "service_version": "0.1.0",
            "deployment.environment": "development",
        }
    )

    provider = TracerProvider(resource=resource)
    otlp_exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
    provider.add_span_processor(BatchSpanProcessor(otlp_exporter))

    trace.set_tracer_provider(provider)


async def main():
    # Setup OpenTelemetry tracer provider BEFORE connecting to client.
    print("Setting up OpenTelemetry tracer provider...")
    otlp_endpoint = "http://localhost:4317"

    setup_tracer_provider(
        service_name="FINANCIAL_RESEARCH_AGENT",
        otlp_endpoint=otlp_endpoint,
    )

    # Get the query from user input.
    query = input("Enter a financial research query: ")
    if not query.strip():
        query = "Write up an analysis of Apple Inc.'s most recent quarter."
        print(f"Using default query: {query}")

    client = await Client.connect(
        "localhost:7233",
        plugins=[
            OpenAIAgentsPlugin(),
        ],
        interceptors=get_interceptors(),
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
