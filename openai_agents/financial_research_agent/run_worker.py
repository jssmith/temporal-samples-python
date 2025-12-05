#!/usr/bin/env python3

from __future__ import annotations

import asyncio
from typing import List

from temporalio.client import Client, Interceptor
from temporalio.contrib.openai_agents import OpenAIAgentsPlugin
from temporalio.worker import Worker
from temporalio.contrib.opentelemetry import TracingInterceptor

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
)
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.openai import OpenAIInstrumentor

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

    # Instrument OpenAI to automatically trace all LLM calls.
    OpenAIInstrumentor(
        enrich_assistant=True,
        enable_trace_context_propagation=True,
    ).instrument()

    client = await Client.connect(
        "localhost:7233",
        plugins=[
            OpenAIAgentsPlugin(),
        ],
        interceptors=get_interceptors(),
    )

    worker = Worker(
        client,
        task_queue="financial-research-task-queue",
        workflows=[FinancialResearchWorkflow],
    )

    print("Starting financial research worker...")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
