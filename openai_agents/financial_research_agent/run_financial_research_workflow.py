#!/usr/bin/env python3

import asyncio

from agents import trace as agents_trace
from openinference.instrumentation.openai_agents import OpenAIAgentsInstrumentor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk import trace as trace_sdk
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.resources import Resource

from temporalio.client import Client

from openai_agents.financial_research_agent.openai_agents_context_interceptor import (
    OpenAIAgentsContextInterceptor,
    OpenAIAgentsPluginNoTemporalSpans,
)
from openai_agents.financial_research_agent.workflows.financial_research_workflow import (
    FinancialResearchWorkflow,
)


def setup_otel_tracing() -> trace_sdk.TracerProvider:
    """Setup OpenTelemetry tracing with OTLP exporter."""
    resource = Resource.create(
        attributes={
            "service.name": "financial-research-agent-client",
        }
    )
    tracer_provider = trace_sdk.TracerProvider(resource=resource)
    otlp_exporter = OTLPSpanExporter(endpoint="http://localhost:4317", insecure=True)
    tracer_provider.add_span_processor(SimpleSpanProcessor(otlp_exporter))

    # Instrument OpenAI Agents SDK - converts native spans to OTel
    OpenAIAgentsInstrumentor().instrument(tracer_provider=tracer_provider)

    return tracer_provider


async def main():
    setup_otel_tracing()

    # Get the query from user input
    query = input("Enter a financial research query: ")
    if not query.strip():
        query = "Write up an analysis of Apple Inc.'s most recent quarter."
        print(f"Using default query: {query}")

    client = await Client.connect(
        "localhost:7233",
        plugins=[
            OpenAIAgentsPluginNoTemporalSpans(),  # Worker-side context propagation
        ],
        interceptors=[
            OpenAIAgentsContextInterceptor(),  # Client-side context propagation
        ],
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
