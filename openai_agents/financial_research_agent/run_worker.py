#!/usr/bin/env python3

import asyncio

from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk import trace as trace_sdk
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.resources import Resource

from temporalio.client import Client
from temporalio.worker import Worker

from openai_agents.financial_research_agent.openai_agents_context_interceptor import (
    OpenAIAgentsPluginNoTemporalSpans,
)
from openai_agents.financial_research_agent.parent_aware_tracing_processor import (
    setup_parent_aware_tracing,
)
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

    # Use custom parent-aware processor that respects OTEL parent context
    # This fixes trace ID propagation from client to worker
    setup_parent_aware_tracing(tracer_provider)

    return tracer_provider


async def main():
    setup_otel_tracing()

    # Use Plugin for model activities and proper sandbox handling
    # Our custom plugin uses OpenAIAgentsContextInterceptor for cleaner traces
    plugin = OpenAIAgentsPluginNoTemporalSpans()

    client = await Client.connect(
        "localhost:7233",
        plugins=[plugin],
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
