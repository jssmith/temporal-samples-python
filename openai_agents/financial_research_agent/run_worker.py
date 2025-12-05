#!/usr/bin/env python3
"""
Worker for the financial research workflow.

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
from opentelemetry.instrumentation.openai import OpenAIInstrumentor

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

# Instrument OpenAI BEFORE importing any code that uses it
OpenAIInstrumentor(
    enrich_assistant=True,
    enable_trace_context_propagation=True,
).instrument()

print("OpenTelemetry tracer provider and OpenAI instrumentation configured")

# =============================================================================
# Now safe to import application code
# =============================================================================
import asyncio

from temporalio.client import Client
from temporalio.contrib.openai_agents import OpenAIAgentsPlugin
from temporalio.worker import Worker

from openai_agents.financial_research_agent.workflows.financial_research_workflow import (
    FinancialResearchWorkflow,
)
from openai_agents.financial_research_agent.activities import (
    run_financial_research_activity,
)
from openai_agents.financial_research_agent.otel_interceptor import (
    OtelContextPropagationInterceptor,
)


async def main():
    # Use our custom OTEL context propagation interceptor (no extra spans)
    client = await Client.connect(
        "localhost:7233",
        plugins=[
            OpenAIAgentsPlugin(),
        ],
        interceptors=[OtelContextPropagationInterceptor()],
    )

    worker = Worker(
        client,
        task_queue="financial-research-task-queue",
        workflows=[FinancialResearchWorkflow],
        activities=[run_financial_research_activity],
    )

    print("Starting financial research worker...")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
