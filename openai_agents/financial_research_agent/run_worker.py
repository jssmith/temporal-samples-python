#!/usr/bin/env python3
"""Run the financial research worker with OpenTelemetry tracing."""

import asyncio
import os

from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor

from temporalio.client import Client
from temporalio.worker import Worker
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner
from temporalio.contrib.openai_agents import OpenAIAgentsPlugin, setup_tracing
from temporalio.contrib.opentelemetry import OtelTracingPlugin, ReplayFilteringSpanProcessor

from openai_agents.financial_research_agent.workflows.financial_research_workflow import (
    FinancialResearchWorkflow,
)

# Default OTLP endpoint - can be overridden via OTEL_EXPORTER_OTLP_ENDPOINT
DEFAULT_OTLP_ENDPOINT = "http://localhost:4317"


def get_otlp_endpoint() -> str:
    """Get OTLP endpoint from environment or use default."""
    return os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", DEFAULT_OTLP_ENDPOINT)


def create_tracer_provider(service_name: str) -> TracerProvider:
    """Create a TracerProvider configured for OTLP export with replay filtering."""
    resource = Resource.create(attributes={"service.name": service_name})
    tracer_provider = TracerProvider(resource=resource)
    # Wrap the span processor with ReplayFilteringSpanProcessor to prevent
    # duplicate spans during workflow replay
    tracer_provider.add_span_processor(
        ReplayFilteringSpanProcessor(
            SimpleSpanProcessor(OTLPSpanExporter(endpoint=get_otlp_endpoint(), insecure=True))
        )
    )
    return tracer_provider


async def main():
    tracer_provider = create_tracer_provider("financial-research-agent-worker")

    # Set up OpenAI Agents tracing with OpenInference
    setup_tracing(tracer_provider)

    # Two-plugin architecture:
    # 1. OpenAIAgentsPlugin - activities, data converter, sandbox, model params
    #    create_spans=False to suppress temporal:* spans from OpenAI Agents interceptor
    # 2. OtelTracingPlugin - OTEL context propagation (replaces default OTEL interceptors)
    openai_plugin = OpenAIAgentsPlugin(create_spans=False)
    otel_plugin = OtelTracingPlugin(tracer_provider=tracer_provider)

    client = await Client.connect(
        "localhost:7233",
        plugins=[openai_plugin, otel_plugin],
    )

    # Configure worker with sandbox passthrough for opentelemetry
    # This is required for OTEL context to propagate correctly inside workflows
    worker = Worker(
        client,
        task_queue="financial-research-task-queue",
        workflows=[FinancialResearchWorkflow],
        workflow_runner=SandboxedWorkflowRunner(
            restrictions=otel_plugin.sandbox_restrictions
        ),
    )

    print("Starting financial research worker with OpenTelemetry tracing...")
    print(f"Traces exported to: {get_otlp_endpoint()}")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
