"""OTEL Tracing Plugin for Temporal workflows with OpenAI Agents.

This plugin provides clean OTEL context propagation by replacing the default
OpenAIAgentsTracingInterceptor with OpenAIAgentsContextInterceptor, which
propagates trace context without creating temporal:* spans.

Usage:
    # Worker setup
    openai_plugin = OpenAIAgentsPlugin()  # Vanilla, for activities/data converter
    otel_plugin = OtelTracingPlugin(tracer_provider)  # For tracing

    client = await Client.connect("localhost:7233", plugins=[openai_plugin, otel_plugin])
    worker = Worker(client, task_queue="...", workflows=[...])

Note: OtelTracingPlugin must come AFTER OpenAIAgentsPlugin in the plugins list
since it replaces the interceptors from the first plugin.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from temporalio.plugin import SimplePlugin
import temporalio.worker

from openai_agents.financial_research_agent.openai_agents_context_interceptor import (
    OpenAIAgentsContextInterceptor,
)
from openai_agents.financial_research_agent.parent_aware_tracing_processor import (
    setup_tracing,
)

if TYPE_CHECKING:
    from opentelemetry.sdk.trace import TracerProvider


class OtelTracingPlugin(SimplePlugin):
    """Plugin for OTEL context propagation in Temporal workflows with OpenAI Agents.

    This plugin:
    1. REPLACES any existing worker interceptors with OpenAIAgentsContextInterceptor
    2. Sets up ParentAwareTracingProcessor for proper OTEL span creation

    The replacement behavior ensures clean traces without temporal:* spans
    while maintaining proper OTEL trace ID continuity across service boundaries.

    Args:
        tracer_provider: The OTEL TracerProvider to use for creating spans.
            If provided, setup_tracing() will be called automatically.
    """

    def __init__(self, tracer_provider: TracerProvider | None = None) -> None:
        # Set up parent-aware tracing if provider given
        if tracer_provider is not None:
            setup_tracing(tracer_provider)

        # Use callable to REPLACE interceptors (not append)
        # This works because SimplePlugin's _resolve_append_parameter() handles
        # callables specially - they receive existing interceptors and return new ones
        def replace_interceptors(
            existing: Sequence[temporalio.worker.Interceptor] | None,
        ) -> Sequence[temporalio.worker.Interceptor]:
            # Return only our interceptor, discarding any from OpenAIAgentsPlugin
            return [OpenAIAgentsContextInterceptor()]

        super().__init__(
            name="OtelTracingPlugin",
            worker_interceptors=replace_interceptors,
            # Also set client interceptors for outbound context propagation
            client_interceptors=[OpenAIAgentsContextInterceptor()],
        )
