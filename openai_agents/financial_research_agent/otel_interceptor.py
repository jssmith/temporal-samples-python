"""OpenTelemetry context propagation interceptor for Temporal workflows."""

from temporalio.contrib.opentelemetry import TracingInterceptor


class OtelContextPropagationInterceptor(TracingInterceptor):
    """
    Interceptor that propagates OpenTelemetry context through Temporal workflows.

    This is a thin wrapper around Temporal's built-in TracingInterceptor.
    """

    def __init__(self):
        """Initialize with default OTel tracer."""
        from opentelemetry import trace

        tracer = trace.get_tracer(__name__)
        super().__init__(tracer=tracer)
