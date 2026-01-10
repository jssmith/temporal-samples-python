# Financial Research Agent

Multi-agent financial research system with specialized roles, extended with Temporal's durable execution.

*Adapted from [OpenAI Agents SDK financial research agent](https://github.com/openai/openai-agents-python/tree/main/examples/financial_research_agent)*

## Architecture

This example shows how you might compose a richer financial research agent using the Agents SDK. The pattern is similar to the `research_bot` example, but with more specialized sub-agents and a verification step.

The flow is:

1. **Planning**: A planner agent turns the end user's request into a list of search terms relevant to financial analysis – recent news, earnings calls, corporate filings, industry commentary, etc.
2. **Search**: A search agent uses the built-in `WebSearchTool` to retrieve terse summaries for each search term. (You could also add `FileSearchTool` if you have indexed PDFs or 10-Ks.)
3. **Sub-analysts**: Additional agents (e.g. a fundamentals analyst and a risk analyst) are exposed as tools so the writer can call them inline and incorporate their outputs.
4. **Writing**: A senior writer agent brings together the search snippets and any sub-analyst summaries into a long-form markdown report plus a short executive summary.
5. **Verification**: A final verifier agent audits the report for obvious inconsistencies or missing sourcing.

## Running the Example

First, start the worker:
```bash
uv run openai_agents/financial_research_agent/run_worker.py
```

Then run the financial research workflow:
```bash
uv run openai_agents/financial_research_agent/run_financial_research_workflow.py
```

Enter a query like:
```
Write up an analysis of Apple Inc.'s most recent quarter.
```

You can also just hit enter to run this query, which is provided as the default.

## Components

### Agents

- **Planner Agent**: Creates a search plan with 5-15 relevant search terms
- **Search Agent**: Uses web search to gather financial information
- **Financials Agent**: Analyzes company fundamentals (revenue, profit, margins)
- **Risk Agent**: Identifies potential red flags and risk factors
- **Writer Agent**: Synthesizes information into a comprehensive report
- **Verifier Agent**: Audits the final report for consistency and accuracy

### Writer Agent Tools

The writer agent has access to tools that invoke the specialist analysts:
- `fundamentals_analysis`: Get financial performance analysis
- `risk_analysis`: Get risk factor assessment

## Temporal Integration

The example demonstrates several Temporal patterns:
- Durable execution of multi-step research workflows
- Parallel execution of web searches using `asyncio.create_task`
- Use of `workflow.as_completed` for handling concurrent tasks
- Proper import handling with `workflow.unsafe.imports_passed_through()`

## OpenTelemetry Tracing

This sample includes full OpenTelemetry trace propagation across client → Temporal → worker boundaries.

### Architecture

Uses a **two-plugin architecture** for clean separation of concerns:

```
┌─────────────────────────────────────┐
│ 1. OpenAIAgentsPlugin (vanilla)     │  ← Agent infrastructure
│    - activities, data_converter     │
│    - workflow_runner, sandbox       │
└──────────────┬──────────────────────┘
               ↓
┌─────────────────────────────────────┐
│ 2. OtelTracingPlugin                │  ← OTEL context propagation
│    - TracingInterceptor(create_spans=False)
│    - Sets OTEL_PYTHON_CONTEXT       │
└─────────────────────────────────────┘
```

### Key Components

| Component | Source | Purpose |
|-----------|--------|---------|
| `OtelTracingPlugin` | `temporalio.contrib.openai_agents` | Plugin that provides OTEL context propagation |
| `OpenAIAgentsPlugin` | `temporalio.contrib.openai_agents` | Plugin for OpenAI Agents SDK integration |

### How It Works

The challenge: Temporal's workflow sandbox isolates Python's `contextvars`, which breaks OpenTelemetry's default context propagation. This means `get_current_span()` returns nothing inside the sandbox.

The solution uses two Temporal SDK features:

1. **TemporalAwareContext** (custom OTEL context implementation):
   - Set via `OTEL_PYTHON_CONTEXT=temporal_aware_context`
   - Stores context on both `contextvars` AND `workflow.instance()`
   - When inside sandbox, falls back to `workflow.instance()` storage
   - Makes `get_current_span()` work transparently inside workflows

2. **TracingInterceptor(create_spans=False)**:
   - Propagates W3C TraceContext via Temporal headers
   - Does NOT create Temporal infrastructure spans
   - Results in clean traces with only your application spans

### Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4317` | OTLP gRPC endpoint |
| `OTEL_PYTHON_CONTEXT` | `temporal_aware_context` | Custom OTEL context (set by plugin) |

### Trace Structure

All spans share a single OTEL trace ID:

```
Financial research trace (client)
└── Financial research trace (worker)
    └── Agent spans...
        └── LLM call spans...
```

### Testing

```bash
uv run python -m pytest openai_agents/financial_research_agent/test_trace_quality.py -v
```

Tests verify: single trace ID, no duplicates, proper parent-child relationships, no disconnected spans.
