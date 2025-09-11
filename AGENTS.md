# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

This repository uses `uv` as the package manager and task runner. All tasks are defined using `poethepoet` (poe).

### Setup
```bash
uv sync  # Install all dependencies
```

### Running samples
```bash
uv run <sample_path>  # e.g., uv run hello/hello_activity.py
```

### Testing
```bash
poe test             # Run all tests via pytest
```

### Code Quality
Use these in general before finishing work

```bash
poe format           # Format code with black and isort
poe lint             # Check formatting, import order, and types
poe lint-types       # Run only mypy type checking
```

Individual commands:
Use these for debugging
- `uv run black .` - Format code
- `uv run isort .` - Sort imports
- `uv run mypy --check-untyped-defs --namespace-packages .` - Type checking

## Architecture Overview

This is a collection of Python samples for the Temporal workflow orchestration platform. The codebase demonstrates various Temporal patterns and integrations.

### Core Patterns

**Workflows**: Defined using `@workflow.defn` decorator with `@workflow.run` methods. Workflows are durable functions that coordinate activities and handle long-running processes.

**Activities**: Defined using `@activity.defn` decorator. Activities contain business logic and interact with external systems. They can be sync or async and require execution timeouts.

**Workers**: Start with `Worker()` context manager, register workflows and activities, and connect to task queues.

**Clients**: Use `Client.connect()` to interact with Temporal server and execute workflows.

### Key Integration Points

- **OpenAI Agents**: Uses `workflow.unsafe.imports_passed_through()` context for external imports
- **AWS Bedrock**: Demonstrates AI workflow orchestration with AWS services  
- **Testing**: Uses `WorkflowEnvironment` with local, time-skipping, or external server environments
- **External Systems**: Activities handle integrations (databases, APIs, file systems)

### Common Patterns

- Dataclasses for activity inputs (recommended over multiple parameters)
- ThreadPoolExecutor for non-async activities
- Timeout specifications (start_to_close_timeout, schedule_to_close_timeout)
- Task queue isolation per sample
- UUID-based workflow and task queue IDs in tests

### Directory Structure

- `hello/` - Basic Temporal features and patterns
- `openai_agents/` - AI agent orchestration examples
- `bedrock/` - AWS Bedrock integration patterns
- `tests/` - Test patterns using WorkflowEnvironment
- Individual feature samples in root directories (polling, encryption, etc.)

## Testing

Tests use `pytest` with `temporalio.testing.WorkflowEnvironment`. The test environment can be configured via `--workflow-environment` option:
- `local` (default) - Local Temporal server
- `time-skipping` - Fast deterministic testing
- Custom server URL for integration testing

Each test typically creates isolated task queues using UUIDs to avoid conflicts.

### Manual Testing of Temporal Workflows

Temporal workflows require manual integration testing with a running Temporal server. This approach is essential for workflows that interact with external services, require user input, or use non-deterministic operations.

#### General Testing Pattern

1. **Identify the worker and starter programs**:
   - **Worker program**: Registers workflows and activities with Temporal, runs continuously
   - **Starter program**: Executes specific workflows, exits when complete
   - Look for files named `*worker*` or `run_worker*` for workers
   - Look for files named `run_*` or `*client*` for starters

2. **Start the worker** (runs continuously):
   ```bash
   # Replace with your package manager and worker path
   python path/to/worker_program.py
   ```

3. **In a separate terminal, run the starter program with timeout**:
   ```bash
   # Replace with your package manager and starter path
   # Use timeout to prevent hanging tests - adjust time as needed
   timeout 60s python path/to/starter_program.py
   ```

4. **Monitor execution**:
   - **Via Temporal CLI**: `temporal workflow list`, `temporal workflow show --workflow-id [id]`
   - **Via Web UI**: http://localhost:8233 (default)
   - **Via program output**: Watch starter program console output

5. **Handle user input** (if required):
   - Some workflows require interactive input
   - Watch the starter program output for prompts
   - Provide input when requested

6. **Verify completion**:
   - Starter program exits with code 0
   - Expected output is printed to console
   - Workflow shows as "Completed" in Temporal UI

7. **Clean shutdown**: Stop the worker process (Ctrl+C)

#### Testing Checklist

For each workflow example:
- [ ] Worker starts without errors and connects to Temporal server
- [ ] Worker registers workflows/activities successfully
- [ ] Starter program connects to Temporal server
- [ ] Workflow executes without errors
- [ ] Expected output is produced
- [ ] Starter program exits with code 0
- [ ] Worker can be cleanly shut down

#### Troubleshooting

**Connection issues**:
```bash
# Ensure Temporal server is running
temporal server start-dev

# Check server status
temporal server status
```

**Worker conflicts**:
```bash
# Find running workers
ps aux | grep [worker_pattern]
kill [process_id]

# Or use more specific patterns
pkill -f "worker"
```

**Environment setup**:
- Ensure required environment variables are set (API keys, connection strings)
- Verify dependencies are installed
- Check that task queue names match between worker and starter

**Debugging workflows**:
- Use Temporal Web UI for detailed workflow history
- Add logging to workflow and activity code
- Use `temporal workflow show --workflow-id [id]` for detailed status
- Consider using `timeout` command to prevent hanging tests

**Timeout handling**:
- **Exit code 124**: Process was killed by timeout - test took too long
- **Exit code 0**: Process completed successfully within timeout
- **Exit code 1-123**: Process failed with an error
- **For interactive workflows**: If timeout occurs quickly (30s), likely needs user input
- **For long workflows**: If timeout at 60s, try extending to 120s or 300s
- **Debugging timeouts**: Check Temporal Web UI to see if workflow is still running

#### Advanced Testing Strategies

**Development workflow**:
1. Start worker once
2. Modify workflow/activity code
3. Restart only the worker (keep testing with same starter)
4. Run starter multiple times for iterative testing

**Monitoring and observability**:
- Use Temporal Web UI for visual workflow inspection
- Monitor task queue backlogs: `temporal task-queue describe --task-queue [name]`
- View workflow history: `temporal workflow show --workflow-id [id] --detailed`
- Export workflow history for analysis

**Automated testing integration**:
- Use `timeout` command to prevent hanging: `timeout 60s python starter.py`
- Check exit codes: `python starter.py && echo "Success" || echo "Failed"`
- Capture and validate output: `python starter.py > output.txt && grep "expected" output.txt`

**Timeout handling**:
- **Default timeout**: 60 seconds is reasonable for most examples
- **Long-running workflows**: Use `timeout 300s` (5 minutes) for complex agent workflows
- **Interactive workflows**: Use `timeout 30s` to quickly detect if input is required
- **Handle timeout exit codes**: Exit code 124 indicates timeout, 0 indicates success

### Repository-Specific Testing

This repository uses `uv` as the package manager. The `openai_agents/` directory contains examples organized by category:

#### Directory Structure and Commands

**Basic Examples** (`openai_agents/basic/`):
```bash
# Terminal 1: Start worker
uv run openai_agents/basic/run_worker.py

# Terminal 2: Run examples with timeout
timeout 60s uv run openai_agents/basic/run_hello_world_workflow.py
timeout 60s uv run openai_agents/basic/run_tools_workflow.py
timeout 60s uv run openai_agents/basic/run_local_image_workflow.py
```

**Agent Patterns** (`openai_agents/agent_patterns/`):
```bash
# Terminal 1: Start worker  
uv run openai_agents/agent_patterns/run_worker.py

# Terminal 2: Run pattern examples with timeout
timeout 60s uv run openai_agents/agent_patterns/run_deterministic_workflow.py
timeout 60s uv run openai_agents/agent_patterns/run_parallelization_workflow.py
timeout 60s uv run openai_agents/agent_patterns/run_routing_workflow.py
```

**Interactive Examples** (require user input):
```bash
# Customer Service - interactive chat (use short timeout to detect interactivity)
uv run openai_agents/customer_service/run_worker.py  # Terminal 1
timeout 30s uv run openai_agents/customer_service/run_customer_service_client.py --conversation-id test-conversation  # Terminal 2

# Financial Research - prompts for input (use short timeout to detect prompt)
uv run openai_agents/financial_research_agent/run_worker.py  # Terminal 1  
timeout 30s uv run openai_agents/financial_research_agent/run_financial_research_workflow.py  # Terminal 2
```

**Long-running Examples** (may need extended timeout):
```bash
# Research Bot - complex multi-agent workflow
uv run openai_agents/research_bot/run_worker.py  # Terminal 1
timeout 300s uv run openai_agents/research_bot/run_research_workflow.py  # Terminal 2

# Tools examples - may involve external API calls
uv run openai_agents/tools/run_worker.py  # Terminal 1
timeout 120s uv run openai_agents/tools/run_web_search_workflow.py  # Terminal 2
```

#### Repository-Specific Setup

**Environment requirements**:
```bash
# Install dependencies
uv sync

# Set OpenAI API key
export OPENAI_API_KEY="your-key-here"

# Start Temporal server
temporal server start-dev
```

**Common task queues used**:
- `openai-agents-basic-task-queue` - Basic examples
- `openai-agents-patterns-task-queue` - Agent patterns  
- `openai-agents-task-queue` - Customer service
- `financial-research-task-queue` - Financial research

**Process management**:
```bash
# Kill specific worker types
pkill -f "openai_agents.*run_worker"

# Find workers by task queue
ps aux | grep "basic.*run_worker"
```