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

1. **Pre-flight checks**:
   ```bash
   # Verify Temporal server is running (choose one method)
   curl -s -o /dev/null -w "%{http_code}" http://localhost:8233 | grep -q 200 && echo "Server running" || echo "Server not running"
   # OR check if gRPC port is open
   nc -z localhost 7233 && echo "gRPC port open" || echo "gRPC port closed"
   # OR check process directly
   ps aux | grep "temporal server start-dev" | grep -v grep && echo "Server process running" || echo "No server process"
   
   # Check required environment variables are set (adjust as needed)
   echo "API Key set: $(echo $OPENAI_API_KEY | head -c 10)..."
   
   # Clean any conflicting workflows from previous runs
   temporal workflow list --limit 5 | grep -q "my-workflow-id" && temporal workflow terminate --workflow-id my-workflow-id
   ```

2. **Identify the worker and starter programs**:
   - **Worker program**: Registers workflows and activities with Temporal, runs continuously
   - **Starter program**: Executes specific workflows, exits when complete
   - Look for files named `*worker*` or `run_worker*` for workers
   - Look for files named `run_*` or `*client*` for starters

3. **Start the worker with error capture**:
   ```bash
   # Start worker in background with error logging
   nohup [package_manager] path/to/worker_program.py > worker.log 2>&1 & echo $!
   
   # Verify worker startup (wait 5 seconds, check logs)
   sleep 5 && (cat worker.log || echo "Worker started successfully")
   ```

4. **Run the starter program with timeout and workflow ID management**:
   ```bash
   # Use timeout to prevent hanging, unique IDs to avoid conflicts
   timeout 60s [package_manager] path/to/starter_program.py
   
   # For scripts with hardcoded workflow IDs, terminate existing workflows first
   # or modify the script to use unique IDs like: id=f'test-{uuid.uuid4()}'
   ```

5. **Monitor execution and diagnose issues**:
   ```bash
   # Check workflow status if execution fails
   temporal workflow show --workflow-id [workflow-id] --detailed
   
   # Monitor worker logs for runtime errors
   tail -f worker.log
   
   # Quick process check
   ps aux | grep [worker_pattern]
   ```

6. **Handle user input** (if required):
   - Some workflows require interactive input
   - Watch the starter program output for prompts
   - Provide input when requested

7. **Verify completion**:
   - Starter program exits with code 0
   - Expected output is printed to console
   - Workflow shows as "Completed" in Temporal UI

8. **Clean shutdown and cleanup**:
   ```bash
   # Stop worker process (use PID from step 3)
   kill [worker_pid]
   
   # Clean up log files
   rm -f worker.log
   ```

#### Quick Test Script Pattern

For rapid testing across different repositories, use this template:

```bash
#!/bin/bash
# Quick Temporal workflow test - replace variables as needed
WORKER_CMD="[package_manager] path/to/worker.py"
STARTER_CMD="[package_manager] path/to/starter.py"
WORKFLOW_ID="test-workflow-$(date +%s)"

# Pre-flight checks  
curl -s -o /dev/null -w "%{http_code}" http://localhost:8233 | grep -q 200 || { echo "Start Temporal server first"; exit 1; }
[[ -n "$REQUIRED_ENV_VAR" ]] || { echo "Set required environment variables"; exit 1; }

# Clean slate
temporal workflow list --limit 5 | grep -q "$WORKFLOW_ID" && temporal workflow terminate --workflow-id "$WORKFLOW_ID"

# Start worker with logging
echo "Starting worker..."
nohup $WORKER_CMD > worker.log 2>&1 & WORKER_PID=$!
sleep 5

# Check worker startup
if grep -i "error\|exception\|failed" worker.log; then
    echo "Worker startup failed - check worker.log"
    kill $WORKER_PID 2>/dev/null
    exit 1
fi

# Run workflow
echo "Running workflow..."
if timeout 60s $STARTER_CMD; then
    echo "✅ Workflow completed successfully"
    EXIT_CODE=0
else
    echo "❌ Workflow failed or timed out"
    echo "Check: temporal workflow show --workflow-id [id] --detailed"
    echo "Worker logs:"
    tail -20 worker.log
    EXIT_CODE=1
fi

# Cleanup
kill $WORKER_PID 2>/dev/null
rm -f worker.log
exit $EXIT_CODE
```

#### Testing Checklist

For each workflow example:
- [ ] Pre-flight checks pass (server, environment, conflicts)
- [ ] Worker starts without errors and connects to Temporal server
- [ ] Worker registers workflows/activities successfully (check worker.log)
- [ ] Starter program connects to Temporal server
- [ ] Workflow executes without errors (check detailed workflow history)
- [ ] Expected output is produced
- [ ] Starter program exits with code 0
- [ ] Worker can be cleanly shut down

#### Troubleshooting

**Connection issues**:
```bash
# Ensure Temporal server is running
temporal server start-dev

# Check server status (choose one method)
curl -s -o /dev/null -w "%{http_code}" http://localhost:8233 | grep -q 200 && echo "Server running" || echo "Server not running"
# OR check if gRPC port is open
nc -z localhost 7233 && echo "gRPC port open" || echo "gRPC port closed"
# OR check process directly  
ps aux | grep "temporal server start-dev" | grep -v grep || echo "No server process found"
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