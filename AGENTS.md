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
   echo "API Key set: $(echo ${OPENAI_API_KEY:+SET})"
   
   # Clean ALL running workflows that might conflict
   # Method 1: By task queue (recommended)
   temporal workflow list --limit 50 | grep "your-task-queue" | grep "Running" | awk '{print $2}' | xargs -r -I {} temporal workflow terminate --workflow-id {}
   
   # Method 2: By workflow type
   temporal workflow list --limit 50 | grep "YourWorkflowType" | grep "Running" | awk '{print $2}' | xargs -r -I {} temporal workflow terminate --workflow-id {}
   
   # Method 3: Specific workflow ID (for hardcoded IDs)
   temporal workflow terminate --workflow-id "your-workflow-id" 2>/dev/null || true
   
   # Kill any existing workers to avoid conflicts
   pkill -f "your_worker_pattern" 2>/dev/null || true
   ```

2. **Identify the worker and starter programs**:
   - **Worker program**: Registers workflows and activities with Temporal, runs continuously
   - **Starter program**: Executes specific workflows, exits when complete
   - Look for files named `*worker*` or `run_worker*` for workers
   - Look for files named `run_*` or `*client*` for starters

3. **Start the worker with error capture**:
   ```bash
   # Start worker in background with error logging
   nohup [package_manager] path/to/worker_program.py > worker.log 2>&1 & WORKER_PID=$!
   echo "Worker PID: $WORKER_PID"
   
   # Verify worker startup (wait 5 seconds, check logs)
   sleep 5
   if grep -i "error\|exception\|failed\|traceback" worker.log; then
       echo "❌ Worker startup failed - check worker.log"
       kill $WORKER_PID 2>/dev/null
       exit 1
   else
       echo "✅ Worker started successfully"
   fi
   
   # Optional: Verify worker registered workflows/activities
   # grep -q "registered.*workflows" worker.log || echo "⚠️  Worker registration unclear"
   ```

4. **Run the starter program with fast error monitoring**:
   ```bash
   # Start workflow in background for real-time monitoring
   [package_manager] path/to/starter_program.py &
   STARTER_PID=$!
   START_TIME=$(date +%s)
   
   # Monitor both workflow status and worker logs every 5 seconds
   while kill -0 $STARTER_PID 2>/dev/null; do
       ELAPSED=$(($(date +%s) - START_TIME))
       
       # Check for errors in worker logs (immediate detection)
       if grep -q "Failed activation\|TypeError\|Exception\|Error" worker.log 2>/dev/null; then
           echo "❌ ERROR detected after ${ELAPSED}s!"
           grep -A 3 "TypeError\|Exception\|Error" worker.log | head -4
           kill $STARTER_PID 2>/dev/null
           exit 1
       fi
       
       # Check workflow status via Temporal server
       STATUS=$(temporal workflow show --workflow-id "workflow-id" --query "ExecutionStatus" 2>/dev/null || echo "PENDING")
       echo "[$(date '+%H:%M:%S')] ${ELAPSED}s - Status: $STATUS"
       
       if [[ "$STATUS" == "Failed" ]]; then
           echo "❌ Workflow failed after ${ELAPSED}s"
           temporal workflow show --workflow-id "workflow-id" --detailed | grep -A 3 "failure.message"
           kill $STARTER_PID 2>/dev/null
           exit 1
       elif [[ "$STATUS" == "Completed" ]]; then
           echo "✅ Workflow completed after ${ELAPSED}s"
           break
       fi
       
       sleep 5
   done
   
   wait $STARTER_PID
   EXIT_CODE=$?
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
   kill $WORKER_PID 2>/dev/null || echo "Worker process not found"
   
   # Wait for graceful shutdown
   sleep 2
   
   # Force kill if still running
   kill -9 $WORKER_PID 2>/dev/null || true
   
   # Clean up log files
   rm -f worker.log
   
   # Optional: Terminate any remaining workflows from this test
   # temporal workflow list --limit 10 | grep "test-" | awk '{print $2}' | xargs -r -I {} temporal workflow terminate --workflow-id {}
   ```

#### Quick Test Script Pattern

For rapid testing across different repositories, use this template:

```bash
#!/bin/bash
# Quick Temporal workflow test - replace variables as needed
WORKER_CMD="[package_manager] path/to/worker.py"
STARTER_CMD="[package_manager] path/to/starter.py"
TASK_QUEUE="your-task-queue-name"
WORKER_PATTERN="worker_search_pattern"
REQUIRED_ENV_VAR="$YOUR_API_KEY"  # Adjust as needed

# Pre-flight checks  
curl -s -o /dev/null -w "%{http_code}" http://localhost:8233 | grep -q 200 || { echo "Start Temporal server first"; exit 1; }
[[ -n "$REQUIRED_ENV_VAR" ]] || { echo "Set required environment variables"; exit 1; }

# Clean slate - terminate conflicting workflows
temporal workflow list --limit 50 | grep "$TASK_QUEUE" | grep "Running" | awk '{print $2}' | xargs -r -I {} temporal workflow terminate --workflow-id {}
pkill -f "$WORKER_PATTERN" 2>/dev/null || true

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

# Run workflow with fast monitoring
echo "Running workflow with real-time monitoring..."
$STARTER_CMD &
STARTER_PID=$!
START_TIME=$(date +%s)

while kill -0 $STARTER_PID 2>/dev/null; do
    ELAPSED=$(($(date +%s) - START_TIME))
    
    # Check for errors in worker logs (immediate detection)
    if grep -q "Failed activation\|TypeError\|Exception\|Error" worker.log 2>/dev/null; then
        echo "❌ ERROR detected after ${ELAPSED}s!"
        grep -A 3 "TypeError\|Exception\|Error" worker.log | head -4
        kill $STARTER_PID 2>/dev/null
        EXIT_CODE=1
        break
    fi
    
    # Check workflow status
    STATUS=$(temporal workflow show --workflow-id "workflow-id" --query "ExecutionStatus" 2>/dev/null || echo "PENDING")
    echo "[$(date '+%H:%M:%S')] ${ELAPSED}s - Status: $STATUS"
    
    if [[ "$STATUS" == "Failed" ]]; then
        echo "❌ Workflow failed after ${ELAPSED}s"
        EXIT_CODE=1
        break
    elif [[ "$STATUS" == "Completed" ]]; then
        echo "✅ Workflow completed after ${ELAPSED}s"
        EXIT_CODE=0
        break
    fi
    
    sleep 5
done

wait $STARTER_PID 2>/dev/null || true

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
- Use fast monitoring pattern to prevent hanging tests and get immediate feedback

**Fast monitoring benefits**:
- **Exit code 0**: Workflow completed successfully
- **Exit code 1**: Workflow failed or encountered errors
- **Real-time feedback**: See progress and errors immediately as they occur
- **For interactive workflows**: Detect prompts quickly with shorter sleep intervals (2-3s)
- **For long workflows**: Monitor continuously without missing failures
- **Debugging**: Get detailed error context from both worker logs and Temporal server

#### Error Analysis: Transient vs Persistent

Before retrying failed workflows, determine if the issue is transient or persistent:

**Retry once if**: Connection errors, server unavailability, resource constraints
**Don't retry if**: `TypeError`, `ImportError`, `AttributeError`, API signature mismatches

Key question: *Does this error indicate a systemic problem (code/config/compatibility) or a temporary condition?*

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
- Use fast monitoring pattern for automated testing
- Check exit codes from the monitoring script: `./monitor_script.sh && echo "Success" || echo "Failed"`
- Capture and validate output: `./monitor_script.sh > output.txt && grep "expected" output.txt`

**Monitoring intervals**:
- **Standard monitoring**: 5 seconds for most workflows
- **Interactive workflows**: 2-3 seconds to quickly detect prompts for user input
- **Long-running workflows**: 5-10 seconds to reduce log noise while maintaining responsiveness

### Adapting Testing Patterns to Any Repository

The general testing patterns above can be adapted to any Temporal Python repository by following these steps:

#### 1. Discover Repository Structure

**Find package manager and commands**:
```bash
# Check for package manager indicators
ls pyproject.toml package.json requirements.txt setup.py Pipfile uv.lock

# Look for task definitions (common locations)
grep -r "scripts\|tasks\|commands" pyproject.toml package.json
cat pyproject.toml | grep -A 10 "\[tool.poe.tasks\]"  # poethepoet tasks
```

**Identify worker and starter files**:
```bash
# Find potential worker files
find . -name "*worker*" -o -name "*run_worker*" | head -10

# Find potential starter/client files  
find . -name "run_*" -o -name "*client*" -o -name "*start*" | head -10

# Look for workflow examples
find . -name "*workflow*" | head -10
```

#### 2. Adapt the Fast Monitoring Pattern

Replace placeholders in the general pattern with repository-specific values:

- **`[package_manager]`** → `python`, `uv run`, `poetry run`, `pipenv run`, etc.
- **`path/to/worker_program.py`** → actual worker file path from step 1
- **`path/to/starter_program.py`** → actual starter file path from step 1
- **`"workflow-id"`** → actual workflow ID from the starter code
- **`"your-task-queue"`** → actual task queue name from worker/starter code

**Find workflow ID and task queue**:
```bash
# Search for workflow ID patterns in starter files
grep -r "workflow.*id\|id.*workflow" . --include="*.py" | head -5

# Search for task queue names
grep -r "task.queue\|task_queue" . --include="*.py" | head -5
```

#### 3. Environment-Specific Adaptations

**Dependencies and setup**:
```bash
# Install dependencies (adapt based on package manager found)
uv sync              # for uv
pip install -r requirements.txt   # for pip
poetry install       # for poetry
pipenv install       # for pipenv
```

**Required environment variables**:
```bash
# Search for environment variable usage
grep -r "os.environ\|getenv\|API_KEY\|SECRET" . --include="*.py" | head -5

# Common variables to check/set
echo "OpenAI API Key: $(echo ${OPENAI_API_KEY:+SET})"
echo "AWS credentials: $(echo ${AWS_ACCESS_KEY_ID:+SET})"
```

#### 4. Workflow Type Identification

**Interactive vs Non-interactive**:
```bash
# Look for input() calls or CLI argument parsing
grep -r "input()\|argparse\|click\|typer" . --include="*.py"

# Check for streaming or continuous operations
grep -r "stream\|continuous\|loop\|async" . --include="*.py" | grep -i workflow
```

**Monitoring interval recommendations**:
- **Standard workflows**: 5 seconds
- **Interactive workflows** (detected input prompts): 2-3 seconds  
- **Long-running/batch workflows**: 10 seconds
- **Real-time/streaming workflows**: 2 seconds

#### 5. Process Management Adaptations

**Worker process patterns**:
```bash
# Adapt kill patterns to repository-specific naming
pkill -f "your_repository_name.*worker"
pkill -f "specific_directory.*run_worker"

# Find running processes
ps aux | grep "your_worker_pattern"
```

#### 6. Testing Script Template

Create a repository-specific testing script by filling in the template:

```bash
#!/bin/bash
# Repository-specific Temporal workflow test
WORKER_CMD="[package_manager] [worker_file_path]"
STARTER_CMD="[package_manager] [starter_file_path]"
WORKFLOW_ID="[actual_workflow_id]"
TASK_QUEUE="[actual_task_queue]"

# [Include the full fast monitoring pattern from above with these variables]
```

This approach ensures the testing patterns work with any repository structure while maintaining the fast feedback benefits.

#### Real-time Workflow Monitoring with Temporal Server

When testing workflows, especially those that may fail or hang, query the Temporal server directly to get immediate diagnostic information. This is often faster and more informative than relying solely on worker logs.

**Key Commands for Workflow Monitoring**:

```bash
# Get detailed workflow event history and current status
temporal workflow show --workflow-id "my-workflow-id" --detailed

# List recent workflows (useful when workflow ID is unknown)
temporal workflow list --limit 10

# Monitor specific task queue activity
temporal task-queue describe --task-queue "openai-agents-basic-task-queue"

# Check for failed workflows in the last hour
temporal workflow list --query "ExecutionStatus='Failed'" --limit 20
```

**Continuous Monitoring Pattern**:

During testing, poll the Temporal server every 5 seconds to catch issues quickly:

```bash
#!/bin/bash
# Monitor workflow during testing - run this in a separate terminal
WORKFLOW_ID="my-workflow-id"  # Replace with actual workflow ID
TASK_QUEUE="openai-agents-basic-task-queue"  # Replace with actual task queue

echo "Monitoring workflow: $WORKFLOW_ID"
echo "Task queue: $TASK_QUEUE"
echo "Press Ctrl+C to stop monitoring"
echo "================================"

while true; do
    echo "[$(date '+%H:%M:%S')] Checking workflow status..."
    
    # Check workflow status
    STATUS=$(temporal workflow show --workflow-id "$WORKFLOW_ID" --query "ExecutionStatus" 2>/dev/null || echo "NOT_FOUND")
    echo "Status: $STATUS"
    
    # If workflow failed, get detailed error
    if [[ "$STATUS" == "Failed" ]]; then
        echo "❌ WORKFLOW FAILED - Getting details..."
        temporal workflow show --workflow-id "$WORKFLOW_ID" --detailed | grep -A 20 "WorkflowTaskFailed\|ActivityTaskFailed"
        break
    elif [[ "$STATUS" == "Completed" ]]; then
        echo "✅ WORKFLOW COMPLETED"
        break
    elif [[ "$STATUS" == "NOT_FOUND" ]]; then
        echo "⚠️  Workflow not found - may not have started yet"
    fi
    
    # Check task queue backlog
    BACKLOG=$(temporal task-queue describe --task-queue "$TASK_QUEUE" --query "pollers" 2>/dev/null || echo "N/A")
    echo "Task queue status: $BACKLOG"
    
    echo "---"
    sleep 5
done
```

**Common Diagnostic Queries**:

```bash
# Find workflows by type (useful when many are running)
temporal workflow list --query "WorkflowType='HelloWorldAgent'" --limit 10

# Get the last 5 events for a workflow (quick status check)
temporal workflow show --workflow-id "my-workflow-id" --query "Events[-5:]"

# Check if worker is connected to task queue
temporal task-queue describe --task-queue "openai-agents-basic-task-queue"

# Monitor all running workflows
temporal workflow list --query "ExecutionStatus='Running'" --limit 50

# Get stack trace for stuck workflows
temporal workflow query --workflow-id "my-workflow-id" --query-type "__stack_trace"
```

**Error Pattern Recognition**:

Common error patterns in workflow event history:

- **`WorkflowTaskFailed`**: Error in workflow code (Python exceptions, import errors)
- **`ActivityTaskFailed`**: Error in activity execution (API failures, timeouts)
- **`WorkflowTaskTimedOut`**: Workflow took too long to process (possible infinite loop)
- **`ActivityTaskTimedOut`**: Activity exceeded timeout (slow external service)

**Integration with Testing Scripts**:

Add these checks to your testing workflow:

```bash
# Enhanced testing pattern with real-time monitoring
WORKFLOW_ID="my-workflow-id"
STARTER_CMD="uv run openai_agents/basic/run_hello_world_workflow.py"

# Start workflow
echo "Starting workflow..."
$STARTER_CMD &
STARTER_PID=$!

# Monitor workflow status every 5 seconds
echo "Monitoring workflow progress..."
while kill -0 $STARTER_PID 2>/dev/null; do
    STATUS=$(temporal workflow show --workflow-id "$WORKFLOW_ID" --query "ExecutionStatus" 2>/dev/null || echo "PENDING")
    echo "[$(date '+%H:%M:%S')] Status: $STATUS"
    
    if [[ "$STATUS" == "Failed" ]]; then
        echo "❌ Workflow failed - getting error details..."
        temporal workflow show --workflow-id "$WORKFLOW_ID" --detailed | grep -A 10 "failure.message"
        kill $STARTER_PID 2>/dev/null
        exit 1
    fi
    
    sleep 5
done

# Final status check
wait $STARTER_PID
EXIT_CODE=$?
FINAL_STATUS=$(temporal workflow show --workflow-id "$WORKFLOW_ID" --query "ExecutionStatus" 2>/dev/null)
echo "Final status: $FINAL_STATUS (exit code: $EXIT_CODE)"
```

**Benefits of Server Queries**:
- **Faster error detection**: Get errors immediately when they occur
- **Detailed stack traces**: See exact failure location and parameters
- **Progress tracking**: Monitor long-running workflows without waiting for completion
- **Worker health**: Verify workers are connected and processing tasks
- **Historical analysis**: Review past executions and patterns

