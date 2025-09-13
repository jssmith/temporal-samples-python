# Automated Temporal Workflow Testing

Quick start guide for running automated tests on all Temporal workflow examples.

## Prerequisites

1. **Start Temporal server:**
   ```bash
   temporal server start-dev
   ```

2. **Set OpenAI API key:**
   ```bash
   export OPENAI_API_KEY="your-api-key-here"
   ```

3. **Install dependencies:**
   ```bash
   uv sync
   ```

## Quick Commands

### Check if everything is ready to test
```bash
uv run python test_temporal_workflows.py --check-prereqs
```

### See all discoverable workflow examples
```bash
uv run python test_temporal_workflows.py --list-only
```

### Test all OpenAI agent examples
```bash
uv run python test_temporal_workflows.py --include openai_agents
```

### Test a specific example group
```bash
uv run python test_temporal_workflows.py --single openai_agents_basic
uv run python test_temporal_workflows.py --single openai_agents_tools
uv run python test_temporal_workflows.py --single openai_agents_agent_patterns
```

### Test everything (all discovered examples)
```bash
uv run python test_temporal_workflows.py
```

### Exclude problematic examples
```bash
uv run python test_temporal_workflows.py --include openai_agents --exclude memory --exclude model_providers
```

## Expected Results

### Successful Examples (should pass):
- **openai_agents_basic** - Core functionality (9 workflows)
- **openai_agents_agent_patterns** - Advanced patterns (8 workflows) 
- **openai_agents_tools** - Tool integrations (4 workflows)
- **openai_agents_customer_service** - Interactive agent (1 workflow)
- **openai_agents_financial_research_agent** - Research system (1 workflow)
- **openai_agents_research_bot** - Multi-agent research (1 workflow)
- **openai_agents_hosted_mcp** - MCP examples (2 workflows)
- **openai_agents_handoffs** - Agent handoffs (1 workflow)

### Expected Failures (by design):
- **openai_agents_memory** - Missing agents library dependency
- **openai_agents_model_providers** - Requires local model servers (Ollama, etc.)
- **openai_agents_reasoning_content** - Uses unavailable deepseek-reasoner model

## Typical Runtime

- **Single example group**: ~30-60 seconds
- **All OpenAI agents**: ~10-15 minutes  
- **Full test suite**: ~15-20 minutes

## Troubleshooting

### Temporal server not running
```bash
# Check if running
curl -s http://localhost:8233

# Start if needed
temporal server start-dev
```

### Missing API key
```bash
# Check if set
echo $OPENAI_API_KEY

# Set it
export OPENAI_API_KEY="your-key"
```

### Tests hanging or timing out
- Check Temporal server logs for errors
- Verify API key has sufficient quota
- Some complex workflows (financial research, research bot) take 2-3 minutes

### Worker conflicts
```bash
# Kill any stuck workers
pkill -f "uv run.*worker.py"
```

### Clean up lingering workflows
```bash
# Check what test workflows are still running
uv run python cleanup_workflows.py --dry-run

# Terminate any lingering test workflows
uv run python cleanup_workflows.py
```

## Advanced Usage

### Run with pytest directly
```bash
uv run pytest tests/test_temporal_workflows_integration.py -v
uv run pytest tests/test_temporal_workflows_integration.py -v -s  # See real-time output
```

### Get help
```bash
uv run python test_temporal_workflows.py --help
```

## Understanding Results

### Success indicators:
- ✅ Workflow completed successfully
- Execution time in seconds
- No errors in output

### Failure indicators:
- ❌ Workflow failed or timed out
- Error message snippet shown
- Check full logs if needed

### Example output:
```
📊 Results for openai_agents_basic: 9/9 workflows passed
   run_hello_world_workflow.py: ✅ (1.9s)
   run_tools_workflow.py: ✅ (3.4s)
   run_lifecycle_workflow.py: ✅ (3.8s)
   ...
```

## What the Framework Tests

The testing framework automatically:
- ✅ Discovers workflow examples by finding worker and starter scripts
- ✅ Starts and stops worker processes for each example group
- ✅ Handles interactive workflows with automated input
- ✅ Runs setup scripts when needed (e.g., knowledge base setup)
- ✅ Monitors execution and reports timing
- ✅ Cleans up processes and workflows between tests
- ✅ Distinguishes expected failures from real problems

This saves significant manual testing time and ensures consistent, reproducible results across all workflow examples.