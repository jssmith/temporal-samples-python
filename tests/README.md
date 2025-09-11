# Generic Temporal Workflow Automated Testing

This directory contains automated integration tests for Temporal workflow examples. The test framework automatically discovers and tests any workflow examples that follow the standard pattern of having worker programs and starter/runner programs.

## Quick Start

1. **Prerequisites:**
   ```bash
   # Start Temporal server
   temporal server start-dev
   
   # Set OpenAI API key
   export OPENAI_API_KEY="your-api-key-here"
   
   # Install dependencies
   uv sync
   ```

2. **Run all discovered tests:**
   ```bash
   python test_temporal_workflows.py
   ```

3. **Run specific examples or patterns:**
   ```bash
   python test_temporal_workflows.py --include openai_agents
   python test_temporal_workflows.py --include basic --exclude memory
   python test_temporal_workflows.py --single openai_agents_basic
   ```

4. **Discover available examples:**
   ```bash
   python test_temporal_workflows.py --list-only
   ```

## How It Works

The test framework automatically discovers Temporal workflow examples by scanning for:

- **Worker scripts**: Files matching patterns like `*worker*.py`, `run_worker.py`, `*_worker.py`
- **Starter scripts**: Files matching patterns like `run_*.py`, `*_workflow.py`, `*client*.py` (excluding worker scripts)
- **Setup scripts**: Files matching patterns like `setup_*.py`, `*_setup.py`

Examples are grouped by directory and automatically tested. The framework supports:
- Interactive workflows (with automated input)
- Setup script execution
- Expected failure handling
- Configurable timeouts
- Workflow cleanup between tests

## Test Features

### Automated Process Management
- Automatically starts and stops worker processes
- Handles worker startup validation and error detection
- Cleans up processes and temporary files after tests

### Interactive Workflow Support  
- Provides automated input for interactive workflows
- Handles workflows that require user input (customer service, financial research, etc.)
- Manages conversation IDs and session state

### Temporal Integration
- Monitors workflow execution status in real-time
- Cleans up running workflows between tests
- Handles workflow timeouts and failures gracefully

### Comprehensive Error Handling
- Distinguishes between expected and unexpected failures
- Provides detailed error reporting with logs
- Validates prerequisites before running tests

## Command Line Usage

```bash
# Run all discovered tests
python test_temporal_workflows.py

# Discover examples without testing
python test_temporal_workflows.py --list-only

# Filter by patterns
python test_temporal_workflows.py --include openai_agents
python test_temporal_workflows.py --exclude memory --exclude model_providers

# Test single example
python test_temporal_workflows.py --single openai_agents_basic

# Check prerequisites only
python test_temporal_workflows.py --check-prereqs

# Get help
python test_temporal_workflows.py --help
```

## Using pytest Directly

You can also run the tests using pytest for more control:

```bash
# Run all discovered workflow tests with verbose output
pytest tests/test_temporal_workflows_integration.py -v

# Run tests with output capture disabled (see real-time output)
pytest tests/test_temporal_workflows_integration.py -v -s

# Run tests and stop on first failure
pytest tests/test_temporal_workflows_integration.py -x

# Run tests matching a pattern
pytest tests/test_temporal_workflows_integration.py -k "openai_agents" -v
```

## Test Configuration

### Timeouts
- Basic workflows: 60 seconds
- Complex workflows (financial research, research bot): 180 seconds  
- Worker startup: 5 seconds
- Interactive workflows: 60-90 seconds depending on complexity

### Expected Behaviors
- **Success (exit code 0)**: Workflow completed successfully
- **Interactive success (exit code 1 with EOF)**: Interactive workflow completed when input exhausted
- **Expected failures**: Known issues like missing dependencies are handled gracefully

### Setup Requirements
Some tests require additional setup:
- **File search**: Automatically runs `setup_knowledge_base.py` to create vector store
- **Interactive workflows**: Uses automated input simulation
- **Complex agents**: Requires longer timeouts due to multiple API calls

## Troubleshooting

### Common Issues

1. **Temporal server not running**
   ```bash
   # Start the server
   temporal server start-dev
   ```

2. **Missing OpenAI API key**
   ```bash
   export OPENAI_API_KEY="your-api-key-here"
   ```

3. **Port conflicts**
   - Ensure Temporal server is running on default ports (7233 for gRPC, 8233 for UI)
   - Kill any existing workers: `pkill -f "openai_agents.*worker"`

4. **Dependencies missing**
   ```bash
   uv sync  # Install all dependencies
   ```

5. **Tests hanging**
   - Check Temporal server logs for errors
   - Verify API key has sufficient quota
   - Some workflows may take longer due to API rate limits

### Expected Failures
Some examples are expected to fail due to missing dependencies or unavailable services:

- **Memory examples**: Missing `OpenAIConversationsSession` from agents library
- **Model provider examples**: Require local model servers (like Ollama) that aren't running
- **Reasoning content**: Uses `deepseek-reasoner` model which isn't available
- **Image generator**: May fail due to result size limits

These failures are tested to ensure they fail for the expected reasons.

## Test Architecture

### `TestTemporalWorkflowsIntegration`
Main test class that automatically discovers and tests all workflow examples. Uses pytest fixtures for setup/teardown.

### `TemporalWorkflowTestRunner`  
Generic helper class that manages:
- Automatic discovery of workflow examples
- Worker process lifecycle management  
- Workflow execution with input automation
- Process cleanup and resource management
- Temporal workflow monitoring

### `WorkflowExampleConfig`
Configuration class that defines how to test a workflow example directory:
- Worker script location
- Starter scripts to test
- Setup scripts to run
- Interactive inputs and timeouts
- Expected failure patterns

### `WorkflowTestResult`
Data class for capturing test results with timing, success status, output, and error information.

## Contributing

The test framework automatically discovers new workflow examples. When adding new Temporal workflow examples:

1. Follow the standard pattern:
   - Worker script: `*worker*.py`, `run_worker.py`, or `*_worker.py`
   - Starter scripts: `run_*.py`, `*_workflow.py`, or `*client*.py`
   - Optional setup scripts: `setup_*.py` or `*_setup.py`

2. For special configuration needs, add logic to `_apply_example_specific_config()` method

3. Examples will be automatically discovered and tested

4. Test the new examples: `python test_temporal_workflows.py --single your_example_name`

## Performance Notes

- Tests run sequentially to avoid resource conflicts
- Each example group starts its own worker process
- Cleanup between tests ensures isolated test environments
- Total test suite runtime varies based on the number of examples and their complexity
- Interactive workflows may take longer due to input simulation
- External API calls (OpenAI, web services) affect timing

## Monitoring

The test runner provides real-time monitoring:
- Worker startup validation
- Workflow execution progress
- Error detection and reporting
- Resource cleanup verification
- Summary statistics and timing information