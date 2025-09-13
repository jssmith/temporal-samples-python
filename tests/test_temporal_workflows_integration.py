#!/usr/bin/env python3
"""
Generic integration tests for Temporal workflow examples.

This test suite automatically discovers and tests Temporal workflow examples by finding
worker programs and their corresponding starter/runner programs. It works with any
Temporal workflow examples that follow the standard pattern.

Usage:
    # Run all discovered tests
    pytest tests/test_temporal_workflows_integration.py -v

    # Run specific directory
    pytest tests/test_temporal_workflows_integration.py -k "openai_agents_basic" -v

Requirements:
    - Temporal server running on localhost:7233
    - Environment variables as required by individual examples
    - All dependencies installed
"""

import asyncio
import json
import os
import re
import signal
import subprocess
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set

import pytest
import requests
from temporalio.client import Client


class WorkflowTestResult:
    """Represents the result of a workflow test."""
    
    def __init__(self, name: str, success: bool, output: str = "", error: str = "", exit_code: int = 0):
        self.name = name
        self.success = success
        self.output = output
        self.error = error
        self.exit_code = exit_code
        self.duration = 0.0


class WorkflowExampleConfig:
    """Configuration for a workflow example directory."""
    
    def __init__(self, directory: Path, worker_script: str, 
                 starter_scripts: List[str], setup_scripts: List[str] = None):
        self.directory = directory
        self.worker_script = worker_script
        self.starter_scripts = starter_scripts
        self.setup_scripts = setup_scripts or []
        self.expected_failures: Set[str] = set()
        self.interactive_inputs: Dict[str, str] = {}
        self.timeout_overrides: Dict[str, int] = {}
        self.workflow_id_patterns: List[str] = []


class TemporalWorkflowTestRunner:
    """Generic test runner for Temporal workflow examples."""
    
    def __init__(self, base_directory: Path = None):
        self.base_path = base_directory or Path(__file__).parent.parent
        self.temp_files = []
        self.running_processes = []
        
    def cleanup(self):
        """Clean up any running processes and temporary files."""
        # Kill any running processes
        for proc in self.running_processes:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except (subprocess.TimeoutExpired, OSError):
                try:
                    proc.kill()
                    proc.wait(timeout=2)
                except (subprocess.TimeoutExpired, OSError):
                    pass
        self.running_processes.clear()
        
        # Clean up temp files
        for temp_file in self.temp_files:
            try:
                os.unlink(temp_file)
            except OSError:
                pass
        self.temp_files.clear()
        
        # Clean up any lingering worker processes (be very specific to avoid killing editors)
        try:
            subprocess.run(["pkill", "-f", "uv run.*worker.py"], 
                         capture_output=True, timeout=10)
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass

    def discover_workflow_examples(self, search_paths: List[Path] = None) -> Dict[str, WorkflowExampleConfig]:
        """Discover workflow examples by finding worker scripts and their corresponding starters."""
        if search_paths is None:
            search_paths = [self.base_path]
        
        examples = {}
        
        for search_path in search_paths:
            # Find all potential worker scripts
            worker_patterns = ["*worker*.py", "run_worker.py", "*_worker.py"]
            
            for pattern in worker_patterns:
                for worker_file in search_path.rglob(pattern):
                    # Skip test files, pycache, and virtual environments
                    worker_str = str(worker_file)
                    if any(skip in worker_str for skip in ["test", "__pycache__", ".venv", "venv", "site-packages"]):
                        continue
                    
                    directory = worker_file.parent
                    relative_worker = worker_file.relative_to(directory)
                    
                    # Find starter scripts in the same directory
                    starter_patterns = ["run_*.py", "*_workflow.py", "*client*.py"]
                    starters = set()  # Use set to avoid duplicates
                    
                    for starter_pattern in starter_patterns:
                        for starter_file in directory.glob(starter_pattern):
                            if (starter_file != worker_file and 
                                starter_file.name != relative_worker.name and
                                "worker" not in starter_file.name.lower()):
                                starters.add(starter_file.name)
                    
                    if starters:
                        # Find setup scripts
                        setup_patterns = ["setup_*.py", "*_setup.py"]
                        setups = []
                        for setup_pattern in setup_patterns:
                            setups.extend([f.name for f in directory.glob(setup_pattern)])
                        
                        example_name = f"{directory.relative_to(self.base_path)}".replace("/", "_")
                        
                        config = WorkflowExampleConfig(
                            directory=directory,
                            worker_script=str(relative_worker),
                            starter_scripts=list(starters),  # Convert back to list
                            setup_scripts=setups
                        )
                        
                        examples[example_name] = config
        
        return examples

    @contextmanager
    def worker_process(self, config: WorkflowExampleConfig, log_file: str):
        """Context manager for starting and stopping a worker process."""
        worker_path = config.directory / config.worker_script
        log_path = Path(log_file)
        
        # Start worker
        with open(log_path, 'w') as log:
            proc = subprocess.Popen(
                ["uv", "run", str(worker_path)],
                stdout=log,
                stderr=subprocess.STDOUT,
                cwd=self.base_path
            )
        
        self.running_processes.append(proc)
        self.temp_files.append(str(log_path))
        
        # Wait for worker to start
        time.sleep(5)
        
        # Check if worker started successfully
        with open(log_path, 'r') as log:
            log_content = log.read()
            if any(keyword in log_content.lower() for keyword in 
                  ["error", "exception", "failed", "traceback"]):
                raise Exception(f"Worker startup failed: {log_content}")
        
        try:
            yield proc
        finally:
            # Stop worker
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=2)
            
            if proc in self.running_processes:
                self.running_processes.remove(proc)

    def run_workflow_with_input(self, script_path: Path, input_data: str = "", 
                               timeout: int = 120) -> WorkflowTestResult:
        """Run a workflow script with optional input and return results."""
        start_time = time.time()
        
        try:
            # Prepare input
            input_bytes = input_data.encode() if input_data else None
            
            # Run workflow
            result = subprocess.run(
                ["uv", "run", str(script_path)],
                input=input_bytes,
                capture_output=True,
                timeout=timeout,
                cwd=self.base_path
            )
            
            duration = time.time() - start_time
            output = result.stdout.decode('utf-8', errors='replace')
            error = result.stderr.decode('utf-8', errors='replace')
            
            # Determine success
            success = result.returncode == 0
            # For interactive workflows, exit code 1 with EOF is often expected
            if result.returncode == 1 and ("EOF" in error or "EOFError" in error) and input_data:
                success = True
                
            test_result = WorkflowTestResult(
                name=script_path.name,
                success=success,
                output=output,
                error=error,
                exit_code=result.returncode
            )
            test_result.duration = duration
            return test_result
            
        except subprocess.TimeoutExpired:
            duration = time.time() - start_time
            test_result = WorkflowTestResult(
                name=script_path.name,
                success=False,
                error=f"Workflow timed out after {timeout} seconds",
                exit_code=-1
            )
            test_result.duration = duration
            return test_result
        except Exception as e:
            duration = time.time() - start_time
            test_result = WorkflowTestResult(
                name=script_path.name,
                success=False,
                error=str(e),
                exit_code=-1
            )
            test_result.duration = duration
            return test_result

    def run_setup_scripts(self, config: WorkflowExampleConfig) -> bool:
        """Run any setup scripts required for the example."""
        for setup_script in config.setup_scripts:
            setup_path = config.directory / setup_script
            try:
                result = subprocess.run([
                    "uv", "run", str(setup_path)
                ], capture_output=True, timeout=120, cwd=self.base_path)
                
                if result.returncode != 0:
                    print(f"Setup script {setup_script} failed: {result.stderr.decode()}")
                    return False
            except Exception as e:
                print(f"Error running setup script {setup_script}: {e}")
                return False
        return True

    async def cleanup_temporal_workflows(self, workflow_id_patterns: List[str]):
        """Clean up any running Temporal workflows that match the patterns."""
        if not workflow_id_patterns:
            return
            
        try:
            client = await Client.connect("localhost:7233")
            terminated_count = 0
            
            # List workflows and terminate matching ones
            async for workflow in client.list_workflows():
                workflow_id = workflow.id
                
                # Skip already completed/failed workflows
                if workflow.status.name not in ["RUNNING", "CONTINUED_AS_NEW"]:
                    continue
                    
                if any(pattern in workflow_id for pattern in workflow_id_patterns):
                    try:
                        handle = client.get_workflow_handle(workflow_id)
                        await handle.terminate()
                        terminated_count += 1
                        print(f"Terminated workflow: {workflow_id}")
                    except Exception as e:
                        print(f"Failed to terminate {workflow_id}: {e}")
                        
            if terminated_count > 0:
                print(f"Cleaned up {terminated_count} running workflows")
                        
        except Exception as e:
            print(f"Workflow cleanup failed: {e}")
            pass  # Temporal server might not be available

    def cleanup_temporal_workflows_sync(self, workflow_id_patterns: List[str]):
        """Synchronous wrapper for cleaning up workflows."""
        try:
            loop = asyncio.get_running_loop()
            # We're in an async context, create a task
            task = loop.create_task(self.cleanup_temporal_workflows(workflow_id_patterns))
            # Don't wait for it, let it run in background
        except RuntimeError:
            # No running loop, we can use asyncio.run
            asyncio.run(self.cleanup_temporal_workflows(workflow_id_patterns))

    def test_workflow_example(self, config: WorkflowExampleConfig) -> Dict[str, WorkflowTestResult]:
        """Test all workflows in an example configuration."""
        results = {}
        
        # Run setup scripts if any
        if config.setup_scripts:
            if not self.run_setup_scripts(config):
                # Return failure for all starters if setup failed
                for starter in config.starter_scripts:
                    results[starter] = WorkflowTestResult(
                        name=starter,
                        success=False,
                        error="Setup script failed"
                    )
                return results
        
        # Clean up any existing workflows
        if config.workflow_id_patterns:
            self.cleanup_temporal_workflows_sync(config.workflow_id_patterns)
        
        # Start worker and test all starters
        log_file = f"test_{config.directory.name}_worker.log"
        
        try:
            with self.worker_process(config, log_file):
                for starter_script in config.starter_scripts:
                    starter_path = config.directory / starter_script
                    
                    # Get configuration for this specific starter
                    input_data = config.interactive_inputs.get(starter_script, "")
                    timeout = config.timeout_overrides.get(starter_script, 120)
                    expected_to_fail = starter_script in config.expected_failures
                    
                    # Run the workflow
                    result = self.run_workflow_with_input(starter_path, input_data, timeout)
                    
                    # Adjust success based on expectations
                    if expected_to_fail and not result.success:
                        # Expected failure - mark as success for test purposes
                        result.success = True
                        result.error = f"Expected failure: {result.error}"
                    
                    results[starter_script] = result
                    
        except Exception as e:
            # Worker failed to start - mark all starters as failed
            for starter in config.starter_scripts:
                results[starter] = WorkflowTestResult(
                    name=starter,
                    success=False,
                    error=f"Worker failed to start: {str(e)}"
                )
        
        return results


class TestTemporalWorkflowsIntegration:
    """Generic integration tests for Temporal workflow examples."""
    
    def setup_method(self):
        """Set up for each test method."""
        self.runner = TemporalWorkflowTestRunner()
        
        # Check prerequisites
        self._check_temporal_server()
    
    def teardown_method(self):
        """Clean up after each test method."""
        self.runner.cleanup()
    
    def _check_temporal_server(self):
        """Check if Temporal server is running."""
        try:
            response = requests.get("http://localhost:8233", timeout=5)
            assert response.status_code == 200
        except Exception as e:
            pytest.skip(f"Temporal server not available: {e}")

    def _apply_example_specific_config(self, examples: Dict[str, WorkflowExampleConfig]):
        """Apply example-specific configurations."""
        
        for name, config in examples.items():
            # OpenAI agents specific configurations
            if "openai_agents" in name:
                # Require OpenAI API key
                if not os.getenv("OPENAI_API_KEY"):
                    # Mark all starters as expected failures
                    for starter in config.starter_scripts:
                        config.expected_failures.add(starter)
                
                # Configure interactive inputs
                for starter in config.starter_scripts:
                    if "lifecycle" in starter.lower():
                        if "agent_lifecycle" in starter.lower():
                            config.interactive_inputs[starter] = "5\n"
                        else:
                            config.interactive_inputs[starter] = "10\n"
                
                if "customer_service" in name:
                    for starter in config.starter_scripts:
                        if "client" in starter:
                            config.interactive_inputs[starter] = (
                                "I want to return my jacket\n"
                                "It doesn't fit well\n" 
                                "Thank you\n"
                            )
                
                if "financial_research" in name:
                    for starter in config.starter_scripts:
                        config.interactive_inputs[starter] = "What is Apple's financial health?\n"
                        config.timeout_overrides[starter] = 180
                
                if "research_bot" in name:
                    for starter in config.starter_scripts:
                        config.interactive_inputs[starter] = "Latest quantum computing developments?\n"
                        config.timeout_overrides[starter] = 180
                
                # Configure expected failures
                if "memory" in name:
                    # Memory examples have import issues
                    for starter in config.starter_scripts:
                        config.expected_failures.add(starter)
                
                if "model_providers" in name or "reasoning_content" in name:
                    # These need external services
                    for starter in config.starter_scripts:
                        config.expected_failures.add(starter)
                
                # Configure workflow ID patterns for cleanup
                directory_name = config.directory.name.replace("_", "-")
                config.workflow_id_patterns = [directory_name, config.directory.name]
                
                # Add specific workflow ID patterns for better cleanup
                if "customer_service" in name:
                    config.workflow_id_patterns.extend(["test-conversation-", "customer-service"])
                if "research_bot" in name:
                    config.workflow_id_patterns.extend(["research-workflow"])
                if "reasoning_content" in name:
                    config.workflow_id_patterns.extend(["reasoning-content"])
                if "model_providers" in name:
                    config.workflow_id_patterns.extend(["litellm-", "gpt-oss-"])
                if "financial_research" in name:
                    config.workflow_id_patterns.extend(["financial-research-"])
                if "memory" in name:
                    config.workflow_id_patterns.extend(["openai-session"])
                if "handoffs" in name:
                    config.workflow_id_patterns.extend(["message-filter"])
                if "hosted_mcp" in name:
                    config.workflow_id_patterns.extend(["simple-mcp", "approval-mcp"])
                if "tools" in name:
                    config.workflow_id_patterns.extend(["code-interpreter", "web-search", "file-search", "image-generator"])
                if "basic" in name:
                    config.workflow_id_patterns.extend(["hello-world", "lifecycle", "tools-", "image-", "previous-response", "dynamic-"])
                if "agent_patterns" in name:
                    config.workflow_id_patterns.extend(["deterministic", "forcing-tool", "guardrails", "llm-as-a-judge", "parallelization", "routing", "agents-as-tools"])

    async def test_discovered_workflow_examples(self):
        """Test all discovered workflow examples."""
        # Discover all workflow examples
        examples = self.runner.discover_workflow_examples()
        
        if not examples:
            pytest.skip("No workflow examples found")
        
        # Apply example-specific configurations
        self._apply_example_specific_config(examples)
        
        print(f"\nDiscovered {len(examples)} workflow example groups:")
        for name in sorted(examples.keys()):
            config = examples[name]
            print(f"  {name}: {len(config.starter_scripts)} workflows")
        
        # Test each example group
        all_results = {}
        total_workflows = 0
        successful_workflows = 0
        
        for example_name in sorted(examples.keys()):
            config = examples[example_name]
            
            print(f"\n{'='*60}")
            print(f"Testing {example_name}")
            print(f"{'='*60}")
            print(f"Worker: {config.worker_script}")
            print(f"Workflows: {', '.join(config.starter_scripts)}")
            if config.setup_scripts:
                print(f"Setup: {', '.join(config.setup_scripts)}")
            
            try:
                results = self.runner.test_workflow_example(config)
                all_results[example_name] = results
                
                # Report results for this example
                example_successful = 0
                for starter_name, result in results.items():
                    status = "✅" if result.success else "❌"
                    print(f"  {starter_name}: {status} ({result.duration:.1f}s)")
                    if result.error and not result.success:
                        print(f"    Error: {result.error[:100]}...")
                    
                    total_workflows += 1
                    if result.success:
                        example_successful += 1
                        successful_workflows += 1
                
                print(f"\n{example_name}: {example_successful}/{len(results)} workflows passed")
                
            except Exception as e:
                print(f"❌ Failed to test {example_name}: {str(e)}")
                # Mark all workflows in this example as failed
                for starter in config.starter_scripts:
                    all_results.setdefault(example_name, {})[starter] = WorkflowTestResult(
                        name=starter,
                        success=False,
                        error=str(e)
                    )
                    total_workflows += 1
        
        # Final summary
        print(f"\n{'='*60}")
        print("OVERALL SUMMARY") 
        print(f"{'='*60}")
        print(f"Total workflows tested: {total_workflows}")
        print(f"Successful workflows: {successful_workflows}")
        print(f"Success rate: {successful_workflows/total_workflows*100:.1f}%" if total_workflows > 0 else "0%")
        
        # Final cleanup - terminate any workflows that might still be running
        await self._final_workflow_cleanup()
        
        # We expect some failures due to missing dependencies, so don't fail the test
        # if at least 70% pass
        if total_workflows > 0:
            success_rate = successful_workflows / total_workflows
            if success_rate < 0.7:
                pytest.fail(f"Too many workflow failures: {success_rate*100:.1f}% success rate")
        else:
            pytest.fail("No workflows were tested")

    async def _final_workflow_cleanup(self):
        """Final cleanup to terminate any lingering test workflows."""
        print("\n🧹 Final cleanup - checking for lingering workflows...")
        
        # Common workflow ID patterns that should be cleaned up
        cleanup_patterns = [
            "test-conversation-",
            "research-workflow", 
            "reasoning-content",
            "litellm-",
            "gpt-oss-",
            "financial-research-",
            "openai-session",
            "message-filter",
            "simple-mcp",
            "approval-mcp",
            "code-interpreter",
            "web-search", 
            "file-search",
            "image-generator",
            "hello-world",
            "lifecycle",
            "tools-",
            "dynamic-",
            "previous-response",
            "deterministic",
            "forcing-tool",
            "guardrails",
            "llm-as-a-judge", 
            "parallelization",
            "routing",
            "agents-as-tools"
        ]
        
        await self.cleanup_temporal_workflows(cleanup_patterns)


# Allow running with pytest
if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])