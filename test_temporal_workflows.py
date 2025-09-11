#!/usr/bin/env python3
"""
Generic test runner for Temporal workflow examples.

This script automatically discovers and tests any Temporal workflow examples that follow
the standard pattern of having worker programs and starter/runner programs. It's not
specific to OpenAI agents - it works with any Temporal examples.

Usage:
    # Test all discovered examples
    python test_temporal_workflows.py

    # Test specific directories/patterns
    python test_temporal_workflows.py --include openai_agents
    python test_temporal_workflows.py --include basic --include tools

    # Exclude certain patterns
    python test_temporal_workflows.py --exclude memory --exclude model_providers

    # Show discovered examples without running tests
    python test_temporal_workflows.py --list-only

    # Check prerequisites
    python test_temporal_workflows.py --check-prereqs

Prerequisites:
    - Temporal server running on localhost:7233 (run: temporal server start-dev)
    - Environment variables as required by individual examples
    - Dependencies installed (run: uv sync)
"""

import argparse
import asyncio
import os
import subprocess
import sys
import time
from pathlib import Path

# Import our test classes
sys.path.insert(0, str(Path(__file__).parent))
from tests.test_temporal_workflows_integration import (
    TemporalWorkflowTestRunner, 
    TestTemporalWorkflowsIntegration
)


class GenericTestRunner:
    """Generic test runner for any Temporal workflow examples."""
    
    def __init__(self, base_directory: Path = None):
        self.base_directory = base_directory or Path(__file__).parent
        self.runner = TemporalWorkflowTestRunner(self.base_directory)
        self.test_instance = None
        
    def check_prerequisites(self):
        """Check if basic prerequisites are met."""
        errors = []
        
        # Check Temporal server
        try:
            import requests
            response = requests.get("http://localhost:8233", timeout=5)
            if response.status_code != 200:
                errors.append("Temporal server not responding correctly")
        except Exception:
            errors.append("Temporal server not running on localhost:8233")
        
        # Check if we can import temporalio
        try:
            import temporalio
        except ImportError:
            errors.append("temporalio library not installed")
        
        # Check uv is available
        try:
            subprocess.run(["uv", "--version"], capture_output=True, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            errors.append("uv package manager not available")
        
        return errors
    
    def discover_examples(self, include_patterns=None, exclude_patterns=None):
        """Discover workflow examples with optional filtering."""
        examples = self.runner.discover_workflow_examples()
        
        if include_patterns:
            examples = {
                name: config for name, config in examples.items()
                if any(pattern in name for pattern in include_patterns)
            }
        
        if exclude_patterns:
            examples = {
                name: config for name, config in examples.items()
                if not any(pattern in name for pattern in exclude_patterns)
            }
        
        return examples
    
    def list_examples(self, include_patterns=None, exclude_patterns=None):
        """List all discovered examples."""
        examples = self.discover_examples(include_patterns, exclude_patterns)
        
        if not examples:
            print("No workflow examples found.")
            return
        
        print(f"Discovered {len(examples)} workflow example groups:")
        print()
        
        for name in sorted(examples.keys()):
            config = examples[name]
            print(f"📁 {name}")
            print(f"   Directory: {config.directory.relative_to(self.base_directory)}")
            print(f"   Worker: {config.worker_script}")
            print(f"   Workflows: {len(config.starter_scripts)}")
            
            if len(config.starter_scripts) <= 3:
                for starter in config.starter_scripts:
                    print(f"     - {starter}")
            else:
                for starter in config.starter_scripts[:2]:
                    print(f"     - {starter}")
                print(f"     - ... and {len(config.starter_scripts) - 2} more")
            
            if config.setup_scripts:
                print(f"   Setup: {', '.join(config.setup_scripts)}")
            print()
    
    async def run_tests(self, include_patterns=None, exclude_patterns=None):
        """Run tests on discovered examples."""
        print("🚀 Starting generic Temporal workflow testing...")
        
        examples = self.discover_examples(include_patterns, exclude_patterns)
        
        if not examples:
            print("❌ No workflow examples found with the given filters.")
            return False
        
        print(f"📊 Testing {len(examples)} workflow example groups")
        if include_patterns:
            print(f"   Including patterns: {', '.join(include_patterns)}")
        if exclude_patterns:
            print(f"   Excluding patterns: {', '.join(exclude_patterns)}")
        print()
        
        # Create and configure test instance
        self.test_instance = TestTemporalWorkflowsIntegration()
        
        try:
            self.test_instance.setup_method()
            
            # Apply discovered examples to the test instance
            self.test_instance.runner = self.runner
            
            # Run the tests
            await self.test_instance.test_discovered_workflow_examples()
            
            print("\n🎉 All tests completed successfully!")
            return True
            
        except Exception as e:
            print(f"\n❌ Tests failed: {str(e)}")
            return False
        finally:
            if self.test_instance:
                self.test_instance.teardown_method()
    
    async def run_single_example(self, example_name: str):
        """Run tests for a single example group."""
        examples = self.runner.discover_workflow_examples()
        
        if example_name not in examples:
            print(f"❌ Example '{example_name}' not found.")
            print("Available examples:")
            for name in sorted(examples.keys()):
                print(f"  - {name}")
            return False
        
        config = examples[example_name]
        
        print(f"🧪 Testing single example: {example_name}")
        print(f"   Directory: {config.directory.relative_to(self.base_directory)}")
        print(f"   Worker: {config.worker_script}")
        print(f"   Workflows: {', '.join(config.starter_scripts)}")
        print()
        
        # Apply example-specific configurations (if any)
        self.test_instance = TestTemporalWorkflowsIntegration()
        self.test_instance.setup_method()
        self.test_instance._apply_example_specific_config({example_name: config})
        
        try:
            results = self.runner.test_workflow_example(config)
            
            successful = sum(1 for r in results.values() if r.success)
            total = len(results)
            
            print(f"\n📊 Results for {example_name}: {successful}/{total} workflows passed")
            
            for workflow_name, result in results.items():
                status = "✅" if result.success else "❌"
                print(f"   {workflow_name}: {status} ({result.duration:.1f}s)")
                if not result.success and result.error:
                    print(f"      Error: {result.error[:100]}...")
            
            return successful > 0
            
        except Exception as e:
            print(f"❌ Failed to test {example_name}: {str(e)}")
            return False
        finally:
            self.test_instance.teardown_method()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generic test runner for Temporal workflow examples",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python test_temporal_workflows.py                        # Test all discovered examples
  python test_temporal_workflows.py --list-only            # List examples without testing
  python test_temporal_workflows.py --include openai       # Test only openai-related examples
  python test_temporal_workflows.py --exclude memory       # Exclude memory examples
  python test_temporal_workflows.py --single basic_hello   # Test single example

Discovery:
  This script automatically discovers workflow examples by looking for:
  - Worker scripts: *worker*.py, run_worker.py, *_worker.py
  - Starter scripts: run_*.py, *_workflow.py, *client*.py (not including worker scripts)
  - Setup scripts: setup_*.py, *_setup.py
  
  Examples are grouped by directory and named based on their relative path.
        """
    )
    
    # Test selection options
    parser.add_argument("--include", action="append", help="Include examples matching pattern")
    parser.add_argument("--exclude", action="append", help="Exclude examples matching pattern") 
    parser.add_argument("--single", help="Test single example by name")
    
    # Discovery options
    parser.add_argument("--list-only", action="store_true", help="List discovered examples without testing")
    parser.add_argument("--check-prereqs", action="store_true", help="Check prerequisites only")
    
    # Configuration options
    parser.add_argument("--base-dir", help="Base directory to search for examples", type=Path)
    
    args = parser.parse_args()
    
    # Initialize runner
    base_directory = args.base_dir or Path(__file__).parent
    runner = GenericTestRunner(base_directory)
    
    # Handle special commands
    if args.check_prereqs:
        print("Checking prerequisites...")
        errors = runner.check_prerequisites()
        if errors:
            print("❌ Prerequisites not met:")
            for error in errors:
                print(f"  - {error}")
            print("\nTo fix:")
            print("  1. Start Temporal server: temporal server start-dev")
            print("  2. Install dependencies: uv sync")
            print("  3. Set any required environment variables")
            sys.exit(1)
        else:
            print("✅ All prerequisites met!")
            sys.exit(0)
    
    if args.list_only:
        runner.list_examples(args.include, args.exclude)
        sys.exit(0)
    
    # Check prerequisites before running tests
    errors = runner.check_prerequisites()
    if errors:
        print("❌ Prerequisites not met:")
        for error in errors:
            print(f"  - {error}")
        print("\nRun --check-prereqs for more information.")
        sys.exit(1)
    
    # Run tests
    async def run_tests():
        if args.single:
            return await runner.run_single_example(args.single)
        else:
            return await runner.run_tests(args.include, args.exclude)
    
    try:
        success = asyncio.run(run_tests())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⏹️  Testing interrupted by user")
        sys.exit(1)


if __name__ == "__main__":
    main()