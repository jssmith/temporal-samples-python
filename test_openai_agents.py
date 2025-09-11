#!/usr/bin/env python3
"""
Simple test runner for OpenAI agents examples.

This script provides an easy way to test all the OpenAI agent workflows automatically.
It can run all tests or specific test groups.

Usage:
    # Test all examples
    python test_openai_agents.py

    # Test specific groups
    python test_openai_agents.py --basic
    python test_openai_agents.py --patterns
    python test_openai_agents.py --tools
    python test_openai_agents.py --complex

    # Show available options
    python test_openai_agents.py --help

Prerequisites:
    - Temporal server running on localhost:7233 (run: temporal server start-dev)
    - OPENAI_API_KEY environment variable set
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
from tests.test_openai_agents_integration import TestOpenAIAgentsIntegration


class SimpleTestRunner:
    """Simple interface for running OpenAI agent tests."""
    
    def __init__(self):
        self.test_instance = None
        
    def check_prerequisites(self):
        """Check if all prerequisites are met."""
        errors = []
        
        # Check Temporal server
        try:
            import requests
            response = requests.get("http://localhost:8233", timeout=5)
            if response.status_code != 200:
                errors.append("Temporal server not responding correctly")
        except Exception:
            errors.append("Temporal server not running on localhost:8233")
        
        # Check OpenAI API key
        if not os.getenv("OPENAI_API_KEY"):
            errors.append("OPENAI_API_KEY environment variable not set")
        
        # Check if we can import required dependencies
        try:
            import temporalio
            import openai
        except ImportError as e:
            errors.append(f"Missing dependency: {e}")
        
        return errors
    
    async def run_test_group(self, group_name: str):
        """Run a specific test group."""
        print(f"\n{'='*60}")
        print(f"Running {group_name.upper()} tests")
        print(f"{'='*60}")
        
        self.test_instance = TestOpenAIAgentsIntegration()
        
        try:
            self.test_instance.setup_method()
            
            if group_name == "basic":
                await self.test_instance.test_basic_examples()
            elif group_name == "patterns":
                await self.test_instance.test_agent_patterns()
            elif group_name == "tools":
                await self.test_instance.test_tools_examples()
            elif group_name == "customer_service":
                await self.test_instance.test_customer_service()
            elif group_name == "complex":
                await self.test_instance.test_complex_agents()
            elif group_name == "mcp":
                await self.test_instance.test_hosted_mcp()
            elif group_name == "handoffs":
                await self.test_instance.test_handoffs()
            elif group_name == "expected_failures":
                await self.test_instance.test_expected_failures()
            else:
                print(f"Unknown test group: {group_name}")
                return False
                
            print(f"✅ {group_name.upper()} tests completed successfully")
            return True
            
        except Exception as e:
            print(f"❌ {group_name.upper()} tests failed: {str(e)}")
            return False
        finally:
            if self.test_instance:
                self.test_instance.teardown_method()
    
    async def run_all_tests(self):
        """Run all test groups."""
        print("🚀 Starting comprehensive OpenAI agents testing...")
        print("This will test all examples in the openai_agents directory.")
        
        test_groups = [
            "basic",
            "patterns", 
            "customer_service",
            "tools",
            "mcp",
            "handoffs",
            "complex",
            "expected_failures"
        ]
        
        results = {}
        start_time = time.time()
        
        for group in test_groups:
            try:
                success = await self.run_test_group(group)
                results[group] = success
            except KeyboardInterrupt:
                print("\n\n⏹️  Testing interrupted by user")
                break
            except Exception as e:
                print(f"❌ Unexpected error in {group}: {e}")
                results[group] = False
        
        # Summary
        total_time = time.time() - start_time
        successful = sum(1 for success in results.values() if success)
        total = len(results)
        
        print(f"\n{'='*60}")
        print("TEST SUMMARY")
        print(f"{'='*60}")
        print(f"Total time: {total_time:.1f} seconds")
        print(f"Test groups: {successful}/{total} passed")
        print()
        
        for group, success in results.items():
            status = "✅ PASSED" if success else "❌ FAILED"
            print(f"  {group:20} {status}")
        
        print(f"\n{'='*60}")
        
        if successful == total:
            print("🎉 All tests passed!")
            return True
        else:
            print(f"⚠️  {total - successful} test group(s) failed")
            return False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Test runner for OpenAI agents examples",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python test_openai_agents.py                    # Run all tests
  python test_openai_agents.py --basic            # Run basic examples only
  python test_openai_agents.py --tools            # Run tools examples only
  python test_openai_agents.py --complex          # Run complex agents only

Test Groups:
  basic            - Basic OpenAI agent examples (hello world, tools, etc.)
  patterns         - Agent pattern examples (guardrails, routing, etc.)
  customer_service - Customer service agent with handoffs
  tools            - Tool-enabled agents (code interpreter, web search, etc.)
  mcp              - Hosted MCP (Model Context Protocol) examples  
  handoffs         - Message filtering with agent handoffs
  complex          - Complex multi-agent systems (financial research, research bot)
  expected_failures- Examples that are expected to fail due to missing dependencies
        """
    )
    
    # Test group options
    parser.add_argument("--basic", action="store_true", help="Test basic examples only")
    parser.add_argument("--patterns", action="store_true", help="Test agent patterns only")
    parser.add_argument("--customer-service", action="store_true", help="Test customer service only")
    parser.add_argument("--tools", action="store_true", help="Test tools examples only")
    parser.add_argument("--mcp", action="store_true", help="Test hosted MCP examples only")
    parser.add_argument("--handoffs", action="store_true", help="Test handoffs example only")
    parser.add_argument("--complex", action="store_true", help="Test complex agents only")
    parser.add_argument("--expected-failures", action="store_true", help="Test expected failures only")
    
    # Other options
    parser.add_argument("--check-prereqs", action="store_true", help="Check prerequisites only")
    parser.add_argument("--list-tests", action="store_true", help="List available test groups")
    
    args = parser.parse_args()
    
    runner = SimpleTestRunner()
    
    # Handle special commands
    if args.check_prereqs:
        print("Checking prerequisites...")
        errors = runner.check_prerequisites()
        if errors:
            print("❌ Prerequisites not met:")
            for error in errors:
                print(f"  - {error}")
            sys.exit(1)
        else:
            print("✅ All prerequisites met!")
            sys.exit(0)
    
    if args.list_tests:
        print("Available test groups:")
        groups = [
            ("basic", "Basic OpenAI agent examples"),
            ("patterns", "Agent pattern examples (guardrails, routing, etc.)"),
            ("customer-service", "Customer service agent with handoffs"),
            ("tools", "Tool-enabled agents (code interpreter, web search, etc.)"),
            ("mcp", "Hosted MCP examples"),
            ("handoffs", "Message filtering with agent handoffs"),
            ("complex", "Complex multi-agent systems"),
            ("expected-failures", "Examples expected to fail due to missing dependencies")
        ]
        for group, description in groups:
            print(f"  {group:20} - {description}")
        sys.exit(0)
    
    # Check prerequisites before running tests
    errors = runner.check_prerequisites()
    if errors:
        print("❌ Prerequisites not met:")
        for error in errors:
            print(f"  - {error}")
        print("\nPlease fix these issues before running tests.")
        print("Run 'python test_openai_agents.py --help' for more information.")
        sys.exit(1)
    
    # Determine which tests to run
    specific_tests = []
    if args.basic:
        specific_tests.append("basic")
    if args.patterns:
        specific_tests.append("patterns")
    if args.customer_service:
        specific_tests.append("customer_service")
    if args.tools:
        specific_tests.append("tools")
    if args.mcp:
        specific_tests.append("mcp")
    if args.handoffs:
        specific_tests.append("handoffs")
    if args.complex:
        specific_tests.append("complex")
    if args.expected_failures:
        specific_tests.append("expected_failures")
    
    async def run_tests():
        if specific_tests:
            # Run specific test groups
            success = True
            for test_group in specific_tests:
                result = await runner.run_test_group(test_group)
                success = success and result
            return success
        else:
            # Run all tests
            return await runner.run_all_tests()
    
    # Run the tests
    try:
        success = asyncio.run(run_tests())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⏹️  Testing interrupted by user")
        sys.exit(1)


if __name__ == "__main__":
    main()