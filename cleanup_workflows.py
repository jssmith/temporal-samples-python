#!/usr/bin/env python3
"""
Standalone workflow cleanup utility.

This script terminates any test workflows that may be left running from interrupted
or failed test runs. Run this if you notice workflows still running after testing.

Usage:
    python cleanup_workflows.py
    python cleanup_workflows.py --dry-run  # Show what would be terminated without doing it
"""

import asyncio
import argparse
from pathlib import Path
import sys

# Import our test framework
sys.path.insert(0, str(Path(__file__).parent))
from tests.test_temporal_workflows_integration import TemporalWorkflowTestRunner


async def cleanup_test_workflows(dry_run=False):
    """Clean up any lingering test workflows."""
    
    print("🧹 Temporal Workflow Cleanup Utility")
    print("=" * 50)
    
    if dry_run:
        print("DRY RUN MODE - showing what would be terminated")
    else:
        print("Terminating any running test workflows...")
    
    # All the common test workflow ID patterns
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
    
    runner = TemporalWorkflowTestRunner()
    
    try:
        from temporalio.client import Client
        
        client = await Client.connect("localhost:7233")
        found_workflows = []
        terminated_count = 0
        
        print("\nScanning for test workflows...")
        
        # List workflows and find matching ones
        async for workflow in client.list_workflows():
            workflow_id = workflow.id
            
            # Skip already completed/failed workflows
            if workflow.status.name not in ["RUNNING", "CONTINUED_AS_NEW"]:
                continue
                
            if any(pattern in workflow_id for pattern in cleanup_patterns):
                found_workflows.append((workflow_id, workflow.workflow_type, workflow.status.name))
        
        if not found_workflows:
            print("✅ No test workflows found running")
            return
        
        print(f"\nFound {len(found_workflows)} test workflows:")
        for workflow_id, workflow_type, status in found_workflows:
            print(f"  🔄 {workflow_id} ({workflow_type}) - {status}")
        
        if dry_run:
            print(f"\nDRY RUN: Would terminate {len(found_workflows)} workflows")
            return
        
        # Terminate them
        print(f"\nTerminating {len(found_workflows)} workflows...")
        for workflow_id, workflow_type, status in found_workflows:
            try:
                handle = client.get_workflow_handle(workflow_id)
                await handle.terminate()
                terminated_count += 1
                print(f"  ✅ Terminated: {workflow_id}")
            except Exception as e:
                print(f"  ❌ Failed to terminate {workflow_id}: {e}")
        
        print(f"\n🎉 Successfully terminated {terminated_count}/{len(found_workflows)} workflows")
        
    except Exception as e:
        print(f"❌ Cleanup failed: {e}")
        if "connection" in str(e).lower():
            print("   Make sure Temporal server is running: temporal server start-dev")
        return False
    
    return True


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Clean up lingering test workflows",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python cleanup_workflows.py              # Terminate all test workflows
  python cleanup_workflows.py --dry-run    # Show what would be terminated
  
This utility finds and terminates workflows with IDs that match common test patterns.
It's safe to run - it only affects workflows created by the test framework.
        """
    )
    
    parser.add_argument(
        "--dry-run", 
        action="store_true", 
        help="Show what would be terminated without actually doing it"
    )
    
    args = parser.parse_args()
    
    try:
        success = asyncio.run(cleanup_test_workflows(args.dry_run))
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n⏹️  Cleanup interrupted")
        sys.exit(1)


if __name__ == "__main__":
    main()