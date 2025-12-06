"""
Activities for the financial research workflow.

Activities run outside the Temporal workflow sandbox, so OTEL spans work correctly here.
"""

from temporalio import activity

from openai_agents.financial_research_agent.financial_research_manager import (
    FinancialResearchManager,
)


@activity.defn
async def run_financial_research_activity(query: str) -> str:
    """
    Activity that runs the entire financial research workflow.
    Spans are created here (outside the workflow sandbox) so they export correctly.
    
    Note: OpenAI instrumentation is done at module level in run_worker.py to ensure
    the instrumentor uses our configured TracerProvider.
    """
    manager = FinancialResearchManager()
    return await manager.run(query)

