from agents import trace
from temporalio import workflow

from openai_agents.financial_research_agent.financial_research_manager import (
    FinancialResearchManager,
)


@workflow.defn
class FinancialResearchWorkflow:
    @workflow.run
    async def run(self, query: str) -> str:
        with trace("Financial research", group_id=workflow.info().workflow_id):
            manager = FinancialResearchManager()
            return await manager.run(query)
