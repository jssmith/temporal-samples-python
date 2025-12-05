from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from openai_agents.financial_research_agent.activities import (
        run_financial_research_activity,
    )


@workflow.defn
class FinancialResearchWorkflow:
    @workflow.run
    async def run(self, query: str) -> str:
        # Run the entire research as a single activity (outside the sandbox)
        # This allows proper OTEL span creation
        return await workflow.execute_activity(
            run_financial_research_activity,
            query,
            start_to_close_timeout=timedelta(minutes=10),
        )
