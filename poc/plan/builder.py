from typing import List, Dict, Any
from pydantic import BaseModel

class PlanStep(BaseModel):
    id: str
    action: str
    inputs: Dict[str, Any] = {}
    outputs: Dict[str, Any] = {}

def build_plan(intent) -> List[PlanStep]:
    # PoC：硬编码 4 步
    return [
        PlanStep(id="step1", action="resolve_concepts", inputs=intent.model_dump()),
        PlanStep(id="step2", action="generate_sql", inputs={}),
        PlanStep(id="step3", action="run_dry_run", inputs={}),
        PlanStep(id="step4", action="run_sql", inputs={}),
        PlanStep(id="step5", action="summarize_result", inputs={}),
    ]
