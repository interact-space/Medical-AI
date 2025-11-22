from langgraph.graph import StateGraph, END
from typing import TypedDict, List, Dict, Any
from poc.intent.parser import parse_intent, ParseContext
from poc.intent.schema import FeasibilityIntent
from poc.plan.builder import build_plan
from poc.execution.executor import execute_plan_steps
import os

class PipelineState(TypedDict, total=False):
    user_input: str
    intent: Dict[str, Any]
    plan: List[Dict[str, Any]]
    execution_dag: List[Dict[str, Any]]
    summary: str

def node_intent(state: PipelineState) -> PipelineState:
    ctx = ParseContext(omop_version=os.getenv("OMOP_VERSION", "OMOP1"))
    intent = parse_intent(state["user_input"], ctx)
    state["intent"] = intent.model_dump()
    return state

def node_plan(state: PipelineState) -> PipelineState:
    # 将字典转换回 FeasibilityIntent 对象
    intent_obj = FeasibilityIntent(**state["intent"])
    plan_steps = build_plan(intent=intent_obj)
    state["plan"] = [s.model_dump() for s in plan_steps]
    return state

def node_execute(state: PipelineState) -> PipelineState:
    audit_steps: List[Dict[str, Any]] = []
    ctx = execute_plan_steps(plan=state["plan"], intent=state["intent"], audit_steps=audit_steps)
    state["execution_dag"] = audit_steps
    # 取最后总结
    state["summary"] = "No summary available"
    for s in reversed(audit_steps):
        if s.get("action") == "summarize_result" and s.get("status") == "success":
            outputs = s.get("outputs", {})
            state["summary"] = outputs.get("summary", "No summary available")
            break
    return state

def build_graph():
    graph = StateGraph(PipelineState)
    graph.add_node("intent", node_intent)
    graph.add_node("plan", node_plan)
    graph.add_node("execute", node_execute)

    graph.set_entry_point("intent")
    graph.add_edge("intent", "plan")
    graph.add_edge("plan", "execute")
    graph.add_edge("execute", END)
    return graph.compile()
