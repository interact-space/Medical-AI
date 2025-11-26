from langgraph.graph import StateGraph, END
from typing import TypedDict, List, Dict, Any, Literal
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
    is_database_query: bool
    rejection_message: str
    sql: str
    risk_assessment: Dict[str, Any]
    needs_user_confirmation: bool
    snapshot_id: str
    execution_confirmed: bool

def node_intent(state: PipelineState) -> PipelineState:
    """解析用户意图，检测是否为数据库查询"""
    ctx = ParseContext(omop_version=os.getenv("OMOP_VERSION", "OMOP1"))
    intent = parse_intent(state["user_input"], ctx)
    intent_dict = intent.model_dump()
    state["intent"] = intent_dict
    state["is_database_query"] = intent_dict.get("is_database_query", True)
    
    # 如果不是数据库查询，生成友好提示
    if not state["is_database_query"]:
        state["rejection_message"] = (
            "❗ 抱歉，你的问题似乎与医疗数据或数据库查询无关。\n"
            "当前系统只能帮助你查询 OMOP 医疗数据，如疾病人数统计、趋势分析、年龄分布等。\n\n"
            "你可以尝试类似：\n"
            "• \"count patients with COPD last year\"\n"
            "• \"trend of type 2 diabetes from 2018 to 2023\"\n"
            "• \"distribution of hypertension by gender\"\n\n"
            "如需帮助，请告诉我要查询的医疗问题。"
        )
    
    return state

def node_plan(state: PipelineState) -> PipelineState:
    # 将字典转换回 FeasibilityIntent 对象
    intent_obj = FeasibilityIntent(**state["intent"])
    plan_steps = build_plan(intent=intent_obj)
    state["plan"] = [s.model_dump() for s in plan_steps]
    return state

def node_execute(state: PipelineState) -> PipelineState:
    """执行计划步骤，包括风险评估和用户确认"""
    audit_steps: List[Dict[str, Any]] = []
    ctx = execute_plan_steps(
        plan=state["plan"], 
        intent=state["intent"], 
        audit_steps=audit_steps,
        user_confirmed=state.get("execution_confirmed", False),
        snapshot_id=state.get("snapshot_id")
    )
    state["execution_dag"] = audit_steps
    
    # 提取 SQL 和风险信息
    for step in audit_steps:
        if step.get("action") == "generate_sql" and step.get("status") == "success":
            state["sql"] = step.get("outputs", {}).get("sql", "")
        if step.get("action") == "run_dry_run" and step.get("status") == "success":
            state["risk_assessment"] = step.get("outputs", {}).get("risk", {})
            state["needs_user_confirmation"] = step.get("outputs", {}).get("risk", {}).get("needs_approval", False)
            if step.get("outputs", {}).get("snapshot_id"):
                state["snapshot_id"] = step.get("outputs", {}).get("snapshot_id")
    
    # 取最后总结
    state["summary"] = "No summary available"
    for s in reversed(audit_steps):
        if s.get("action") == "summarize_result" and s.get("status") == "success":
            outputs = s.get("outputs", {})
            state["summary"] = outputs.get("summary", "No summary available")
            break
    
    return state


def should_continue(state: PipelineState) -> Literal["end", "execute"]:
    """条件路由：如果不是数据库查询，直接结束"""
    if not state.get("is_database_query", True):
        return "end"
    return "execute"

def build_graph():
    graph = StateGraph(PipelineState)
    graph.add_node("intent", node_intent)
    graph.add_node("plan", node_plan)
    graph.add_node("execute", node_execute)

    graph.set_entry_point("intent")
    
    # 条件路由：如果不是数据库查询，直接结束
    graph.add_conditional_edges(
        "intent",
        should_continue,
        {
            "end": END,
            "execute": "plan"
        }
    )
    
    graph.add_edge("plan", "execute")
    graph.add_edge("execute", END)
    return graph.compile()
