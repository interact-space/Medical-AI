import datetime, json
from typing import Dict, Any, List
from sqlalchemy import text
from poc.utils.sqlglot_utils import is_read_only, wrap_count_subquery, pretty
from poc.utils.risk_policy import assess_risk
from .sql_generator import intent_to_sql
from poc.intent.schema import FeasibilityIntent
from poc.db.database import DatabaseManager
from poc.db.config import settings


# OMOP 概念映射：条件名称 -> concept_id
# 注意：实际项目中应该从 concept 表查询，这里使用硬编码映射
CONDITION_CONCEPT_MAP = {
    "type 2 diabetes": 319835,  # 根据用户数据
    "t2dm": 319835,
    "diabetes": 319835,
    "hypertension": 201826,  # 根据用户数据
}

# 性别映射：字符串 -> concept_id
GENDER_CONCEPT_MAP = {
    "M": 8507,  # Male
    "F": 8532,  # Female
    "male": 8507,
    "female": 8532,
}

def resolve_concepts(intent: Dict[str, Any]) -> Dict[str, Any]:
    """
    将条件名称映射到 OMOP concept_id
    将性别字符串映射到 gender_concept_id
    """
    # 映射条件
    cond = intent.get("condition")
    if cond:
        cond_lower = cond.lower()
        concept_id = CONDITION_CONCEPT_MAP.get(cond_lower)
        if concept_id:
            intent["condition_concept_id"] = concept_id
        else:
            # 如果找不到映射，保留原值（可能需要查询 concept 表）
            intent["condition_concept_id"] = None
            intent["condition_name"] = cond  # 保留原始名称用于调试
    
    # 映射性别
    if intent.get("demographic_filters") and intent["demographic_filters"].get("gender"):
        gender = intent["demographic_filters"]["gender"]
        gender_concept_id = GENDER_CONCEPT_MAP.get(gender.upper() if isinstance(gender, str) else str(gender).upper())
        if gender_concept_id:
            intent["demographic_filters"]["gender_concept_id"] = gender_concept_id
    
    return intent

def generate_sql(intent: Dict[str, Any]) -> str:
    """
    生成 SQL 时重新构造 FeasibilityIntent，这样 intent_to_sql
    可以使用 .task_type 等属性访问
    注意：intent 字典可能包含额外的字段（如 condition_concept_id），
    这些字段需要传递给 SQL 生成器
    """
    # 创建 FeasibilityIntent 对象（只包含 schema 定义的字段）
    intent_obj = FeasibilityIntent(**{k: v for k, v in intent.items() if k in FeasibilityIntent.model_fields})
    # 将完整的 intent 字典（包含额外字段）传递给 SQL 生成器
    sql = intent_to_sql(intent_obj, extra_fields=intent)
    return sql

def run_sql(sql: str) -> List[Dict[str, Any]]:
    db = DatabaseManager(settings.DB_URL, echo=True)
    with db.session() as s:
        rs = s.execute(text(sql))
        cols = rs.keys()
        rows = rs.fetchall()
        return [dict(zip(cols, r)) for r in rows]

def run_dry(sql: str) -> int:
    dry = wrap_count_subquery(sql)
    out = run_sql(dry)
    if out and "estimated_rows" in out[0]:
        return int(out[0]["estimated_rows"])
    return -1

def execute_plan_steps(
    plan: List[Dict[str, Any]], 
    intent: Dict[str, Any], 
    audit_steps: List[Dict[str, Any]],
    user_confirmed: bool = False,
    snapshot_id: str = None
):
    ctx = {"intent": intent.copy()}
    created_snapshot_id = None
    
    for step in plan:
        record = {
            "step_id": step["id"],
            "action": step["action"],
            "start_at": datetime.datetime.utcnow().isoformat(),
            "inputs": step.get("inputs", {}),
            "outputs": {},
            "status": "pending"
        }
        try:
            if step["action"] == "resolve_concepts":
                ctx["intent"] = resolve_concepts(ctx["intent"])
                record["outputs"] = {"intent": ctx["intent"]}

            elif step["action"] == "generate_sql":
                sql = generate_sql(ctx["intent"])
                record["outputs"] = {"sql": pretty(sql)}
                ctx["sql"] = sql

            elif step["action"] == "run_dry_run":
                est = run_dry(ctx["sql"])
                rp = assess_risk(ctx["sql"], estimated_rows=est)
                record["outputs"] = {"estimated_rows": est, "risk": rp}
                ctx["estimated_rows"] = est
                ctx["risk"] = rp
                
                # 如果是高风险操作，创建快照
                if rp.get("needs_approval") and not created_snapshot_id:
                    from poc.utils.snapshot_manager import create_snapshot_for_operation
                    created_snapshot_id = create_snapshot_for_operation(
                        operation_type=rp.get("statement_type", "UNKNOWN"),
                        sql=ctx["sql"],
                        user_input=intent.get("research_question", "")
                    )
                    record["outputs"]["snapshot_id"] = created_snapshot_id
                    ctx["snapshot_id"] = created_snapshot_id

            elif step["action"] == "run_sql":
                # 风险闸门：需要用户确认的高风险操作
                if ctx.get("risk", {}).get("needs_approval") and not user_confirmed:
                    raise RuntimeError(
                        f"High risk operation requires user confirmation. "
                        f"Risk level: {ctx.get('risk', {}).get('risk', 'unknown')}. "
                        f"Snapshot ID: {created_snapshot_id or snapshot_id or 'N/A'}"
                    )
                
                # 检查是否为只读操作
                if not is_read_only(ctx["sql"]):
                    # 非只读操作也需要确认
                    if not user_confirmed:
                        raise RuntimeError(
                            f"Non-read-only SQL requires user confirmation. "
                            f"Snapshot ID: {created_snapshot_id or snapshot_id or 'N/A'}"
                        )
                
                res = run_sql(ctx["sql"])
                ctx["result"] = res
                record["outputs"] = {"result": res}
                
                # 记录快照ID（如果有）
                if created_snapshot_id or snapshot_id:
                    record["outputs"]["snapshot_id"] = created_snapshot_id or snapshot_id

            elif step["action"] == "summarize_result":
                # 生成友好的操作总结
                timestamp = datetime.datetime.utcnow().strftime("%Y年%m月%d日")
                
                if ctx.get("result"):
                    first = ctx["result"][0]
                    n = list(first.values())[0]
                    
                    operation_type = ctx.get("risk", {}).get("statement_type", "SELECT")
                    operation_desc = {
                        "SELECT": "查询",
                        "INSERT": "添加",
                        "UPDATE": "更新",
                        "DELETE": "删除"
                    }.get(operation_type, "操作")
                    
                    summary = f"{timestamp}，用户执行了{operation_desc}操作，返回结果：{n}"
                    if created_snapshot_id or snapshot_id:
                        summary += f"（快照ID: {created_snapshot_id or snapshot_id}）"
                else:
                    summary = f"{timestamp}，操作完成"
                
                record["outputs"] = {"summary": summary}

            record["status"] = "success"
        except Exception as e:
            record["status"] = "error"
            record["error"] = str(e)
        finally:
            record["end_at"] = datetime.datetime.utcnow().isoformat()
            audit_steps.append(record)

    return ctx
