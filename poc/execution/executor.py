import datetime, json
from typing import Dict, Any, List
from sqlalchemy import text
from sqlalchemy.orm import Session
from poc.db.session import SessionLocal
from poc.utils.sqlglot_utils import is_read_only, wrap_count_subquery, pretty
from poc.utils.risk_policy import assess_risk

# 简单概念标准化（可合并到 intent parser）
CONCEPT_MAP = {
    "type 2 diabetes": "type 2 diabetes",
    "t2dm": "type 2 diabetes",
}

def resolve_concepts(intent: Dict[str, Any]) -> Dict[str, Any]:
    cond = intent.get("condition")
    if cond:
        norm = CONCEPT_MAP.get(cond.lower(), cond)
        intent["condition"] = norm
    return intent

def generate_sql(intent: Dict[str, Any]) -> str:
    # 只支持 metric=count 的最小模板
    condition = intent.get("condition")
    start = intent.get("time_window_start")
    end   = intent.get("time_window_end")

    base = "SELECT COUNT(*) AS n FROM condition_occurrence WHERE 1=1"
    if condition:
        base += f" AND condition = '{condition}'"
    if start and end:
        base += f" AND date BETWEEN '{start}' AND '{end}'"
    return base

def run_sql(sql: str) -> List[Dict[str, Any]]:
    with SessionLocal() as s:
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

def execute_plan_steps(plan: List[Dict[str, Any]], intent: Dict[str, Any], audit_steps: List[Dict[str, Any]]):
    ctx = {"intent": intent.copy()}
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

            elif step["action"] == "run_sql":
                # 风险闸门
                if not is_read_only(ctx["sql"]) or ctx.get("risk", {}).get("needs_approval"):
                    raise RuntimeError("High risk or non-read-only SQL, execution blocked.")
                res = run_sql(ctx["sql"])
                ctx["result"] = res
                record["outputs"] = {"result": res}

            elif step["action"] == "summarize_result":
                # 最小化总结
                n = None
                if ctx.get("result"):
                    first = ctx["result"][0]
                    n = list(first.values())[0]
                record["outputs"] = {"summary": f"Result count = {n}"}

            record["status"] = "success"
        except Exception as e:
            record["status"] = "error"
            record["error"] = str(e)
        finally:
            record["end_at"] = datetime.datetime.utcnow().isoformat()
            audit_steps.append(record)

    return ctx
