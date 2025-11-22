from .log_manager import load_run
from poc.execution.executor import run_sql
from poc.utils.sqlglot_utils import is_read_only

def replay(run_id: str):
    run = load_run(run_id)
    steps = run.get("execution_dag", [])
    re_results = []
    for s in steps:
        action = s.get("action")
        if action in ("run_dry_run", "summarize_result", "resolve_concepts", "generate_sql"):
            # 这些为只读或推理步骤；可选择性重建，但此处演示只复核 SQL/dry-run 关键结果
            re_results.append({"step_id": s["step_id"], "action": action, "status": "skipped"})
        elif action == "run_sql":
            sql = None
            # 在 generate_sql 的输出里找 sql（简易做法）
            for prev in steps:
                if prev.get("action") == "generate_sql":
                    sql = prev.get("outputs", {}).get("sql")
            if not sql:
                re_results.append({"step_id": s["step_id"], "action": action, "status": "error", "error": "no sql"})
                continue
            if not is_read_only(sql):
                re_results.append({"step_id": s["step_id"], "action": action, "status": "blocked"})
                continue
            res = run_sql(sql)
            re_results.append({"step_id": s["step_id"], "action": action, "status": "replayed", "result": res})
        else:
            re_results.append({"step_id": s["step_id"], "action": action, "status": "unknown"})
    return {"run_id": run_id, "replay_results": re_results}