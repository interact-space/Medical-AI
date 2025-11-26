import os, json, datetime
from dotenv import load_dotenv
# from poc.db.init_database import init_db
from poc.graph.dag_builder import build_graph
from poc.audit.log_manager import save_run
from poc.audit.replay import replay


load_dotenv()

def run_pipeline(nl_query: str):
    graph = build_graph()
    result = graph.invoke({"user_input": nl_query})
    # 组织审计 JSON
    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    run_id = f"RUN_{ts}"
    run_obj = {
        "run_id": run_id,
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "omop_version": os.getenv("OMOP_VERSION", "OMOP1"),
        "intent": result.get("intent"),
        "plan": result.get("plan"),
        "execution_dag": result.get("execution_dag"),
        "summary": result.get("summary"),
        "env": {
            "llm_mode": os.getenv("LLM_MODE", "local"),
            "llm_model": os.getenv("LLM_MODEL", ""),
            "db_url": os.getenv("DATABASE_URL", "")
        }
    }
    run_id, path = save_run(run_obj)
    print(f"✅ Run saved: {path}")
    print(f"🧾 Summary: {run_obj['summary']}")
    return run_id, run_obj

if __name__ == "__main__":
    # 1) 初始化 demo DB（可重复运行）
    print("🚀 Starting OMOP DAG pipeline ...")
    # init_db()
    # 2) 运行一次
    q = 'Find how many patients had type 2 diabetes between 2020 and 2024'
    run_id, run_obj = run_pipeline(q)
    # 3) Replay
    print("🔁 Replay now...")
    re = replay(run_id)
    print(json.dumps(re, ensure_ascii=False, indent=2))
