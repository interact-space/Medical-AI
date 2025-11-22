# From NLP → Intent → Plan → Execution DAG → Audit + Replay (OMOP1 PoC, LangGraph + SQLGlot)

## What is this
- 自然语言 → Intent(JSON)（LLM）
- Intent → Plan（LangGraph Node）
- Plan → Execution DAG（生成 SQL、SQLGlot dry-run、风险检查、执行）
- 审计到 JSON（runs/RUN_*.json）
- Replay：不调用 LLM，仅依据审计 JSON 重放只读步骤

## Stack
- LangGraph：编排 / DAG
- SQLGlot：SQL AST、dry-run（SELECT COUNT(*) FROM (...))、风险要素分析
- SQLAlchemy + SQLite：最小 DB（person / condition_occurrence）
- OpenAI 兼容接口：可走 Ollama（本地）或 OpenAI（云端）
- 审计：JSON 文件（runs/）

## Quick Start
```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# 如用 Ollama（本地）：
# LLM_MODE=local, LLM_BASE_URL=http://127.0.0.1:11434/v1, LLM_API_KEY=ollama, LLM_MODEL=llama3:latest

python -m poc.db.init_db
python -m poc.app
 