import sqlglot
from sqlglot import parse_one, exp

READ_ONLY_TYPES = {"SELECT"}

def get_statement_type(sql: str) -> str:
    try:
        node = parse_one(sql)
        return node.key.upper()
    except Exception:
        return "UNKNOWN"

def is_read_only(sql: str) -> bool:
    t = get_statement_type(sql)
    return t in READ_ONLY_TYPES

def wrap_count_subquery(sql: str) -> str:
    # SELECT COUNT(*) FROM ( <sql> ) t
    return f"SELECT COUNT(*) AS estimated_rows FROM ({sql}) t"

def get_tables(sql: str):
    try:
        node = parse_one(sql)
        return [t.name for t in node.find_all(exp.Table)]
    except Exception:
        return []

def pretty(sql: str) -> str:
    try:
        return sqlglot.transpile(sql, read="duckdb", write="sqlite")[0]
    except Exception:
        return sql
