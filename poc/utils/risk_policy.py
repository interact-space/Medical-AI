from .sqlglot_utils import get_statement_type, get_tables

def assess_risk(sql: str, estimated_rows: int | None = None):
    st = get_statement_type(sql)
    risk = "low"
    needs_approval = False

    if st in ("DELETE", "UPDATE", "ALTER", "DROP", "TRUNCATE", "CREATE"):
        risk = "high"
        needs_approval = True
    elif st == "SELECT":
        risk = "low"
    else:
        risk = "medium"

    if estimated_rows is not None:
        if estimated_rows > 10000:
            # 简单规则：行数大提升风险
            risk = "medium" if risk == "low" else risk

    return {
        "statement_type": st,
        "tables": get_tables(sql),
        "risk": risk,
        "needs_approval": needs_approval
    }
