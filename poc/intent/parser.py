import os, json
from .schema import FeasibilityIntent, ParseContext
from poc.utils.llm_client import get_llm
from dotenv import load_dotenv

load_dotenv()

SYSTEM_TMPL = """You extract intents for OMOP feasibility queries.
Return **JSON only**. No extra words.
Fields:
- condition
- time_window_start
- time_window_end
- metric

Constraints:
- Normalize 'type 2 diabetes' -> 'type 2 diabetes' (we'll map to T2DM later).
- Dates must be YYYY-MM-DD if present.
- metric is usually 'count'.

OMOP_VERSION: {omop_version}
"""

USER_TMPL = """Extract the structured intent from this request.
Return JSON only.

User request: "{user_query}"
"""

def parse_intent(user_query: str, ctx: ParseContext) -> FeasibilityIntent:
    client, model = get_llm()
    sys = SYSTEM_TMPL.format(omop_version=ctx.omop_version)
    usr = USER_TMPL.format(user_query=user_query)

    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": sys},
            {"role": "user", "content": usr}
        ],
        temperature=0.1,
    )
    raw = resp.choices[0].message.content.strip()
    try:
        data = json.loads(raw)
    except Exception:
        # fallback: try to find the first JSON block
        import re
        m = re.search(r"\{.*\}", raw, re.S)
        if not m:
            raise ValueError(f"LLM did not return JSON: {raw}")
        data = json.loads(m.group(0))

    return FeasibilityIntent(**data)
