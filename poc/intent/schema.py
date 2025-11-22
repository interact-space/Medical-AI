from pydantic import BaseModel, Field
from typing import Optional

class FeasibilityIntent(BaseModel):
    condition: Optional[str] = Field(None, description="Standardized condition string, e.g., 'T2DM' or 'type 2 diabetes'")
    time_window_start: Optional[str] = Field(None, description="YYYY-MM-DD")
    time_window_end: Optional[str] = Field(None, description="YYYY-MM-DD")
    metric: str = Field(..., description="e.g., 'count'")

class ParseContext(BaseModel):
    omop_version: str = "OMOP1"
