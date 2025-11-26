from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Literal

class FeasibilityIntent(BaseModel):
    # 查询类型检测
    is_database_query: bool = Field(default=True, description="是否为数据库查询，False表示非数据库相关查询")
    rejection_reason: Optional[str] = Field(default=None, description="如果不是数据库查询，拒绝原因")
    
    task_type: Optional[str] = Field(default=None, description="count / distribution / trend / compare / cohort / stats / insert / update / delete / select")
    
    # 研究对象相关（人群定义）
    condition: Optional[str] = None
    drug: Optional[str] = None
    procedure: Optional[str] = None
    visit_type: Optional[str] = None  # inpatient/outpatient/emergency
    demographic_filters: Optional[dict] = Field(
        default=None,
        description="e.g. {'gender': 'M', 'age_range':[40,60]}"
    )  

    # 时间范围
    time_window_start: Optional[str] = None
    time_window_end: Optional[str] = None

    # 分组/分析维度
    group_by: Optional[list] = None   # ['gender', 'year', 'age_group']

    # 指标
    metric: Optional[str] = None  # "count" / "avg" / "sum" / "rate"

    # 研究目的（可用于解释）
    research_question: Optional[str] = None
    
    # 操作类型（用于非查询操作）
    operation_type: Optional[Literal["insert", "update", "delete", "select"]] = None
    operation_target: Optional[str] = None  # 操作的目标表或实体


class ParseContext(BaseModel):
    omop_version: str = "OMOP1"
