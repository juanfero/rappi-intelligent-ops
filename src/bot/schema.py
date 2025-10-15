from typing import List, Optional, Literal, Dict, Any
from pydantic import BaseModel, Field

TaskType = Literal["filter","compare","trend","aggregate","multivariable","inference","contextual"]

class TimeSpec(BaseModel):
    range: Optional[str] = "L8W-L0W"
    compare_to: Optional[Literal["prev_week","prev_period","none"]] = "none"

class Filters(BaseModel):
    country: Optional[str] = None
    city: Optional[str] = None
    zone: Optional[str] = None
    zone_type: Optional[Literal["Wealthy","Non-Wealthy"]] = None

class Ops(BaseModel):
    agg: Optional[Literal["mean","sum","pct_change","median"]] = None
    top_k: Optional[int] = None
    order: Optional[Literal["asc","desc"]] = "desc"
    explain: Optional[bool] = False

class AnalyticsSpec(BaseModel):
    task: TaskType
    metrics: List[str]
    filters: Filters = Filters()
    group_by: Optional[List[str]] = None
    time: TimeSpec = TimeSpec()
    ops: Ops = Ops()
    visualization: Optional[Literal["table","bar","line"]] = None
    context: Dict[str, Any] = Field(default_factory=dict)
