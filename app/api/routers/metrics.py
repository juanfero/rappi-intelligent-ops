from typing import Optional
from fastapi import APIRouter, Query
from pydantic import BaseModel
from src.data.db import get_conn

router = APIRouter(prefix="/metrics", tags=["metrics"])

class MetricsSummary(BaseModel):
    country: Optional[str] = None
    zone: Optional[str] = None
    week: Optional[int] = None
    total_orders: int
    avg_gross_profit_ue: float

@router.get("", response_model=MetricsSummary)
def metrics_summary(
    country: Optional[str] = Query(None, description="CO, MX, AR..."),
    zone: Optional[str]   = Query(None, description="BOG, LIM..."),
    week: Optional[int]   = Query(None, ge=0, le=52),
):
    where, params = [], []
    if country: where.append("country = ?"); params.append(country)
    if zone:    where.append("zone = ?");    params.append(zone)
    if week is not None: where.append("week = ?"); params.append(week)
    w = ("WHERE " + " AND ".join(where)) if where else ""

    sql = f"""
      SELECT COALESCE(SUM(orders),0)::INT AS total_orders,
             COALESCE(AVG(gross_profit_ue),0.0) AS avg_gross_profit_ue
      FROM ops.metrics
      {w}
    """
    con = get_conn()
    row = con.execute(sql, params).fetchone()
    con.close()
    total = int(row[0]) if row and row[0] is not None else 0
    avggp = float(row[1]) if row and row[1] is not None else 0.0
    return MetricsSummary(country=country, zone=zone, week=week,
                          total_orders=total, avg_gross_profit_ue=avggp)

