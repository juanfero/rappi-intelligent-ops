from typing import Optional, List
from fastapi import APIRouter, Query
from pydantic import BaseModel
from src.data.db import get_conn

router = APIRouter(prefix="/timeseries", tags=["metrics"])

class Point(BaseModel):
    week: int
    orders: int

@router.get("/orders", response_model=List[Point])
def orders_ts(
    country: Optional[str] = Query(None),
    zone: Optional[str] = Query(None),
):
    where = []
    params = []
    if country:
        where.append("country = ?")
        params.append(country)
    if zone:
        where.append("zone = ?")
        params.append(zone)
    w = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
        SELECT week::INT AS week, SUM(orders)::INT AS orders
        FROM ops.metrics
        {w}
        GROUP BY week
        ORDER BY week
    """
    con = get_conn()
    rows = con.execute(sql, params).fetchall()
    con.close()
    return [{"week": int(w), "orders": int(o)} for (w, o) in rows]
