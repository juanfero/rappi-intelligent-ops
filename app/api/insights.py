from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from src.insights.engine import generate_insights
from src.insights.report import save_report

router = APIRouter(prefix="/insights", tags=["insights"])

@router.get("/")
def get_insights(country: Optional[str] = Query(default=None),
                 city: Optional[str] = Query(default=None),
                 zone: Optional[str] = Query(default=None),
                 save: bool = Query(default=True)):
    try:
        # normaliza a mayúsculas (tu DuckDB usa UPPER() en filtros internos)
        scope = {
            "country": country.upper() if country else None,
            "city": city.upper() if city else None,
            "zone": zone.upper() if zone else None,
        }
        payload = generate_insights(scope)
        paths = save_report(payload) if save else None
        return {"insights": payload, "files": paths}
    except Exception as e:
        # Evita 500 opaco hacia Streamlit
        raise HTTPException(status_code=400, detail=f"Insights error: {repr(e)}")

