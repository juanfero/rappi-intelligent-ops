# src/bot/schema.py
from typing import List, Optional, Literal, Dict, Any
from pydantic import BaseModel, Field, field_validator
import re

TaskType = Literal["filter", "compare", "trend", "aggregate", "multivariable", "inference", "contextual"]


class TimeSpec(BaseModel):
    # Semana ISO por defecto: últimas 8 hasta la actual
    range: Optional[str] = "L8W-L0W"
    compare_to: Optional[Literal["prev_week", "prev_period", "none"]] = "none"


class Filters(BaseModel):
    country: Optional[str] = None
    city: Optional[str] = None
    zone: Optional[str] = None
    # Acepta ambas variantes comunes; la coerción las llevará a una forma canónica segura
    zone_type: Optional[Literal["Wealthy", "Non Wealthy", "Non-Wealthy"]] = None

    @field_validator("zone_type", mode="before")
    @classmethod
    def _coerce_zone_type(cls, v):
        """
        Normaliza variantes de zone_type para evitar errores de validación:
        - Soporta: "nonwealthy", "non-wealthy", "non_wealthy", "Non Wealthy", etc.
        - Soporta: "WEALTHY", "WeAlThY", etc.
        Devuelve "Non Wealthy" o "Wealthy" (formas canónicas incluidas en Literal).
        """
        if v is None:
            return v
        if not isinstance(v, str):
            return v

        s = v.strip().lower()
        # homogeneiza separadores a espacio
        s = re.sub(r"[-_]+", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        # variante sin espacios para capturar "nonwealthy"
        s_nospace = s.replace(" ", "")

        # Mapeos a canónicos
        # "nonwealthy" y similares -> "Non Wealthy"
        if s_nospace in {"nonwealthy", "non-weal thy".replace(" ", ""), "non_wealthy".replace("_", "")} \
           or s in {"non wealthy", "no wealthy"}:
            return "Non Wealthy"

        # Si contiene 'wealthy' pero no empieza por 'non' => "Wealthy"
        if "wealthy" in s and not s.startswith("non"):
            return "Wealthy"

        # Mantén el valor original si ya es una forma aceptada por Literal
        # (Pydantic validará después contra el Literal)
        return v


class Ops(BaseModel):
    agg: Optional[Literal["mean", "sum", "pct_change", "median"]] = None
    top_k: Optional[int] = None
    order: Optional[Literal["asc", "desc"]] = "desc"
    explain: Optional[bool] = False


class AnalyticsSpec(BaseModel):
    task: TaskType
    metrics: List[str]
    # Usa default_factory para evitar compartir instancias por defecto
    filters: Filters = Field(default_factory=Filters)
    group_by: Optional[List[str]] = None
    time: TimeSpec = Field(default_factory=TimeSpec)
    ops: Ops = Field(default_factory=Ops)
    visualization: Optional[Literal["table", "bar", "line"]] = None
    context: Dict[str, Any] = Field(default_factory=dict)
