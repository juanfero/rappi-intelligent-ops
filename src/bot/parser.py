# src/bot/parser.py
from __future__ import annotations

import os
import re
from typing import Optional, List, Dict

from .schema import AnalyticsSpec, Filters, Ops, TimeSpec

# ---------------------------------------------------------------------
# Diccionario de métricas (canónicas) y sus sinónimos en minúsculas
# ---------------------------------------------------------------------
METRIC_SYNONYMS: Dict[str, List[str]] = {
    "Lead Penetration": [
        "lead penetration", "penetración de leads", "penetracion de leads",
        "lp", "lead pen", "% lead", "penetracion", "penetration"
    ],
    "Perfect Orders": [
        "perfect orders", "ordenes perfectas", "órdenes perfectas",
        "perfect order", "po", "% po", "pedidos perfectos"
    ],
    "Gross Profit UE": [
        "gross profit ue", "gp ue", "margen por orden", "gross profit per order",
        "gross profit", "gp_ue", "gp/ue"
    ],
    # Canónico para conteo de órdenes. Si en tu data se llama distinto,
    # puedes mapearlo aquí (ej. "Total Orders").
    "Orders": [
        "orders", "órdenes", "ordenes", "pedidos",
        "num orders", "número de órdenes", "numero de ordenes",
        "cantidad de órdenes", "cantidad de pedidos"
    ],
}

# Si en tu parquet la métrica de órdenes tiene otro nombre,
# mapea aquí el canónico "Orders" -> "NombreRealEnTuData".
# Si no necesitas alias, deja "Orders":"Orders".
METRIC_ALIASES_TO_DATA: Dict[str, str] = {
    "Lead Penetration": "Lead Penetration",
    "Perfect Orders": "Perfect Orders",
    "Gross Profit UE": "Gross Profit UE",
    "Orders": "Orders",            # cámbialo a "Total Orders" si así se llama en tu data
}

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text.lower()).strip()

def match_metric(q: str) -> Optional[str]:
    """Devuelve el nombre canónico de la métrica si la encuentra."""
    qn = normalize(q)

    # 1) búsqueda por nombre canónico literal
    for canonical in METRIC_SYNONYMS.keys():
        if canonical.lower() in qn:
            return canonical

    # 2) búsqueda por sinónimos
    for canonical, syns in METRIC_SYNONYMS.items():
        for s in syns:
            if s in qn:
                return canonical

    return None

def normalize_canonical_metric_for_data(canonical: str) -> str:
    """Convierte el canónico al nombre real usado en tu data (alias)."""
    return METRIC_ALIASES_TO_DATA.get(canonical, canonical)

def extract_country(q: str) -> Optional[str]:
    countries = {
        "colombia": "CO", "méxico": "MX", "mexico": "MX", "perú": "PE", "peru": "PE",
        "chile": "CL", "argentina": "AR", "brasil": "BR", "uruguay": "UY", "ecuador": "EC",
    }
    qn = normalize(q)
    for k, code in countries.items():
        if k in qn:
            return code
    return None

def extract_zone_type(q: str) -> Optional[str]:
    qn = normalize(q)
    if "wealthy" in qn or "rica" in qn:
        return "Wealthy"
    if "non wealthy" in qn or "non-wealthy" in qn or "no rica" in qn or "popular" in qn:
        return "Non-Wealthy"
    return None

def extract_topk(q: str) -> Optional[int]:
    # "top 5" / "mejores 10" / "5 zonas"
    m = re.search(r"\b(top|mejores?)\s+(\d+)\b", normalize(q))
    if m:
        return int(m.group(2))
    m2 = re.search(r"\b(\d+)\s+zonas?\b", normalize(q))
    if m2:
        return int(m2.group(1))
    return None

def ask_is_this_week(q: str) -> bool:
    qn = normalize(q)
    return any(w in qn for w in ["esta semana", "semana actual", "l0w"])

def ask_last_n_weeks(q: str) -> Optional[int]:
    m = re.search(r"últim[oa]s?\s+(\d+)\s+semanas?", normalize(q))
    return int(m.group(1)) if m else None

def detect_task(q: str) -> str:
    qn = normalize(q)
    if any(w in qn for w in ["compara", "comparar", "diferencia entre"]):
        return "compare"
    if any(w in qn for w in ["evolución", "tendencia", "trend"]):
        return "trend"
    if any(w in qn for w in ["promedio", "media", "suma", "total"]):
        return "aggregate"
    if "alto" in qn and "bajo" in qn:
        return "multivariable"
    if any(w in qn for w in ["crecen", "crecimiento", "aumentan", "suben"]):
        return "inference"
    if "zonas problem" in qn:
        return "contextual"
    if any(w in qn for w in ["top", "mayor", "menor", "mejores", "peores"]):
        return "filter"
    return "filter"

# ---------------------------------------------------------------------
# Reglas → AnalyticsSpec
# ---------------------------------------------------------------------
def to_spec(question: str, memory: dict) -> AnalyticsSpec:
    canonical_metric = match_metric(question) or "Orders"  # fallback seguro
    task = detect_task(question)
    country = extract_country(question) or memory.get("country")
    zone_type = extract_zone_type(question) or memory.get("zone_type")
    topk = extract_topk(question)

    # tiempo
    time_range = "L0W" if ask_is_this_week(question) else "L8W-L0W"
    n_last = ask_last_n_weeks(question)
    if n_last and 1 <= n_last <= 12:
        time_range = f"L{n_last}W-L0W"

    # group_by y visualización por defecto
    if task == "aggregate":
        group_by = ["country"]
    elif task == "compare":
        group_by = ["zone_type"] if zone_type else ["zone"]
    elif task in ("trend", "multivariable", "inference", "contextual"):
        group_by = ["zone"]
    else:
        group_by = ["zone"]

    visualization = "table"
    if task in ("compare", "aggregate"):
        visualization = "bar"
    if task == "trend":
        visualization = "line"

    # arma spec base
    spec = AnalyticsSpec(
        task=task,
        metrics=[normalize_canonical_metric_for_data(canonical_metric)],
        filters=Filters(country=country, zone_type=zone_type),
        group_by=group_by,
        time=TimeSpec(
            range=time_range,
            compare_to="prev_week" if "semana pasada" in normalize(question) else "none",
        ),
        ops=Ops(top_k=topk, agg="mean" if "promedio" in normalize(question) else None),
        visualization=visualization,
        context=memory or {},
    )

    # ---------------------------
    # OVERRIDES de negocio
    # ---------------------------
    qn = normalize(question)

    # Multivariable: necesitamos LP y PO, no una sola métrica
    if spec.task == "multivariable":
        spec.metrics = [
            normalize_canonical_metric_for_data("Lead Penetration"),
            normalize_canonical_metric_for_data("Perfect Orders"),
        ]

    # Inference (crecimiento en órdenes): fuerza Orders y 5 semanas
    if spec.task == "inference" or ("orden" in qn or "orders" in qn or "pedidos" in qn):
        if any(w in qn for w in ["crec", "aument", "sub"]):
            spec.metrics = [normalize_canonical_metric_for_data("Orders")]
            if not ask_last_n_weeks(question):
                spec.time.range = "L5W-L0W"

    return spec

# ---------------------------------------------------------------------
# LLM opcional (OpenAI). Si no hay API key, usa reglas.
# ---------------------------------------------------------------------
def to_spec_llm(question: str, memory: dict) -> AnalyticsSpec:
    api = os.getenv("OPENAI_API_KEY")
    if not api:
        return to_spec(question, memory)

    try:
        from openai import OpenAI
        from json import loads

        client = OpenAI(api_key=api)
        SYSTEM = "Eres un parser. Devuelves SOLO un JSON válido del esquema AnalyticsSpec."
        USER = f"Pregunta: {question}\nMemoria: {memory}\nDevuelve JSON."
        resp = client.responses.create(
            model="gpt-4.1-mini",
            input=[{"role": "system", "content": SYSTEM},
                   {"role": "user", "content": USER}],
            temperature=0.1,
            response_format={"type": "json_object"},
        )
        data = loads(resp.output_text)
        spec = AnalyticsSpec(**data)

        # Aplica los mismos overrides por si el LLM no los respetó
        # (reconstruimos un spec final fusionando reglas y LLM)
        fallback = to_spec(question, memory)
        # Conserva lo del LLM pero re-aplica las reglas críticas
        if spec.task == "multivariable":
            spec.metrics = [
                normalize_canonical_metric_for_data("Lead Penetration"),
                normalize_canonical_metric_for_data("Perfect Orders"),
            ]
        if spec.task == "inference" or ("orden" in normalize(question) or "orders" in normalize(question)):
            if any(w in normalize(question) for w in ["crec", "aument", "sub"]):
                spec.metrics = [normalize_canonical_metric_for_data("Orders")]
                if not ask_last_n_weeks(question):
                    spec.time.range = "L5W-L0W"

        # Normaliza alias finales hacia nombres de tu data
        spec.metrics = [normalize_canonical_metric_for_data(m) for m in spec.metrics]
        return spec
    except Exception:
        return to_spec(question, memory)
