# src/bot/parser.py
from __future__ import annotations

import os
import re
import unicodedata
from functools import lru_cache
from pathlib import Path
from typing import Optional, List, Dict, Tuple

from .schema import AnalyticsSpec, Filters, Ops, TimeSpec
from .metrics import match_metric_from_catalog, props_for_metric, label_for_metric

try:
    import duckdb  # type: ignore
except Exception:  # pragma: no cover - si duckdb no está instalado aún
    duckdb = None

# ---------------------------------------------------------------------
# Diccionario de métricas (canónicas) y sus sinónimos (fallback legado)
# ---------------------------------------------------------------------
METRIC_SYNONYMS: Dict[str, List[str]] = {
    "Lead Penetration": [
        "lead penetration",
        "penetracion de leads", "penetración de leads",
        "penetracion de clientes potenciales", "penetración de clientes potenciales",
        "penetracion de clientes", "penetración de clientes",
        "penetracion de usuarios", "penetración de usuarios",
        "lp", "lead pen", "% lead", "penetracion", "penetration",
    ],
    "Perfect Orders": [
        "perfect orders",
        "ordenes perfectas", "órdenes perfectas",
        "perfect order", "pedido perfecto", "pedidos perfectos",
        "cumplimiento de pedido perfecto", "nivel de cumplimiento de pedido perfecto",
        "po", "% po",
    ],
    "Gross Profit UE": [
        "gross profit ue", "gp ue",
        "margen por orden", "gross profit per order",
        "gross profit", "gp_ue", "gp/ue"
    ],
    "Orders": [
        "orders", "ordenes", "órdenes", "pedidos",
        "num orders", "numero de ordenes", "número de órdenes",
        "cantidad de ordenes", "cantidad de órdenes", "cantidad de pedidos"
    ],
}

METRIC_ALIASES_TO_DATA: Dict[str, str] = {
    "Lead Penetration": "Lead Penetration",
    "Perfect Orders": "Perfect Orders",
    "Gross Profit UE": "Gross Profit UE",
    "Orders": "Orders",
}

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
SPANISH_NUMBER_WORDS: Dict[str, int] = {
    "uno": 1, "una": 1,
    "dos": 2, "tres": 3, "cuatro": 4, "cinco": 5, "seis": 6,
    "siete": 7, "ocho": 8, "nueve": 9, "diez": 10, "once": 11, "doce": 12,
    "veinte": 20,
}
NUMBER_WORD_PATTERN = "|".join(sorted(SPANISH_NUMBER_WORDS.keys(), key=len, reverse=True))

def _strip_accents(text: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn")

def normalize(text: str) -> str:
    text = _strip_accents(text.lower())
    return re.sub(r"\s+", " ", text).strip()

_DESC_TRIGGERS = {"mayor", "maximo", "mas alto", "top", "superior", "mejor", "highest", "max"}
_ASC_TRIGGERS  = {"menor", "minimo", "mas bajo", "peor", "peores", "lowest", "min", "inferior", "bottom"}
_MONTHS = {"enero","febrero","marzo","abril","mayo","junio","julio","agosto","septiembre","octubre","noviembre","diciembre"}

def _is_month_ctx(q: str) -> bool:
    t = normalize(q)
    return (" mayo " in f" {t} ") and any(m in t for m in _MONTHS)

def decide_order_and_n(q: str) -> Tuple[str, Optional[int]]:
    qn = normalize(q)
    m = re.search(r"\b(top|bottom)\s+(\d+)\b", qn)
    if m:
        order = "desc" if m.group(1) == "top" else "asc"
        return order, int(m.group(2))
    m = re.search(r"\b(top|bottom)\s+([a-z]+)\b", qn)
    if m and m.group(2) in SPANISH_NUMBER_WORDS:
        order = "desc" if m.group(1) == "top" else "asc"
        return order, SPANISH_NUMBER_WORDS[m.group(2)]
    m = re.search(r"\b(las|los)\s+(\d+)\s+(mejores|peores|mayores|menores)\b", qn)
    if m:
        order = "desc" if ("mejor" in m.group(3) or "mayor" in m.group(3)) else "asc"
        return order, int(m.group(2))
    desc = any(w in qn for w in _DESC_TRIGGERS)
    asc  = any(w in qn for w in _ASC_TRIGGERS)
    if not _is_month_ctx(q) and " mayo " in f" {qn} ":
        desc = True
    if asc and desc:
        if any(w in qn for w in ["peor","peores","menor","menores","mas bajo","bottom"]):
            return "asc", None
        return "desc", None
    if desc: return "desc", None
    if asc:  return "asc", None
    return "desc", None

@lru_cache(maxsize=1)
def _load_geo_catalog() -> Tuple[Dict[str, Tuple[str, Optional[str], Optional[str]]],
                                 Dict[str, Tuple[str, Optional[str]]],
                                 Dict[str, str]]:
    zone_index: Dict[str, Tuple[str, Optional[str], Optional[str]]] = {}
    city_index: Dict[str, Tuple[str, Optional[str]]] = {}
    country_index: Dict[str, str] = {}

    if duckdb is None:
        return zone_index, city_index, country_index

    db_path = Path("data/processed/warehouse.duckdb")
    if not db_path.exists():
        return zone_index, city_index, country_index

    try:
        con = duckdb.connect(str(db_path), read_only=True)
    except Exception:
        return zone_index, city_index, country_index

    try:
        df = con.execute("SELECT DISTINCT COUNTRY, CITY, ZONE FROM zone_weekly_metrics").df()
    except Exception:
        con.close()
        return zone_index, city_index, country_index

    con.close()

    for _, row in df.iterrows():
        country = row.get("COUNTRY")
        city = row.get("CITY")
        zone = row.get("ZONE")
        if isinstance(zone, str) and zone.strip():
            zone_index[normalize(zone)] = (zone, city if isinstance(city, str) else None,
                                           country if isinstance(country, str) else None)
        if isinstance(city, str) and city.strip():
            city_index.setdefault(normalize(city), (city, country if isinstance(country, str) else None))
        if isinstance(country, str) and country.strip():
            country_index.setdefault(normalize(country), country)

    city_index.setdefault("cdmx", ("Ciudad de Mexico", "Mexico"))
    city_index.setdefault("mexico city", ("Ciudad de Mexico", "Mexico"))
    city_index.setdefault("bogota", ("Bogota", "Colombia"))
    zone_index.setdefault("chapinero", ("Chapinero", "Bogota", "Colombia"))
    return zone_index, city_index, country_index

def match_metric(q: str) -> Optional[str]:
    qn = normalize(q)
    for canonical in METRIC_SYNONYMS.keys():
        if canonical.lower() in qn:
            return canonical
    for canonical, syns in METRIC_SYNONYMS.items():
        for s in syns:
            if s in qn:
                return canonical
    return None

def normalize_canonical_metric_for_data(canonical: str) -> str:
    return METRIC_ALIASES_TO_DATA.get(canonical, canonical)

def extract_country(q: str) -> Optional[str]:
    qn = normalize(q)
    _, _, country_index = _load_geo_catalog()
    for norm_name, country in country_index.items():
        if norm_name and norm_name in qn:
            return country
    countries_by_name = {
        "colombia": "Colombia",
        "mexico": "Mexico",
        "peru": "Peru",
        "chile": "Chile",
        "argentina": "Argentina",
        "brasil": "Brasil",
        "uruguay": "Uruguay",
        "ecuador": "Ecuador",
    }
    for k, name in countries_by_name.items():
        if k in qn:
            return name
    return None

def extract_location(q: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    qn = normalize(q)
    zones, cities, _ = _load_geo_catalog()
    zone_match = next((vals for key, vals in zones.items() if key and key in qn), None)
    if zone_match:
        zone, city, country = zone_match
        return country, city, zone
    city_match = next((vals for key, vals in cities.items() if key and key in qn), None)
    if city_match:
        city, country = city_match
        return country, city, None
    return None, None, None

def extract_zone_type(q: str) -> Optional[str]:
    qn = normalize(q)
    has_wealthy = any(w in qn for w in ["wealthy", "rica", "altas rentas"])
    has_non_wealthy = any(w in qn for w in ["non wealthy", "non-wealthy", "no rica", "popular"])
    if has_wealthy and has_non_wealthy:
        return None
    if has_wealthy:
        return "Wealthy"
    if has_non_wealthy:
        return "Non Wealthy"
    return None

def mentions_zone_segments(q: str) -> bool:
    qn = normalize(q)
    return any(w in qn for w in ["wealthy", "non wealthy", "non-wealthy", "rica", "no rica", "popular"])

def extract_topk(q: str) -> Optional[int]:
    nq = normalize(q)
    m = re.search(r"\b(top|bottom|mejores?|peores?)\s+(\d+)\b", nq)
    if m:
        return int(m.group(2))
    m2 = re.search(r"\b(\d+)\s+zonas?\b", nq)
    if m2:
        return int(m2.group(1))
    m3 = re.search(rf"\b(top|bottom|mejores?|peores?)\s+({NUMBER_WORD_PATTERN})\b", nq)
    if m3:
        return SPANISH_NUMBER_WORDS[m3.group(2)]
    return None

def ask_is_this_week(q: str) -> bool:
    qn = normalize(q)
    return any(w in qn for w in ["esta semana", "semana actual"])

def ask_last_n_weeks(q: str) -> Optional[int]:
    qn = normalize(q)
    m = re.search(r"(ultim[oa]s?|últim[oa]s?)\s+(\d+)\s+semanas?", qn)
    if m:
        return int(m.group(2))
    m2 = re.search(rf"(ultim[oa]s?|últim[oa]s?)\s+({NUMBER_WORD_PATTERN})\s+semanas?", qn)
    if m2:
        return SPANISH_NUMBER_WORDS[m2.group(2)]
    return None

def detect_task(q: str) -> str:
    qn = normalize(q)
    if any(w in qn for w in ["compara", "comparar", "diferencia entre"]):
        return "compare"
    if any(w in qn for w in ["evolucion", "tendencia", "trend"]):
        return "trend"
    if any(w in qn for w in ["promedio", "media", "suma", "total"]):
        return "aggregate"
    if "alto" in qn and "bajo" in qn:
        return "multivariable"
    if any(w in qn for w in ["crecen", "crecimiento", "aumentan", "suben"]):
        return "inference"
    if "zonas problem" in qn or "problematic" in qn:
        return "contextual"
    if any(w in qn for w in ["top", "bottom", "mayor", "menor", "mejores", "peores"]):
        return "filter"
    return "filter"

# ---------------------------------------------------------------------
# Reglas → AnalyticsSpec (integrado con catálogo YAML)
# ---------------------------------------------------------------------
def to_spec(question: str, memory: dict) -> AnalyticsSpec:
    # 1) Métrica desde catálogo (con fallback legado)
    cat_match = match_metric_from_catalog(question)
    if cat_match:
        data_metric, mprops = cat_match
    else:
        canonical_metric = match_metric(question) or "Orders"
        data_metric = normalize_canonical_metric_for_data(canonical_metric)
        mprops = props_for_metric(data_metric)

    # 2) Intent y ubicación
    task = detect_task(question)
    detected_country = extract_country(question)
    loc_country, loc_city, loc_zone = extract_location(question)

    country = (loc_country or detected_country)  # evita “país pegado”
    city = loc_city or memory.get("city")
    zone = loc_zone or memory.get("zone")
    zone_type = extract_zone_type(question) or memory.get("zone_type")
    segment_mentioned = mentions_zone_segments(question)

    # 3) TopK y orden
    topk = extract_topk(question)
    order, n_from_text = decide_order_and_n(question)
    if topk is None and n_from_text:
        topk = n_from_text

    # 4) Tiempo
    time_range = "L0W" if ask_is_this_week(question) else "L8W-L0W"
    n_last = ask_last_n_weeks(question)
    if n_last and 1 <= n_last <= 12:
        time_range = f"L{n_last}W-L0W"

    # 5) Group-by & visualización
    if task == "aggregate":
        group_by = ["country"]
    elif task == "compare":
        group_by = ["zone_type"] if segment_mentioned else ["zone"]
    elif task == "trend":
        group_by = ["week"]
    elif task in ("multivariable", "inference", "contextual"):
        group_by = ["zone"]
    else:
        group_by = ["zone"]

    visualization = "table"
    if task in ("compare", "aggregate"):
        visualization = "bar"
    if task == "trend":
        visualization = "line"

    # 6) Construye spec base (contexto con metadatos de la métrica)
    explicit_country = bool(country)
    spec = AnalyticsSpec(
        task=task,
        metrics=[data_metric],  # nombre real de data desde el catálogo
        filters=Filters(country=country, city=city, zone=zone, zone_type=zone_type),
        group_by=group_by,
        time=TimeSpec(
            range=time_range,
            compare_to="prev_week" if "semana pasada" in normalize(question) else "none",
        ),
        ops=Ops(
            top_k=topk,
            agg="mean" if "promedio" in normalize(question) else None,
            order=order
        ),
        visualization=visualization,
        context={
            **(memory or {}),
            "explicit_country": explicit_country,
            "metric_label": label_for_metric(data_metric),
            "metric_value_type": mprops.get("value_type"),
            "metric_range_hint": mprops.get("range_hint"),
            "metric_higher_is_better": mprops.get("higher_is_better", True),
        },
    )

    # 7) Si el usuario no pidió agregador, usa el del catálogo
    if spec.ops.agg is None:
        spec.ops.agg = mprops.get("agg_default", "mean")

    # ---------------------------
    # OVERRIDES de negocio
    # ---------------------------
    qn = normalize(question)

    if spec.task == "multivariable":
        spec.metrics = ["Lead Penetration", "Perfect Orders"]
        try:
            setattr(spec.ops, "high_low", {
                "high": {"metric": "Lead Penetration", "p": 0.70},
                "low":  {"metric": "Perfect Orders",   "p": 0.30},
            })
        except Exception:
            pass

    if spec.task == "inference" or ("orden" in qn or "orders" in qn or "pedidos" in qn):
        if any(w in qn for w in ["crec", "crecim", "aument", "sub"]):
            spec.metrics = ["Orders"]
            if not ask_last_n_weeks(question):
                spec.time.range = "L5W-L0W"

    # Limpieza final: si agrupas por país, NO arrastres filtro de país
    if spec.group_by and "country" in (spec.group_by or []):
        spec.filters.country = None

    return spec

# ---------------------------------------------------------------------
# LLM opcional
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

        fallback = to_spec(question, memory)

        if spec.task == "multivariable":
            spec.metrics = ["Lead Penetration", "Perfect Orders"]
            try:
                setattr(spec.ops, "high_low", {
                    "high": {"metric": "Lead Penetration", "p": 0.70},
                    "low":  {"metric": "Perfect Orders",   "p": 0.30},
                })
            except Exception:
                pass

        if spec.task == "inference" or ("orden" in normalize(question) or "orders" in normalize(question)):
            if any(w in normalize(question) for w in ["crec", "crecim", "aument", "sub"]):
                spec.metrics = ["Orders"]
                if not ask_last_n_weeks(question):
                    spec.time.range = "L5W-L0W"

        if not getattr(spec.ops, "order", None):
            spec.ops.order = fallback.ops.order
        if not getattr(spec.ops, "top_k", None):
            spec.ops.top_k = fallback.ops.top_k
        if spec.task == "trend" and ("week" not in (spec.group_by or [])):
            spec.group_by = ["week"]
        if spec.filters and spec.filters.zone_type == "Non-Wealthy":
            spec.filters.zone_type = "Non Wealthy"

        # Aplica agg_default del catálogo si falta
        mprops = props_for_metric(spec.metrics[0] if spec.metrics else "Orders")
        if not getattr(spec.ops, "agg", None):
            spec.ops.agg = mprops.get("agg_default", "mean")

        # Explicita semántica de país y limpieza por group_by=country (igual que reglas)
        spec.context = {**(memory or {}), "explicit_country": bool(getattr(spec.filters, "country", None))}
        if spec.group_by and "country" in (spec.group_by or []):
            spec.filters.country = None

        return spec
    except Exception:
        return to_spec(question, memory)
