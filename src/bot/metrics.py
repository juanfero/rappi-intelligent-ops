# src/bot/metrics.py
from __future__ import annotations
from typing import Dict, Tuple, Optional
from pathlib import Path
import yaml
import re

CATALOG_PATH = Path("catalog/metrics.yaml")

def _normalize(s: str) -> str:
    s = s.lower()
    s = re.sub(r"\s+", " ", s).strip()
    return s

def load_metric_catalog(path: Path = CATALOG_PATH) -> Dict:
    if not path.exists():
        # Fallback mínimo si el YAML no está (no rompe el bot)
        return {
            "version": 1,
            "metrics": {
                "orders": {"label": "Orders", "data_name": "Orders", "synonyms": ["orders","ordenes","órdenes","pedidos"],
                           "value_type": "count", "agg_default": "sum", "higher_is_better": True, "range_hint": None}
            }
        }
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    return data or {}

CAT = load_metric_catalog()

# Índices rápidos
_LABEL_BY_DATA = { m["data_name"]: m.get("label", m["data_name"])
                   for m in CAT.get("metrics", {}).values() }
_DATA_BY_LABEL = { m.get("label", k): m["data_name"]
                   for k, m in CAT.get("metrics", {}).items() }

# mapa de sinónimos → data_name
_SYNONYM_INDEX: Dict[str, str] = {}
for key, meta in CAT.get("metrics", {}).items():
    data_name = meta["data_name"]
    # añade el label y el data_name como sinónimos
    for w in set([meta.get("label", ""), data_name] + meta.get("synonyms", [])):
        if not w: 
            continue
        _S = _normalize(w)
        _SYNONYM_INDEX[_S] = data_name

def match_metric_from_catalog(utterance: str) -> Optional[Tuple[str, Dict]]:
    """
    Devuelve (data_name, props) si encuentra una métrica por label, data_name o sinónimos.
    """
    q = _normalize(utterance)
    # match por substring (tolerante)
    for syn, data_name in _SYNONYM_INDEX.items():
        if syn and syn in q:
            # devuelve las props completas del catálogo
            for meta in CAT.get("metrics", {}).values():
                if meta["data_name"] == data_name:
                    return data_name, meta
    return None

def props_for_metric(data_name: str) -> Dict:
    for meta in CAT.get("metrics", {}).values():
        if meta["data_name"] == data_name:
            return meta
    # fallback neutro
    return {"data_name": data_name, "value_type": "ratio", "agg_default": "mean", "higher_is_better": True, "range_hint": [0,1]}

def label_for_metric(data_name: str) -> str:
    return _LABEL_BY_DATA.get(data_name, data_name)
