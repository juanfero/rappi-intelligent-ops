# src/insights/report.py
from __future__ import annotations
from typing import Dict, Any, List
from pathlib import Path
from datetime import datetime
import json

def _section_md(title: str) -> str:
    return f"\n## {title}\n"

def _list_md(items: List[Dict[str,Any]], limit=None) -> str:
    if not items:
        return "_Sin hallazgos relevantes._\n"
    md = ""
    for it in (items[:limit] if limit else items):
        md += f"- **{it['title']}** — {it['summary']}  \n"
        md += f"  *Recomendación:* {it['recommendation']}\n"
    return md

def to_markdown(payload: Dict[str,Any], title="Reporte de Insights — Rappi Intelligent Ops") -> str:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    md = f"# {title}\n\n_Generado: {ts}_\n"
    md += _section_md("Resumen Ejecutivo (Top 3–5)")
    md += _list_md(payload.get("executive_summary", []))

    md += _section_md("Anomalías (WoW)")
    md += _list_md(payload.get("anomalies", []))

    md += _section_md("Tendencias preocupantes (8 semanas)")
    md += _list_md(payload.get("trends", []))

    md += _section_md("Benchmarking (pares)")
    md += _list_md(payload.get("benchmarking", []))

    md += _section_md("Correlaciones entre métricas")
    md += _list_md(payload.get("correlations", []))

    md += _section_md("Oportunidades")
    md += _list_md(payload.get("opportunities", []))

    md += "\n---\n_Métricas y criterios: ±10% WoW, ≥3 corridas, z≥1.5, |ρ|≥0.5._\n"
    return md

def save_report(payload: Dict[str,Any], out_dir="reports", base_name="insights_report"):
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    md = to_markdown(payload)
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    md_path = Path(out_dir) / f"{base_name}_{ts}.md"
    html_path = Path(out_dir) / f"{base_name}_{ts}.html"
    json_path = Path(out_dir) / f"{base_name}_{ts}.json"

    with open(md_path, "w", encoding="utf-8") as f:
        f.write(md)

    # Construimos el body HTML por separado para evitar backslashes en expresiones de f-strings
    html_body = md.replace("\n", "<br/>\n")
    html = (
        "<!doctype html><html><head>"
        '<meta charset="utf-8"><title>Reporte Insights</title>'
        "<style>body{font-family:system-ui,Arial,sans-serif;max-width:960px;margin:2rem auto;line-height:1.5}"
        "h1,h2{margin-top:2rem}</style></head><body>"
        f"{html_body}"
        "</body></html>"
    )
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    return {"markdown": str(md_path), "html": str(html_path), "json": str(json_path)}
