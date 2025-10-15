# src/insights/engine.py
from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional
from pathlib import Path
import duckdb
import pandas as pd
import numpy as np
from scipy.stats import linregress, spearmanr

from .config import (
    METRIC_POLARITY, ANOMALY_WOW_THRESHOLD, TREND_MIN_RUN, TREND_MIN_R2,
    BENCHMARK_Z_ABS, CORR_MIN_ABS, MIN_POINTS_TIME, TOP_N, RECO_TEMPLATES
)

DUCK = "data/processed/warehouse.duckdb"

@dataclass
class Insight:
    category: str              # "anomaly" | "trend" | "benchmark" | "correlation" | "opportunity"
    country: Optional[str]
    city: Optional[str]
    zone: Optional[str]
    metric: Optional[str]
    title: str
    summary: str               # 1-2 frases, con números
    severity: float            # 0-1 (para ordenar)
    recommendation: str
    extra: Dict[str, Any]      # payload adicional

def _pct_change(cur: float, prev: float) -> Optional[float]:
    if prev is None or pd.isna(prev) or prev == 0:
        return None
    return (cur - prev) / abs(prev)

def _run_length(seq: List[float], direction: str) -> int:
    if len(seq) < 2:
        return 0
    run = 0
    for i in range(len(seq)-1, 0, -1):
        diff = seq[i] - seq[i-1]
        if direction == "down" and diff < 0: run += 1
        elif direction == "up" and diff > 0: run += 1
        else: break
    return run

def _severity_from_pct(p: float) -> float:
    return min(1.0, abs(p) / 0.20)  # 10% => 0.5 ; 20% => 1.0

def _severity_from_z(z: float) -> float:
    return min(1.0, abs(z) / 3.0)

def _severity_from_slope(slope: float, scale: float) -> float:
    if scale <= 0:
        return min(1.0, abs(slope))
    return min(1.0, abs(slope) / (0.5 * scale))

def _open_con():
    if not Path(DUCK).exists():
        raise RuntimeError("No existe data/processed/warehouse.duckdb. Corre src/data/prepare_data.py")
    con = duckdb.connect(DUCK, read_only=True)
    con.execute("LOAD parquet;")
    return con

def _fetch_current_prev(con) -> pd.DataFrame:
    sql = """
    WITH cur AS (
      SELECT COUNTRY, CITY, ZONE, METRIC, VALUE
      FROM zone_weekly_metrics WHERE WEEK_OFFSET=0
    ),
    prev AS (
      SELECT COUNTRY, CITY, ZONE, METRIC, VALUE AS prev_value
      FROM zone_weekly_metrics WHERE WEEK_OFFSET=1
    )
    SELECT c.COUNTRY, c.CITY, c.ZONE, c.METRIC, c.VALUE, p.prev_value
    FROM cur c LEFT JOIN prev p
    USING (COUNTRY, CITY, ZONE, METRIC)
    """
    return con.execute(sql).df()

def detect_anomalies(con) -> List[Insight]:
    df = _fetch_current_prev(con)
    out: List[Insight] = []
    for (country, city, zone, metric), g in df.groupby(["COUNTRY","CITY","ZONE","METRIC"]):
        cur = g["VALUE"].iloc[0]
        prev = g["prev_value"].iloc[0]
        pct = _pct_change(cur, prev)
        if pct is None:
            continue
        if abs(pct) >= ANOMALY_WOW_THRESHOLD:
            pol = METRIC_POLARITY.get(metric, True)
            direction = "mejora" if pct > 0 else "deterioro"
            concerning = (pct < 0) if pol else (pct > 0)
            sev = _severity_from_pct(pct)
            title = f"{metric}: {direction} WoW de {pct:+.1%} en {zone} ({country})"
            reco_key = f"{metric.replace(' ','_')}_low" if concerning else "Benchmark_negative"
            recommendation = RECO_TEMPLATES.get(reco_key, "Profundizar causa raíz y plan de acción.")
            out.append(Insight(
                category="anomaly", country=country, city=city, zone=zone, metric=metric,
                title=title,
                summary=f"Semana actual vs anterior: {cur:.3f} vs {prev:.3f}. Umbral ±{ANOMALY_WOW_THRESHOLD:.0%}.",
                severity=sev, recommendation=recommendation,
                extra={"current": float(cur), "prev": float(prev), "pct_change": float(pct)}
            ))
    return sorted(out, key=lambda x: x.severity, reverse=True)

def detect_trends(con) -> List[Insight]:
    sql = """
    SELECT COUNTRY, CITY, ZONE, METRIC, WEEK_OFFSET, VALUE
    FROM zone_weekly_metrics
    WHERE WEEK_OFFSET BETWEEN 0 AND 8
    ORDER BY COUNTRY, CITY, ZONE, METRIC, WEEK_OFFSET
    """
    df = con.execute(sql).df()
    out: List[Insight] = []
    for (country, city, zone, metric), g in df.groupby(["COUNTRY","CITY","ZONE","METRIC"]):
        vals = g.sort_values("WEEK_OFFSET")["VALUE"].to_numpy(dtype=float)
        if len(vals) < MIN_POINTS_TIME:
            continue
        x = np.arange(len(vals))
        slope, _, r, _, _ = linregress(x, vals)
        run_down = _run_length(vals.tolist(), "down")
        pol = METRIC_POLARITY.get(metric, True)
        deterioro = (slope < 0 and pol) or (slope > 0 and not pol)
        r2 = r**2
        scale = float(np.nanstd(vals))
        sev = _severity_from_slope(slope, scale)
        if (deterioro and r2 >= TREND_MIN_R2) or (run_down >= TREND_MIN_RUN):
            title = f"{metric}: tendencia desfavorable en {zone} ({country})"
            summary = f"Pendiente: {slope:+.3f} (R²={r2:.2f}); {run_down} caídas consecutivas. Ventana: 8 semanas."
            recommendation = RECO_TEMPLATES.get(f"{metric.replace(' ','_')}_low",
                                                "Plan de recuperación con acciones semanales y monitoreo.")
            out.append(Insight(
                category="trend", country=country, city=city, zone=zone, metric=metric,
                title=title, summary=summary, severity=sev,
                recommendation=recommendation,
                extra={"slope": float(slope), "r2": float(r2), "runs_down": run_down}
            ))
    return sorted(out, key=lambda x: x.severity, reverse=True)

def detect_benchmarking(con) -> List[Insight]:
    # Chequea si existe ZONE_TYPE (si no, usa solo país)
    has_zone_type = False
    try:
        con.execute("SELECT ZONE_TYPE FROM zone_weekly_metrics WHERE WEEK_OFFSET=0 LIMIT 1").df()
        has_zone_type = True
    except Exception:
        has_zone_type = False

    group_cols = ["COUNTRY", "ZONE_TYPE"] if has_zone_type else ["COUNTRY"]
    sql = f"""
    SELECT {", ".join(group_cols)}, ZONE, METRIC, VALUE
    FROM zone_weekly_metrics
    WHERE WEEK_OFFSET=0
    """
    df = con.execute(sql).df()
    out: List[Insight] = []

    for (grp_vals, metric), g in df.groupby([tuple(group_cols), "METRIC"]):
        values = g["VALUE"].astype(float)
        if values.std(ddof=0) == 0:  # evitar div/0
            continue
        mean = float(values.mean())
        std = float(values.std(ddof=0)) + 1e-9
        g = g.copy()
        g["z"] = (values - mean) / std
        for _, row in g.iterrows():
            z_val = float(row["z"])
            if abs(z_val) >= BENCHMARK_Z_ABS:
                country = row["COUNTRY"]
                zone = row["ZONE"]
                title = f"{row['METRIC']}: desempeño {'alto' if z_val>0 else 'bajo'} vs pares en {zone} ({country})"
                summary = f"z-score={z_val:+.2f} respecto al peer group ({'país + tipo' if has_zone_type else 'país'})."
                reco = RECO_TEMPLATES.get("Benchmark_negative", "Auditar gaps vs pares y replicar buenas prácticas.")
                out.append(Insight(
                    category="benchmark", country=country, city=None, zone=zone, metric=row["METRIC"],
                    title=title, summary=summary, severity=_severity_from_z(z_val),
                    recommendation=reco,
                    extra={"z": z_val, "peer_group": "country+zone_type" if has_zone_type else "country"}
                ))
    return sorted(out, key=lambda x: x.severity, reverse=True)[:TOP_N * 2]

def detect_correlations(con) -> List[Insight]:
    """
    Calcula correlaciones por zona (serie temporal) entre pares de métricas que existan.
    Marca pares fuertes |ρ|>=CORR_MIN_ABS si hay puntos suficientes.
    """
    # Qué métricas hay
    metrics = con.execute("SELECT DISTINCT METRIC FROM zone_weekly_metrics").df()["METRIC"].tolist()
    metrics = [m for m in metrics if isinstance(m, str)]
    out: List[Insight] = []

    # Obtener todas las zonas
    zones = con.execute("SELECT DISTINCT COUNTRY, CITY, ZONE FROM zone_weekly_metrics").df()

    for _, zr in zones.iterrows():
        country, city, zone = zr["COUNTRY"], zr["CITY"], zr["ZONE"]

        # Construir la lista de columnas condicionales SIN f-strings anidados
        cols = []
        for m in metrics:
            safe_m = m.replace("'", "''")  # por si hay comillas simples en el nombre de la métrica
            # Usamos .format para evitar backslashes en expresiones de f-strings
            cols.append("MAX(CASE WHEN METRIC='{}' THEN VALUE END) AS \"{}\"".format(safe_m, m))
        cols_expr = ", ".join(cols)

        sql = f"""
        WITH m AS (
          SELECT WEEK_OFFSET, METRIC, VALUE
          FROM zone_weekly_metrics
          WHERE UPPER(COUNTRY)=UPPER('{country}')
            AND UPPER(CITY)=UPPER('{city}')
            AND UPPER(ZONE)=UPPER('{zone}')
            AND WEEK_OFFSET BETWEEN 0 AND 8
        )
        SELECT WEEK_OFFSET,
               {cols_expr}
        FROM m
        GROUP BY WEEK_OFFSET
        ORDER BY WEEK_OFFSET
        """
        wide = con.execute(sql).df()
        if len(wide) < MIN_POINTS_TIME:
            continue

        # Correlaciones por pares
        cols = [c for c in wide.columns if c != "WEEK_OFFSET"]
        for i in range(len(cols)):
            for j in range(i + 1, len(cols)):
                a, b = cols[i], cols[j]
                if wide[a].isna().all() or wide[b].isna().all():
                    continue
                try:
                    rho, _ = spearmanr(wide[a], wide[b], nan_policy='omit')
                except Exception:
                    continue
                if rho is not None and not np.isnan(rho) and abs(rho) >= CORR_MIN_ABS:
                    title = f"Correlación {a} ↔ {b} en {zone} ({country})"
                    summary = f"ρ={rho:+.2f} con ≥{len(wide)} puntos (8-9 semanas)."
                    reco_key = "Correlation_LP_PO" if set([a, b]) == set(["Lead Penetration", "Perfect Orders"]) else "Benchmark_negative"
                    recommendation = RECO_TEMPLATES.get(reco_key, "Explorar causalidades y plan conjunto para mejorar ambas.")
                    sev = min(1.0, (abs(rho) - CORR_MIN_ABS) / (1 - CORR_MIN_ABS + 1e-9))
                    out.append(Insight(
                        category="correlation",
                        country=country, city=city, zone=zone, metric=None,
                        title=title, summary=summary, severity=sev,
                        recommendation=recommendation,
                        extra={"rho": float(rho), "metrics": [a, b]}
                    ))

    # Top correlaciones
    out = sorted(out, key=lambda x: x.severity, reverse=True)[:TOP_N]
    return out


def detect_opportunities(con) -> List[Insight]:
    """
    Oportunidad: ORDERS creciendo (slope>0) + Perfect Orders bajo (<p40 país) en L0W
    Requiere vista zone_weekly_orders.
    """
    out: List[Insight] = []
    try:
        orders = con.execute("""
        SELECT COUNTRY, CITY, ZONE, WEEK_OFFSET, ORDERS
        FROM zone_weekly_orders
        WHERE WEEK_OFFSET BETWEEN 0 AND 5
        ORDER BY COUNTRY, CITY, ZONE, WEEK_OFFSET
        """).df()
    except Exception:
        return out  # si no existe la vista, no reportamos oportunidades

    for (country, city, zone), g in orders.groupby(["COUNTRY","CITY","ZONE"]):
        g = g.sort_values("WEEK_OFFSET")
        if len(g) < MIN_POINTS_TIME:
            continue
        x = np.arange(len(g))
        slope, *_ = linregress(x, g["ORDERS"].astype(float))
        if slope <= 0:
            continue

        po = con.execute("""
        SELECT COUNTRY, CITY, ZONE, VALUE
        FROM zone_weekly_metrics
        WHERE WEEK_OFFSET=0 AND METRIC='Perfect Orders'
        """).df()
        po_country = po[po["COUNTRY"].str.upper()==country.upper()]
        if po_country.empty: 
            continue
        p40 = float(po_country["VALUE"].quantile(0.40))

        po_zone = po[(po["COUNTRY"].str.upper()==country.upper()) &
                     (po["CITY"].str.upper()==city.upper()) &
                     (po["ZONE"].str.upper()==zone.upper())]
        if po_zone.empty:
            continue
        po_val = float(po_zone["VALUE"].iloc[0])

        if po_val < p40:
            sev = min(1.0, abs(slope) / (np.nanstd(orders["ORDERS"]) + 1e-9))
            out.append(Insight(
                category="opportunity", country=country, city=city, zone=zone, metric="Perfect Orders",
                title=f"Oportunidad: Órdenes creciendo pero PO bajo en {zone} ({country})",
                summary=f"Crecimiento ORDERS (slope={slope:+.2f}), PO actual={po_val:.2%} (<p40 país={p40:.2%}).",
                severity=sev,
                recommendation=RECO_TEMPLATES.get("Orders_growth_high_PO_low",
                                                  "Estabilizar operación (PO) para sostener crecimiento."),
                extra={"orders_slope": float(slope), "po": po_val, "po_p40": p40}
            ))
    return sorted(out, key=lambda x: x.severity, reverse=True)

def generate_insights(scope: Dict[str, Optional[str]] | None = None) -> Dict[str, Any]:
    """
    scope: filtros opcionales {"country": "CO", "city": "MEDELLIN", "zone": None}
    Nota: Hoy los detectores corren globalmente; si quieres, puedes pasar 'scope'
    a cada consulta DuckDB. Aquí solo robustecemos y evitamos fallos.
    """
    con = _open_con()

    def _safe(name, fn):
        try:
            return fn()
        except Exception as e:
            # Log suave a consola para que veas qué detector rompió y por qué,
            # pero nunca rompemos toda la respuesta.
            print(f"[insights:{name}] ERROR:", repr(e))
            return []

    anomalies = _safe("anomalies", lambda: detect_anomalies(con))[:TOP_N]
    trends    = _safe("trends",    lambda: detect_trends(con))[:TOP_N]
    bench     = _safe("benchmark", lambda: detect_benchmarking(con))[:TOP_N]
    corrs     = _safe("corrs",     lambda: detect_correlations(con))[:TOP_N]
    opps      = _safe("opps",      lambda: detect_opportunities(con))[:TOP_N]

    all_items = anomalies + trends + bench + corrs + opps
    executive = sorted(all_items, key=lambda x: x.severity, reverse=True)[:5]

    return {
        "executive_summary": [asdict(x) for x in executive],
        "anomalies": [asdict(x) for x in anomalies],
        "trends": [asdict(x) for x in trends],
        "benchmarking": [asdict(x) for x in bench],
        "correlations": [asdict(x) for x in corrs],
        "opportunities": [asdict(x) for x in opps],
        "meta": {"counts": {
            "executive": len(executive),
            "anomalies": len(anomalies),
            "trends": len(trends),
            "benchmarking": len(bench),
            "correlations": len(corrs),
            "opportunities": len(opps),
        }}
    }

