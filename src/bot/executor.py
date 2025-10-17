# src/bot/executor.py
from typing import Dict, Any, List, Tuple, Optional
import re
import os
import duckdb
from .schema import AnalyticsSpec

# Ruta por defecto a la base DuckDB
DUCK = "data/processed/warehouse.duckdb"


# -----------------------------
# Helpers de tiempo y filtros
# -----------------------------
def _offset_bounds(range_str: Optional[str]) -> Tuple[int, int]:
    """
    Convierte 'LkW-L0W' o 'L0W' a límites (lo, hi) de WEEK_OFFSET.
    Semántica:
      - L0W          -> (0, 0)  (solo semana actual)
      - L8W-L0W      -> (0, 7)  (últimas 8 semanas: offsets 0..7)
      - L5W-L0W      -> (0, 4)
    Fallback seguro: (0, 0)
    """
    if not range_str:
        return (0, 0)
    r = range_str.strip().upper()
    if r == "L0W":
        return (0, 0)
    m = re.fullmatch(r"L(\d+)W-L0W", r)
    if m:
        n = int(m.group(1))
        return (0, max(n - 1, 0))
    # Soporta también 'LkW-LmW' genérico si algún día lo usas (opcional)
    m2 = re.fullmatch(r"L(\d+)W-L(\d+)W", r)
    if m2:
        a, b = int(m2.group(1)), int(m2.group(2))
        lo, hi = min(a, b), max(a, b)
        return (lo, hi)
    return (0, 0)


def _pretty_range(r: Optional[str]) -> str:
    r = (r or "").strip().upper()
    if r == "L0W":
        return "Week 0 (actual)"
    m = re.fullmatch(r"L(\d+)W-L0W", r)
    if m:
        return f"Últimas {int(m.group(1))} semanas"
    return r or "L0W"


def _filters_where(f: Dict[str, Any]) -> str:
    """
    Construye condiciones AND ...; robusto a ZONE_TYPE con/sin guion.
    """
    w = ""
    if f.get("country"):
        w += f" AND UPPER(COUNTRY)=UPPER('{f['country']}')"
    if f.get("city"):
        w += f" AND UPPER(CITY)=UPPER('{f['city']}')"
    if f.get("zone"):
        w += f" AND UPPER(ZONE)=UPPER('{f['zone']}')"
    if f.get("zone_type"):
        zt = f["zone_type"].upper().replace("-", " ").strip()
        w += f" AND REPLACE(UPPER(ZONE_TYPE),'-',' ') = '{zt}'"
    # Si más adelante soportas otras dimensiones filtrables, añádelas aquí.
    return w


def _metric_where(metrics: List[str]) -> str:
    # Compara en minúsculas contra METRIC textual
    safe = ", ".join([f"LOWER('{m}')" for m in metrics])
    return f"AND LOWER(METRIC) IN ({safe})"


_ALLOWED_DIMS = {
    "country": "COUNTRY",
    "city": "CITY",
    "zone": "ZONE",
    "zone_type": "ZONE_TYPE",
    "zone_prioritization": "ZONE_PRIORITIZATION",
}


def _safe_dim(group_by: Optional[List[str]], default: str = "zone_type") -> str:
    """
    Devuelve una dimensión segura (columna) para GROUP BY/SELECT,
    tomando el primer elemento de group_by si es válido.
    """
    if group_by:
        cand = (group_by[0] or "").strip().lower()
        if cand in _ALLOWED_DIMS:
            return _ALLOWED_DIMS[cand]
    return _ALLOWED_DIMS.get(default, "ZONE_TYPE")


def _safe_group_cols(group_by: Optional[List[str]], fallback: List[str]) -> List[str]:
    if not group_by:
        return [c.upper() for c in fallback]
    cols = []
    for g in group_by:
        key = (g or "").strip().lower()
        cols.append(_ALLOWED_DIMS.get(key, key.upper()))
    return cols


# -----------------------------
# Ejecutor principal
# -----------------------------
def execute(spec: AnalyticsSpec) -> Dict[str, Any]:
    con = duckdb.connect(DUCK, read_only=True)
    out: Dict[str, Any] = {"visualization": spec.visualization, "suggestions": []}

    # Flag de debug (para exponer SQL)
    dbg = bool(getattr(spec.ops, "explain", False) or os.getenv("ENV") == "dev")

    # Métricas a usar en WHERE (multivariable requiere ambas)
    if spec.task == "multivariable":
        metrics_for_where = ["Lead Penetration", "Perfect Orders"]
    else:
        metrics_for_where = spec.metrics

    # WHERE base (filtros + métricas + tiempo + nulos fuera)
    lo, hi = _offset_bounds(spec.time.range)
    where = (
        "WHERE 1=1 "
        + _filters_where(spec.filters.model_dump())
        + " "
        + _metric_where(metrics_for_where)
        + " AND VALUE IS NOT NULL"
        + f" AND WEEK_OFFSET BETWEEN {lo} AND {hi}"
    )

    # Label de tiempo legible
    time_label = _pretty_range(spec.time.range)

    # ---------------- filter ----------------
    if spec.task == "filter":
        # Si el rango es L0W se lista la semana actual; si es una ventana, promediamos por zona en la ventana.
        if lo == hi == 0:
            sql = f"""
            SELECT COUNTRY, CITY, ZONE, ZONE_TYPE, METRIC, VALUE
            FROM zone_weekly_metrics
            {where}
            ORDER BY VALUE {"DESC" if (spec.ops.order or 'desc').lower() != "asc" else "ASC"}
            {f"LIMIT {spec.ops.top_k}" if spec.ops.top_k else ""}
            """
            if dbg:
                out["debug_sql"] = sql
            df = con.execute(sql).pl()
            out.update(
                title=f"Top {spec.ops.top_k or ''} zonas — {spec.metrics[0]} ({time_label})",
                data=df.to_dicts(),
                suggestions=["¿Ver tendencia 8 semanas?", "¿Comparar con semana pasada?", "¿Exportar a CSV?"],
            )
        else:
            # promedio por zona en la ventana temporal
            sql = f"""
            WITH base AS (
              SELECT COUNTRY, CITY, ZONE, ZONE_TYPE, METRIC, VALUE
              FROM zone_weekly_metrics
              {where}
            ),
            agg AS (
              SELECT COUNTRY, CITY, ZONE, ZONE_TYPE, METRIC,
                     AVG(VALUE) AS value
              FROM base
              GROUP BY COUNTRY, CITY, ZONE, ZONE_TYPE, METRIC
            )
            SELECT COUNTRY, CITY, ZONE, ZONE_TYPE, METRIC, value
            FROM agg
            ORDER BY value {"DESC" if (spec.ops.order or 'desc').lower() != "asc" else "ASC"}
            {f"LIMIT {spec.ops.top_k}" if spec.ops.top_k else ""}
            """
            if dbg:
                out["debug_sql"] = sql
            df = con.execute(sql).pl()
            out.update(
                title=f"Top {spec.ops.top_k or ''} zonas — {spec.metrics[0]} ({time_label}, promedio ventana)",
                data=df.to_dicts(),
                suggestions=["¿Ver semana a semana?", "¿Cambiar a mediana?", "¿Exportar a CSV?"],
            )
        con.close()
        return out

    # ---------------- compare ----------------
    if spec.task == "compare":
        # Compara por la primera dimensión indicada en group_by (fallback: ZONE_TYPE)
        dim_col = _safe_dim(spec.group_by, default="zone_type")
        sql = f"""
        WITH base AS (
          SELECT {dim_col} AS grp, VALUE
          FROM zone_weekly_metrics
          {where}
        )
        SELECT grp,
               AVG(VALUE) AS value,
               COUNT(*)    AS n_rows
        FROM base
        GROUP BY grp
        ORDER BY value DESC
        """
        if dbg:
            out["debug_sql"] = sql
        df = con.execute(sql).pl()
        dim_name = spec.group_by[0] if (spec.group_by and len(spec.group_by) > 0) else "ZONE_TYPE"
        out.update(
            title=f"Comparación {spec.metrics[0]} por {dim_name.upper()} ({time_label})",
            data=df.to_dicts(),
            suggestions=["¿Desglosar por city?", "¿Ver distribución por segmento?"],
        )
        con.close()
        return out

    # ---------------- trend ----------------
    if spec.task == "trend":
        # Serie temporal: agregamos por WEEK_OFFSET (y filtros ya aplicados)
        sql = f"""
        WITH base AS (
          SELECT WEEK_OFFSET, VALUE
          FROM zone_weekly_metrics
          {where}
        )
        SELECT WEEK_OFFSET AS week, AVG(VALUE) AS value
        FROM base
        GROUP BY WEEK_OFFSET
        ORDER BY week
        """
        if dbg:
            out["debug_sql"] = sql
        df = con.execute(sql).pl()
        out.update(
            title=f"Evolución {spec.metrics[0]} ({time_label})",
            data=df.to_dicts(),
            suggestions=["¿Calcular pendiente y R²?", "¿Resaltar 3 caídas/altas consecutivas?"],
        )
        con.close()
        return out

    # ---------------- aggregate ----------------
    if spec.task == "aggregate":
        group = _safe_group_cols(spec.group_by, fallback=["country"])
        g = ", ".join(group)
        agg = (spec.ops.agg or "mean").lower()
        agg_sql = "AVG" if agg == "mean" else agg.upper()
        sql = f"""
        WITH base AS (
          SELECT {g}, VALUE
          FROM zone_weekly_metrics
          {where}
        )
        SELECT {g.replace(", ", ", ")} AS grp, {agg_sql}(VALUE) AS value
        FROM base
        GROUP BY {g}
        ORDER BY value DESC
        """
        if dbg:
            out["debug_sql"] = sql
        df = con.execute(sql).pl()
        out.update(
            title=f"{agg.capitalize()} de {spec.metrics[0]} por {', '.join(spec.group_by or ['country'])} ({time_label})",
            data=df.to_dicts(),
            suggestions=["¿Ver evolución por país 8 semanas?", "¿Top/bottom 5 países?"],
        )
        con.close()
        return out

    # ---------------- multivariable ----------------
    if spec.task == "multivariable":
        # Alto LP (>= p70) y Bajo PO (<= p30) por país, usando promedio en ventana si lo>0
        sql = f"""
        WITH base AS (
          SELECT COUNTRY, CITY, ZONE, WEEK_OFFSET, METRIC, VALUE
          FROM zone_weekly_metrics
          {where} AND METRIC IN ('Lead Penetration','Perfect Orders')
        ),
        wide AS (
          SELECT
            COUNTRY, CITY, ZONE,
            AVG(CASE WHEN METRIC='Lead Penetration' THEN VALUE END) AS LP,
            AVG(CASE WHEN METRIC='Perfect Orders'      THEN VALUE END) AS PO
          FROM base
          GROUP BY COUNTRY, CITY, ZONE
        ),
        stats AS (
          SELECT
            COUNTRY,
            quantile_cont(LP, 0.70) OVER (PARTITION BY COUNTRY) AS p70_lp,
            quantile_cont(PO, 0.30) OVER (PARTITION BY COUNTRY) AS p30_po,
            COUNTRY AS c
          FROM wide
        )
        SELECT w.COUNTRY, w.CITY, w.ZONE, w.LP, w.PO
        FROM wide w
        JOIN stats s ON w.COUNTRY = s.c
        WHERE w.LP IS NOT NULL AND w.PO IS NOT NULL
          AND w.LP >= s.p70_lp AND w.PO <= s.p30_po
        ORDER BY w.LP DESC, w.PO ASC
        """
        if dbg:
            out["debug_sql"] = sql
        df = con.execute(sql).pl()
        out.update(
            title=f"Zonas con ALTO LP y BAJO PO ({time_label})",
            data=df.to_dicts(),
            suggestions=["¿Ver peer group?", "¿Tendencia 8 semanas?"],
        )
        con.close()
        return out

    # ---------------- inference ----------------
    if spec.task == "inference":
        # Usa el rango pedido para la pendiente (slope) de ORDERS
        lo_o, hi_o = _offset_bounds(spec.time.range or "L5W-L0W")
        sql_slope = f"""
        WITH base AS (
          SELECT COUNTRY, CITY, ZONE, WEEK_OFFSET, ORDERS
          FROM zone_weekly_orders
          WHERE 1=1 {_filters_where(spec.filters.model_dump())}
            AND WEEK_OFFSET BETWEEN {lo_o} AND {hi_o}
        ),
        with_x AS (
          SELECT COUNTRY, CITY, ZONE, WEEK_OFFSET, ORDERS,
                 ROW_NUMBER() OVER (PARTITION BY COUNTRY, CITY, ZONE ORDER BY WEEK_OFFSET) - 1 AS x
          FROM base
        ),
        stats AS (
          SELECT COUNTRY, CITY, ZONE,
                 covar_samp(ORDERS, x) / NULLIF(var_samp(x), 0) AS slope
          FROM with_x
          GROUP BY COUNTRY, CITY, ZONE
        )
        SELECT * FROM stats
        ORDER BY slope DESC
        LIMIT {spec.ops.top_k or 10}
        """
        if dbg:
            out["debug_sql"] = sql_slope
        top_df = con.execute(sql_slope).pl()

        # Correlaciones con métricas operativas en el MISMO rango
        out_rows = []
        for r in top_df.iter_rows(named=True):
            country, city, zone = r["COUNTRY"], r["CITY"], r["ZONE"]
            corr_sql = f"""
            WITH m AS (
              SELECT WEEK_OFFSET, METRIC, VALUE
              FROM zone_weekly_metrics
              WHERE UPPER(COUNTRY)=UPPER('{country}')
                AND UPPER(CITY)=UPPER('{city}')
                AND UPPER(ZONE)=UPPER('{zone}')
                AND WEEK_OFFSET BETWEEN {lo_o} AND {hi_o}
                AND VALUE IS NOT NULL
                AND METRIC IN ('Lead Penetration','Perfect Orders','Gross Profit UE')
            ),
            wide AS (
              SELECT WEEK_OFFSET,
                     MAX(CASE WHEN METRIC='Lead Penetration' THEN VALUE END) AS LP,
                     MAX(CASE WHEN METRIC='Perfect Orders'   THEN VALUE END) AS PO,
                     MAX(CASE WHEN METRIC='Gross Profit UE'  THEN VALUE END) AS GP
              FROM m GROUP BY WEEK_OFFSET
            )
            SELECT
              corr(LP, PO) AS corr_lp_po,
              corr(LP, GP) AS corr_lp_gp,
              corr(PO, GP) AS corr_po_gp
            FROM wide
            """
            corr = con.execute(corr_sql).pl().to_dicts()[0]
            out_rows.append({**r, **corr})

        out.update(
            title=f"Zonas que más crecen en Órdenes ({time_label}) + correlaciones",
            data=out_rows,
            suggestions=["¿Drivers por peer group?", "¿Playbook por zona?"],
        )
        con.close()
        return out

    # ---------------- contextual ----------------
    if spec.task == "contextual":
        # Caso intencionalmente WoW (semana actual vs anterior)
        sql = f"""
        WITH cur AS (
          SELECT COUNTRY, CITY, ZONE, METRIC, VALUE
          FROM zone_weekly_metrics
          { _filters_where(spec.filters.model_dump()) } AND METRIC IN ('Lead Penetration','Perfect Orders','Gross Profit UE')
            AND VALUE IS NOT NULL
            AND WEEK_OFFSET = 0
        ),
        prev AS (
          SELECT COUNTRY, CITY, ZONE, METRIC, VALUE AS prev_value
          FROM zone_weekly_metrics
          { _filters_where(spec.filters.model_dump()) } AND METRIC IN ('Lead Penetration','Perfect Orders','Gross Profit UE')
            AND VALUE IS NOT NULL
            AND WEEK_OFFSET = 1
        ),
        joined AS (
          SELECT c.COUNTRY, c.CITY, c.ZONE, c.METRIC, c.VALUE, p.prev_value,
                 CASE WHEN p.prev_value IS NULL OR p.prev_value=0 THEN NULL
                      ELSE (c.VALUE - p.prev_value)/p.prev_value END AS pct_change
          FROM cur c LEFT JOIN prev p USING (COUNTRY, CITY, ZONE, METRIC)
        )
        SELECT *
        FROM joined
        WHERE (METRIC='Perfect Orders' AND VALUE < 0.85)
           OR (pct_change IS NOT NULL AND pct_change < -0.1)
        ORDER BY METRIC, VALUE ASC
        """
        if dbg:
            out["debug_sql"] = sql
        df = con.execute(sql).pl()
        out.update(
            title="Zonas problemáticas (PO bajo o caída >10% WoW)",
            data=df.to_dicts(),
            suggestions=["¿Priorizar por país/ciudad?", "¿Recomendaciones por tipo de problema?"],
        )
        con.close()
        return out

    con.close()
    return {"error": f"Tarea no implementada: {spec.task}"}
