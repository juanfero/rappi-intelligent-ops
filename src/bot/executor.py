from typing import Dict, Any, List
import duckdb
from .schema import AnalyticsSpec

DUCK = "data/processed/warehouse.duckdb"

def _time_where(r: str) -> str:
    # Acepta "L8W-L0W" o "L0W"
    r = r.strip().upper()
    if "-" in r:
        a, b = r.split("-")
        ai = int(a.replace("L", "").replace("W", ""))
        bi = int(b.replace("L", "").replace("W", ""))
        lo, hi = min(ai, bi), max(ai, bi)
        return f"AND WEEK_OFFSET BETWEEN {lo} AND {hi}"
    if r.startswith("L") and r.endswith("W"):
        w = int(r[1:-1])
        return f"AND WEEK_OFFSET = {w}"
    return ""

def _filters_where(f: Dict[str, Any]) -> str:
    w = ""
    if f.get("country"):
        w += f" AND UPPER(COUNTRY)=UPPER('{f['country']}')"
    if f.get("city"):
        w += f" AND UPPER(CITY)=UPPER('{f['city']}')"
    if f.get("zone"):
        w += f" AND UPPER(ZONE)=UPPER('{f['zone']}')"
    if f.get("zone_type"):
        w += f" AND UPPER(ZONE_TYPE)=UPPER('{f['zone_type']}')"
    return w

def _metric_where(metrics: List[str]) -> str:
    # compara en minúsculas
    safe = ", ".join([f"LOWER('{m}')" for m in metrics])
    return f"AND LOWER(METRIC) IN ({safe})"


def execute(spec: AnalyticsSpec) -> Dict[str, Any]:
    con = duckdb.connect(DUCK, read_only=True)
    out: Dict[str, Any] = {"visualization": spec.visualization, "suggestions": []}

    # Para multivariable necesitamos LP y PO
    if spec.task == "multivariable":
        metrics_for_where = ["Lead Penetration", "Perfect Orders"]
    else:
        metrics_for_where = spec.metrics

    where = (
        "WHERE 1=1 "
        + _filters_where(spec.filters.model_dump())
        + " "
        + _metric_where(metrics_for_where)
        + " "
        + _time_where(spec.time.range)
    )



    if spec.task == "filter":
        sql = f"""
        SELECT COUNTRY, CITY, ZONE, METRIC, VALUE
        FROM zone_weekly_metrics
        {where} AND WEEK_OFFSET = 0
        ORDER BY VALUE {"DESC" if spec.ops.order != "asc" else "ASC"}
        {f"LIMIT {spec.ops.top_k}" if spec.ops.top_k else ""}
        """
        df = con.execute(sql).pl()
        out.update(
            title=f"Top {spec.ops.top_k or ''} zonas — {spec.metrics[0]} (L0W)",
            data=df.to_dicts(),
            suggestions=["¿Ver tendencia 8 semanas?", "¿Comparar con semana pasada?", "¿Exportar a CSV?"],
        )
        con.close()
        return out

    if spec.task == "compare":
        sql = f"""
        SELECT ZONE_TYPE AS grp, AVG(VALUE) AS value, COUNT(DISTINCT ZONE) AS n_zones
        FROM zone_weekly_metrics
        {where} AND WEEK_OFFSET = 0
        GROUP BY ZONE_TYPE
        ORDER BY value DESC
        """
        df = con.execute(sql).pl()
        out.update(
            title=f"Comparación {spec.metrics[0]} por ZONE_TYPE (L0W)",
            data=df.to_dicts(),
            suggestions=["¿Desglosar por city?", "¿Ver distribución por segmento?"],
        )
        con.close()
        return out

    if spec.task == "trend":
        sql = f"""
        SELECT COUNTRY, CITY, ZONE, METRIC, WEEK_OFFSET, VALUE
        FROM zone_weekly_metrics
        {where}
        ORDER BY WEEK_OFFSET
        """
        df = con.execute(sql).pl()
        out.update(
            title=f"Evolución {spec.metrics[0]} ({spec.time.range})",
            data=df.to_dicts(),
            suggestions=["¿Calcular pendiente y R²?", "¿Resaltar 3 caídas/altas consecutivas?"],
        )
        con.close()
        return out

    if spec.task == "aggregate":
        group = spec.group_by or ["country"]
        g = ", ".join(c.upper() for c in group)
        agg = spec.ops.agg or "mean"
        agg_sql = "AVG" if agg == "mean" else agg.upper()
        sql = f"""
        SELECT {g} AS grp, {agg_sql}(VALUE) AS value
        FROM zone_weekly_metrics
        {where} AND WEEK_OFFSET = 0
        GROUP BY {g}
        ORDER BY value DESC
        """
        df = con.execute(sql).pl()
        out.update(
            title=f"{agg.capitalize()} de {spec.metrics[0]} por {', '.join(group)} (L0W)",
            data=df.to_dicts(),
            suggestions=["¿Ver evolución por país 8 semanas?", "¿Top/bottom 5 países?"],
        )
        con.close()
        return out

    if spec.task == "multivariable":
        sql = f"""
        WITH cur AS (
          SELECT COUNTRY, CITY, ZONE, METRIC, VALUE
          FROM zone_weekly_metrics {where} AND WEEK_OFFSET = 0
        ),
        lp AS (SELECT COUNTRY, CITY, ZONE, VALUE AS LP FROM cur WHERE METRIC = 'Lead Penetration'),
        po AS (SELECT COUNTRY, CITY, ZONE, VALUE AS PO FROM cur WHERE METRIC = 'Perfect Orders'),
        joined AS (SELECT lp.COUNTRY, lp.CITY, lp.ZONE, lp.LP, po.PO FROM lp JOIN po USING (COUNTRY, CITY, ZONE)),
        stats AS (
          SELECT COUNTRY,
                 quantile_cont(LP, 0.75) OVER (PARTITION BY COUNTRY) AS p75_lp,
                 quantile_cont(PO, 0.25) OVER (PARTITION BY COUNTRY) AS p25_po,
                 COUNTRY AS c
          FROM joined
        )
        SELECT j.COUNTRY, j.CITY, j.ZONE, j.LP, j.PO
        FROM joined j
        JOIN stats s ON j.COUNTRY = s.c
        WHERE j.LP >= s.p75_lp AND j.PO <= s.p25_po
        ORDER BY j.LP DESC
        """
        df = con.execute(sql).pl()
        out.update(
            title="Zonas con ALTO LP y BAJO PO (L0W)",
            data=df.to_dicts(),
            suggestions=["¿Ver peer group?", "¿Tendencia 8 semanas?"],
        )
        con.close()
        return out

    if spec.task == "inference":
        sql_slope = f"""
        WITH base AS (
          SELECT COUNTRY, CITY, ZONE, WEEK_OFFSET, ORDERS
          FROM zone_weekly_orders
          WHERE 1=1 {_filters_where(spec.filters.model_dump())}
            AND WEEK_OFFSET BETWEEN 0 AND 5
        ),
        with_x AS (
          SELECT COUNTRY, CITY, ZONE, WEEK_OFFSET, ORDERS,
                 ROW_NUMBER() OVER (PARTITION BY COUNTRY, CITY, ZONE ORDER BY WEEK_OFFSET) - 1 AS x
          FROM base
        ),
        stats AS (
          SELECT COUNTRY, CITY, ZONE,
                 covar_samp(ORDERS, x) / var_samp(x) AS slope
          FROM with_x
          GROUP BY COUNTRY, CITY, ZONE
        )
        SELECT * FROM stats
        ORDER BY slope DESC
        LIMIT {spec.ops.top_k or 10}
        """
        top_df = con.execute(sql_slope).pl()

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
                AND WEEK_OFFSET BETWEEN 0 AND 5
                AND METRIC IN ('Lead Penetration','Perfect Orders','Gross Profit UE')
            ),
            wide AS (
              SELECT WEEK_OFFSET,
                     MAX(CASE WHEN METRIC='Lead Penetration' THEN VALUE END) AS LP,
                     MAX(CASE WHEN METRIC='Perfect Orders' THEN VALUE END) AS PO,
                     MAX(CASE WHEN METRIC='Gross Profit UE' THEN VALUE END) AS GP
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
            title="Zonas que más crecen en Órdenes (5 semanas) + correlaciones",
            data=out_rows,
            suggestions=["¿Drivers por peer group?", "¿Playbook por zona?"],
        )
        con.close()
        return out

    if spec.task == "contextual":
        sql = f"""
        WITH cur AS (
          SELECT COUNTRY, CITY, ZONE, METRIC, VALUE
          FROM zone_weekly_metrics {where} AND WEEK_OFFSET = 0
        ),
        prev AS (
          SELECT COUNTRY, CITY, ZONE, METRIC, VALUE AS prev_value
          FROM zone_weekly_metrics {where} AND WEEK_OFFSET = 1
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
