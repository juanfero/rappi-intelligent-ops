from pathlib import Path
import duckdb
import polars as pl
import pandas as pd  # fallback para excel
from src.config import DATA_RAW, DATA_PROCESSED, DUCKDB_PATH

DATA_PROCESSED.mkdir(parents=True, exist_ok=True)

EXCEL_FILE = "metrics.xlsx"  # <-- cambia aquí si tu archivo se llama distinto

def _normalize_columns(df: pl.DataFrame) -> pl.DataFrame:
    mapping = {c: str(c).strip().lower().replace(" ", "_") for c in df.columns}
    return df.rename(mapping)

def read_source() -> pl.DataFrame:
    xlsx = DATA_RAW / EXCEL_FILE
    if not xlsx.exists():
        raise FileNotFoundError(f"No encuentro {xlsx}. Pon tu archivo en data/raw/")

    # 1) Intento con Polars (rápido). Puede devolver un dict si hay varias hojas.
    try:
        df_or_dict = pl.read_excel(xlsx, sheet_id=0)  # usa 0 (primera hoja)
        if isinstance(df_or_dict, dict):              # si devuelve dict, toma la primera hoja
            first_key = next(iter(df_or_dict))
            df = df_or_dict[first_key]
        else:
            df = df_or_dict
    except Exception:
        # 2) Fallback estable con Pandas + openpyxl → luego convertimos a Polars
        pdf = pd.read_excel(xlsx, sheet_name=0, engine="openpyxl")
        df = pl.from_pandas(pdf)

    df = _normalize_columns(df)
    return df

def persist(df: pl.DataFrame):
    pq = DATA_PROCESSED / "metrics.parquet"
    df.write_parquet(pq)

    con = duckdb.connect(str(DUCKDB_PATH))
    con.execute("CREATE SCHEMA IF NOT EXISTS ops;")
    con.execute("DROP TABLE IF EXISTS ops.metrics;")
    con.execute(
        "CREATE TABLE ops.metrics AS SELECT * FROM read_parquet(?)",
        [str(pq)],
    )
    con.close()
    return pq, DUCKDB_PATH

if __name__ == "__main__":
    df = read_source()
    pq, db = persist(df)
    print("OK parquet:", pq)
    print("OK duckdb:", db)
