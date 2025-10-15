import duckdb
from src.config import DUCKDB_PATH

def get_conn():
    return duckdb.connect(str(DUCKDB_PATH), read_only=True)

