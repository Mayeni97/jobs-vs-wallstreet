import os
from pathlib import Path
import datetime as dt
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# --- Load .env from repo root (.. relative to this file) ---
REPO_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = REPO_ROOT / ".env"
load_dotenv(dotenv_path=ENV_PATH)

# ---------------- BLS ----------------
BLS_API_KEY = os.getenv("BLS_API_KEY")  # optional

def bls_fetch(series_ids, start_year=2000, end_year=None, timeout=60):
    if end_year is None:
        end_year = dt.date.today().year
    payload = {"seriesid": list(series_ids), "startyear": str(start_year), "endyear": str(end_year)}
    if BLS_API_KEY:
        payload["registrationkey"] = BLS_API_KEY
    r = requests.post("https://api.bls.gov/publicAPI/v2/timeseries/data/", json=payload, timeout=timeout)
    r.raise_for_status()
    j = r.json()
    if j.get("status") != "REQUEST_SUCCEEDED":
        raise RuntimeError(f"BLS API error: {j}")
    return j["Results"]["series"]

# ---------------- DB ----------------
REQ_VARS = ["PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"]

def _build_url_from_pg_env() -> str:
    host = os.getenv("PGHOST", "").strip()
    port = os.getenv("PGPORT", "5432").strip()
    db   = os.getenv("PGDATABASE", "").strip()
    user = os.getenv("PGUSER", "").strip()
    pwd  = os.getenv("PGPASSWORD", "").strip()
    missing = [k for k in REQ_VARS if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing DB env vars: {', '.join(missing)}")
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}?sslmode=require"

def get_engine():
    # Prefer DATABASE_URL if provided; otherwise use discrete PG* vars
    url = os.getenv("DATABASE_URL", "").strip()
    if url:
        # ensure sslmode=require
        if "sslmode=" not in url:
            url += ("&" if "?" in url else "?") + "sslmode=require"
    else:
        url = _build_url_from_pg_env()
    return create_engine(url, pool_pre_ping=True)

def assert_connect(engine=None) -> None:
    eng = engine or get_engine()
    with eng.connect() as c:
        c.execute(text("select 1"))

def upsert(engine, table: str, df: pd.DataFrame, pkeys: list[str]) -> None:
    if df.empty:
        print(f"{table}: no rows to load")
        return
    with engine.begin() as conn:
        df.to_sql("_staging", conn, if_exists="replace", index=False)
        cols = list(df.columns)
        set_cols = [c for c in cols if c not in pkeys]
        set_clause = ", ".join([f"{c}=EXCLUDED.{c}" for c in set_cols]) or "nothing"
        collist = ", ".join(cols)
        pkeylist = ", ".join(pkeys)
        conn.execute(text(f"""
            INSERT INTO {table} ({collist})
            SELECT {collist} FROM _staging
            ON CONFLICT ({pkeylist}) DO UPDATE SET {set_clause};
        """))
        conn.execute(text("DROP TABLE _staging"))
    print(f"Upserted {len(df)} rows into {table}")