# etl/load_to_db.py
import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

REQ = ["PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"]
missing = [k for k in REQ if not os.getenv(k)]
if missing:
    raise SystemExit(f"Missing .env variables: {', '.join(missing)}")

PGHOST = os.getenv("PGHOST").strip()
PGPORT = os.getenv("PGPORT", "5432").strip()
PGDATABASE = os.getenv("PGDATABASE").strip()
PGUSER = os.getenv("PGUSER").strip()
PGPASSWORD = os.getenv("PGPASSWORD").strip()

URL = (
    f"postgresql+psycopg2://{PGUSER}:{PGPASSWORD}"
    f"@{PGHOST}:{PGPORT}/{PGDATABASE}?sslmode=require"
)
engine = create_engine(URL, pool_pre_ping=True)

def assert_connect():
    with engine.connect() as c:
        c.execute(text("select 1"))
    print("✅ DB connection OK")

def upsert(table: str, df: pd.DataFrame, pkeys: list[str]):
    if df.empty:
        print(f"⚠️ {table}: no rows to load")
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
    print(f"✅ Upserted {len(df):,} rows into {table}")

def load_equities():
    path = "equities_monthly.csv"
    if not os.path.exists(path):
        print("⚠️ equities_monthly.csv not found; skipping")
        return

    df = pd.read_csv(path, parse_dates=["period_date"])

    # Coerce and drop bad rows (e.g., pre-inception months)
    df["adj_close"] = pd.to_numeric(df["adj_close"], errors="coerce")
    df["monthly_return"] = pd.to_numeric(df["monthly_return"], errors="coerce")
    before = len(df)
    df = df.dropna(subset=["adj_close"])  # adj_close must NOT be null per schema
    after = len(df)
    if after < before:
        print(f"ℹ️ Dropped {before - after} rows with null adj_close (pre-inception gaps).")

    # Types
    df["adj_close"] = df["adj_close"].astype(float)
    # monthly_return can be NaN for first month per ticker — that's OK (column is nullable)

    print("Preview equities (cleaned):\n", df.head())
    upsert("equities_monthly", df, ["period_date","ticker"])
    
def load_unemployment():
    p = "bls_unemployment.csv"
    if not os.path.exists(p):
        print("⚠️ bls_unemployment.csv not found; skipping")
        return
    df = pd.read_csv(p, parse_dates=["period_date"])
    if "unemployment_rate" not in df.columns and "value" in df.columns:
        df = df.rename(columns={"value": "unemployment_rate"})
    df = df[["period_date","unemployment_rate"]]
    upsert("unemployment_headline", df, ["period_date"])

def load_ces():
    p = "bls_ces_supersectors.csv"
    if not os.path.exists(p):
        print("⚠️ bls_ces_supersectors.csv not found; skipping")
        return
    df = pd.read_csv(p, parse_dates=["period_date"])
    df = df[["period_date","sector_code","sector_name","employment_thousands"]]
    upsert("employment_sector", df, ["period_date","sector_code"])

def main():
    print(f"Connecting to {PGUSER}@{PGHOST}:{PGPORT}/{PGDATABASE} … (SSL required)")
    assert_connect()
    load_equities()
    load_unemployment()
    load_ces()
    print("🎉 Done")

if __name__ == "__main__":
    main()