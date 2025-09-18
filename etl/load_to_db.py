import os
import pandas as pd
from common import get_engine, assert_connect, upsert

def load_equities(engine):
    path = "equities_monthly.csv"
    if not os.path.exists(path):
        print("equities_monthly.csv not found; skipping")
        return
    df = pd.read_csv(path, parse_dates=["period_date"])
    # enforce numeric types and drop bad rows
    df["adj_close"] = pd.to_numeric(df["adj_close"], errors="coerce")
    df["monthly_return"] = pd.to_numeric(df["monthly_return"], errors="coerce")
    df = df.dropna(subset=["adj_close"])
    upsert(engine, "equities_monthly", df, ["period_date", "ticker"])

def load_unemployment(engine):
    path = "bls_unemployment.csv"
    if not os.path.exists(path):
        print("bls_unemployment.csv not found; skipping")
        return
    df = pd.read_csv(path, parse_dates=["period_date"])
    if "unemployment_rate" not in df and "value" in df.columns:
        df = df.rename(columns={"value": "unemployment_rate"})
    df = df[["period_date", "unemployment_rate"]]
    df["unemployment_rate"] = pd.to_numeric(df["unemployment_rate"], errors="coerce")
    df = df.dropna(subset=["unemployment_rate"])
    upsert(engine, "unemployment_headline", df, ["period_date"])

def load_ces(engine):
    path = "bls_ces_supersectors.csv"
    if not os.path.exists(path):
        print("bls_ces_supersectors.csv not found; skipping")
        return
    df = pd.read_csv(path, parse_dates=["period_date"])
    df = df[["period_date", "sector_code", "sector_name", "employment_thousands"]]
    df["employment_thousands"] = pd.to_numeric(df["employment_thousands"], errors="coerce")
    df = df.dropna(subset=["employment_thousands"])
    upsert(engine, "employment_sector", df, ["period_date", "sector_code"])

def main():
    engine = get_engine()
    assert_connect(engine)
    load_equities(engine)
    load_unemployment(engine)
    load_ces(engine)
    print("Done")

if __name__ == "__main__":
    main()