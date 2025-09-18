import os
import sys
import datetime as dt
import pandas as pd
from common import bls_fetch

DEFAULT_START = int(os.getenv("BLS_START_YEAR", "2000"))

CES_SUPERSECTORS = {
    "CES0500000001": "Total Private",
    "CES1000000001": "Mining and Logging",
    "CES2000000001": "Construction",
    "CES3000000001": "Manufacturing",
    "CES4000000001": "Trade, Transportation, and Utilities",
    "CES4200000001": "Wholesale Trade",
    "CES4300000001": "Retail Trade",
    "CES4400000001": "Transportation and Warehousing",
    "CES4800000001": "Leisure and Hospitality",
    "CES5500000001": "Financial Activities",
    "CES6000000001": "Professional and Business Services",
    "CES7000000001": "Education and Health Services",
    "CES8000000001": "Other Services",
    "CES9000000001": "Government",
    "CES5000000001": "Information",
}

def main():
    # Allow CLI override: python etl/bls_ces_ingest.py 2000 2025
    start_year = int(sys.argv[1]) if len(sys.argv) >= 2 else DEFAULT_START
    end_year = int(sys.argv[2]) if len(sys.argv) >= 3 else dt.date.today().year

    print(f"Fetching BLS CES supersectors {start_year} → {end_year} ...")
    series = bls_fetch(list(CES_SUPERSECTORS.keys()), start_year=start_year, end_year=end_year)
    recs = []
    for s in series:
        code = s["seriesID"]; name = CES_SUPERSECTORS.get(code, code)
        for item in s["data"]:
            y = int(item["year"]); m = int(item["period"][1:])
            recs.append({
                "period_date": dt.date(y, m, 1),
                "sector_code": code,
                "sector_name": name,
                "employment_thousands": pd.to_numeric(item["value"], errors="coerce"),
            })
    df = (pd.DataFrame(recs)
          .dropna()
          .drop_duplicates()
          .sort_values(["sector_name", "period_date"])
          .reset_index(drop=True))
    df.to_csv("bls_ces_supersectors.csv", index=False)
    print(f"Saved bls_ces_supersectors.csv ({df['period_date'].min()} → {df['period_date'].max()})")

if __name__ == "__main__":
    main()