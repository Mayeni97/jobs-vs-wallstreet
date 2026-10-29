import datetime as dt
import os
import sys

import pandas as pd

try:
    from .common import bls_fetch
except ImportError:  # Support running this file directly from the repo root.
    from common import bls_fetch

DEFAULT_START = int(os.getenv("BLS_START_YEAR", "2000"))

HEADLINE_SERIES = ["LNS14000000"]  # U-3 unemployment rate, SA, monthly


def main():
    # Allow CLI override: python etl/bls_ingest.py 2000 2025
    start_year = int(sys.argv[1]) if len(sys.argv) >= 2 else DEFAULT_START
    end_year = int(sys.argv[2]) if len(sys.argv) >= 3 else dt.date.today().year

    rows = []
    for window_start in range(start_year, end_year + 1, 10):
        window_end = min(window_start + 9, end_year)
        print(f"Fetching BLS headline unemployment {window_start} → {window_end} ...")
        series = bls_fetch(
            HEADLINE_SERIES, start_year=window_start, end_year=window_end
        )
        for s in series:
            for item in s["data"]:
                y = int(item["year"])
                m = int(item["period"][1:])
                rows.append(
                    {
                        "period_date": dt.date(y, m, 1),
                        "unemployment_rate": pd.to_numeric(
                            item["value"], errors="coerce"
                        ),
                    }
                )
    df = (
        pd.DataFrame(rows)
        .dropna()
        .drop_duplicates()
        .sort_values("period_date")
        .reset_index(drop=True)
    )
    df.to_csv("bls_unemployment.csv", index=False)
    print(
        f"Saved bls_unemployment.csv ({df['period_date'].min()} → {df['period_date'].max()})"
    )


if __name__ == "__main__":
    main()
