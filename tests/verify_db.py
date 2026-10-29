import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from etl.common import get_engine

DATASETS = {
    "equities_monthly": ("equities_monthly.csv", "period_date"),
    "unemployment_headline": ("bls_unemployment.csv", "period_date"),
    "employment_sector": ("bls_ces_supersectors.csv", "period_date"),
    "laus_state_unemployment": ("laus_state_unemployment.csv", "period_date"),
}


def main():
    with get_engine().connect() as conn:
        for table, (path, date_column) in DATASETS.items():
            csv_max = (
                pd.to_datetime(
                    pd.read_csv(Path(path), usecols=[date_column])[date_column]
                )
                .max()
                .date()
            )
            db_max = conn.execute(
                text(f"select max({date_column}) from {table}")
            ).scalar_one()
            count = conn.execute(text(f"select count(*) from {table}")).scalar_one()
            if db_max < csv_max:
                raise RuntimeError(
                    f"{table}: database ends at {db_max}, before CSV date {csv_max}"
                )
            if count == 0:
                raise RuntimeError(f"{table}: database table is empty")
            print(f"{table}: {count} rows through {db_max} — OK")


if __name__ == "__main__":
    main()
