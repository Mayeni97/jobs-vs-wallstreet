import datetime as dt
import os

import pandas as pd

try:
    from .common import bls_fetch
except ImportError:  # Support running this file directly from the repo root.
    from common import bls_fetch


DEFAULT_START = int(os.getenv("BLS_START_YEAR", "2000"))

# BLS LAUS seasonally adjusted state unemployment-rate series.
STATE_FIPS = {
    "AL": ("01", "Alabama"),
    "AK": ("02", "Alaska"),
    "AZ": ("04", "Arizona"),
    "AR": ("05", "Arkansas"),
    "CA": ("06", "California"),
    "CO": ("08", "Colorado"),
    "CT": ("09", "Connecticut"),
    "DE": ("10", "Delaware"),
    "DC": ("11", "District of Columbia"),
    "FL": ("12", "Florida"),
    "GA": ("13", "Georgia"),
    "HI": ("15", "Hawaii"),
    "ID": ("16", "Idaho"),
    "IL": ("17", "Illinois"),
    "IN": ("18", "Indiana"),
    "IA": ("19", "Iowa"),
    "KS": ("20", "Kansas"),
    "KY": ("21", "Kentucky"),
    "LA": ("22", "Louisiana"),
    "ME": ("23", "Maine"),
    "MD": ("24", "Maryland"),
    "MA": ("25", "Massachusetts"),
    "MI": ("26", "Michigan"),
    "MN": ("27", "Minnesota"),
    "MS": ("28", "Mississippi"),
    "MO": ("29", "Missouri"),
    "MT": ("30", "Montana"),
    "NE": ("31", "Nebraska"),
    "NV": ("32", "Nevada"),
    "NH": ("33", "New Hampshire"),
    "NJ": ("34", "New Jersey"),
    "NM": ("35", "New Mexico"),
    "NY": ("36", "New York"),
    "NC": ("37", "North Carolina"),
    "ND": ("38", "North Dakota"),
    "OH": ("39", "Ohio"),
    "OK": ("40", "Oklahoma"),
    "OR": ("41", "Oregon"),
    "PA": ("42", "Pennsylvania"),
    "RI": ("44", "Rhode Island"),
    "SC": ("45", "South Carolina"),
    "SD": ("46", "South Dakota"),
    "TN": ("47", "Tennessee"),
    "TX": ("48", "Texas"),
    "UT": ("49", "Utah"),
    "VT": ("50", "Vermont"),
    "VA": ("51", "Virginia"),
    "WA": ("53", "Washington"),
    "WV": ("54", "West Virginia"),
    "WI": ("55", "Wisconsin"),
    "WY": ("56", "Wyoming"),
}


def main():
    start_year = int(os.getenv("BLS_START_YEAR", str(DEFAULT_START)))
    end_year = dt.date.today().year
    states = list(STATE_FIPS.items())
    records = []

    # Ten-year windows work with both registered and public BLS API access.
    for window_start in range(start_year, end_year + 1, 10):
        window_end = min(window_start + 9, end_year)
        for offset in range(0, len(states), 25):
            batch = states[offset : offset + 25]
            series_ids = [f"LASST{fips}0000000000003" for _, (fips, _) in batch]
            series = bls_fetch(series_ids, start_year=window_start, end_year=window_end)
            state_by_series = {
                f"LASST{fips}0000000000003": (code, name)
                for code, (fips, name) in batch
            }
            for item in series:
                state_code, state_name = state_by_series[item["seriesID"]]
                for observation in item["data"]:
                    if observation["period"].startswith("M"):
                        records.append(
                            {
                                "period_date": dt.date(
                                    int(observation["year"]),
                                    int(observation["period"][1:]),
                                    1,
                                ),
                                "state_code": state_code,
                                "state_name": state_name,
                                "unemployment_rate": pd.to_numeric(
                                    observation["value"], errors="coerce"
                                ),
                            }
                        )

    df = pd.DataFrame(records)
    if df.empty:
        raise RuntimeError("BLS returned no LAUS state unemployment observations")
    df = (
        df.dropna()
        .drop_duplicates()
        .sort_values(["state_code", "period_date"])
        .reset_index(drop=True)
    )
    df.to_csv("laus_state_unemployment.csv", index=False)
    print(
        "Saved laus_state_unemployment.csv "
        f"({len(df)} rows; {df['period_date'].min()} → {df['period_date'].max()})"
    )


if __name__ == "__main__":
    main()
