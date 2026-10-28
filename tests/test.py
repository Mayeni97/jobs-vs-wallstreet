from pathlib import Path

import pandas as pd

s = pd.read_csv("equities_monthly.csv", parse_dates=["period_date"])

assert not s.empty, "equities_monthly.csv must contain rows"
assert {"period_date", "ticker", "adj_close", "monthly_return"}.issubset(s.columns)

# shape + columns
print(s.shape)
print(s.columns.tolist())

# tickers present
print(sorted(s["ticker"].unique().tolist()))

# date range per ticker
print(s.groupby("ticker")["period_date"].agg(["min", "max", "count"]))

# duplicates?
dupes = s.duplicated(subset=["ticker", "period_date"]).sum()
print("dupes:", dupes)
assert dupes == 0, "ticker/month rows must be unique"

# NaNs in returns (should be 1 per ticker at first month)
print(s["monthly_return"].isna().sum(), "NaNs in monthly_return")
print(
    s.groupby("ticker")["monthly_return"].apply(lambda x: x.isna().sum()).sort_values()
)

ok = (
    s.sort_values(["ticker", "period_date"])
    .groupby("ticker")["period_date"]
    .is_monotonic_increasing.all()
)
print("dates sorted per ticker:", ok)
assert ok, "dates must be sorted per ticker"

laus_path = Path("laus_state_unemployment.csv")
if laus_path.exists():
    laus = pd.read_csv(laus_path, parse_dates=["period_date"])
    assert len(laus["state_code"].unique()) == 51, "LAUS must contain 50 states plus DC"
    assert not laus.duplicated(subset=["state_code", "period_date"]).any()

print(s[s.ticker == "^GSPC"].head(8))
