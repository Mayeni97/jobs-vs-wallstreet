import pandas as pd
s = pd.read_csv("equities_monthly.csv", parse_dates=["period_date"])

# shape + columns
print(s.shape); print(s.columns.tolist())

# tickers present
print(sorted(s['ticker'].unique().tolist()))

# date range per ticker
print(s.groupby('ticker')['period_date'].agg(['min','max','count']))

# duplicates?
dupes = s.duplicated(subset=['ticker','period_date']).sum()
print("dupes:", dupes)

# NaNs in returns (should be 1 per ticker at first month)
print(s['monthly_return'].isna().sum(), "NaNs in monthly_return")
print(s.groupby('ticker')['monthly_return'].apply(lambda x: x.isna().sum()).sort_values())

ok = s.sort_values(['ticker','period_date']).groupby('ticker')['period_date'].is_monotonic_increasing.all()
print("dates sorted per ticker:", ok)

print(s[s.ticker=="^GSPC"].head(8))