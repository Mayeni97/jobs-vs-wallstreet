import os
import requests
import pandas as pd
import datetime as dt

# 🔑 You'll need a free BLS API key later:
# https://www.bls.gov/developers/
BLS_API_KEY = os.getenv("BLS_API_KEY")

# Series IDs:
# LNS14000000 = Unemployment Rate (U-3, monthly, seasonally adjusted)
HEADLINE_SERIES = ["LNS14000000"]

def fetch_bls(series_ids, start_year=2000, end_year=None):
    """Fetch data from BLS API and return as DataFrame."""
    if end_year is None:
        end_year = dt.date.today().year

    payload = {
        "seriesid": series_ids,
        "startyear": str(start_year),
        "endyear": str(end_year),
        "registrationkey": BLS_API_KEY,
    }
    r = requests.post("https://api.bls.gov/publicAPI/v2/timeseries/data/",
                      json=payload, timeout=30)
    r.raise_for_status()
    j = r.json()

    rows = []
    for series in j["Results"]["series"]:
        sid = series["seriesID"]
        for item in series["data"]:
            year = int(item["year"])
            month = int(item["period"][1:])  # "M01" → 1
            value = float(item["value"])
            rows.append({
                "series_id": sid,
                "period_date": dt.date(year, month, 1),
                "value": value,
            })

    df = pd.DataFrame(rows)
    return df.sort_values("period_date")

def main():
    print("Fetching BLS unemployment data...")
    df = fetch_bls(HEADLINE_SERIES, start_year=2000)
    df.to_csv("bls_unemployment.csv", index=False)
    print("Saved bls_unemployment.csv")

if __name__ == "__main__":
    main()