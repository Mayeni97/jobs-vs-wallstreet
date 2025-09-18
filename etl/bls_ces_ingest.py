# etl/bls_ingest.py
import os
import datetime as dt
import requests
import pandas as pd

BLS_API_KEY = os.getenv("BLS_API_KEY")  # optional

HEADLINE_SERIES = ["LNS14000000"]  # U-3 unemployment rate, SA, monthly

# CES supersectors — All employees, thousands, SA
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

def _bls_post(series_ids, start_year=2000, end_year=None):
    if end_year is None:
        end_year = dt.date.today().year
    payload = {"seriesid": series_ids, "startyear": str(start_year), "endyear": str(end_year)}
    if BLS_API_KEY:
        payload["registrationkey"] = BLS_API_KEY
    r = requests.post("https://api.bls.gov/publicAPI/v2/timeseries/data/",
                      json=payload, timeout=60)
    r.raise_for_status()
    j = r.json()
    if j.get("status") != "REQUEST_SUCCEEDED":
        raise RuntimeError(f"BLS API error: {j}")
    return j["Results"]["series"]

def fetch_unemployment():
    series = _bls_post(HEADLINE_SERIES)
    rows = []
    for s in series:
        for item in s["data"]:
            y, m = int(item["year"]), int(item["period"][1:])
            rows.append({"period_date": dt.date(y, m, 1),
                         "unemployment_rate": float(item["value"])})
    return pd.DataFrame(rows).sort_values("period_date").reset_index(drop=True)

def fetch_ces_supersectors():
    ids = list(CES_SUPERSECTORS.keys())
    series = _bls_post(ids)
    recs = []
    for s in series:
        code = s["seriesID"]
        name = CES_SUPERSECTORS.get(code, code)
        for item in s["data"]:
            y, m = int(item["year"]), int(item["period"][1:])
            recs.append({
                "period_date": dt.date(y, m, 1),
                "sector_code": code,
                "sector_name": name,
                "employment_thousands": float(item["value"]),
            })
    return pd.DataFrame(recs).sort_values(["sector_name","period_date"]).reset_index(drop=True)

def main():
    print("Fetching BLS unemployment + CES…")
    fetch_unemployment().to_csv("bls_unemployment.csv", index=False)
    fetch_ces_supersectors().to_csv("bls_ces_supersectors.csv", index=False)
    print("✅ Saved bls_unemployment.csv and bls_ces_supersectors.csv")

if __name__ == "__main__":
    main()