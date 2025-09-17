import yfinance as yf
import pandas as pd

TICKERS = ["^GSPC","XLY","XLP","XLE","XLF","XLV","XLI","XLB","XLK","XLU","XLRE"]

def fetch_monthly_prices(start="2000-01-01"):
    # auto_adjust=False so we have an 'Adj Close' column
    data = yf.download(TICKERS, start=start, progress=False, auto_adjust=False)

    # Grab just the adjusted close; handles MultiIndex columns when multiple tickers
    adj = data["Adj Close"]

    # Month-end resample
    monthly = adj.resample("ME").last()

    # Wide -> long
    df = monthly.reset_index().melt(id_vars=["Date"], var_name="ticker", value_name="adj_close")
    df = df.rename(columns={"Date": "period_date"}).sort_values(["ticker","period_date"]).reset_index(drop=True)

    # Monthly return per ticker (no forward fill)
    df["monthly_return"] = df.groupby("ticker")["adj_close"].pct_change(fill_method=None)
    return df

def main():
    df = fetch_monthly_prices()
    df.to_csv("equities_monthly.csv", index=False)
    print("Saved equities_monthly.csv")

if __name__ == "__main__":
    main()