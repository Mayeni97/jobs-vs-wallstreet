from datetime import date
import pandas as pd
import yfinance as yf

# S&P 500 + sector ETFs (add/remove as needed)
TICKERS = [
    "^GSPC", "XLY", "XLP", "XLE", "XLF",
    "XLV", "XLI", "XLB", "XLK", "XLU", "XLRE", "XLC"
]

def main():
    print(f"Fetching {len(TICKERS)} tickers...")
    data = yf.download(
        TICKERS,
        start="1999-01-01",
        end=str(date.today()),
        auto_adjust=True,
        progress=False,
    )

    # yfinance returns MultiIndex columns for multiple tickers
    if isinstance(data.columns, pd.MultiIndex):
        px = data["Close"].copy()
    else:
        # Single ticker fallback; rename to a consistent column
        px = data.rename(columns={"Close": TICKERS[0]})

    # Month-end buckets, last price in each month
    monthly = px.resample("ME").last()

    # Long format
    df = monthly.stack().reset_index()
    df.columns = ["period_date", "ticker", "adj_close"]

    # Compute monthly returns per ticker
    df["monthly_return"] = df.groupby("ticker")["adj_close"].pct_change()

    # Drop rows without prices (e.g., pre-inception)
    before = len(df)
    df = df.dropna(subset=["adj_close"])
    dropped = before - len(df)
    if dropped:
        print(f"Dropped {dropped} rows with null adj_close (pre-inception)")

    df.to_csv("equities_monthly.csv", index=False)
    print("Saved equities_monthly.csv")

if __name__ == "__main__":
    main()