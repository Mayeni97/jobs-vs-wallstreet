# Jobs vs Wall Street

Jobs vs Wall Street is a data pipeline and interactive Streamlit dashboard that
explores how U.S. labor-market conditions move alongside the stock market.
It combines official Bureau of Labor Statistics data with monthly S&P 500 and
sector ETF data, stores the results in Supabase PostgreSQL, and presents the
results through an interactive dashboard.

## What it does

- Tracks the national unemployment rate from BLS.
- Tracks employment by major industry from the BLS Current Employment Statistics survey.
- Maps seasonally adjusted unemployment for all 50 states plus Washington, D.C. using BLS LAUS.
- Compares unemployment and employment trends with the S&P 500 and sector ETFs.
- Provides date-window filters, ticker descriptions, interactive charts, and state trends.
- Includes an exploratory 12-month linear unemployment trend projection.
- Refreshes data through GitHub Actions and loads it into Supabase.

The forecast is intentionally labeled experimental. It is a simple trend projection,
not a validated economic forecast or investment recommendation. Correlation in the
dashboard is descriptive and does not prove causation.

## Architecture

```text
BLS API + Yahoo Finance
          │
          ▼
      etl/*.py  ──► CSV snapshots ──► Supabase PostgreSQL
                                           │
                                           ▼
                                  dashboard/app.py
                                  Streamlit dashboard
```

## Project structure

```text
dashboard/app.py              Streamlit dashboard
etl/common.py                 BLS client and database connection helpers
etl/bls_ingest.py             National unemployment ETL
etl/bls_ces_ingest.py         Industry employment ETL
etl/laus_ingest.py            State unemployment ETL
etl/stocks_ingest.py          S&P 500 and sector ETF ETL
etl/load_to_db.py             CSV-to-Supabase loader
supabase/schema.sql           Database tables and indexes
tests/test.py                 Data integrity checks
.github/workflows/ci.yml      Lint, format, import, and data checks
.github/workflows/refresh-*.yml Staggered production refreshes
```

## Supabase setup

1. Create a Supabase project.
2. Open the Supabase SQL Editor and run [`supabase/schema.sql`](supabase/schema.sql).
3. Copy a PostgreSQL URI from Supabase **Connect**. Use a URI beginning with
   `postgresql://`, not the Supabase project URL beginning with `https://`.

For local development, copy [`.env.example`](.env.example) to `.env` and fill in
either `DATABASE_URL` or all five `PG*` values. Never commit `.env`.

Load the generated CSV snapshots into Supabase with:

```bash
python etl/load_to_db.py
```

The loader is safe to run repeatedly because it upserts by each table's primary key.
The generated CSV snapshots are also refreshed and committed to GitHub after each
successful scheduled run, so the repository keeps a visible copy of the latest
loaded data.

## Run locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run dashboard/app.py
```

Open `http://localhost:8501`.

## Automated refresh

The refreshes are deliberately staggered to reduce provider rate-limit pressure:

- Stocks: daily at 14:00 UTC.
- National unemployment: Mondays at 14:00 UTC.
- Industry employment: Tuesdays at 14:00 UTC.
- State unemployment (LAUS): Wednesdays at 14:00 UTC.

Each workflow fetches one source, loads Supabase, verifies that the database's
latest date matches the generated CSV, and commits the refreshed snapshot. GitHub
Actions prevents overlapping refreshes. Each workflow can also be run manually.

Add these repository secrets for GitHub Actions:

```text
PGHOST
PGPORT
PGDATABASE
PGUSER
PGPASSWORD
```

`BLS_API_KEY` is optional but recommended for larger historical BLS requests.
The workflow also supports manual runs from the GitHub Actions page.

## Deploy to Streamlit Cloud

1. Create an app from this GitHub repository.
2. Set the main file to `dashboard/app.py`.
3. Add `DATABASE_URL` or the five `PG*` values under Streamlit app secrets.
4. Deploy. Future pushes to `master` will trigger redeployment.

## Quality checks

```bash
ruff check .
black --check .
python -m compileall -q .
python tests/test.py
```

## Future improvements

- Add financial-news sentiment analysis.
- Evaluate forecasting models against held-out historical data.
- Add forecast uncertainty intervals.
- Add ETL run history and failure notifications.
