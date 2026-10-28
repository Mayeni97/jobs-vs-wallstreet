# Jobs vs Wall Street 

This project tracks **U.S. unemployment and sector employment data** (via BLS)  
and compares it with **stock market performance** (S&P 500 + sector ETFs).  

It combines **data engineering + analytics** to deliver insights into how labor market changes relate to financial markets.

---

## Features
- Automated **ETL pipeline** to fetch fresh data:
  - **Unemployment / Employment** from BLS API  
  - **Stock data** from Yahoo Finance (`yfinance`)  
- **PostgreSQL database** to store historical & updated data  
- **GitHub Actions** for CI/CD & scheduled refreshes  
- **Streamlit dashboard** for live interactive visualization  

---

## Project Structure
jobs-vs-wallstreet/
- etl/           # Scripts to fetch, clean, and load data
- dashboard/     # Streamlit app (interactive dashboard)
- ntests/         # Unit tests for transformations
- README.md      # Project overview
- requirements.txt # Python dependencies
- .github/workflows/ci.yml # CI/CD pipeline

---

##  Goals
- Show correlations between **unemployment/employment** and **stock performance**  
- Provide **up-to-date sector-level insights**  
- Demonstrate skills in:
  - Data Engineering (ETL + pipelines)
  - Data Analytics (visualizations, metrics)
  - DevOps (GitHub Actions CI/CD)
  - Cloud Deployment (Streamlit Cloud + Postgres)

---

## Tech Stack
- **Python** (Pandas, Requests, YFinance, SQLAlchemy, Streamlit, Plotly)
- **PostgreSQL** (Supabase/Neon for cloud DB)
- **GitHub Actions** (CI/CD + scheduled jobs)
- **Streamlit Cloud** (dashboard hosting)

The dashboard includes state unemployment mapping from BLS LAUS and an
experimental 12-month unemployment trend projection.

## Supabase setup

The project uses Supabase as its PostgreSQL database. Create a Supabase project,
open the SQL Editor, and run [`supabase/schema.sql`](supabase/schema.sql).

Copy the Supabase PostgreSQL connection string from **Project Settings → Database**.
Use the pooled connection string when deploying the dashboard, and save it as
`DATABASE_URL`.

For local runs, create a `.env` file in the repository root:

```env
DATABASE_URL=postgresql://postgres.<project-ref>:<password>@<pooler-host>:6543/postgres?sslmode=require
```

Then load the generated CSV files into Supabase with:

```bash
python etl/load_to_db.py
```

For Streamlit Cloud, add the same value under **App settings → Secrets** as:

```toml
DATABASE_URL = "postgresql://..."
```

For GitHub Actions, add these repository secrets so the ETL workflows can
refresh Supabase:

```text
PGHOST
PGPORT
PGDATABASE
PGUSER
PGPASSWORD
```

`BLS_API_KEY` is optional, but recommended for larger historical BLS requests.
The workflows use the `PG*` secrets directly; `DATABASE_URL` is not required
for GitHub Actions.

## Deploy the dashboard

1. Open [Streamlit Community Cloud](https://share.streamlit.io/) and sign in with GitHub.
2. Create an app from this repository and select `dashboard/app.py` as the main file.
3. In the app settings, add the Supabase connection string as a secret:

```toml
DATABASE_URL = "postgresql://..."
```

4. Deploy the app. Future pushes to `master` will trigger a redeploy.

Streamlit Cloud can also use the five `PG*` secrets instead of `DATABASE_URL`.

The repository includes [`.env.example`](.env.example) as a safe local template.
Never commit `.env` or paste database credentials into source files.

## Operations checklist

- Confirm the GitHub Actions `refresh` workflow succeeds after each schedule.
- Review the dashboard's latest-data dates after a refresh.
- Keep the database credentials current when the Supabase password changes.
- Run the workflow manually after a BLS revision or provider outage.

---

## Dashboard (Planned)
- Line charts: unemployment vs S&P 500  
- Sector breakdowns: employment vs sector ETFs  
- Correlation metrics: employment growth vs stock returns  
- Filters for sector & ticker  

---

## Future Improvements
- Add **sentiment analysis** from financial news headlines
- Add validated forecasting models and forecast uncertainty intervals
