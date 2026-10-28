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

For GitHub Actions, add a repository secret named `DATABASE_URL` so the ETL
workflows can refresh Supabase.

---

## Dashboard (Planned)
- Line charts: unemployment vs S&P 500  
- Sector breakdowns: employment vs sector ETFs  
- Correlation metrics: employment growth vs stock returns  
- Filters for sector & ticker  

---

## Future Improvements
- Add **state-level unemployment (LAUS)** for geo maps  
- Add **sentiment analysis** from financial news headlines  
- Add **forecasting models** (ARIMA, Prophet, ML) for unemployment trends
