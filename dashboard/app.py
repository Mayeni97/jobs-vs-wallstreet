# dashboard/app.py
import os
import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError
from streamlit.errors import StreamlitSecretNotFoundError

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from etl.common import get_engine as get_database_engine

# ---------------- Setup ----------------
st.set_page_config(page_title="Jobs vs Wall Street", layout="wide")
st.markdown(
    """
    <style>
        [data-testid="stMetric"] {
            background: linear-gradient(135deg, rgba(31, 119, 180, 0.10), rgba(148, 103, 189, 0.08));
            border: 1px solid rgba(127, 127, 127, 0.18);
            border-radius: 14px;
            padding: 16px 18px;
        }
        [data-testid="stMetricValue"] { font-variant-numeric: tabular-nums; }
        [data-testid="stSidebarContent"] { padding-top: 2rem; }
        h1 { letter-spacing: -0.03em; }
    </style>
""",
    unsafe_allow_html=True,
)


def _setting(name: str) -> str:
    """Read Streamlit Cloud secrets first, then local environment variables."""
    try:
        value = st.secrets.get(name)
    except StreamlitSecretNotFoundError:
        value = None
    return str(value or os.getenv(name, "")).strip()


DATABASE_URL = _setting("DATABASE_URL")
if DATABASE_URL.startswith(("http://", "https://")):
    st.error(
        "DATABASE_URL must be Supabase's PostgreSQL connection string, not the "
        "Supabase project URL. Copy it from Project Settings → Database → "
        "Connection string, then choose the URI format."
    )
    st.stop()

if not DATABASE_URL:
    required = ["PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"]
    missing = [name for name in required if not _setting(name)]
    if missing:
        st.error(
            "Add a Supabase DATABASE_URL to Streamlit secrets or a local .env file. "
            f"Missing fallback settings: {', '.join(missing)}"
        )
        st.stop()

    DATABASE_URL = str(
        URL.create(
            "postgresql+psycopg2",
            username=_setting("PGUSER"),
            password=_setting("PGPASSWORD"),
            host=_setting("PGHOST"),
            port=int(_setting("PGPORT") or "5432"),
            database=_setting("PGDATABASE"),
            query={"sslmode": "require"},
        )
    )
elif "sslmode=" not in DATABASE_URL:
    DATABASE_URL += "&sslmode=require" if "?" in DATABASE_URL else "?sslmode=require"


@st.cache_resource
def get_engine():
    os.environ["DATABASE_URL"] = DATABASE_URL
    return get_database_engine()


@st.cache_data(ttl=60 * 15)
def q(sql: str, params: dict | None = None) -> pd.DataFrame:
    with get_engine().connect() as c:
        return pd.read_sql(text(sql), c, params=params or {})


st.title("Jobs vs Wall Street 📊")
st.caption(
    "U.S. unemployment & sector employment vs S&P 500 and sector ETFs — monthly aligned"
)
st.info(
    "Use the controls on the left to explore how labor-market changes moved alongside "
    "the broad market and a selected sector. Correlation is descriptive, not causal."
)

# ---------------- Universe autodetect ----------------
try:
    tickers = q("select distinct ticker from equities_monthly order by 1;")[
        "ticker"
    ].tolist()
except SQLAlchemyError:
    st.error(
        "The dashboard could not query Supabase. Check DATABASE_URL and confirm "
        "the database tables have been created and loaded."
    )
    st.stop()
if not tickers:
    st.error("No equities in DB. Load data first.")
    st.stop()

SPX = "^GSPC" if "^GSPC" in tickers else ("GSPC" if "GSPC" in tickers else None)
if not SPX:
    st.error("S&P 500 ticker not found (expected ^GSPC or GSPC).")
    st.stop()

KNOWN_ETFS = [
    "XLB",
    "XLE",
    "XLF",
    "XLI",
    "XLK",
    "XLP",
    "XLU",
    "XLV",
    "XLY",
    "XLRE",
    "XLC",
]
ETFS = [t for t in KNOWN_ETFS if t in tickers]
if not ETFS:
    st.error("No sector ETFs found in equities_monthly.")
    st.stop()

# Overall monthly overlap across tables
meta = q("""
    select 
      (select min(date_trunc('month', period_date)) from equities_monthly)   as eq_min,
      (select max(date_trunc('month', period_date)) from equities_monthly)   as eq_max,
      (select min(date_trunc('month', period_date)) from unemployment_headline) as un_min,
      (select max(date_trunc('month', period_date)) from unemployment_headline) as un_max,
      (select min(date_trunc('month', period_date)) from employment_sector)  as ces_min,
      (select max(date_trunc('month', period_date)) from employment_sector)  as ces_max
""")
eq_min = pd.to_datetime(meta.loc[0, "eq_min"]).date()
eq_max = pd.to_datetime(meta.loc[0, "eq_max"]).date()
un_min = pd.to_datetime(meta.loc[0, "un_min"]).date()
un_max = pd.to_datetime(meta.loc[0, "un_max"]).date()
ces_min = pd.to_datetime(meta.loc[0, "ces_min"]).date()
ces_max = pd.to_datetime(meta.loc[0, "ces_max"]).date()

start_default = max(eq_min, un_min, ces_min)
end_default = min(eq_max, un_max, ces_max)

ETF_TO_CES = {
    "XLB": "Manufacturing",
    "XLE": "Mining and Logging",
    "XLF": "Financial Activities",
    "XLI": "Manufacturing",
    "XLK": "Information",
    "XLP": "Retail Trade",
    "XLU": "Utilities",
    "XLV": "Education and Health Services",
    "XLY": "Leisure and Hospitality",
    "XLRE": "Construction",
    "XLC": "Information",
}

TICKER_LABELS = {
    "^GSPC": "S&P 500 index",
    "GSPC": "S&P 500 index",
    "XLB": "Materials",
    "XLE": "Energy",
    "XLF": "Financials",
    "XLI": "Industrials",
    "XLK": "Technology",
    "XLP": "Consumer Staples",
    "XLU": "Utilities",
    "XLV": "Health Care",
    "XLY": "Consumer Discretionary",
    "XLRE": "Real Estate",
    "XLC": "Communication Services",
}

# ---------------- Sidebar ----------------
with st.sidebar:
    st.header("Controls")
    st.caption(f"Data overlap: {start_default} → {end_default}")
    window = st.selectbox(
        "Time window",
        ["All data", "5 years", "1 year", "Custom"],
        index=0,
    )
    if window == "Custom":
        start = st.date_input(
            "Start month",
            value=start_default,
            min_value=start_default,
            max_value=end_default,
            key="start_date",
        )
        end = st.date_input(
            "End month",
            value=end_default,
            min_value=start_default,
            max_value=end_default,
            key="end_date",
        )
    else:
        years = {"All data": None, "5 years": 5, "1 year": 1}[window]
        start = (
            start_default
            if years is None
            else max(
                start_default,
                (pd.Timestamp(end_default) - pd.DateOffset(years=years)).date(),
            )
        )
        end = end_default
        st.caption(f"Showing: {start} → {end}")
    default_idx = ETFS.index("XLK") if "XLK" in ETFS else 0
    sector_etf = st.selectbox(
        "Sector ETF",
        ETFS,
        index=default_idx,
        format_func=lambda ticker: f"{ticker} — {TICKER_LABELS.get(ticker, 'Sector ETF')}",
    )
    sector_name = ETF_TO_CES.get(sector_etf, "Manufacturing")
    st.caption(f"Selected: {sector_etf} — {TICKER_LABELS.get(sector_etf, sector_etf)}")
    st.divider()
    st.caption("Latest available data")
    st.write(f"Stocks: {eq_max}")
    st.write(f"Unemployment: {un_max}")
    st.write(f"Employment: {ces_max}")
    if end_default < pd.Timestamp.today().date():
        st.warning(
            "Some source data may be delayed; the dashboard ends at the latest loaded month."
        )

params = {
    "start": str(start),
    "end": str(end),
    "spx": SPX,
    "etf": sector_etf,
    "sector_name": sector_name,
}

# ---------------- Monthly-aligned data ----------------
unemp = q(
    """
    select date_trunc('month', period_date)::date as ym,
           avg(unemployment_rate) as unemployment_rate
    from unemployment_headline
    where period_date between :start and :end
    group by 1
    order by 1
""",
    params,
)

equities = q(
    """
    select date_trunc('month', period_date)::date as ym,
           ticker, 
           max(adj_close) as adj_close,          -- end-of-month
           avg(monthly_return) as monthly_return
    from equities_monthly
    where period_date between :start and :end
      and ticker in (:spx, :etf)
    group by 1,2
    order by 1,2
""",
    params,
)

sector_jobs = q(
    """
    select date_trunc('month', period_date)::date as ym,
           sector_name,
           avg(employment_thousands) as employment_thousands
    from employment_sector
    where period_date between :start and :end
      and sector_name = :sector_name
    group by 1,2
    order by 1,2
""",
    params,
)


def nonempty(df, name):
    if df.empty:
        st.warning(f"No rows for {name} in the selected window.")
        return False
    return True


ok_u = nonempty(unemp, "unemployment")
ok_e = nonempty(equities, "equities")
ok_s = nonempty(sector_jobs, "sector employment")


# ---------------- KPIs ----------------
def pct_window(series: pd.Series, months: int) -> float | None:
    if len(series) < months or months <= 0:
        return None
    # cumulative (1+r) product over last N months
    window = series.iloc[-months:]
    return window.add(1).prod() - 1.0


kpi = st.columns(3)

if ok_u and ok_e:
    # Unemployment level + MoM / YoY deltas (percentage points)
    u = unemp.set_index("ym").sort_index()
    latest_u = u.index.max()
    prev_m = latest_u - pd.offsets.MonthBegin(1)
    prev_y = (latest_u - pd.DateOffset(years=1)).to_period("M").to_timestamp()
    u_latest = float(u.loc[latest_u, "unemployment_rate"])
    u_mom = (
        (u_latest - float(u.loc[prev_m, "unemployment_rate"]))
        if prev_m in u.index
        else None
    )
    u_yoy = (
        (u_latest - float(u.loc[prev_y, "unemployment_rate"]))
        if prev_y in u.index
        else None
    )

    kpi[0].metric(
        "Unemployment rate (latest)",
        f"{u_latest:.1f}%",
        (f"{u_mom:+.1f} pp MoM" if u_mom is not None else "—")
        + (f" · {u_yoy:+.1f} pp YoY" if u_yoy is not None else ""),
    )

    # S&P 500 momentum (1m / 3m / 12m)
    spx = equities[equities["ticker"] == SPX].copy().sort_values("ym")
    r_spx = spx["monthly_return"].dropna()
    r1 = r_spx.iloc[-1] if len(r_spx) >= 1 else None
    r3 = pct_window(r_spx, 3)
    r12 = pct_window(r_spx, 12)
    kpi[1].metric(
        f"{SPX} momentum",
        f"{(r1*100):+.1f}%" if r1 is not None else "—",
        (
            f"3m {(r3*100):+.1f}% · 12m {(r12*100):+.1f}%"
            if (r3 is not None and r12 is not None)
            else "—"
        ),
    )

    # Sector ETF momentum + correlation with jobs Δ% (m/m)
    etf = equities[equities["ticker"] == sector_etf].copy().sort_values("ym")
    r_etf = etf["monthly_return"].dropna()
    r1_e = r_etf.iloc[-1] if len(r_etf) >= 1 else None
    r3_e = pct_window(r_etf, 3)
    r12_e = pct_window(r_etf, 12)

    sj = sector_jobs.sort_values("ym").copy()
    sj["emp_pct_change"] = sj["employment_thousands"].pct_change()
    corr_df = pd.merge(
        etf[["ym", "monthly_return"]],
        sj[["ym", "emp_pct_change"]],
        on="ym",
        how="inner",
    ).dropna()
    corr_val = (
        corr_df["monthly_return"].corr(corr_df["emp_pct_change"])
        if len(corr_df) > 2
        else None
    )

    kpi[2].metric(
        f"{sector_etf} momentum · jobs corr",
        f"{(r1_e*100):+.1f}%" if r1_e is not None else "—",
        (
            (
                f"3m {(r3_e*100):+.1f}% · 12m {(r12_e*100):+.1f}%"
                + (f" · corr {corr_val:+.2f}" if corr_val is not None else " · corr —")
            )
            if (r3_e is not None and r12_e is not None)
            else "—"
        ),
    )

# ---------------- Plots ----------------
st.subheader("Market and labor trends")
st.caption(
    "Indexed series start at 100 so movements can be compared despite different units."
)
col1, col2 = st.columns(2)

# A) Unemployment vs S&P (indexed)
if ok_u and ok_e:
    spx = equities[equities["ticker"] == SPX].copy()
    if not spx.empty:
        spx["spx_indexed"] = (spx["adj_close"] / spx["adj_close"].iloc[0]) * 100.0
        df_a = unemp.merge(spx[["ym", "spx_indexed"]], on="ym", how="inner")
        if not df_a.empty:
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(
                go.Scatter(
                    x=df_a["ym"],
                    y=df_a["unemployment_rate"],
                    name="Unemployment rate",
                    line={"color": "#d62728"},
                ),
                secondary_y=False,
            )
            fig.add_trace(
                go.Scatter(
                    x=df_a["ym"],
                    y=df_a["spx_indexed"],
                    name=f"{SPX} indexed",
                    line={"color": "#1f77b4"},
                ),
                secondary_y=True,
            )
            fig.update_layout(title=f"Unemployment vs {SPX} ({start} → {end})")
            fig.update_xaxes(title_text="Month")
            fig.update_yaxes(title_text="Unemployment rate (%)", secondary_y=False)
            fig.update_yaxes(title_text=f"{SPX} (indexed to 100)", secondary_y=True)
            fig.update_layout(
                height=430, hovermode="x unified", margin={"t": 60, "b": 20}
            )
            col1.plotly_chart(fig, use_container_width=True)
        else:
            col1.info("No overlapping months for unemployment and S&P.")
    else:
        col1.info(f"No {SPX} rows in selected window.")

# B) Sector jobs vs ETF (indexed)
if ok_s and ok_e:
    etf = equities[equities["ticker"] == sector_etf].copy()
    if not etf.empty:
        etf["etf_indexed"] = (etf["adj_close"] / etf["adj_close"].iloc[0]) * 100.0
        df_b = sector_jobs.merge(etf[["ym", "etf_indexed"]], on="ym", how="inner")
        if not df_b.empty:
            fig2 = make_subplots(specs=[[{"secondary_y": True}]])
            fig2.add_trace(
                go.Scatter(
                    x=df_b["ym"],
                    y=df_b["employment_thousands"],
                    name=f"{sector_name} jobs (thousands)",
                    line={"color": "#2ca02c"},
                ),
                secondary_y=False,
            )
            fig2.add_trace(
                go.Scatter(
                    x=df_b["ym"],
                    y=df_b["etf_indexed"],
                    name=f"{sector_etf} indexed",
                    line={"color": "#9467bd"},
                ),
                secondary_y=True,
            )
            fig2.update_layout(
                title=f"{sector_name} Jobs vs {sector_etf} ({start} → {end})"
            )
            fig2.update_xaxes(title_text="Month")
            fig2.update_yaxes(title_text="Employment (thousands)", secondary_y=False)
            fig2.update_yaxes(
                title_text=f"{sector_etf} (indexed to 100)", secondary_y=True
            )
            fig2.update_layout(
                height=430, hovermode="x unified", margin={"t": 60, "b": 20}
            )
            col2.plotly_chart(fig2, use_container_width=True)
        else:
            col2.info("No overlapping months for sector jobs and ETF.")
    else:
        col2.info(f"No {sector_etf} rows in selected window.")

# C) Correlation snapshot table (monthly)
st.subheader("Correlation snapshot (monthly)")
st.caption(
    "Returns and labor-market levels can move together without one causing the other."
)
if ok_u and ok_e:
    corr = q(
        """
        with spx as (
          select date_trunc('month', period_date)::date as ym, monthly_return as spx_return
          from equities_monthly where ticker = :spx
        ), etf as (
          select date_trunc('month', period_date)::date as ym, monthly_return as etf_return
          from equities_monthly where ticker = :etf
        ), u as (
          select date_trunc('month', period_date)::date as ym, avg(unemployment_rate) as unemployment_rate
          from unemployment_headline
          group by 1
        )
        select
          corr(spx.spx_return, u.unemployment_rate) as corr_spx_unemp,
          corr(etf.etf_return, u.unemployment_rate) as corr_etf_unemp,
          count(*) as observations
        from spx join u using (ym)
        join etf using (ym)
        where ym between :start and :end
    """,
        params,
    )
    corr_display = corr.rename(
        columns={
            "corr_spx_unemp": f"{SPX} vs unemployment",
            "corr_etf_unemp": f"{sector_etf} vs unemployment",
            "observations": "Monthly observations",
        }
    )
    for column in [f"{SPX} vs unemployment", f"{sector_etf} vs unemployment"]:
        corr_display[column] = corr_display[column].round(2)
    st.dataframe(corr_display, use_container_width=True, hide_index=True)
else:
    st.info("Not enough data for correlation in the selected window.")

st.caption(
    "All series aligned by month (date_trunc('month')) to avoid day mismatches. ‘Momentum’ is 1m/3m/12m total return."
)
