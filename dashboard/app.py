import os
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

# --------- Page setup ---------
st.set_page_config(page_title="Jobs vs Wall Street", layout="wide")
st.markdown(
    """
    <style>
      .stMetric { text-align: center; }
      .css-1v0mbdj, .e1f1d6gn0 { font-variant-numeric: tabular-nums; }
    </style>
    """,
    unsafe_allow_html=True,
)

# --------- DB engine (secrets first, then env) ---------
def _cfg_from_secrets_or_env():
    if "pg" in st.secrets:  # Streamlit Cloud
        c = st.secrets["pg"]
        return {
            "host": c["host"],
            "port": str(c.get("port", 5432)),
            "db": c["database"],
            "user": c["user"],
            "pwd": c["password"],
        }
    # local fallback to env
    req = ["PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"]
    missing = [k for k in req if not os.getenv(k)]
    if missing:
        st.error(f"Missing DB config ({', '.join(missing)}). "
                 f"Use Streamlit secrets on Cloud, or export env vars locally.")
        st.stop()
    return {
        "host": os.getenv("PGHOST"),
        "port": os.getenv("PGPORT", "5432"),
        "db": os.getenv("PGDATABASE"),
        "user": os.getenv("PGUSER"),
        "pwd": os.getenv("PGPASSWORD"),
    }

@st.cache_resource
def get_engine():
    c = _cfg_from_secrets_or_env()
    url = (
        f"postgresql+psycopg2://{c['user']}:{c['pwd']}@"
        f"{c['host']}:{c['port']}/{c['db']}?sslmode=require"
    )
    return create_engine(url, pool_pre_ping=True)

@st.cache_data(ttl=60 * 15)
def q(sql: str, params: dict | None = None) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})

st.title("Jobs vs Wall Street")
st.caption("U.S. unemployment and sector employment vs S&P 500 and sector ETFs — monthly aligned.")

# --------- Universe & date overlap ---------
tickers = q("select distinct ticker from equities_monthly order by 1;")["ticker"].tolist()
if not tickers:
    st.error("No equities in the database. Load data first.")
    st.stop()

SPX = "^GSPC" if "^GSPC" in tickers else ("GSPC" if "GSPC" in tickers else None)
if not SPX:
    st.error("S&P 500 ticker not found (expected ^GSPC or GSPC).")
    st.stop()

KNOWN_ETFS = ["XLB","XLE","XLF","XLI","XLK","XLP","XLU","XLV","XLY","XLRE","XLC"]
ETFS = [t for t in KNOWN_ETFS if t in tickers]
if not ETFS:
    st.error("No sector ETFs found in equities_monthly.")
    st.stop()

meta = q(
    """
    select 
      (select min(date_trunc('month', period_date)) from equities_monthly)   as eq_min,
      (select max(date_trunc('month', period_date)) from equities_monthly)   as eq_max,
      (select min(date_trunc('month', period_date)) from unemployment_headline) as un_min,
      (select max(date_trunc('month', period_date)) from unemployment_headline) as un_max,
      (select min(date_trunc('month', period_date)) from employment_sector)  as ces_min,
      (select max(date_trunc('month', period_date)) from employment_sector)  as ces_max
    """
)
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

# --------- Sidebar controls ---------
with st.sidebar:
    st.header("Controls")
    st.caption(f"Data overlap (monthly): {start_default} → {end_default}")
    start = st.date_input(
        "Start month", value=start_default, min_value=start_default, max_value=end_default
    )
    end = st.date_input(
        "End month", value=end_default, min_value=start_default, max_value=end_default
    )
    default_idx = ETFS.index("XLK") if "XLK" in ETFS else 0
    sector_etf = st.selectbox("Sector ETF", ETFS, index=default_idx)
    sector_name = ETF_TO_CES.get(sector_etf, "Manufacturing")

params = {"start": str(start), "end": str(end), "spx": SPX, "etf": sector_etf, "sector_name": sector_name}

# --------- Queries (monthly aligned) ---------
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
           max(adj_close) as adj_close,
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

def nonempty(df, label):
    if df.empty:
        st.warning(f"No rows for {label} in the selected window.")
        return False
    return True

ok_u = nonempty(unemp, "unemployment")
ok_e = nonempty(equities, "equities")
ok_s = nonempty(sector_jobs, "sector employment")

# --------- KPIs ---------
def pct_window(series: pd.Series, months: int):
    if len(series) < months or months <= 0:
        return None
    window = series.iloc[-months:]
    return window.add(1).prod() - 1.0

kpi = st.columns(3)

if ok_u and ok_e:
    u = unemp.set_index("ym").sort_index()
    latest_u = u.index.max()
    prev_m = latest_u - pd.offsets.MonthBegin(1)
    prev_y = (latest_u - pd.DateOffset(years=1)).to_period("M").to_timestamp()
    u_latest = float(u.loc[latest_u, "unemployment_rate"])
    u_mom = (u_latest - float(u.loc[prev_m, "unemployment_rate"])) if prev_m in u.index else None
    u_yoy = (u_latest - float(u.loc[prev_y, "unemployment_rate"])) if prev_y in u.index else None
    kpi[0].metric(
        "Unemployment rate (latest)",
        f"{u_latest:.1f}%",
        (f"{u_mom:+.1f} pp MoM" if u_mom is not None else "—")
        + (f" · {u_yoy:+.1f} pp YoY" if u_yoy is not None else ""),
    )

    spx = equities[equities["ticker"] == SPX].copy().sort_values("ym")
    r_spx = spx["monthly_return"].dropna()
    r1 = r_spx.iloc[-1] if len(r_spx) >= 1 else None
    r3 = pct_window(r_spx, 3)
    r12 = pct_window(r_spx, 12)
    kpi[1].metric(
        f"{SPX} momentum",
        f"{(r1*100):+.1f}%" if r1 is not None else "—",
        f"3m {(r3*100):+.1f}% · 12m {(r12*100):+.1f}%" if (r3 is not None and r12 is not None) else "—",
    )

    etf = equities[equities["ticker"] == sector_etf].copy().sort_values("ym")
    r_etf = etf["monthly_return"].dropna()
    r1_e = r_etf.iloc[-1] if len(r_etf) >= 1 else None
    r3_e = pct_window(r_etf, 3)
    r12_e = pct_window(r_etf, 12)

    sj = sector_jobs.sort_values("ym").copy()
    sj["emp_pct_change"] = sj["employment_thousands"].pct_change()
    corr_df = pd.merge(etf[["ym", "monthly_return"]], sj[["ym", "emp_pct_change"]], on="ym", how="inner").dropna()
    corr_val = corr_df["monthly_return"].corr(corr_df["emp_pct_change"]) if len(corr_df) > 2 else None

    kpi[2].metric(
        f"{sector_etf} momentum · jobs corr",
        f"{(r1_e*100):+.1f}%" if r1_e is not None else "—",
        (
            f"3m {(r3_e*100):+.1f}% · 12m {(r12_e*100):+.1f}%"
            + (f" · corr {corr_val:+.2f}" if corr_val is not None else " · corr —")
        )
        if (r3_e is not None and r12_e is not None)
        else "—",
    )

# --------- Plots ---------
col1, col2 = st.columns(2)

if ok_u and ok_e:
    spx = equities[equities["ticker"] == SPX].copy()
    if not spx.empty:
        spx["spx_indexed"] = (spx["adj_close"] / spx["adj_close"].iloc[0]) * 100.0
        df_a = unemp.merge(spx[["ym", "spx_indexed"]], on="ym", how="inner")
        if not df_a.empty:
            fig = px.line(
                df_a,
                x="ym",
                y=["unemployment_rate", "spx_indexed"],
                labels={"value": "Value", "ym": "Month", "variable": "Series"},
                title=f"Unemployment vs {SPX} (both monthly; {start} → {end})",
            )
            col1.plotly_chart(fig, use_container_width=True)
        else:
            col1.info("No overlapping months for unemployment and S&P.")

if ok_s and ok_e:
    etf = equities[equities["ticker"] == sector_etf].copy()
    if not etf.empty:
        etf["etf_indexed"] = (etf["adj_close"] / etf["adj_close"].iloc[0]) * 100.0
        df_b = sector_jobs.merge(etf[["ym", "etf_indexed"]], on="ym", how="inner")
        if not df_b.empty:
            fig2 = px.line(
                df_b,
                x="ym",
                y=["employment_thousands", "etf_indexed"],
                labels={"value": "Value", "ym": "Month", "variable": "Series"},
                title=f"{sector_name} Jobs (k) vs {sector_etf} (indexed to 100; {start} → {end})",
            )
            col2.plotly_chart(fig2, use_container_width=True)
        else:
            col2.info("No overlapping months for sector jobs and ETF.")

# --------- Correlation snapshot ---------
st.subheader("Correlation snapshot (monthly)")
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
          corr(etf.etf_return, u.unemployment_rate) as corr_etf_unemp
        from spx join u using (ym)
        join etf using (ym)
        where ym between :start and :end
        """,
        params,
    )
    st.write(corr)
else:
    st.info("Not enough data for correlation in the selected window.")

st.caption(
    "All series aligned by month (date_trunc('month')) to avoid day mismatches. "
    "‘Momentum’ is 1m/3m/12m total return."
)