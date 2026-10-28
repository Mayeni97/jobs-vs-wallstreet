-- Run this once in the Supabase SQL Editor before loading the CSV data.

create table if not exists public.equities_monthly (
    period_date date not null,
    ticker text not null,
    adj_close double precision not null,
    monthly_return double precision,
    primary key (period_date, ticker)
);

create table if not exists public.unemployment_headline (
    period_date date primary key,
    unemployment_rate double precision not null
);

create table if not exists public.employment_sector (
    period_date date not null,
    sector_code text not null,
    sector_name text not null,
    employment_thousands double precision not null,
    primary key (period_date, sector_code)
);

create index if not exists equities_monthly_ticker_date_idx
    on public.equities_monthly (ticker, period_date);

create index if not exists employment_sector_name_date_idx
    on public.employment_sector (sector_name, period_date);
