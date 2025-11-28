# FamaFrenchMonthly_Live.py
# ABOUTME: Recreates monthly Fama-French-style factors (MKT-RF, SMB, HML, UMD)
#          using free live data and previously built FF-style portfolio assignments.
"""
Inputs:
- ../pyData/Static/ff3_portfolios.csv
    Columns:
        - ticker
        - size_port in {S, B}
        - bm_port   in {L, M, H}
        - mom_port  in {L, M, W}
        - formation_date (last trading day of the June formation year)

External live data fetched at runtime:
- Yahoo Finance (prices) via yfinance
- FRED 3-month T-bill rate (TB3MS) via pandas_datareader (if installed)

Outputs:
- ../pyData/Intermediate/monthlyFF.parquet
    Columns:
        - time_avail_m (first day of month, datetime64)
        - mktrf
        - smb
        - hml
        - rf
        - umd

How to run:
    python3 FamaFrenchMonthly_Live.py

Notes:
- Uses the current ff3_portfolios.csv assignment for all months it computes.
  You should rebuild ff3_portfolios.csv once per year after the June rebalance
  using BuildFF3Portfolios_FromFreeData.py.
- For each month t, monthly returns are based on last trading day of t
  vs last trading day of t-1. Value weights use lagged month-end market caps.
"""

import os
import sys
import datetime as dt

import numpy as np
import pandas as pd
import yfinance as yf

try:
    from pandas_datareader import data as pdr
    HAS_FRED = True
except ImportError:
    HAS_FRED = False

from dotenv import load_dotenv

# Make parent dir importable for config
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
try:
    from config import MAX_ROWS_DL
except Exception:
    MAX_ROWS_DL = 0  # fallback if not present

load_dotenv()

print("=" * 60, flush=True)
print("📈 FamaFrenchMonthly_Live.py - Monthly Fama-French-Style Factors", flush=True)
print("=" * 60, flush=True)

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------

PORTFOLIO_FILE = "../pyData/Static/ff3_portfolios.csv"
OUTPUT_FILE = "../pyData/Intermediate/monthlyFF.parquet"

# How far back to build history if file doesn't exist
DEFAULT_START_DATE = "2015-01-01"

ROW_LIMIT = MAX_ROWS_DL if MAX_ROWS_DL and MAX_ROWS_DL > 0 else None


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

def log(msg: str):
    print(msg, flush=True)


def load_portfolio_assignments(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    required = {"ticker", "size_port", "bm_port", "mom_port"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Portfolio file missing columns: {missing}")
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    # formation_date is not strictly required but nice to have
    if "formation_date" in df.columns:
        df["formation_date"] = pd.to_datetime(df["formation_date"]).dt.date
    return df


def determine_month_range(output_path: str, default_start: str) -> tuple[dt.date, dt.date]:
    """
    Determine [start_date, end_date] for which we need to compute monthly factors.
    start_date is a calendar date; we'll convert to month periods later.
    """
    today = dt.date.today()
    # We only compute up through the last completed month
    end_date = (today.replace(day=1) - dt.timedelta(days=1))  # last day of prior month

    if os.path.exists(output_path):
        existing = pd.read_parquet(output_path)
        last_ts = existing["time_avail_m"].max()
        last_month = pd.Period(last_ts, freq="M")
        # Start from month after last
        next_month_start = (last_month + 1).to_timestamp().date()
        log(f"Appending monthly factors from {next_month_start} to {end_date}")
        return next_month_start, end_date
    else:
        start_date = dt.datetime.strptime(default_start, "%Y-%m-%d").date()
        log(f"Building monthly factors from scratch: {start_date} to {end_date}")
        return start_date, end_date


def download_daily_prices(tickers, start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    """
    Download daily adjusted close prices for tickers from Yahoo Finance.
    Returns long DataFrame: [time_d, ticker, adj_close].
    """
    if len(tickers) == 0:
        raise ValueError("No tickers in portfolio assignments.")

    log(f"Downloading daily prices for {len(tickers)} tickers from {start_date} to {end_date}...")
    data = yf.download(
        tickers=tickers,
        start=start_date,
        end=end_date + dt.timedelta(days=1),
        auto_adjust=True,
        group_by="ticker",
        progress=False,
        threads=True,
    )

    if isinstance(data.columns, pd.MultiIndex):
        closes = {}
        for t in tickers:
            if (t, "Adj Close") in data.columns:
                closes[t] = data[(t, "Adj Close")]
        close_df = pd.DataFrame(closes)
    else:
        close_df = data["Adj Close"].to_frame()
        close_df.columns = tickers

    close_df = close_df.dropna(how="all")
    close_df.index.name = "time_d"

    long = close_df.stack().reset_index()
    long.columns = ["time_d", "ticker", "adj_close"]
    long["ticker"] = long["ticker"].astype(str).str.upper()
    long["time_d"] = pd.to_datetime(long["time_d"]).dt.date
    return long


def get_shares_outstanding(tickers) -> dict:
    """
    Approximate current shares outstanding using yfinance.
    Used as proxy for historical shares for value weights.
    """
    shares = {}
    log("Fetching shares outstanding from Yahoo (approximate)...")
    for t in tickers:
        so = None
        try:
            info = yf.Ticker(t).fast_info
            if hasattr(info, "get"):
                so = info.get("shares_outstanding", None)
                if so is None:
                    so = info.get("sharesOutstanding", None)
        except Exception:
            so = None
        shares[t] = so
    return shares


def compute_monthly_returns_and_caps(price_df: pd.DataFrame, shares_map: dict) -> pd.DataFrame:
    """
    Given daily prices [time_d, ticker, adj_close], compute:
      - month_end price per ticker
      - monthly return
      - month-end market cap (adj_close * shares)
      - lagged market cap (used for value weights)
    Returns DataFrame:
      [ticker, month, time_m_end, ret_m, mktcap, mktcap_lag]
    where month is a pandas.Period('M'), time_m_end is last trading day of month.
    """
    df = price_df.copy()
    df["month"] = df["time_d"].apply(lambda d: pd.Period(d, freq="M"))

    # Get last trading day and price within each month per ticker
    df = df.sort_values(["ticker", "time_d"])
    last_px = df.groupby(["ticker", "month"], as_index=False).tail(1)
    last_px = last_px.rename(columns={"time_d": "time_m_end", "adj_close": "px_end"})

    # Sort by ticker, month, compute monthly returns from px_end
    last_px = last_px.sort_values(["ticker", "month"])
    last_px["px_end_lag"] = last_px.groupby("ticker")["px_end"].shift(1)
    last_px["ret_m"] = last_px["px_end"] / last_px["px_end_lag"] - 1.0
    # drop first month where we can't compute ret
    last_px = last_px.dropna(subset=["ret_m"])

    # Market caps at month end (using current shares as proxy)
    last_px["shares_out"] = last_px["ticker"].map(shares_map).astype("float64")
    last_px["mktcap"] = last_px["px_end"] * last_px["shares_out"]

    # Lagged market caps for value weights
    last_px["mktcap_lag"] = last_px.groupby("ticker")["mktcap"].shift(1)
    last_px = last_px.dropna(subset=["mktcap_lag"])

    return last_px[["ticker", "month", "time_m_end", "ret_m", "mktcap", "mktcap_lag"]]


def fetch_monthly_rf(start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    """
    Fetch monthly risk-free rate from FRED (3-month T-bill TB3MS).
    Convert to simple monthly rate (approx TB3MS / 100 / 12).
    If FRED is not available, returns RF = 0.
    """
    if not HAS_FRED:
        log("pandas_datareader not installed; setting RF = 0 for all months.")
        months = pd.period_range(start=start_date, end=end_date, freq="M")
        rf = pd.DataFrame(
            {
                "month": months,
                "rf": 0.0,
            }
        )
        return rf

    # Expand a little earlier
    start = start_date - dt.timedelta(days=365)
    log("Fetching TB3MS from FRED for RF estimation...")
    tbill = pdr.DataReader("TB3MS", "fred", start, end_date)
    tbill = tbill.rename(columns={"TB3MS": "tb3ms"})
    tbill.index = pd.to_datetime(tbill.index)
    tbill["month"] = tbill.index.to_period("M")

    # Take last observation per month and convert to simple monthly rate
    tbill_monthly = tbill.groupby("month", as_index=False).tail(1)
    tbill_monthly["rf"] = (tbill_monthly["tb3ms"] / 100.0) / 12.0

    # Restrict to requested months
    months = pd.period_range(start=start_date, end=end_date, freq="M")
    rf = (
        pd.DataFrame({"month": months})
        .merge(tbill_monthly[["month", "rf"]], on="month", how="left")
        .sort_values("month")
    )
    rf["rf"] = rf["rf"].fillna(method="ffill").fillna(0.0)
    return rf


def value_weighted_return(df: pd.DataFrame, ret_col="ret_m", weight_col="mktcap_lag") -> float:
    """
    Compute value-weighted portfolio return for a given month.
    If weights are all missing or zero, fall back to equal-weight.
    """
    df = df.dropna(subset=[ret_col])
    if df.empty:
        return np.nan

    r = df[ret_col].values
    w = df[weight_col].fillna(0.0).values

    if np.all(w <= 0) or np.isnan(w).all():
        return float(np.nanmean(r))
    else:
        w = np.where(w < 0, 0, w)
        if w.sum() == 0:
            return float(np.nanmean(r))
        return float(np.average(r, weights=w))


def compute_factors_for_month(month_df: pd.DataFrame, port_assign: pd.DataFrame) -> dict:
    """
    Compute MKT, SMB, HML, UMD for a single month.
    month_df: [ticker, ret_m, mktcap_lag]
    port_assign: [ticker, size_port, bm_port, mom_port]
    """
    merged = month_df.merge(port_assign, on="ticker", how="inner")

    # Market return (value-weighted)
    r_mkt = value_weighted_return(merged, "ret_m", "mktcap_lag")

    # 2x3 size x value portfolios
    ports_sv = {}
    for size in ["S", "B"]:
        for bm in ["L", "M", "H"]:
            key = f"{size}{bm}"
            ports_sv[key] = merged[
                (merged["size_port"] == size) & (merged["bm_port"] == bm)
            ]

    r_SL = value_weighted_return(ports_sv["SL"])
    r_SM = value_weighted_return(ports_sv["SM"])
    r_SH = value_weighted_return(ports_sv["SH"])
    r_BL = value_weighted_return(ports_sv["BL"])
    r_BM = value_weighted_return(ports_sv["BM"])
    r_BH = value_weighted_return(ports_sv["BH"])

    # SMB (size factor)
    smb = ((r_SL + r_SM + r_SH) / 3.0) - ((r_BL + r_BM + r_BH) / 3.0)

    # HML (value factor)
    hml = 0.5 * (r_SH + r_BH) - 0.5 * (r_SL + r_BL)

    # Momentum portfolios: size x mom_port (L, M, W)
    ports_sm = {}
    for size in ["S", "B"]:
        for mom in ["L", "M", "W"]:
            key = f"{size}{mom}"
            ports_sm[key] = merged[
                (merged["size_port"] == size) & (merged["mom_port"] == mom)
            ]

    r_SLm = value_weighted_return(ports_sm["SL"])
    r_SMm = value_weighted_return(ports_sm["SM"])
    r_SWm = value_weighted_return(ports_sm["SW"])
    r_BLm = value_weighted_return(ports_sm["BL"])
    r_BMm = value_weighted_return(ports_sm["BM"])
    r_BWm = value_weighted_return(ports_sm["BW"])

    # UMD (momentum factor: winners minus losers)
    umd = 0.5 * (r_SWm + r_BWm) - 0.5 * (r_SLm + r_BLm)

    return {
        "mkt": r_mkt,
        "smb": smb,
        "hml": hml,
        "umd": umd,
    }


# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------

def main():
    # 1. Load portfolio assignments
    port_assign = load_portfolio_assignments(PORTFOLIO_FILE)
    tickers = port_assign["ticker"].unique().tolist()
    log(f"Loaded portfolio assignments for {len(tickers)} tickers.")

    # 2. Determine monthly date range
    start_date, end_date = determine_month_range(OUTPUT_FILE, DEFAULT_START_DATE)
    if start_date > end_date:
        log("No new months to compute. Exiting.")
        return

    # 3. Download daily prices with one month cushion before start_date
    #    so we can compute the first month's return correctly.
    start_date_extended = (start_date - dt.timedelta(days=40))
    price_daily = download_daily_prices(tickers, start_date_extended, end_date)
    if price_daily.empty:
        raise ValueError("No price data downloaded; check dates/universe.")

    # 4. Get shares and compute monthly returns + lagged market caps
    shares_map = get_shares_outstanding(tickers)
    monthly = compute_monthly_returns_and_caps(price_daily, shares_map)

    # Filter to months within [start_date, end_date]
    first_month = pd.Period(start_date, freq="M")
    last_month = pd.Period(end_date, freq="M")
    monthly = monthly[
        (monthly["month"] >= first_month) & (monthly["month"] <= last_month)
    ].copy()

    if monthly.empty:
        log("No monthly returns in the requested range.")
        return

    # Optional debug: limit number of months
    if ROW_LIMIT and ROW_LIMIT > 0:
        unique_months = sorted(monthly["month"].unique())
        if len(unique_months) > ROW_LIMIT:
            cutoff = unique_months[ROW_LIMIT - 1]
            monthly = monthly[monthly["month"] <= cutoff]
            log(f"DEBUG MODE: Limiting to first {ROW_LIMIT} months (<= {cutoff}).")

    # 5. Fetch monthly RF
    rf_df = fetch_monthly_rf(
        monthly["time_m_end"].min(), monthly["time_m_end"].max()
    )

    # 6. Compute factors month by month
    factors = []
    for month, mdf in monthly.groupby("month"):
        fac = compute_factors_for_month(mdf.copy(), port_assign)
        rf_row = rf_df[rf_df["month"] == month]
        rf_val = float(rf_row["rf"].iloc[0]) if not rf_row.empty else 0.0

        # time_avail_m: first day of that month (to mimic your WRDS script)
        time_avail_m = month.to_timestamp()  # period -> first day of month

        factors.append(
            {
                "time_avail_m": time_avail_m,
                "mktrf": fac["mkt"] - rf_val,
                "smb": fac["smb"],
                "hml": fac["hml"],
                "rf": rf_val,
                "umd": fac["umd"],
            }
        )

    ff_monthly = pd.DataFrame(factors).sort_values("time_avail_m")

    # 7. Append to existing parquet, if any
    if os.path.exists(OUTPUT_FILE):
        existing = pd.read_parquet(OUTPUT_FILE)
        ff_monthly = pd.concat([existing, ff_monthly], ignore_index=True)
        ff_monthly = ff_monthly.drop_duplicates(subset=["time_avail_m"]).sort_values("time_avail_m")

    # 8. Save
    ff_monthly["time_avail_m"] = pd.to_datetime(ff_monthly["time_avail_m"])
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    ff_monthly.to_parquet(OUTPUT_FILE)

    log(f"Monthly FF-style factors computed with {len(ff_monthly)} records.")
    log(
        f"Date range: {ff_monthly['time_avail_m'].min().strftime('%Y-%m-%d')} "
        f"to {ff_monthly['time_avail_m'].max().strftime('%Y-%m-%d')}"
    )
    log("\nSample data (tail):")
    print(ff_monthly.tail().to_string(index=False), flush=True)

    print("=" * 60, flush=True)
    print("✅ FamaFrenchMonthly_Live.py completed successfully", flush=True)
    print("=" * 60, flush=True)


if __name__ == "__main__":
    main()
