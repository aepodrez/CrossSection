# FamaFrenchDaily_Live.py
# ABOUTME: Recreates daily Fama-French-style factors using free live data
"""
Inputs:
- ../pyData/Static/ff3_portfolios.csv
  (ticker, size_port {S,B}, bm_port {L,M,H}, mom_port {L,M,W})

- Free data sources at runtime:
  - Yahoo Finance (prices)
  - FRED (risk-free rate, optional)

Outputs:
- ../pyData/Intermediate/dailyFF.parquet

How to run (after market close):
    python3 FamaFrenchDaily_Live.py

Notes:
- This approximates Ken French’s methodology using your own universe
- You must maintain the portfolio assignments file separately (see helper script)
"""

import os
import sys
import datetime as dt

import numpy as np
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

try:
    from pandas_datareader import data as pdr   # for RF from FRED
    HAS_FRED = True
except ImportError:
    HAS_FRED = False

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
try:
    from config import MAX_ROWS_DL
except Exception:
    MAX_ROWS_DL = 0  # fallback if not present

print("=" * 60, flush=True)
print("📈 FamaFrenchDaily_Live.py - Daily Live Fama-French-Style Factors", flush=True)
print("=" * 60, flush=True)

load_dotenv()

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------

PORTFOLIO_FILE = "../pyData/Static/ff3_portfolios.csv"
OUTPUT_FILE = "../pyData/Intermediate/dailyFF.parquet"

# How far back to build history if OUTPUT_FILE doesn't exist yet
DEFAULT_START_DATE = "2015-01-01"

# If you want to limit rows for debugging (similar to WRDS script)
ROW_LIMIT = MAX_ROWS_DL if MAX_ROWS_DL and MAX_ROWS_DL > 0 else None

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

def load_portfolio_assignments(path: str) -> pd.DataFrame:
    """Load FF-style portfolio assignments (static for the current June–June year)."""
    df = pd.read_csv(path)
    required_cols = {"ticker", "size_port", "bm_port", "mom_port"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Portfolio file missing columns: {missing}")
    df["ticker"] = df["ticker"].str.upper().str.strip()
    return df


def determine_date_range(output_path: str, default_start: str) -> tuple[dt.date, dt.date]:
    """Figure out [start, end] dates we still need to compute."""
    today = dt.date.today()
    # We want to include yesterday's close at minimum
    end_date = today

    if os.path.exists(output_path):
        existing = pd.read_parquet(output_path)
        last_date = existing["time_d"].max().date()
        start_date = last_date + dt.timedelta(days=1)
        print(f"Appending factors from {start_date} to {end_date}", flush=True)
        return start_date, end_date
    else:
        start_date = dt.datetime.strptime(default_start, "%Y-%m-%d").date()
        print(f"Building factors from scratch: {start_date} to {end_date}", flush=True)
        return start_date, end_date


def download_price_history(tickers, start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    """
    Download adjusted close prices for tickers from Yahoo Finance.
    Returns a long DataFrame: [time_d, ticker, adj_close].
    """
    if len(tickers) == 0:
        raise ValueError("No tickers in portfolio file.")

    print(f"Downloading prices from Yahoo Finance for {len(tickers)} tickers...", flush=True)
    data = yf.download(
        tickers=tickers,
        start=start_date,
        end=end_date + dt.timedelta(days=1),  # yfinance end is exclusive
        auto_adjust=True,
        group_by="ticker",
        progress=False,
        threads=True,
    )

    # yfinance shape depends on single vs multi ticker; normalize
    # We will extract a panel: index = date, columns = tickers (Adj Close)
    if isinstance(data.columns, pd.MultiIndex):
        closes = {}
        for t in tickers:
            if (t, "Adj Close") in data.columns:
                closes[t] = data[(t, "Adj Close")]
        close_df = pd.DataFrame(closes)
    else:
        # Single ticker
        close_df = data["Adj Close"].to_frame()
        close_df.columns = tickers

    close_df = close_df.dropna(how="all")
    close_df.index.name = "time_d"

    # Melt to long form for joining later
    long = close_df.stack().reset_index()
    long.columns = ["time_d", "ticker", "adj_close"]
    long["ticker"] = long["ticker"].astype(str).str.upper()
    long["time_d"] = pd.to_datetime(long["time_d"]).dt.date
    return long


def compute_daily_returns(price_df: pd.DataFrame) -> pd.DataFrame:
    """
    Given [time_d, ticker, adj_close], compute simple daily returns by ticker.
    """
    price_df = price_df.sort_values(["ticker", "time_d"])
    price_df["ret"] = price_df.groupby("ticker")["adj_close"].pct_change()
    price_df = price_df.dropna(subset=["ret"])
    return price_df[["time_d", "ticker", "adj_close", "ret"]]


def get_shares_outstanding(tickers) -> dict:
    """
    Fetch a *current* estimate of shares outstanding from Yahoo Finance.
    Used as a proxy for historical shares for value weights.
    """
    shares = {}
    print("Fetching shares outstanding (approximate, current) from Yahoo...", flush=True)
    for t in tickers:
        try:
            info = yf.Ticker(t).fast_info
        except Exception:
            info = {}
        so = None
        for key in ["shares_outstanding", "sharesOutstanding"]:
            if hasattr(info, "get"):
                so = info.get(key, None)
            else:
                so = None
            if so is not None:
                break
        shares[t] = so  # may be None
    return shares


def compute_market_caps(price_df: pd.DataFrame, shares_map: dict) -> pd.DataFrame:
    """
    Approximate daily market caps = adj_close * current shares_outstanding.
    This is not CRSP-grade, but good enough for live factor approximation.
    """
    price_df = price_df.copy()
    price_df["shares_out"] = price_df["ticker"].map(shares_map).astype("float64")
    # If shares missing for some ticker, fallback to equal weighting inside portfolios
    price_df["mktcap"] = price_df["adj_close"] * price_df["shares_out"]
    return price_df


def fetch_risk_free(start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    """
    Fetch risk-free rate from FRED (3-month T-bill, TB3MS) and convert to daily.
    If FRED not available, returns RF = 0.
    """
    if not HAS_FRED:
        print("pandas_datareader not installed; setting RF = 0", flush=True)
        rf = pd.DataFrame(
            {
                "time_d": pd.date_range(start=start_date, end=end_date, freq="D").date,
                "rf": 0.0,
            }
        )
        return rf

    print("Downloading risk-free rate from FRED (TB3MS)...", flush=True)
    start = start_date - dt.timedelta(days=365)
    tbill = pdr.DataReader("TB3MS", "fred", start, end_date)
    tbill = tbill.rename(columns={"TB3MS": "tb3ms"})
    tbill.index = tbill.index.date
    tbill = tbill.dropna()

    # Convert annualized percentage (approx monthly) to daily simple rate
    # e.g., 5% -> 0.05 / 252
    tbill["rf_daily"] = (tbill["tb3ms"] / 100.0) / 252.0

    # Forward-fill within month and across days
    all_days = pd.date_range(start=start_date, end=end_date, freq="D").date
    rf_daily = (
        pd.DataFrame({"time_d": all_days})
        .merge(
            tbill[["rf_daily"]].reset_index().rename(columns={"index": "time_d"}),
            on="time_d",
            how="left",
        )
        .sort_values("time_d")
    )
    rf_daily["rf_daily"] = rf_daily["rf_daily"].ffill().fillna(0.0)
    rf_daily = rf_daily.rename(columns={"rf_daily": "rf"})
    return rf_daily


# ------------------------------------------------------------------------------
# Factor construction
# ------------------------------------------------------------------------------

def value_weighted_return(df: pd.DataFrame, ret_col="ret", weight_col="mktcap") -> float:
    """
    Compute value-weighted portfolio return.
    If all weights are missing/zero, fall back to equal-weight.
    """
    df = df.dropna(subset=[ret_col])
    if df.empty:
        return np.nan

    w = df[weight_col].fillna(0.0).values
    r = df[ret_col].values

    if np.all(w <= 0) or np.isnan(w).all():
        # equal weight
        return r.mean()
    else:
        w = np.where(w < 0, 0, w)
        if w.sum() == 0:
            return r.mean()
        return np.average(r, weights=w)


def compute_factors_for_day(day_df: pd.DataFrame, port_assign: pd.DataFrame) -> dict:
    """
    Compute MKT, SMB, HML, UMD for a single date.
    day_df: [ticker, ret, mktcap]
    port_assign: [ticker, size_port, bm_port, mom_port]
    """
    merged = day_df.merge(port_assign, on="ticker", how="inner")

    # Market return: value-weighted over all stocks
    r_mkt = value_weighted_return(merged, "ret", "mktcap")

    # 2x3 size x value portfolios: S/B x L/M/H
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

    # SMB (size factor) per FF3 2x3 definition
    smb = ((r_SL + r_SM + r_SH) / 3.0) - ((r_BL + r_BM + r_BH) / 3.0)

    # HML (value factor)
    hml = 0.5 * (r_SH + r_BH) - 0.5 * (r_SL + r_BL)

    # Momentum 2x3: size x mom_port (L, M, W)
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

    # UMD (winners minus losers)
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
    # Load assignments
    port_assign = load_portfolio_assignments(PORTFOLIO_FILE)
    tickers = port_assign["ticker"].unique().tolist()
    print(f"Loaded {len(tickers)} tickers from portfolio assignment file.", flush=True)

    # Determine date range
    start_date, end_date = determine_date_range(OUTPUT_FILE, DEFAULT_START_DATE)
    if start_date > end_date:
        print("No new dates to compute. Exiting.", flush=True)
        return

    # Download prices & compute daily returns
    price_raw = download_price_history(tickers, start_date, end_date)
    price_ret = compute_daily_returns(price_raw)

    if ROW_LIMIT is not None and ROW_LIMIT > 0:
        # limit by unique dates for debugging
        unique_dates = sorted(price_ret["time_d"].unique())
        if len(unique_dates) > ROW_LIMIT:
            cutoff = unique_dates[ROW_LIMIT - 1]
            price_ret = price_ret[price_ret["time_d"] <= cutoff]
            print(f"DEBUG MODE: Limiting to first {ROW_LIMIT} trading days.", flush=True)

    # Market caps (approximate)
    shares_map = get_shares_outstanding(tickers)
    price_ret = compute_market_caps(price_ret, shares_map)

    # Risk-free rates
    rf_df = fetch_risk_free(price_ret["time_d"].min(), price_ret["time_d"].max())

    # Compute factors day by day
    factors = []
    for day, day_df in price_ret.groupby("time_d"):
        fac = compute_factors_for_day(day_df.copy(), port_assign)
        rf_row = rf_df[rf_df["time_d"] == day]
        rf_val = float(rf_row["rf"].iloc[0]) if not rf_row.empty else 0.0

        factors.append(
            {
                "time_d": day,
                "mktrf": fac["mkt"] - rf_val,
                "smb": fac["smb"],
                "hml": fac["hml"],
                "rf": rf_val,
                "umd": fac["umd"],
            }
        )

    ff_daily = pd.DataFrame(factors).sort_values("time_d")

    # Append to existing file if present
    if os.path.exists(OUTPUT_FILE):
        existing = pd.read_parquet(OUTPUT_FILE)
        ff_daily = pd.concat([existing, ff_daily], ignore_index=True)
        ff_daily = ff_daily.drop_duplicates(subset=["time_d"]).sort_values("time_d")

    # Save to parquet
    ff_daily["time_d"] = pd.to_datetime(ff_daily["time_d"])
    ff_daily.to_parquet(OUTPUT_FILE)

    print(f"Daily live FF-style factors computed with {len(ff_daily)} records", flush=True)
    print(
        f"Date range: {ff_daily['time_d'].min().strftime('%Y-%m-%d')} "
        f"to {ff_daily['time_d'].max().strftime('%Y-%m-%d')}",
        flush=True,
    )
    print("\nSample data:", flush=True)
    print(ff_daily.tail().to_string(index=False), flush=True)
    print("=" * 60, flush=True)
    print("✅ FamaFrenchDaily_Live.py completed successfully", flush=True)
    print("=" * 60, flush=True)


if __name__ == "__main__":
    main()
