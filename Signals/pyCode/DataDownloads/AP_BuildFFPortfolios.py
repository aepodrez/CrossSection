# BuildFF3Portfolios_FromFreeData.py
# ABOUTME: Builds Fama-French-style size/value/momentum portfolios using free data, including
#          Book Equity (BE) fetched directly from SEC EDGAR (companyfacts API).
"""
Inputs:
- ../pyData/Static/universe.csv
    Required columns:
        - ticker (e.g., AAPL, MSFT)
    Optional:
        - is_nyse (1 if NYSE, 0 otherwise). If missing, all treated as NYSE.

External data sources (fetched inside this script):
- SEC company tickers:
    https://www.sec.gov/files/company_tickers.json
- SEC companyfacts per CIK:
    https://data.sec.gov/api/xbrl/companyfacts/CIK##########.json
- Yahoo Finance for prices & shares outstanding

Outputs:
- ../pyData/Static/ff3_portfolios.csv
    Columns:
        - ticker
        - formation_date (last trading day of June)
        - size_port  in {S, B}
        - bm_port    in {L, M, H}
        - mom_port   in {L, M, W}

How to run:
    python3 BuildFF3Portfolios_FromFreeData.py

Notes:
- Uses a Compustat-style BE = SE + TXDITC - PS, where:
    SE ≈ StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest
         or StockholdersEquity if needed
    TXDITC ≈ DeferredTaxAndOtherLiabilities / DeferredIncomeTaxesAndInvestmentTaxCredit etc.
    PS ≈ PreferredStockRedemptionValue / PreferredStock etc.
- Applies a 6-month lag to BE relative to the June formation date.
- SEC requires a good User-Agent; set SEC_EMAIL env var to your email.
"""

import os
import sys
import json
import time
import math
import datetime as dt
from typing import Optional, Tuple, List

import numpy as np
import pandas as pd
import requests
import yfinance as yf
from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
try:
    from config import MAX_ROWS_DL
except Exception:
    MAX_ROWS_DL = 0  # fallback if not present

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------

UNIVERSE_FILE = "../../pyData/Static/universe.csv"  # Fallback if pickle not available
SP500_UNIVERSE_PKL = "../../pyData/Static/sp500_universe.pkl"
COMPANY_TICKERS_FILE = "../../pyData/Static/company_tickers.json"  # local cache
EDGAR_COMPANYFACTS_DIR = "../../pyData/EDGAR/companyfacts"         # local cache dir
OUTPUT_FILE = "../../pyData/Static/ff3_portfolios.csv"

VERBOSE = True
ROW_LIMIT = MAX_ROWS_DL if MAX_ROWS_DL and MAX_ROWS_DL > 0 else None

# Rate limiting for SEC
SEC_SLEEP_SEC = 0.25  # 4 requests/sec max

load_dotenv()
SEC_EMAIL = os.getenv("SEC_EMAIL", "your_email@example.com")  # please set in .env


# ------------------------------------------------------------------------------
# Logging helper
# ------------------------------------------------------------------------------

def log(msg: str):
    if VERBOSE:
        print(msg, flush=True)


# ------------------------------------------------------------------------------
# Date helpers
# ------------------------------------------------------------------------------

def most_recent_june(today: Optional[dt.date] = None) -> dt.date:
    """
    Return the most recent June 30 (calendar date).
    """
    if today is None:
        today = dt.date.today()
    year = today.year
    if today.month < 6:
        year -= 1
    return dt.date(year, 6, 30)


# ------------------------------------------------------------------------------
# Universe loading
# ------------------------------------------------------------------------------

def load_universe(path: str = None) -> pd.DataFrame:
    """
    Load ticker universe. Tries pickle file first, then CSV fallback.
    Creates DataFrame with ticker and is_nyse columns.
    """
    # First, try loading from pickle file
    import pickle
    from pathlib import Path
    if Path(SP500_UNIVERSE_PKL).exists():
        try:
            with open(SP500_UNIVERSE_PKL, 'rb') as f:
                tickers = pickle.load(f)
            log(f"Loaded {len(tickers)} tickers from sp500_universe.pkl")
            # Create DataFrame with ticker column
            df = pd.DataFrame({'ticker': tickers})
            df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
            # Set is_nyse: default to 1 (NYSE) for all, can be refined later
            # For now, we'll use a simple heuristic or default to NYSE
            df["is_nyse"] = 1  # Default to NYSE for breakpoint calculation
            log("Set is_nyse=1 for all tickers (defaulting to NYSE for breakpoints)")
            return df
        except Exception as e:
            log(f"⚠️  Could not load from pickle file: {e}")
            log("Falling back to CSV file...")
    
    # Fallback to CSV file if pickle doesn't exist or fails
    if path is None:
        path = UNIVERSE_FILE
    
    if not Path(path).exists():
        raise FileNotFoundError(
            f"Universe file not found: {path}\n"
            f"Please ensure {SP500_UNIVERSE_PKL} exists or create {path}"
        )
    
    df = pd.read_csv(path)
    if "ticker" not in df.columns:
        raise ValueError("universe.csv must contain column 'ticker'")
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    if "is_nyse" not in df.columns:
        log("Column 'is_nyse' not found; treating all as NYSE for breakpoints.")
        df["is_nyse"] = 1
    else:
        df["is_nyse"] = df["is_nyse"].fillna(0).astype(int)
    return df


# ------------------------------------------------------------------------------
# SEC company tickers mapping (ticker -> CIK)
# ------------------------------------------------------------------------------

def get_sec_session() -> requests.Session:
    s = requests.Session()
    ua = f"FFLiveFactorScript/1.0 ({SEC_EMAIL})"
    s.headers.update({
        "User-Agent": ua,
        "Accept-Encoding": "gzip, deflate",
        "Accept": "application/json"
    })
    return s


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def download_company_tickers(session: requests.Session) -> dict:
    """
    Download SEC company_tickers.json and return mapping {ticker: cik10str}.
    """
    url = "https://www.sec.gov/files/company_tickers.json"
    log(f"Downloading SEC company_tickers.json from {url}")
    r = session.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()

    mapping = {}
    # data is of form {0: {...}, 1: {...}, ...} or list of dicts
    # Handle both old format (dict) and new format (list)
    items = data.items() if isinstance(data, dict) else enumerate(data)
    
    for _, row in items:
        if not isinstance(row, dict):
            continue
        ticker = row.get("ticker", "").upper().strip()
        # Handle both 'cik_str' and 'cik' keys
        cik = row.get("cik_str") or row.get("cik")
        if not ticker or cik is None:
            continue
        # Convert to 10-digit string format
        try:
            cik_str = str(int(cik)).zfill(10)
        except (ValueError, TypeError):
            continue
        mapping[ticker] = cik_str
    return mapping


def load_ticker_to_cik(session: requests.Session) -> dict:
    ensure_dir(os.path.dirname(COMPANY_TICKERS_FILE))
    if os.path.exists(COMPANY_TICKERS_FILE):
        log(f"Loading cached SEC company tickers from {COMPANY_TICKERS_FILE}")
        with open(COMPANY_TICKERS_FILE, "r") as f:
            data = json.load(f)
        return data
    mapping = download_company_tickers(session)
    with open(COMPANY_TICKERS_FILE, "w") as f:
        json.dump(mapping, f)
    return mapping


# ------------------------------------------------------------------------------
# EDGAR companyfacts → Book Equity
# ------------------------------------------------------------------------------

def download_companyfacts(session: requests.Session, cik10: str) -> Optional[dict]:
    """
    Download companyfacts JSON for a given CIK (10-digit string), with local caching.
    """
    ensure_dir(EDGAR_COMPANYFACTS_DIR)
    cache_path = os.path.join(EDGAR_COMPANYFACTS_DIR, f"CIK{cik10}.json")

    if os.path.exists(cache_path):
        with open(cache_path, "r") as f:
            return json.load(f)

    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik10}.json"
    log(f"Downloading companyfacts for CIK{cik10} from {url}")
    r = session.get(url, timeout=30)
    if r.status_code == 404:
        log(f"  No companyfacts for CIK{cik10} (404)")
        return None
    r.raise_for_status()
    data = r.json()
    with open(cache_path, "w") as f:
        json.dump(data, f)
    time.sleep(SEC_SLEEP_SEC)
    return data


def _extract_last_annual_fact(facts: dict, tag_candidates: List[str], lag_cutoff: dt.date):
    """
    From a companyfacts 'facts["us-gaap"]' dict, iterate tag_candidates and
    return the last annual (10-K / 20-F etc.) USD value with end date <= lag_cutoff.
    """
    best_end = None
    best_val = None

    for tag in tag_candidates:
        tag_obj = facts.get(tag)
        if not tag_obj:
            continue
        units = tag_obj.get("units", {})
        usd_list = units.get("USD")
        if not usd_list:
            continue
        for item in usd_list:
            # We want annual filings, forms like 10-K, 20-F, 40-F etc.
            form = item.get("form", "")
            if not form:
                continue
            form = form.upper()
            if not ("10-K" in form or "20-F" in form or "40-F" in form or "10-K/A" in form):
                continue
            end = item.get("end")
            if not end:
                continue
            try:
                end_date = dt.datetime.strptime(end, "%Y-%m-%d").date()
            except Exception:
                continue
            if end_date > lag_cutoff:
                continue
            val = item.get("val")
            if val is None:
                continue
            # Keep the latest end_date
            if best_end is None or end_date > best_end:
                best_end = end_date
                best_val = float(val)

    return best_val, best_end


def compute_be_from_companyfacts(companyfacts: dict, lag_cutoff: dt.date) -> Tuple[Optional[float], Optional[dt.date]]:
    """
    Compute Compustat-style Book Equity:
        BE = SE + TXDITC - PS
    using common us-gaap tags from companyfacts.

    Returns (BE, fye) or (None, None) if cannot compute.
    """
    facts = companyfacts.get("facts", {}).get("us-gaap", {})
    if not facts:
        return None, None

    # Tag candidates (most common)
    se_tags = [
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
        "StockholdersEquity",
        "StockholdersEquity_0",
        "StockholdersEquityAndNoncontrollingInterest"
    ]

    txditc_tags = [
        "DeferredIncomeTaxesAndOtherLiabilities",
        "DeferredIncomeTaxesAndInvestmentTaxCredit",
        "DeferredTaxLiabilities",
        "DeferredIncomeTaxLiabilitiesNet",
        "DeferredTaxAndOtherLiabilitiesNoncurrent"
    ]

    ps_tags = [
        "RedeemablePreferredStockCarryingAmount",
        "PreferredStockValue",
        "PreferredStock",
        "PreferredStockCarryingAmount",
        "PreferredStockAndPreferenceStockIncludingAdditionalPaidInCapital"
    ]

    se_val, se_end = _extract_last_annual_fact(facts, se_tags, lag_cutoff)
    if se_val is None:
        return None, None

    txditc_val, _ = _extract_last_annual_fact(facts, txditc_tags, lag_cutoff)
    ps_val, _ = _extract_last_annual_fact(facts, ps_tags, lag_cutoff)

    if math.isnan(se_val):
        return None, None

    if txditc_val is None or math.isnan(txditc_val):
        txditc_val = 0.0
    if ps_val is None or math.isnan(ps_val):
        ps_val = 0.0

    be = se_val + txditc_val - ps_val
    return be, se_end


def build_be_for_universe(universe: pd.DataFrame, formation_date: dt.date) -> pd.DataFrame:
    """
    For each ticker in the universe, map to CIK, fetch companyfacts, compute BE with a 6-month lag.
    Returns DataFrame: [ticker, be, fye].
    """
    lag_cutoff = formation_date - dt.timedelta(days=182)
    log(f"Computing Book Equity with lag cutoff: {lag_cutoff}")

    sess = get_sec_session()
    ticker_to_cik = load_ticker_to_cik(sess)

    rows = []
    for i, row in universe.iterrows():
        ticker = row["ticker"]
        cik10 = ticker_to_cik.get(ticker)
        if not cik10:
            # Not a primary SEC registrant or not in mapping.
            log(f"  Ticker {ticker}: no CIK mapping.")
            continue

        cf = download_companyfacts(sess, cik10)
        if cf is None:
            continue

        be, fye = compute_be_from_companyfacts(cf, lag_cutoff)
        if be is None or fye is None:
            log(f"  Ticker {ticker}: no valid BE found before {lag_cutoff}")
            continue

        rows.append({"ticker": ticker, "be": be, "fye": fye})

    if not rows:
        log("⚠️  Warning: Failed to compute BE for any tickers in universe.")
        log("   This may be due to missing SEC data or incorrect CIK mappings.")
        log("   Returning empty DataFrame.")
        return pd.DataFrame(columns=["ticker", "be", "fye"])

    be_df = pd.DataFrame(rows)
    be_df["ticker"] = be_df["ticker"].astype(str).str.upper().str.strip()
    return be_df


# ------------------------------------------------------------------------------
# Yahoo Finance: prices, returns, shares, ME, momentum
# ------------------------------------------------------------------------------

def download_price_history(tickers, start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    """
    Download adjusted close prices for tickers from Yahoo Finance.
    Returns long DataFrame: [time_d, ticker, adj_close].
    """
    if len(tickers) == 0:
        raise ValueError("No tickers to download.")

    log(f"Downloading prices from Yahoo Finance for {len(tickers)} tickers...")
    data = yf.download(
        tickers=tickers,
        start=start_date,
        end=end_date + dt.timedelta(days=1),
        auto_adjust=True,
        group_by="ticker",
        progress=False,
        threads=True,
    )

    # When auto_adjust=True, yfinance returns "Close" (already adjusted), not "Adj Close"
    if isinstance(data.columns, pd.MultiIndex):
        closes = {}
        for t in tickers:
            # Try "Close" first (when auto_adjust=True), then "Adj Close" (when auto_adjust=False)
            if (t, "Close") in data.columns:
                closes[t] = data[(t, "Close")]
            elif (t, "Adj Close") in data.columns:
                closes[t] = data[(t, "Adj Close")]
        close_df = pd.DataFrame(closes)
    else:
        # Single ticker case
        if "Close" in data.columns:
            close_df = data["Close"].to_frame()
            close_df.columns = tickers
        elif "Adj Close" in data.columns:
            close_df = data["Adj Close"].to_frame()
            close_df.columns = tickers
        else:
            log(f"⚠️  Warning: No Close or Adj Close column found in yfinance data")
            return pd.DataFrame(columns=["time_d", "ticker", "adj_close"])

    close_df = close_df.dropna(how="all")
    close_df.index.name = "time_d"

    long = close_df.stack().reset_index()
    long.columns = ["time_d", "ticker", "adj_close"]
    long["ticker"] = long["ticker"].astype(str).str.upper()
    long["time_d"] = pd.to_datetime(long["time_d"]).dt.date
    return long


def get_last_trading_day(price_df: pd.DataFrame, target_date: dt.date) -> dt.date:
    valid_days = price_df["time_d"].unique()
    valid_days = [d for d in valid_days if d <= target_date]
    if not valid_days:
        raise ValueError(f"No trading days on or before {target_date}.")
    return max(valid_days)


def compute_daily_returns(price_df: pd.DataFrame) -> pd.DataFrame:
    price_df = price_df.sort_values(["ticker", "time_d"])
    price_df["ret"] = price_df.groupby("ticker")["adj_close"].pct_change()
    price_df = price_df.dropna(subset=["ret"])
    return price_df


def get_shares_outstanding(tickers) -> dict:
    shares = {}
    log("Fetching shares outstanding from Yahoo (approximate)...")
    for t in tickers:
        so = None
        try:
            # Use regular info (fast_info doesn't have shares_outstanding)
            ticker_obj = yf.Ticker(t)
            info = ticker_obj.info
            so = info.get("sharesOutstanding", info.get("impliedSharesOutstanding", None))
            # Convert to int if it's a float
            if so is not None:
                so = int(so)
        except Exception as e:
            log(f"   Warning: Could not get shares for {t}: {e}")
            so = None
        if so is not None:
            log(f"   {t}: {so:,.0f} shares")
        else:
            log(f"   {t}: shares not available")
        shares[t] = so
    return shares


def compute_me_at_date(price_df: pd.DataFrame, formation_date: dt.date, shares_map: dict) -> Tuple[pd.DataFrame, dt.date]:
    last_day = get_last_trading_day(price_df, formation_date)
    log(f"Formation trading date (for ME): {last_day}")
    day_px = price_df[price_df["time_d"] == last_day].copy()
    
    if day_px.empty:
        log(f"⚠️  Warning: No price data for formation date {last_day}")
        return pd.DataFrame(columns=["ticker", "me"]), last_day
    
    day_px["shares_out"] = day_px["ticker"].map(shares_map)
    
    # Check for missing shares data
    missing_shares = day_px[day_px["shares_out"].isna()]
    if not missing_shares.empty:
        log(f"⚠️  Warning: Missing shares outstanding for {len(missing_shares)} tickers")
        for ticker in missing_shares["ticker"]:
            log(f"   {ticker}: shares_out = None")
    
    # Convert to float, handling None values
    day_px["shares_out"] = pd.to_numeric(day_px["shares_out"], errors='coerce')
    day_px["me"] = day_px["adj_close"] * day_px["shares_out"]
    
    # Filter out rows with invalid ME
    day_px = day_px[day_px["me"].notna() & (day_px["me"] > 0)].copy()
    
    return day_px[["ticker", "me"]], last_day


def compute_momentum(price_ret_df: pd.DataFrame, formation_trading_date: dt.date) -> pd.DataFrame:
    """
    FF momentum: past 12 months excluding the most recent month.
    Approx window: [t-12m to t-1m] i.e. [t-365 days, t-21 days].
    """
    end_excl = formation_trading_date - dt.timedelta(days=21)
    start_incl = formation_trading_date - dt.timedelta(days=365)

    win = price_ret_df[
        (price_ret_df["time_d"] >= start_incl)
        & (price_ret_df["time_d"] <= end_excl)
    ].copy()

    if win.empty:
        raise ValueError("Not enough return history to compute momentum.")

    win["one_plus_r"] = 1.0 + win["ret"]
    mom = (
        win.groupby("ticker")["one_plus_r"]
        .agg(lambda x: float(np.prod(x) - 1.0))
        .reset_index()
        .rename(columns={"one_plus_r": "mom12"})
    )
    return mom


# ------------------------------------------------------------------------------
# Portfolio assignment
# ------------------------------------------------------------------------------

def assign_portfolios(
    universe_df: pd.DataFrame,
    me_df: pd.DataFrame,
    be_df: pd.DataFrame,
    mom_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merge ME, BE, momentum into universe; compute NYSE breakpoints; assign
    size_port, bm_port, mom_port.
    """
    df = universe_df.merge(me_df, on="ticker", how="left")
    df = df.merge(be_df[["ticker", "be"]], on="ticker", how="left")
    df = df.merge(mom_df, on="ticker", how="left")

    df["bm"] = df["be"] / df["me"]

    nyse = df[df["is_nyse"] == 1].copy()

    size_bp = np.nanmedian(nyse["me"].values)
    bm_valid = nyse["bm"].dropna().values
    mom_valid = nyse["mom12"].dropna().values

    if len(bm_valid) == 0 or len(mom_valid) == 0 or math.isnan(size_bp):
        log("⚠️  Warning: Insufficient data to compute breakpoints (size/BM/momentum).")
        log("   Need at least 2 tickers with valid data to compute portfolio breakpoints.")
        log("   Assigning default portfolio values (B/M/L) for available tickers.")
        # Use default breakpoints - assign all to middle buckets
        size_bp = df["me"].median() if not df["me"].isna().all() else 1e9
        bm_30, bm_70 = df["bm"].quantile([0.3, 0.7]) if len(bm_valid) > 0 else (0.5, 2.0)
        mom_30, mom_70 = df["mom12"].quantile([0.3, 0.7]) if len(mom_valid) > 0 else (-0.1, 0.1)
        if math.isnan(size_bp):
            size_bp = 1e9
        if math.isnan(bm_30) or math.isnan(bm_70):
            bm_30, bm_70 = 0.5, 2.0
        if math.isnan(mom_30) or math.isnan(mom_70):
            mom_30, mom_70 = -0.1, 0.1
    else:
        bm_30, bm_70 = np.nanpercentile(bm_valid, [30, 70])
        mom_30, mom_70 = np.nanpercentile(mom_valid, [30, 70])

    log(f"Size breakpoint (NYSE median ME): {size_bp:,.2f}")
    log(f"BM breakpoints (30/70): {bm_30:.4f}, {bm_70:.4f}")
    log(f"Momentum breakpoints (30/70): {mom_30:.4f}, {mom_70:.4f}")

    def size_bucket(me):
        if math.isnan(me):
            return np.nan
        return "S" if me <= size_bp else "B"

    def bm_bucket(bm):
        if math.isnan(bm):
            return np.nan
        if bm <= bm_30:
            return "L"
        elif bm >= bm_70:
            return "H"
        else:
            return "M"

    def mom_bucket(m):
        if math.isnan(m):
            return np.nan
        if m <= mom_30:
            return "L"
        elif m >= mom_70:
            return "W"
        else:
            return "M"

    df["size_port"] = df["me"].apply(size_bucket)
    df["bm_port"] = df["bm"].apply(bm_bucket)
    df["mom_port"] = df["mom12"].apply(mom_bucket)

    # Log what we have before filtering
    log(f"Portfolio assignment summary:")
    log(f"  Total tickers: {len(df)}")
    log(f"  With valid size_port: {df['size_port'].notna().sum()}")
    log(f"  With valid bm_port: {df['bm_port'].notna().sum()}")
    log(f"  With valid mom_port: {df['mom_port'].notna().sum()}")

    # Filter to only rows with all portfolio assignments
    df_filtered = df[
        df["size_port"].notna()
        & df["bm_port"].notna()
        & df["mom_port"].notna()
    ].copy()

    if len(df_filtered) == 0 and len(df) > 0:
        log("⚠️  Warning: No tickers have all required data (ME, BE, momentum).")
        log("   This may be due to missing data or calculation errors.")
        log("   Returning empty portfolio assignments.")

    return df_filtered[["ticker", "size_port", "bm_port", "mom_port"]]


# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------

def main():
    print("=" * 60, flush=True)
    print("🏗 BuildFF3Portfolios_FromFreeData.py - FF-style sorting with EDGAR BE", flush=True)
    print("=" * 60, flush=True)

    # 1. Load universe (from pickle file if available, otherwise CSV)
    universe = load_universe()
    if ROW_LIMIT and ROW_LIMIT > 0:
        universe = universe.head(ROW_LIMIT).copy()
        log(f"DEBUG: Limiting universe to first {len(universe)} tickers.")

    # 2. Formation date (calendar June 30) and BE lag cutoff is handled inside BE builder
    formation_calendar_date = most_recent_june()
    log(f"Most recent June calendar date: {formation_calendar_date}")

    # 3. Compute BE from EDGAR for all tickers with 6-month lag
    be_df = build_be_for_universe(universe, formation_calendar_date)
    log(f"Computed BE for {len(be_df)} tickers from EDGAR.")

    # 4. Download price history for ME & momentum
    end_date = formation_calendar_date
    start_date = end_date - dt.timedelta(days=400)

    tickers = universe["ticker"].unique().tolist()
    try:
        prices = download_price_history(tickers, start_date, end_date)
        if prices.empty:
            log("⚠️  Warning: No price data downloaded; check universe or dates.")
            log("   This may be due to yfinance API issues or date range problems.")
            return
    except Exception as e:
        log(f"⚠️  Warning: Error downloading prices: {e}")
        log("   Skipping price download step.")
        return

    # 5. Daily returns
    price_ret = compute_daily_returns(prices)

    # 6. ME at formation trading date
    shares_map = get_shares_outstanding(tickers)
    me_df, formation_trading_date = compute_me_at_date(prices, formation_calendar_date, shares_map)
    log(f"Computed ME for {len(me_df)} tickers on {formation_trading_date}.")

    # 7. Momentum
    mom_df = compute_momentum(price_ret, formation_trading_date)
    log(f"Computed 12m momentum for {len(mom_df)} tickers.")

    # 8. Assign portfolios
    assigned = assign_portfolios(universe, me_df, be_df, mom_df)
    assigned["formation_date"] = formation_trading_date

    log(f"Assigned portfolios for {len(assigned)} tickers.")

    # 9. Save
    assigned.to_csv(OUTPUT_FILE, index=False)
    log(f"Saved portfolio assignments to {OUTPUT_FILE}")

    print("\nSample assignments:", flush=True)
    print(assigned.head().to_string(index=False), flush=True)
    print("=" * 60, flush=True)
    print("✅ BuildFF3Portfolios_FromFreeData.py completed successfully", flush=True)
    print("=" * 60, flush=True)


if __name__ == "__main__":
    main()
