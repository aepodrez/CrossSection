# QFactorModel_Live.py
# ABOUTME: Constructs a live, HXZ-style Q-factor file using EDGAR (edgartools),
# ABOUTME: Yahoo Finance (prices + shares), and FRED (1M T-bill risk-free).
# ABOUTME: Uses 2x3x3 size/IA/ROE sorts and equal-weighted portfolios to build daily ME, IA, ROE factors.
"""
Inputs:
- ../pyData/Static/sp500_universe.pkl   (pickle file containing list of tickers)

Outputs:
- ../pyData/Intermediate/d_qfactor_live.parquet

How to run:
    python QFactorModel_Live.py

IMPORTANT:
- This is an APPROXIMATE, "q-style" factor implementation.
- It does NOT exactly reproduce the Hou-Xue-Zhang (2015) q-factors,
  which rely on CRSP+Compustat, specific NYSE breakpoints, and
  detailed book-equity construction.

Dependencies:
    pip install edgartools yfinance fredapi python-dotenv pandas numpy pyarrow

Environment variables:
    EDGAR_IDENTITY : required for SEC EDGAR (e.g. "Your Name you@example.com")
    FRED_API_KEY   : optional, required for true T-bill r_f_qfac from FRED DGS1MO
"""

import os
from datetime import datetime, timedelta
import requests
import json
from typing import Dict, Optional

import numpy as np
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

from edgar import set_identity, Company

# fredapi is optional – we fall back gracefully if missing
try:
    from fredapi import Fred
    HAS_FRED = True
except ImportError:  # fredapi not installed
    Fred = None
    HAS_FRED = False

# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------

BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
# pyData is at Signals/pyData, not pyCode/pyData, so go up one more level
UNIVERSE_PKL = os.path.join(BASE_DIR, "..", "pyData", "Static", "sp500_universe.pkl")
OUTPUT_PARQUET = os.path.join(BASE_DIR, "..", "pyData", "Intermediate", "d_qfactor_live.parquet")

# How far back to build daily factor series - Last 2 years
LOOKBACK_DAYS = 730  # 2 years (730 days)

# -----------------------------------------------------------------------------
# INIT
# -----------------------------------------------------------------------------

load_dotenv()

EDGAR_IDENTITY = os.getenv("EDGAR_IDENTITY", None)
if EDGAR_IDENTITY is None:
    raise RuntimeError(
        "EDGAR_IDENTITY env var not set. "
        "Set EDGAR_IDENTITY in your .env to an email / app id for SEC access."
    )

set_identity(EDGAR_IDENTITY)

print("=" * 80, flush=True)
print("📈 QFactorModel_Live.py - Live HXZ-style Q-Factors from EDGAR + Yahoo + FRED", flush=True)
print("=" * 80, flush=True)


# -----------------------------------------------------------------------------
# TICKER TO CIK MAPPING (FALLBACK FOR EDGARTOOLS LIMITATION)
# -----------------------------------------------------------------------------

_ticker_to_cik_cache_qfactor = None

def get_ticker_to_cik_mapping_qfactor() -> Dict[str, str]:
    """
    Get ticker to CIK mapping from SEC company_tickers.json.
    Caches the result to avoid repeated downloads.
    """
    global _ticker_to_cik_cache_qfactor
    
    if _ticker_to_cik_cache_qfactor is not None:
        return _ticker_to_cik_cache_qfactor
    
    url = "https://www.sec.gov/files/company_tickers.json"
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'AP_QFactorModel/1.0 (test@example.com)',
        'Accept': 'application/json'
    })
    
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        mapping = {}
        items = data.items() if isinstance(data, dict) else enumerate(data)
        
        for _, row in items:
            if not isinstance(row, dict):
                continue
            ticker = row.get("ticker", "").upper().strip()
            cik = row.get("cik_str") or row.get("cik")
            if not ticker or cik is None:
                continue
            try:
                cik_str = str(int(cik)).zfill(10)
                mapping[ticker] = cik_str
            except (ValueError, TypeError):
                continue
        
        _ticker_to_cik_cache_qfactor = mapping
        return mapping
    except Exception as e:
        print(f"⚠️  Warning: Could not load SEC ticker mapping: {e}")
        return {}


def get_company_by_ticker_or_cik_qfactor(ticker: str) -> Optional:
    """
    Get Company object by ticker, with CIK fallback.
    
    edgartools Company(ticker) sometimes returns None even for valid SEC-registered
    companies. This function tries ticker first, then falls back to CIK lookup.
    """
    # Try direct ticker lookup first
    company = Company(ticker)
    if company is not None:
        return company
    
    # Fallback: Look up CIK and try that
    ticker_to_cik = get_ticker_to_cik_mapping_qfactor()
    cik = ticker_to_cik.get(ticker.upper())
    
    if cik:
        try:
            company = Company(cik)
            if company is not None:
                return company
        except Exception:
            pass
    
    return None


# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------

def load_universe(path: str = None) -> pd.DataFrame:
    """Load ticker universe from pickle file (sp500_universe.pkl)."""
    import pickle
    from pathlib import Path
    
    # Use pickle file path if not specified
    if path is None:
        path = UNIVERSE_PKL
    
    if not os.path.exists(path):
        raise FileNotFoundError(f"Universe file not found: {path}")
    
    try:
        # Load tickers from pickle file
        with open(path, 'rb') as f:
            tickers = pickle.load(f)
        
        # Create DataFrame with ticker column
        uni = pd.DataFrame({'ticker': tickers})
        uni["ticker"] = (
            uni["ticker"]
            .astype(str)
            .str.strip()
            .str.upper()
        )
        uni = uni.dropna(subset=["ticker"]).drop_duplicates(subset=["ticker"])

        print(f"Loaded {len(uni)} tickers from {path}.", flush=True)
        return uni
    except Exception as e:
        raise FileNotFoundError(f"Error loading universe from {path}: {e}")


def get_ia_roe_from_edgar(ticker: str):
    """
    Get IA and ROE for a given ticker using EDGAR standardized financials.

    IA  = (TotalAssets_t - TotalAssets_{t-1}) / TotalAssets_{t-1}
    ROE = NetIncome_t / StockholdersEquity_t

    Uses edgartools.Company.get_financials():
        - get_total_assets(period_offset)
        - get_net_income(period_offset)
        - get_stockholders_equity(period_offset)
    """
    try:
        # Get company object (with CIK fallback)
        c = get_company_by_ticker_or_cik_qfactor(ticker)
        if c is None:
            return np.nan, np.nan
        
        fin = c.get_financials()
        if fin is None:
            return np.nan, np.nan

        # period_offset=0 -> most recent, 1 -> previous period
        assets_t_raw = fin.get_total_assets(0)
        assets_tm1_raw = fin.get_total_assets(1)
        ni_t_raw = fin.get_net_income(0)
        be_t_raw = fin.get_stockholders_equity(0)

        # Convert to numeric, handling strings and None values
        def to_float(val):
            """Safely convert value to float, returning np.nan if conversion fails."""
            if val is None:
                return np.nan
            try:
                return float(val)
            except (ValueError, TypeError):
                return np.nan

        assets_t = to_float(assets_t_raw)
        assets_tm1 = to_float(assets_tm1_raw)
        ni_t = to_float(ni_t_raw)
        be_t = to_float(be_t_raw)

        # Calculate IA (Investment-to-Assets)
        ia = np.nan
        if not np.isnan(assets_t) and not np.isnan(assets_tm1) and assets_tm1 != 0:
            ia = (assets_t - assets_tm1) / assets_tm1

        # Calculate ROE (Return on Equity)
        roe = np.nan
        if not np.isnan(ni_t) and not np.isnan(be_t) and be_t != 0:
            roe = ni_t / be_t

        return ia, roe

    except Exception as e:
        print(f"  [WARN] Fundamentals failed for {ticker}: {e}", flush=True)
        return np.nan, np.nan


def get_fundamentals_for_universe(tickers: list) -> pd.DataFrame:
    """Loop over tickers and collect IA and ROE."""
    rows = []
    for i, tk in enumerate(tickers, start=1):
        print(f"[{i}/{len(tickers)}] Pulling fundamentals from EDGAR for {tk}...", flush=True)
        ia, roe = get_ia_roe_from_edgar(tk)
        rows.append({"ticker": tk, "ia": ia, "roe": roe})

    fund = pd.DataFrame(rows)
    print("Fundamentals summary (non-null counts):", flush=True)
    print(fund[["ia", "roe"]].notna().sum(), flush=True)
    return fund


def download_prices(tickers: list, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    Download daily adjusted close prices for tickers using yfinance.
    Returns a DataFrame: index = dates, columns = tickers.
    """
    print(
        f"Downloading Yahoo Finance prices for {len(tickers)} tickers "
        f"from {start_date.date()} to {end_date.date()}...",
        flush=True,
    )

    data = yf.download(
        tickers=tickers,
        start=start_date,
        end=end_date + timedelta(days=1),  # inclusive end
        auto_adjust=True,
        progress=False,
        group_by="ticker",
        threads=True,
    )

    # yfinance shape depends on whether you pass a list or string.
    if isinstance(data.columns, pd.MultiIndex):
        # Keep only Adj Close
        data = data.xs("Adj Close", axis=1, level=1)
    else:
        # Single ticker -> Series; convert to DataFrame
        if len(tickers) == 1:
            data = data.to_frame(name=tickers[0])

    data = data.sort_index()
    print(f"Downloaded price matrix shape: {data.shape}", flush=True)
    return data


def get_shares_history_for_universe(
    tickers: list,
    start_date: datetime,
    end_date: datetime,
) -> pd.DataFrame:
    """
    Get historical shares outstanding per ticker using yfinance.

    Strategy:
      1. Try Ticker.get_shares_full(start, end) if available.
      2. Fallback: use quarterly_income_stmt row 'Basic Average Shares'
         (or similar) when get_shares_full fails.

    Returns:
        DataFrame with columns: ticker, period_end (datetime), shares
    """
    rows = []
    for i, tk in enumerate(tickers, start=1):
        print(f"[{i}/{len(tickers)}] Pulling share history from yfinance for {tk}...", flush=True)
        try:
            t = yf.Ticker(tk)

            # --- Try get_shares_full first (best source) ---
            shares_df = None
            try:
                sh = t.get_shares_full(start=start_date, end=end_date)
                if isinstance(sh, pd.DataFrame) and not sh.empty:
                    df = sh.copy()
                    if not isinstance(df.index, pd.DatetimeIndex):
                        df.index = pd.to_datetime(df.index, errors="coerce")
                    df = df.dropna(subset=[df.columns[0]])
                    df = df.reset_index().rename(columns={"index": "period_end", df.columns[0]: "shares"})
                    df["ticker"] = tk
                    shares_df = df[["ticker", "period_end", "shares"]]
            except Exception:
                shares_df = None

            # --- Fallback: quarterly_income_stmt 'Basic Average Shares' ---
            if shares_df is None:
                qis = t.quarterly_income_stmt
                if isinstance(qis, pd.DataFrame) and not qis.empty:
                    idx = qis.index.to_series().astype(str)
                    mask = idx.str.contains("Basic Average Shares", case=False, na=False)
                    if not mask.any():
                        mask = idx.str.contains("Basic Shares", case=False, na=False)

                    if mask.any():
                        row = qis[mask].iloc[0]
                        df = (
                            row.to_frame(name="shares")
                            .reset_index()
                            .rename(columns={"index": "period_end"})
                        )
                        df["ticker"] = tk
                        df["period_end"] = pd.to_datetime(df["period_end"], errors="coerce")
                        df = df.dropna(subset=["period_end", "shares"])
                        shares_df = df[["ticker", "period_end", "shares"]]

            if shares_df is not None and not shares_df.empty:
                rows.append(shares_df)

        except Exception as e:
            print(f"  [WARN] Shares history failed for {tk}: {e}", flush=True)

    if not rows:
        print("[WARN] No share history retrieved; size will fall back to fast_info market_cap.", flush=True)
        return pd.DataFrame(columns=["ticker", "period_end", "shares"])

    shares_panel = pd.concat(rows, ignore_index=True)
    shares_panel = shares_panel.dropna(subset=["shares"])
    shares_panel = shares_panel.sort_values(["ticker", "period_end"])
    print(
        f"Retrieved {len(shares_panel)} share observations "
        f"across {shares_panel['ticker'].nunique()} tickers.",
        flush=True,
    )
    return shares_panel


def get_shares_on_date(shares_panel: pd.DataFrame, asof_date: datetime) -> pd.Series:
    """
    For each ticker, get the latest shares observation with period_end <= asof_date.
    Returns:
        Series indexed by ticker with shares (float).
    """
    if shares_panel is None or shares_panel.empty:
        return pd.Series(dtype="float64")

    tmp = shares_panel[shares_panel["period_end"] <= asof_date].copy()
    if tmp.empty:
        return pd.Series(dtype="float64")

    tmp = tmp.sort_values(["ticker", "period_end"])
    last = (
        tmp.groupby("ticker")
        .tail(1)
        .set_index("ticker")["shares"]
    )

    return last


def assign_size_bucket(size_series: pd.Series) -> pd.Series:
    """
    Assign size buckets: 1 = small, 2 = big using median breakpoint.
    (HXZ use median NYSE ME; here we use universe median as approximation.)
    """
    s = size_series.dropna()
    if s.empty:
        return pd.Series(index=size_series.index, dtype="float64")

    med = s.median()

    def bucket(x):
        if pd.isna(x):
            return np.nan
        return 1 if x <= med else 2

    return size_series.apply(bucket)


def assign_terciles(series: pd.Series, low_q=0.3, high_q=0.7) -> pd.Series:
    """
    Assign terciles for IA / ROE:
        1 = low  (bottom 30%)
        2 = mid  (30%-70%)
        3 = high (top 30%)
    """
    s = series.dropna()
    if s.empty:
        return pd.Series(index=series.index, dtype="float64")

    q_low = s.quantile(low_q)
    q_high = s.quantile(high_q)

    def bucket(x):
        if pd.isna(x):
            return np.nan
        if x <= q_low:
            return 1
        elif x <= q_high:
            return 2
        else:
            return 3

    return series.apply(bucket)


def build_cell_returns(return_df: pd.DataFrame, meta: pd.DataFrame) -> pd.DataFrame:
    """
    Build equal-weighted cell returns R_ijk,t for the 18 size/IA/ROE portfolios.

    return_df: index = dates, columns = tickers
    meta: DataFrame with columns: ticker, i (size 1/2), j (IA 1/2/3), k (ROE 1/2/3)

    Returns:
        DataFrame with MultiIndex columns (i, j, k), shape [dates x 18]
    """
    # Only keep tickers that are both in meta and in returns
    common_tickers = sorted(set(return_df.columns).intersection(set(meta["ticker"])))
    meta = meta[meta["ticker"].isin(common_tickers)].copy()
    return_df = return_df[common_tickers].copy()

    cell_series_dict = {}
    for i in [1, 2]:
        for j in [1, 2, 3]:
            for k in [1, 2, 3]:
                mask = (meta["i"] == i) & (meta["j"] == j) & (meta["k"] == k)
                cell_tickers = meta.loc[mask, "ticker"].tolist()

                if not cell_tickers:
                    cell_ret = pd.Series(np.nan, index=return_df.index)
                else:
                    # Equal-weighted average across tickers in the cell
                    cell_ret = return_df[cell_tickers].mean(axis=1, skipna=True)

                cell_series_dict[(i, j, k)] = cell_ret

    cell_df = pd.DataFrame(cell_series_dict)
    cell_df.columns = pd.MultiIndex.from_tuples(cell_df.columns, names=["i", "j", "k"])
    return cell_df


def build_q_style_factors_from_cells(cell_df: pd.DataFrame) -> pd.DataFrame:
    """
    Given daily cell returns R_ijk,t, apply the HXZ 2x3x3 formulas (eq. 1-3)
    but using equal-weighted cells.

    RMe_t   = avg over j,k of R_1jk,t - avg over j,k of R_2jk,t
    RI/A_t  = avg over i,k of R_i1k,t - avg over i,k of R_i3k,t
    RRoe_t  = avg over i,j of R_ij3,t - avg over i,j of R_ij1,t

    Returns:
        DataFrame with columns: r_me_qfac, r_ia_qfac, r_roe_qfac
        (raw returns, NOT in percent)
    """
    # Size factor (ME): small minus big
    small_cols = [col for col in cell_df.columns if col[0] == 1]
    big_cols = [col for col in cell_df.columns if col[0] == 2]
    r_me = cell_df[small_cols].mean(axis=1) - cell_df[big_cols].mean(axis=1)

    # Investment factor (IA): low IA minus high IA
    low_ia_cols = [col for col in cell_df.columns if col[1] == 1]
    high_ia_cols = [col for col in cell_df.columns if col[1] == 3]
    r_ia = cell_df[low_ia_cols].mean(axis=1) - cell_df[high_ia_cols].mean(axis=1)

    # Profitability factor (ROE): high ROE minus low ROE
    high_roe_cols = [col for col in cell_df.columns if col[2] == 3]
    low_roe_cols = [col for col in cell_df.columns if col[2] == 1]
    r_roe = cell_df[high_roe_cols].mean(axis=1) - cell_df[low_roe_cols].mean(axis=1)

    factors = pd.DataFrame(
        {
            "r_me_qfac": r_me,
            "r_ia_qfac": r_ia,
            "r_roe_qfac": r_roe,
        }
    )
    return factors


def get_market_factor(dates: pd.DatetimeIndex) -> pd.Series:
    """
    Approximate market factor using SPY daily returns (raw, not excess).
    You can later subtract a proper risk-free series to make it excess returns.
    """
    start = dates.min() - timedelta(days=5)
    end = dates.max() + timedelta(days=5)
    print(f"Downloading SPY prices from {start.date()} to {end.date()}...", flush=True)

    spy_prices = yf.download(
        "SPY",
        start=start,
        end=end + timedelta(days=1),
        auto_adjust=True,
        progress=False,
    )["Adj Close"].sort_index()

    spy_ret = spy_prices.pct_change()
    spy_ret = spy_ret.reindex(dates).astype(float)
    return spy_ret


def get_risk_free_series(dates: pd.DatetimeIndex) -> pd.Series:
    """
    Build daily risk-free series from FRED DGS1MO (1M Treasury constant maturity).

    - Downloads daily yields in percent (annualized) from FRED.
    - Converts to daily decimal returns via yield / 100 / 252 (trading-day approx).
    - Aligns and forward-fills over the factor dates.

    If fredapi or FRED_API_KEY is missing / fails, returns 0 and logs a warning.
    """
    if not HAS_FRED:
        print("[WARN] fredapi not installed; using r_f_qfac = 0.", flush=True)
        return pd.Series(0.0, index=dates)

    fred_key = os.getenv("FRED_API_KEY", None)
    if not fred_key:
        print("[WARN] FRED_API_KEY not set; using r_f_qfac = 0.", flush=True)
        return pd.Series(0.0, index=dates)

    fred = Fred(api_key=fred_key)

    start = dates.min() - timedelta(days=7)
    end = dates.max() + timedelta(days=7)
    print(
        f"Downloading 1M T-bill yields (DGS1MO) from FRED "
        f"{start.date()} to {end.date()}...",
        flush=True,
    )

    try:
        rf_yield = fred.get_series(
            "DGS1MO",
            observation_start=start.date(),
            observation_end=end.date(),
        )
        rf_yield = rf_yield.astype(float)

        # Convert annualized yield (%) to daily decimal return
        rf_daily = (rf_yield / 100.0) / 252.0

        # Align to our factor dates, forward-filling
        rf_daily = rf_daily.reindex(
            pd.DatetimeIndex(sorted(set(dates).union(set(rf_daily.index))))
        ).sort_index().ffill()

        rf_daily = rf_daily.reindex(dates)
        return rf_daily

    except Exception as e:
        print(f"[WARN] FRED request failed ({e}); using r_f_qfac = 0.", flush=True)
        return pd.Series(0.0, index=dates)


# -----------------------------------------------------------------------------
# MAIN LOGIC
# -----------------------------------------------------------------------------

def main():
    # 1) Universe
    uni = load_universe(UNIVERSE_PKL)
    tickers = uni["ticker"].tolist()

    # 2) Time window
    end_date = datetime.today()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)
    print(f"Building live factors from {start_date.date()} to {end_date.date()}...", flush=True)

    # 3) Fundamentals from EDGAR -> IA & ROE
    fund = get_fundamentals_for_universe(tickers)

    # 4) Daily prices for universe from yfinance
    price_df = download_prices(
        tickers=fund["ticker"].tolist(),
        start_date=start_date,
        end_date=end_date,
    )
    if price_df.empty:
        raise RuntimeError("No price data downloaded; check tickers/universe.")

    # Formation date for size: last trading day in our window
    formation_price_date = price_df.index.max()
    print(f"Using {formation_price_date.date()} as size formation date.", flush=True)

    # 5) Shares history and historical market cap at formation date
    shares_panel = get_shares_history_for_universe(
        tickers=fund["ticker"].tolist(),
        start_date=start_date - timedelta(days=365),  # a bit earlier to be safe
        end_date=end_date,
    )
    shares_on_form_date = get_shares_on_date(shares_panel, formation_price_date)

    price_on_form_date = price_df.loc[formation_price_date]

    # Align both series to the fundamentals universe
    shares_on_form_date = shares_on_form_date.reindex(fund["ticker"]).astype(float)
    price_on_form_date = price_on_form_date.reindex(fund["ticker"]).astype(float)

    mkt_cap_hist = shares_on_form_date * price_on_form_date

    # 6) Fallback market cap via yfinance.fast_info
    print("Fetching fallback market caps via yfinance.fast_info...", flush=True)
    size_rows = []
    for i, tk in enumerate(fund["ticker"].tolist(), start=1):
        try:
            print(f"  [{i}/{len(fund)}] {tk}", flush=True)
            info = yf.Ticker(tk).fast_info
            mcap = info.get("market_cap", np.nan)
        except Exception as e:
            print(f"    [WARN] fast_info failed for {tk}: {e}", flush=True)
            mcap = np.nan
        size_rows.append({"ticker": tk, "mkt_cap_fastinfo": mcap})

    size_df = pd.DataFrame(size_rows).set_index("ticker")

    # Combine historical and fallback caps
    mkt_cap_df = pd.DataFrame(
        {
            "ticker": fund["ticker"],
            "mkt_cap_hist": mkt_cap_hist.values,
        }
    ).set_index("ticker")
    mkt_cap_combined = mkt_cap_df.join(size_df, how="left")

    # Final size measure: prefer historical, fallback to fast_info
    mkt_cap_combined["mkt_cap"] = mkt_cap_combined["mkt_cap_hist"]
    mask_missing_hist = mkt_cap_combined["mkt_cap"].isna()
    mkt_cap_combined.loc[mask_missing_hist, "mkt_cap"] = mkt_cap_combined.loc[
        mask_missing_hist, "mkt_cap_fastinfo"
    ]

    # 7) Merge fundamentals + size
    meta = (
        fund.set_index("ticker")
        .join(mkt_cap_combined[["mkt_cap"]], how="left")
    )

    print("Summary of market cap availability:", flush=True)
    print(meta["mkt_cap"].notna().value_counts(), flush=True)

    # 8) Assign size / IA / ROE buckets (single formation cross-section)
    print("Assigning size (2) and IA/ROE (3x3) buckets...", flush=True)
    meta["i"] = assign_size_bucket(meta["mkt_cap"])
    meta["j"] = assign_terciles(meta["ia"])
    meta["k"] = assign_terciles(meta["roe"])

    # Drop stocks lacking any of the necessary buckets
    meta = meta.dropna(subset=["i", "j", "k"]).copy()
    meta["i"] = meta["i"].astype(int)
    meta["j"] = meta["j"].astype(int)
    meta["k"] = meta["k"].astype(int)
    meta = meta.reset_index()

    print("Bucket counts (i,j,k):", flush=True)
    print(meta[["i", "j", "k"]].value_counts().sort_index(), flush=True)

    if meta.empty:
        raise RuntimeError("No securities with complete IA/ROE/size buckets.")

    # 9) Convert prices to daily returns over the window
    ret_df = price_df[meta["ticker"].tolist()].pct_change().dropna(how="all")

    # 10) 18 cell returns (equal-weighted)
    print("Building 18 (2x3x3) cell returns...", flush=True)
    cell_df = build_cell_returns(ret_df, meta)

    # 11) Build ME, IA, ROE factors from cell returns
    print("Constructing q-style ME/IA/ROE factors...", flush=True)
    qfactors = build_q_style_factors_from_cells(cell_df)

    # 12) Market factor (SPY) & risk-free (1M T-bill from FRED if available)
    print("Constructing market and risk-free series...", flush=True)
    r_mkt = get_market_factor(qfactors.index)
    r_f = get_risk_free_series(qfactors.index)

    # 13) Combine into final DataFrame similar to original d_qfactor.parquet
    out_df = pd.DataFrame(
        {
            "time_d": qfactors.index,
            "r_f_qfac": r_f.values,
            "r_mkt_qfac": r_mkt.values,
            "r_me_qfac": qfactors["r_me_qfac"].values,
            "r_ia_qfac": qfactors["r_ia_qfac"].values,
            "r_roe_qfac": qfactors["r_roe_qfac"].values,
        }
    )

    out_df = out_df.sort_values("time_d").reset_index(drop=True)

    # 14) Save
    os.makedirs(os.path.dirname(OUTPUT_PARQUET), exist_ok=True)
    out_df.to_parquet(OUTPUT_PARQUET)
    print(f"\n✅ Live q-style factor data saved to: {OUTPUT_PARQUET}", flush=True)
    print(f"Date range: {out_df['time_d'].min().date()} to {out_df['time_d'].max().date()}", flush=True)
    print("\nSample rows:")
    print(out_df.tail().to_string(index=False), flush=True)


if __name__ == "__main__":
    main()
