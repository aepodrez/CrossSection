# ABOUTME: Downloads IBES unadjusted actual earnings from Refinitiv Platform and fills time series gaps
# ABOUTME: Creates monthly time series with forward-filled variables for each ticker
"""
Inputs:
- Refinitiv Platform credentials (via .env REFINITIV_APP_KEY, REFINITIV_USERNAME, REFINITIV_PASSWORD)
- Universe of symbols (S&P 500 by default)

Outputs:
- ../pyData/Intermediate/AP_IBES_UnadjustedActuals.parquet

How to run: python AP_IBESUnadjustedActuals.py
"""

import os
import sys
import time
import warnings
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import refinitiv.data as rd
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from utils.refinitiv_utils import (
    convert_tickers_to_rics,
    initialize_refinitiv_platform_session,
)
from utils.ibes_utils import rd_get_data_with_refresh

warnings.filterwarnings("ignore")

print("=" * 70, flush=True)
print("📊 AP_IBESUnadjustedActuals.py - IBES Actual Earnings from Refinitiv Platform", flush=True)
print("=" * 70, flush=True)

load_dotenv(BASE_DIR / ".env")

OUTPUT_DIR = Path("../pyData/Intermediate")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# =============================================================================
# CONFIGURATION
# =============================================================================

END_DATE = datetime.now().strftime("%Y-%m-%d")
START_DATE = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")  # 2 years ago

DEBUG_MODE = False
HTTP_REQUEST_TIMEOUT = int(os.getenv("RD_HTTP_TIMEOUT", "60"))
BATCH_SIZE = int(os.getenv("IBES_BATCH_SIZE", "10"))
MAX_RETRIES = int(os.getenv("RD_MAX_RETRIES", "2"))
RETRY_BACKOFF = int(os.getenv("RD_RETRY_BACKOFF", "2"))

if DEBUG_MODE:
    START_DATE = "2023-01-01"
    print(f"🔧 DEBUG MODE: {START_DATE} to {END_DATE}")
else:
    print(f"🚀 PRODUCTION MODE: {START_DATE} to {END_DATE}")

# =============================================================================
# PLATFORM SETUP AND UNIVERSE
# =============================================================================


def load_sp500_universe():
    """
    Load S&P 500 ticker universe from pickle file.
    Returns list of ticker strings.
    """
    import pickle

    universe_path = Path("../pyData/Static/sp500_universe.pkl")

    if universe_path.exists():
        try:
            with open(universe_path, "rb") as f:
                tickers = pickle.load(f)
            print(f"✓ Loaded {len(tickers)} tickers from sp500_universe.pkl")
            return tickers
        except Exception as e:
            print(f"⚠️  Could not load sp500_universe.pkl: {e}")

    ap_crsp_path = Path("../pyData/Intermediate/AP_monthlyCRSP.parquet")
    if ap_crsp_path.exists():
        try:
            print("Loading tickers from AP_monthlyCRSP.parquet...")
            crsp_df = pd.read_parquet(ap_crsp_path, columns=["ticker"])
            tickers = crsp_df["ticker"].dropna().unique().tolist()
            print(f"✓ Found {len(tickers)} unique tickers from AP_CRSPMonthly")
            return tickers
        except Exception as e:
            print(f"⚠️  Could not load from AP_CRSPMonthly: {e}")

    print("⚠️  No universe file found. Using sample tickers.")
    return ["AAPL", "MSFT", "GOOGL", "AMZN", "META"]


def get_sp500_rics():
    """
    Get a universe of tickers converted to RICs.
    """
    print("\n" + "=" * 60)
    print("📋 Loading ticker universe...")
    print("=" * 60)

    tickers = load_sp500_universe()
    rics = convert_tickers_to_rics(
        tickers,
        batch_size=BATCH_SIZE,
        max_retries=MAX_RETRIES,
        retry_backoff=RETRY_BACKOFF,
    )

    print(f"\n📊 Processing {len(rics)} instruments")
    return rics


# =============================================================================
# BULK DOWNLOAD FUNCTION
# =============================================================================


def bulk_download_ibes_actual_earnings(rics, start_date, end_date):
    """
    Download IBES actual earnings from Refinitiv Platform.
    """

    fields = [
        "TR.EPSActValue",  # int0a - Actual EPS (unadjusted)
        "TR.EPSActReportDate",  # statpers - Report date
        "TR.ISPeriodEndDate",  # fiscal period end
        "TR.TickerSymbol",  # ticker symbol
        "TR.CompanyName",  # company name
        "TR.FinancialPeriodAbsolute",  # fiscal period (e.g., FY2025)
        "TR.Currency",  # currency
        "TR.CommonSharesOutstanding",  # shares outstanding (proxy for shoutIBESUnadj)
    ]

    params = {
        "Scale": 6,
        "SDate": start_date,
        "EDate": end_date,
        "FRQ": "Q",
        "Curn": "USD",
    }

    all_data = []
    total_symbols = len(rics)

    for i in range(0, len(rics), BATCH_SIZE):
        batch = rics[i : i + BATCH_SIZE]

        print(f"  Batch {i//BATCH_SIZE + 1}/{(total_symbols-1)//BATCH_SIZE + 1} ({len(batch)} instruments)...", end=" ")

        df_batch = rd_get_data_with_refresh(
            universe=batch,
            fields=fields,
            parameters=params,
            max_retries=MAX_RETRIES,
            retry_backoff=RETRY_BACKOFF,
            http_timeout=HTTP_REQUEST_TIMEOUT,
            error_prefix="Error",
            print_inline=True,
        )

        if not df_batch.empty:
            all_data.append(df_batch)
            print(f"Retrieved {len(df_batch)} records")
        else:
            print("No data")

        time.sleep(0.5)

    print(f"✓ Completed {len(all_data)} batches")

    if not all_data:
        return pd.DataFrame()

    result = pd.concat(all_data, ignore_index=True)
    result["tickerIBES"] = result["Instrument"].str.split(".").str[0]

    return result


# =============================================================================
# PROCESSING
# =============================================================================


def process_actuals_data(df):
    """Process actual earnings data to match original format."""
    if df.empty:
        return df

    print("\nProcessing IBES actual earnings data...")
    print(f"Columns received: {list(df.columns)}")

    # Avoid duplicate tickerIBES columns when Instrument is also present
    if "tickerIBES" in df.columns and "Instrument" in df.columns:
        df = df.drop(columns=["tickerIBES"])

    column_mapping = {
        "Instrument": "tickerIBES",
        "EPS Actual": "int0a",
        "Earnings Per Share - Actual": "int0a",
        "EPS Actual Date": "statpers",
        "EPS Actual Report Date": "statpers",
        "Report Date": "statpers",
        "IS Period End Date": "fpedats",
        "Period End Date": "fpedats",
        "Income Statement Period End Date": "fpedats",
        "Common Shares Outstanding": "shoutIBESUnadj",
    }

    df = df.rename(columns=column_mapping)
    df = df.loc[:, ~df.columns.duplicated()]

    ticker_cols = [col for col in df.columns if col == "tickerIBES"]
    if len(ticker_cols) > 1:
        # Keep the first tickerIBES column and drop any extras to avoid 2D grouper issues
        df = df.drop(columns=ticker_cols[1:])

    for col in ["statpers", "fpedats"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    if "statpers" in df.columns:
        df["time_avail_m"] = df["statpers"].dt.to_period("M").dt.to_timestamp()
        df["time_avail_m"] = pd.to_datetime(df["time_avail_m"])

    if "int0a" in df.columns:
        df["int0a"] = pd.to_numeric(df["int0a"], errors="coerce")

    if "shoutIBESUnadj" in df.columns:
        df["shoutIBESUnadj"] = pd.to_numeric(df["shoutIBESUnadj"], errors="coerce")

    if "tickerIBES" not in df.columns and "Instrument" in df.columns:
        df["tickerIBES"] = df["Instrument"].str.split(".").str[0]

    if len(df) > 0 and {"tickerIBES", "time_avail_m"}.issubset(df.columns):
        initial_count = len(df)
        df = df.drop_duplicates(["tickerIBES", "time_avail_m"], keep="first")
        print(f"After removing within-month duplicates: {len(df)} records (dropped {initial_count - len(df)})")

    return df


def fill_time_series_gaps(df):
    """
    Fill monthly gaps per ticker by forward filling key fields.
    """
    if df.empty:
        return df

    if "time_avail_m" not in df.columns:
        print("⚠️  time_avail_m missing; skipping gap fill.")
        return df

    print("Filling time series gaps...")

    def fill_ticker_gaps(group):
        group = group.sort_values("time_avail_m")

        if len(group) <= 1:
            return group

        min_time = group["time_avail_m"].min()
        max_time = group["time_avail_m"].max()
        full_time_range = pd.date_range(start=min_time, end=max_time, freq="MS")

        group = group.set_index("time_avail_m").reindex(full_time_range)
        group.index.name = "time_avail_m"
        group = group.reset_index()

        fill_vars = ["int0a", "fpedats", "shoutIBESUnadj", "tickerIBES"]
        for var in fill_vars:
            if var in group.columns:
                group[var] = group[var].ffill()

        return group

    # Ensure unique columns and a single tickerIBES column before grouping
    df = df.loc[:, ~df.columns.duplicated()]
    ticker_cols = [col for col in df.columns if col == "tickerIBES"]
    if len(ticker_cols) > 1:
        df = df.drop(columns=ticker_cols[1:])

    df = df.groupby("tickerIBES", group_keys=False).apply(fill_ticker_gaps).reset_index(drop=True)
    print(f"After filling time series gaps: {len(df)} records")
    return df


# =============================================================================
# MAIN
# =============================================================================


def main():
    print("=" * 70)
    print("IBES Unadjusted Actual Earnings Download from Refinitiv Platform")
    print("=" * 70)

    initialize_refinitiv_platform_session(http_timeout=HTTP_REQUEST_TIMEOUT)

    if DEBUG_MODE:
        rics = ["AAPL.O", "MSFT.O", "GOOGL.O", "AMZN.O", "META.O"]
        print(f"DEBUG MODE: Using {len(rics)} sample tickers")
    else:
        rics = get_sp500_rics()

    print(f"\n📥 Downloading IBES actual earnings from Refinitiv Platform...")
    actuals_df = bulk_download_ibes_actual_earnings(rics, START_DATE, END_DATE)

    if actuals_df.empty:
        print("\n❌ No data downloaded. Exiting.")
        rd.close_session()
        return

    processed_df = process_actuals_data(actuals_df)
    processed_df = fill_time_series_gaps(processed_df)

    if len(processed_df) > 0:
        processed_df.to_parquet(OUTPUT_DIR / "AP_IBES_UnadjustedActuals.parquet", index=False)
        print("\n" + "=" * 70)
        print(f"IBES Unadjusted Actuals data saved to: {OUTPUT_DIR / 'AP_IBES_UnadjustedActuals.parquet'}")
        print(f"Total records: {len(processed_df)}")

        if "time_avail_m" in processed_df.columns:
            print(f"Date range: {processed_df['time_avail_m'].min()} to {processed_df['time_avail_m'].max()}")
        print(f"Unique tickers: {processed_df['tickerIBES'].nunique()}")
    else:
        print("\nNo data to save after processing.")

    rd.close_session()
    print("\n" + "=" * 70)
    print("Download complete!")


if __name__ == "__main__":
    main()
