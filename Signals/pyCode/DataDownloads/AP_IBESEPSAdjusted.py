# ABOUTME: Downloads IBES EPS estimates (adjusted for splits) with actuals from Refinitiv Platform
# ABOUTME: Processes data to monthly frequency, removes missing estimates, and applies column standardization
"""
Inputs:
- Refinitiv Platform credentials (via .env REFINITIV_APP_KEY)

Outputs:
- ../pyData/Intermediate/AP_IBES_EPS_Adj.parquet

How to run: python3 AP_IBESEPSAdjusted.py
"""

import os
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
import refinitiv.data as rd
from dotenv import load_dotenv
import warnings

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from utils.refinitiv_utils import (
    convert_tickers_to_rics as resolve_tickers_to_rics,
    initialize_refinitiv_platform_session,
)
from utils.ibes_utils import rd_get_data_with_refresh

warnings.filterwarnings("ignore")

# Load environment variables
load_dotenv()

# Configuration
OUTPUT_DIR = Path("../pyData/Intermediate")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "AP_IBES_EPS_Adj.parquet"
DEBUG_MODE = False  # Set to True to limit the number of instruments
MAX_INSTRUMENTS = 100  # Only used if DEBUG_MODE is True
BATCH_SIZE = int(os.getenv("IBES_BATCH_SIZE", "10"))  # Smaller batches to avoid timeouts
HTTP_REQUEST_TIMEOUT = int(os.getenv("RD_HTTP_TIMEOUT", "60"))  # seconds
MAX_RETRIES = int(os.getenv("RD_MAX_RETRIES", "2"))
RETRY_BACKOFF = int(os.getenv("RD_RETRY_BACKOFF", "2"))  # seconds


def load_sp500_universe():
    """
    Load S&P 500 ticker universe from pickle file.
    Returns list of ticker strings.
    """
    import pickle
    universe_path = Path("../pyData/Static/sp500_universe.pkl")
    
    if universe_path.exists():
        try:
            with open(universe_path, 'rb') as f:
                tickers = pickle.load(f)
            print(f"✓ Loaded {len(tickers)} tickers from sp500_universe.pkl")
            return tickers
        except Exception as e:
            print(f"⚠️  Could not load sp500_universe.pkl: {e}")
    
    # Fallback: Try to load from AP_CRSPMonthly
    ap_crsp_path = Path("../pyData/Intermediate/AP_monthlyCRSP.parquet")
    if ap_crsp_path.exists():
        try:
            print("Loading tickers from AP_monthlyCRSP.parquet...")
            crsp_df = pd.read_parquet(ap_crsp_path, columns=['ticker'])
            tickers = crsp_df['ticker'].dropna().unique().tolist()
            print(f"✓ Found {len(tickers)} unique tickers from AP_CRSPMonthly")
            return tickers
        except Exception as e:
            print(f"⚠️  Could not load from AP_CRSPMonthly: {e}")
    
    # Final fallback: sample list
    print("⚠️  No universe file found. Using sample tickers.")
    return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NVDA', 'JPM', 'V', 'JNJ']


def convert_tickers_to_rics(tickers):
    return resolve_tickers_to_rics(
        tickers,
        batch_size=BATCH_SIZE,
        max_retries=MAX_RETRIES,
        retry_backoff=RETRY_BACKOFF,
    )


def get_active_universe():
    """
    Get a universe of US stocks to download IBES data for.
    Loads from SP500 universe pickle file and converts to RIC format.
    """
    print("\n" + "="*60)
    print("📋 Loading ticker universe...")
    print("="*60)
    
    # Load tickers from SP500 universe
    tickers = load_sp500_universe()
    
    # Limit for debug mode
    if DEBUG_MODE:
        tickers = tickers[:MAX_INSTRUMENTS]
        print(f"DEBUG MODE: Limited to {len(tickers)} tickers")
    
    # Convert tickers to RIC format
    rics = convert_tickers_to_rics(tickers)
    
    print(f"\n📊 Processing {len(rics)} instruments")
    return rics


def get_ibes_data(rics, start_date="2000-01-01", end_date=None):
    """
    Download IBES adjusted EPS estimates and actuals from Refinitiv.
    Uses monthly frequency for forward quarter (Period=FQ1 by default).
    """
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    print(f"\nDownloading IBES data for {len(rics)} instruments...")
    print(f"Date range: {start_date} to {end_date}")

    fields = [
        "TR.EPSMeanEst",
        "TR.EPSMeanEst.fperiodenddate",
        "TR.EPSMeanEst.date",
        "TR.EPSMedianEst",
        "TR.EPSEstStdDev",
        "TR.EPSNumOfEst",
        "TR.EPSActValue",
        "TR.EPSActAnnouncementDate",
        "TR.PriceClose",
        "TR.CompanyMarketCap",
    ]

    parameters = {
        "SDate": start_date,
        "EDate": end_date,
        "Frq": "M",
        "Period": "FQ1",  # FQ1 for next fiscal quarter; change to FQ0 for current quarter
    }

    all_data = []
    for i in range(0, len(rics), BATCH_SIZE):
        batch_rics = rics[i : i + BATCH_SIZE]
        print(f"Processing batch {i//BATCH_SIZE + 1}/{(len(rics)-1)//BATCH_SIZE + 1} ({len(batch_rics)} instruments)...")

        df_batch = rd_get_data_with_refresh(
            universe=batch_rics,
            fields=fields,
            parameters=parameters,
            max_retries=MAX_RETRIES,
            retry_backoff=RETRY_BACKOFF,
            http_timeout=HTTP_REQUEST_TIMEOUT,
            error_prefix="  Error processing batch",
            print_inline=False,
        )

        if not df_batch.empty:
            all_data.append(df_batch)
            print(f"  Retrieved {len(df_batch)} records")
        else:
            print("  No data returned for this batch")

    if len(all_data) == 0:
        print("Warning: No data retrieved!")
        return pd.DataFrame()

    df = pd.concat(all_data, ignore_index=True)
    print(f"\nTotal records downloaded: {len(df)}")
    return df


def process_ibes_data(df):
    """
    Process and clean the IBES data to match the WRDS script format.
    """
    print("\nProcessing IBES data...")
    print(f"Columns received: {list(df.columns)}")
    print("\nSample of raw data:")
    print(df.head())

    column_mapping = {
        "Instrument": "tickerIBES",
        "Earnings Per Share - Mean Estimate": "meanest",
        "EPS Median": "medest",
        "EPS Standard Deviation": "stdev",
        "EPS Number of Estimates": "numest",
        "Earnings Per Share - Actual": "actual",
        "EPS Actual Announcement Date": "anndats_act",
        "Period End Date": "fpedats",
        "Date": "statpers",
        "Price Close": "price",
        "Company Market Cap": "mktcap",
    }

    df = df.rename(columns=column_mapping)

    for col in ["statpers", "fpedats", "anndats_act"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    if "statpers" in df.columns:
        df["time_avail_m"] = df["statpers"].dt.to_period("M").dt.to_timestamp()
        df["time_avail_m"] = pd.to_datetime(df["time_avail_m"])

    df["fpi"] = 1

    if "meanest" in df.columns:
        initial_count = len(df)
        df = df.dropna(subset=["meanest"])
        print(f"Removed {initial_count - len(df)} records with missing meanest")
    else:
        print("Warning: 'meanest' column not found after renaming")

    if len(df) > 0:
        df = df.sort_values(["tickerIBES", "fpi", "time_avail_m", "statpers"])
        df = df.drop_duplicates(["tickerIBES", "fpi", "time_avail_m"], keep="last")
        print(f"After keeping last obs per month: {len(df)} records")

    if {"mktcap", "price"}.issubset(df.columns):
        df["shout"] = df["mktcap"] / df["price"]
        df = df.drop(columns=["mktcap"], errors="ignore")

    return df


def main():
    print("=" * 70)
    print("IBES EPS Data Download from Refinitiv Platform")
    print("=" * 70)

    initialize_refinitiv_platform_session(http_timeout=HTTP_REQUEST_TIMEOUT)
    rics = get_active_universe()

    if DEBUG_MODE:
        print(f"\nDEBUG MODE: Limiting to {len(rics)} instruments")

    df_ibes = get_ibes_data(rics)

    if len(df_ibes) == 0:
        print("\nNo data downloaded. Exiting.")
        rd.close_session()
        return

    df_processed = process_ibes_data(df_ibes)

    if len(df_processed) > 0:
        df_processed.to_parquet(OUTPUT_FILE, index=False)
        print("\n" + "=" * 70)
        print(f"IBES EPS data saved to: {OUTPUT_FILE}")
        print(f"Total records: {len(df_processed)}")

        if "time_avail_m" in df_processed.columns:
            print(f"Date range: {df_processed['time_avail_m'].min()} to {df_processed['time_avail_m'].max()}")

        print("\nColumn names:")
        print(list(df_processed.columns))

        print("\nSample data (first 5 rows):")
        print(df_processed.head())

        print("\nData summary:")
        print(df_processed.describe())
    else:
        print("\nNo data to save after processing.")

    rd.close_session()
    print("\n" + "=" * 70)
    print("Download complete!")


if __name__ == "__main__":
    main()
