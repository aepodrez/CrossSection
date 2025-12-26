import os
import sys
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
print("📊 AP_IBESRecommendations.py - IBES Recommendations from Refinitiv Platform", flush=True)
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
# BULK DOWNLOAD FUNCTION
# =============================================================================

def load_sp500_universe():
    """Load S&P 500 ticker universe from pickle file."""
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


def bulk_download_ibes_recommendations(rics, start_date, end_date):
    """Download IBES analyst recommendations via Refinitiv Platform."""
    fields = [
        "TR.RecEstValue.date",  # Date
        "TR.RecEstValue",  # Recommendation code (1-5)
        "TR.BrkRecLabel",  # Recommendation text
        "TR.RecLabelEstBrokerName",  # Broker name
        "TR.AnalystName",  # Analyst name
    ]

    params = {
        "Scale": 6,
        "SDate": start_date,
        "EDate": end_date,
        "FRQ": "D",
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

    if not all_data:
        return pd.DataFrame()

    result = pd.concat(all_data, ignore_index=True)
    result["tickerIBES"] = result["Instrument"].str.split(".").str[0]
    return result

# =============================================================================
# DATA PROCESSING
# =============================================================================

def process_recommendations_data(df):
    """Process recommendations data to match original format"""
    
    if df.empty:
        return df
    
    print(f"\n🔄 Processing recommendations data...")
    
    # Clean recommendation codes - drop missing values
    if 'ireccd' in df.columns:
        initial_count = len(df)
        df = df.dropna(subset=['ireccd'])
        print(f"  Removed {initial_count - len(df)} records with missing ireccd")
    
    # Create monthly time availability variable
    if 'anndats' in df.columns:
        df['time_avail_m'] = df['anndats'].dt.to_period('M').dt.to_timestamp()
        df['time_avail_m'] = pd.to_datetime(df['time_avail_m'])
    
    # Fill missing text fields with empty strings
    for col in ['itext', 'broker_name', 'analyst_name']:
        if col in df.columns:
            df[col] = df[col].fillna('')
    
    # Reorder columns
    key_cols = ['tickerIBES', 'amaskcd', 'anndats', 'time_avail_m', 'ireccd']
    other_cols = [col for col in df.columns if col not in key_cols]
    final_cols = [col for col in key_cols if col in df.columns] + other_cols
    df = df[[col for col in final_cols if col in df.columns]]
    
    return df

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main execution function"""
    initialize_refinitiv_platform_session(http_timeout=HTTP_REQUEST_TIMEOUT)

    if DEBUG_MODE:
        rics = ["AAPL.O", "MSFT.O", "GOOGL.O", "AMZN.O", "META.O"]
        print(f"\nDEBUG MODE: Using {len(rics)} sample tickers")
    else:
        rics = get_sp500_rics()

    print(f"\n📥 Downloading IBES recommendations from Refinitiv Platform...")
    rec_df = bulk_download_ibes_recommendations(rics, START_DATE, END_DATE)

    if rec_df.empty:
        print("\n❌ No recommendations data retrieved")
        rd.close_session()
        sys.exit(1)

    final_df = process_recommendations_data(rec_df)

    if final_df.empty:
        print("\n❌ Data processing resulted in empty DataFrame")
        rd.close_session()
        sys.exit(1)

    print(f"\n💾 Saving output...")
    output_file = OUTPUT_DIR / "AP_IBES_Recommendations.parquet"
    final_df.to_parquet(output_file, index=False)

    print(f"  ✓ Saved: {output_file}")
    print(f"    Records: {len(final_df):,}")
    print(f"    Size: {output_file.stat().st_size / 1024:.1f} KB")

    print(f"\n📊 Summary Statistics:")
    print(f"  Total observations: {len(final_df):,}")
    print(f"  Unique tickers: {final_df['tickerIBES'].nunique()}")

    if "time_avail_m" in final_df.columns:
        print(f"  Date range: {final_df['time_avail_m'].min()} to {final_df['time_avail_m'].max()}")
        print(f"  Months covered: {final_df['time_avail_m'].nunique()}")

    if "ireccd" in final_df.columns:
        print(f"\n  Recommendation distribution:")
        rec_counts = final_df["ireccd"].value_counts().sort_index()
        rec_labels = {1: "Strong Buy", 2: "Buy", 3: "Hold", 4: "Sell", 5: "Strong Sell"}
        for code, count in rec_counts.items():
            label = rec_labels.get(int(code), f"Unknown ({int(code)})")
            pct = count / len(final_df) * 100
            print(f"    {int(code)} ({label}): {count:,} ({pct:.1f}%)")

    print(f"\n  Sample data:")
    sample_cols = ["tickerIBES", "amaskcd", "anndats", "time_avail_m", "ireccd"]
    available_cols = [col for col in sample_cols if col in final_df.columns]
    print(final_df[available_cols].head(10).to_string(index=False))

    rd.close_session()

    print("\n" + "=" * 70)
    print("✅ AP_IBESRecommendations.py completed successfully!")
    print("=" * 70)
    print("\nOutputs:")
    print("  1. AP_IBES_Recommendations.parquet - IBES analyst recommendations")
    print("\nNext steps:")
    print("  - Use AP_IBES_Recommendations.parquet in place of IBES_Recommendations.parquet")
    print("  - Use for recommendation-based predictors (ConsRecomm, Recomm_ShortInterest)")
    print("\n💡 Data Source:")
    print("  - Refinitiv Platform (proprietary, requires subscription)")

if __name__ == "__main__":
    main()
