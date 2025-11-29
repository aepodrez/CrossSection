# ABOUTME: Downloads IBES EPS estimates (unadjusted for splits) from Eikon/LSEG API for multiple forecast periods
# ABOUTME: Processes and standardizes IBES data, keeping last observation per ticker/period combination
"""
Inputs:
- Eikon/LSEG API (via eikon package)
- EIKON_APP_KEY from .env file
- Universe of symbols (S&P 500 by default)

Outputs:
- ../pyData/Intermediate/AP_IBES_EPS_Unadj.parquet

Requirements:
    pip install eikon pandas python-dotenv

How to run: python AP_IBESEPSUnadjusted.py

Notes:
- Requires active Eikon/Refinitiv subscription
- Fetches multiple forecast periods:
  * FPI=0: Long-term growth
  * FPI=1: Next fiscal year (FY1)
  * FPI=2: Two years ahead (FY2)
  * FPI=6: Current quarter
- Unadjusted for stock splits (raw estimates)
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
import warnings
warnings.filterwarnings('ignore')
import time

try:
    import eikon as ek
    EIKON_AVAILABLE = True
except ImportError:
    print("⚠️  eikon not installed. Install with: pip install eikon")
    EIKON_AVAILABLE = False

print("=" * 70, flush=True)
print("📊 AP_IBESEPSUnadjusted.py - IBES EPS Unadjusted from Eikon", flush=True)
print("=" * 70, flush=True)

load_dotenv()

OUTPUT_DIR = Path("../pyData/Intermediate")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# =============================================================================
# CONFIGURATION
# =============================================================================

# Date range - Last 2 years
END_DATE = datetime.now().strftime("%Y-%m-%d")
START_DATE = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")  # 2 years ago

DEBUG_MODE = False

if DEBUG_MODE:
    START_DATE = "2023-01-01"
    print(f"🔧 DEBUG MODE: {START_DATE} to {END_DATE}")
else:
    print(f"🚀 PRODUCTION MODE: {START_DATE} to {END_DATE}")

# =============================================================================
# HELPER FUNCTIONS (Reuse)
# =============================================================================

def convert_to_ric(symbol: str, exchange: str = "NASDAQ") -> str:
    if "." in symbol:
        return symbol
    exchange_map = {"NYSE": ".N", "NASDAQ": ".OQ", "AMEX": ".A"}
    suffix = exchange_map.get(exchange, ".OQ")
    return f"{symbol}{suffix}"

def get_sp500_tickers_with_exchange():
    try:
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        tables = pd.read_html(url)
        df = tables[0]
        ric_symbols = []
        for _, row in df.iterrows():
            symbol = row['Symbol'].replace('.', '-')
            if 'NYSE' in str(row.get('Exchange', '')):
                ric = convert_to_ric(symbol, "NYSE")
            else:
                ric = convert_to_ric(symbol, "NASDAQ")
            ric_symbols.append(ric)
        print(f"✓ Retrieved {len(ric_symbols)} S&P 500 tickers (RIC format)")
        return ric_symbols
    except Exception as e:
        print(f"⚠️  Could not fetch S&P 500 list: {e}")
        return ["AAPL.OQ", "MSFT.OQ", "GOOGL.OQ", "AMZN.OQ", "META.OQ"]

def connect_eikon():
    if not EIKON_AVAILABLE:
        return False
    eikon_app_key = os.getenv("EIKON_APP_KEY")
    if not eikon_app_key:
        print("\n❌ EIKON_APP_KEY not found in .env file")
        return False
    try:
        ek.set_app_key(eikon_app_key)
        test_df, test_err = ek.get_data(["AAPL.OQ"], ["TR.CompanyName"])
        if test_err:
            print(f"❌ Eikon connection test failed: {test_err}")
            return False
        print("✓ Eikon connection successful")
        return True
    except Exception as e:
        print(f"❌ Eikon connection error: {e}")
        return False

# =============================================================================
# BULK DOWNLOAD FUNCTION (MULTIPLE PERIODS)
# =============================================================================

def bulk_download_ibes_eps_unadjusted(symbols, start_date, end_date):
    """
    Download IBES EPS estimates for multiple forecast periods.
    
    Per IBESGuide.md: FPI codes represent:
    - 0: Long-term growth (5-year)
    - 1: Next fiscal year (FY1)
    - 2: Two years ahead (FY2)
    - 6: Current quarter
    """
    
    # We need to fetch different periods separately since Eikon uses "Period" parameter
    periods_to_fetch = [
        ("FY1", "1"),   # Next year
        ("FY2", "2"),   # Two years ahead
        # Note: LTG and quarterly need different fields/approach
    ]
    
    all_data = []
    
    for period_code, fpi_value in periods_to_fetch:
        print(f"\n  Fetching Period={period_code} (FPI={fpi_value})...")
        
        fields = [
            "TR.EPSMeanEstimate.date",
            "TR.EPSMeanEstimate.fperiod",
            "TR.EPSMeanEstimate",
            "TR.EPSNumberOfEstimates",
            "TR.EPSStdDev",
            "TR.EPSMedianEstimate",
        ]
        
        params = {
            "Scale": 6,
            "SDate": start_date,
            "EDate": end_date,
            "FRQ": "M",
            "Curn": "USD",
            "Period": period_code,
            "RH": "date",
        }
        
        batch_size = 50
        successful = 0
        failed = 0
        
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i+batch_size]
            
            if i % 250 == 0:
                print(f"    Processing batch {i//batch_size + 1}/{(len(symbols)-1)//batch_size + 1}")
            
            for symbol in batch:
                try:
                    df, err = ek.get_data([symbol], fields, params)
                    
                    if err or df is None or df.empty:
                        failed += 1
                        continue
                    
                    df["Instrument"] = symbol
                    df["fpi"] = fpi_value  # Add FPI indicator
                    
                    # Rename columns
                    df = df.rename(columns={
                        "TR.EPSMeanEstimate.date": "statpers",
                        "TR.EPSMeanEstimate.fperiod": "fpedats",
                        "TR.EPSMeanEstimate": "meanest",
                        "TR.EPSNumberOfEstimates": "numest",
                        "TR.EPSStdDev": "stdev",
                        "TR.EPSMedianEstimate": "medest",
                    })
                    
                    # Parse dates
                    df["statpers"] = pd.to_datetime(df["statpers"], errors="coerce")
                    df["fpedats"] = pd.to_datetime(df["fpedats"], errors="coerce")
                    
                    all_data.append(df)
                    successful += 1
                    time.sleep(0.2)
                    
                except Exception as e:
                    failed += 1
                    continue
            
            time.sleep(2)
        
        print(f"    ✓ Period {period_code}: {successful}/{len(symbols)} symbols (failed: {failed})")
    
    # Fetch Long-Term Growth (FPI=0)
    print(f"\n  Fetching Long-Term Growth (FPI=0)...")
    ltg_fields = [
        "TR.LTGMean.date",
        "TR.LTGMean",
        "TR.LTGMedian",
        "TR.LTGNumberOfEstimates",
    ]
    
    ltg_params = {
        "Scale": 6,
        "SDate": start_date,
        "EDate": "0",  # Use "0" for current (LSEG docs)
        "Frq": "M",    # Note: "Frq" not "FRQ" for LTG
        "Curn": "USD",
        "RH": "date",
    }
    
    successful_ltg = 0
    failed_ltg = 0
    
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        
        if i % 250 == 0:
            print(f"    Processing batch {i//batch_size + 1}/{(len(symbols)-1)//batch_size + 1}")
        
        for symbol in batch:
            try:
                df, err = ek.get_data([symbol], ltg_fields, ltg_params)
                
                if err or df is None or df.empty:
                    failed_ltg += 1
                    continue
                
                df["Instrument"] = symbol
                df["fpi"] = "0"  # LTG = FPI 0
                
                # Rename LTG columns to match standard format
                rename_map = {}
                for col in df.columns:
                    if "LTGMean.date" in col or ("LTG" in col and "Mean" in col and "date" in col):
                        rename_map[col] = "statpers"
                    elif "LTGMean" in col and "date" not in col:
                        rename_map[col] = "meanest"
                    elif "LTGMedian" in col:
                        rename_map[col] = "medest"
                    elif "LTGNumber" in col:
                        rename_map[col] = "numest"
                
                df = df.rename(columns=rename_map)
                
                # Parse dates
                if "statpers" in df.columns:
                    df["statpers"] = pd.to_datetime(df["statpers"], errors="coerce")
                
                # LTG doesn't have fpedats (forecast period end)
                df["fpedats"] = pd.NaT
                df["stdev"] = np.nan  # LTG typically doesn't have stdev
                
                all_data.append(df)
                successful_ltg += 1
                time.sleep(0.2)
                
            except Exception as e:
                failed_ltg += 1
                continue
        
        time.sleep(2)
    
    print(f"    ✓ LTG: {successful_ltg}/{len(symbols)} symbols (failed: {failed_ltg})")
    
    if not all_data:
        return pd.DataFrame()
    
    # Combine all periods
    result = pd.concat(all_data, ignore_index=True)
    result["tickerIBES"] = result["Instrument"].str.split(".").str[0]
    
    return result

# =============================================================================
# DATA PROCESSING
# =============================================================================

def process_unadjusted_data(df):
    """Process unadjusted IBES data to match original format"""
    
    if df.empty:
        return df
    
    print(f"\n🔄 Processing unadjusted IBES data...")
    
    # Create monthly time variable
    df['time_avail_m'] = df['statpers'].dt.to_period('M').dt.to_timestamp()
    df['time_avail_m'] = pd.to_datetime(df['time_avail_m'])
    
    # Remove missing mean estimates
    initial_count = len(df)
    df = df.dropna(subset=['meanest'])
    print(f"  Removed {initial_count - len(df)} records with missing meanest")
    
    # Keep last observation per ticker-fpi-month
    df = df.sort_values(['tickerIBES', 'fpi', 'time_avail_m', 'statpers'])
    df = df.drop_duplicates(['tickerIBES', 'fpi', 'time_avail_m'], keep='last')
    
    print(f"  After keeping last obs per month: {len(df)} records")
    
    # Select final columns (match original format)
    final_cols = ['tickerIBES', 'statpers', 'fpi', 'numest', 'medest', 'meanest', 'stdev', 'fpedats', 'time_avail_m']
    df = df[[col for col in final_cols if col in df.columns]]
    
    return df

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main execution function"""
    
    if not EIKON_AVAILABLE:
        print("\n❌ Cannot proceed without eikon package. Please install:")
        print("   pip install eikon")
        sys.exit(1)
    
    if not connect_eikon():
        print("\n❌ Eikon connection failed")
        sys.exit(1)
    
    print(f"\n📋 Loading ticker universe...")
    if DEBUG_MODE:
        symbols = ["AAPL.OQ", "MSFT.OQ", "GOOGL.OQ", "AMZN.OQ", "META.OQ"]
        print(f"  DEBUG MODE: Using {len(symbols)} sample tickers")
    else:
        symbols = get_sp500_tickers_with_exchange()
    
    print(f"\n📥 Downloading IBES EPS unadjusted estimates from Eikon...")
    print(f"  Fetching FPI=0 (LTG), FPI=1 (FY1), FPI=2 (FY2)...")
    ibes_df = bulk_download_ibes_eps_unadjusted(symbols, START_DATE, END_DATE)
    
    if ibes_df.empty:
        print("\n❌ No IBES data retrieved")
        sys.exit(1)
    
    final_df = process_unadjusted_data(ibes_df)
    
    if final_df.empty:
        print("\n❌ Data processing resulted in empty DataFrame")
        return
    
    print(f"\n💾 Saving output...")
    
    output_file = OUTPUT_DIR / "AP_IBES_EPS_Unadj.parquet"
    final_df.to_parquet(output_file, index=False)
    
    print(f"  ✓ Saved: {output_file}")
    print(f"    Records: {len(final_df):,}")
    print(f"    Size: {output_file.stat().st_size / 1024:.1f} KB")
    
    print(f"\n📊 Summary Statistics:")
    print(f"  Total observations: {len(final_df):,}")
    print(f"  Unique tickers: {final_df['tickerIBES'].nunique()}")
    print(f"  Date range: {final_df['time_avail_m'].min()} to {final_df['time_avail_m'].max()}")
    
    # Records by FPI
    print(f"\n  Records by FPI (Forecast Period Indicator):")
    fpi_counts = final_df['fpi'].value_counts().sort_index()
    fpi_labels = {"0": "Long-term growth", "1": "Next year (FY1)", "2": "Two years (FY2)"}
    for fpi, count in fpi_counts.items():
        label = fpi_labels.get(fpi, f"Unknown ({fpi})")
        pct = count / len(final_df) * 100
        print(f"    FPI={fpi} ({label}): {count:,} ({pct:.1f}%)")
    
    # Sample data
    print(f"\n  Sample data:")
    sample_cols = ['tickerIBES', 'fpi', 'time_avail_m', 'meanest', 'numest']
    available_cols = [col for col in sample_cols if col in final_df.columns]
    print(final_df[available_cols].head(15).to_string(index=False))
    
    print("\n" + "=" * 70)
    print("✅ AP_IBESEPSUnadjusted.py completed successfully!")
    print("=" * 70)
    print("\nOutputs:")
    print("  1. AP_IBES_EPS_Unadj.parquet - IBES EPS unadjusted estimates")
    print("\nNext steps:")
    print("  - Use AP_IBES_EPS_Unadj.parquet in place of IBES_EPS_Unadj.parquet")
    print("  - Includes FPI=0 (LTG), FPI=1 (FY1), FPI=2 (FY2)")

if __name__ == "__main__":
    main()

