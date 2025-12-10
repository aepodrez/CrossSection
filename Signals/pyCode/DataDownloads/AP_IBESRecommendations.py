# ABOUTME: Downloads IBES analyst recommendations from Eikon/LSEG API (live data)
# ABOUTME: Processes recommendation codes (1-5 scale) and creates monthly time availability
"""
Inputs:
- Eikon/LSEG API (via eikon package)
- EIKON_APP_KEY from .env file
- Universe of symbols (S&P 500 by default)

Outputs:
- ../pyData/Intermediate/AP_IBES_Recommendations.parquet

Requirements:
    pip install eikon pandas python-dotenv

How to run: python AP_IBESRecommendations.py

Notes:
- Requires active Eikon/Refinitiv subscription
- Recommendation scale: 1=Strong Buy, 2=Buy, 3=Hold, 4=Sell, 5=Strong Sell
- Daily frequency data
- Rate limited: ~5 requests/second
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

# Try to import eikon
try:
    import eikon as ek
    EIKON_AVAILABLE = True
except ImportError:
    print("⚠️  eikon not installed. Install with: pip install eikon")
    EIKON_AVAILABLE = False

# Print script header
print("=" * 70, flush=True)
print("📊 AP_IBESRecommendations.py - IBES Recommendations from Eikon", flush=True)
print("=" * 70, flush=True)

# Load environment variables
load_dotenv()

# Output directory
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
# HELPER FUNCTIONS (Reuse from AP_IBESEPSAdjusted.py)
# =============================================================================

def convert_to_ric(symbol: str, exchange: str = "NASDAQ") -> str:
    """Convert ticker to RIC format"""
    if "." in symbol:
        return symbol
    
    exchange_map = {"NYSE": ".N", "NASDAQ": ".OQ", "AMEX": ".A"}
    suffix = exchange_map.get(exchange, ".OQ")
    return f"{symbol}{suffix}"

def load_sp500_universe():
    """Load S&P 500 ticker universe from pickle file."""
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
    
    # Final fallback: Wikipedia
    try:
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        tables = pd.read_html(url)
        df = tables[0]
        tickers = df['Symbol'].replace('.', '-').tolist()
        print(f"✓ Retrieved {len(tickers)} S&P 500 tickers from Wikipedia")
        return tickers
    except Exception as e:
        print(f"⚠️  Could not fetch S&P 500 list: {e}")
        return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META']


def get_sp500_tickers_with_exchange():
    """Get S&P 500 tickers in RIC format from pickle file."""
    print("\n" + "="*60)
    print("📋 Loading ticker universe...")
    print("="*60)
    
    # Load tickers from SP500 universe
    tickers = load_sp500_universe()
    
    # Convert to RIC format
    ric_symbols = []
    # Common NYSE tickers
    nyse_tickers = {
        'JPM', 'V', 'JNJ', 'WMT', 'PG', 'UNH', 'HD', 'DIS', 'BAC', 'MA',
        'XOM', 'CVX', 'KO', 'PEP', 'T', 'VZ', 'MRK', 'ABT', 'TMO', 'DHR'
    }
    
    for ticker in tickers:
        symbol = str(ticker).replace('.', '-').upper()
        if symbol in nyse_tickers:
            ric = convert_to_ric(symbol, "NYSE")
        else:
            ric = convert_to_ric(symbol, "NASDAQ")
        ric_symbols.append(ric)
    
    print(f"✓ Converted {len(tickers)} tickers to {len(ric_symbols)} RICs")
    return ric_symbols

def connect_eikon():
    """Connect to Eikon API"""
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
# BULK DOWNLOAD FUNCTION
# =============================================================================

def bulk_download_ibes_recommendations(symbols, start_date, end_date):
    """
    Bulk download IBES analyst recommendations from Eikon.
    
    Per IBESGuide.md specifications.
    """
    
    # Eikon fields for IBES recommendations
    fields = [
        "TR.RecEstValue.date",          # Date
        "TR.RecEstValue",               # Recommendation code (1-5)
        "TR.BrkRecLabel",               # Recommendation text
        "TR.RecLabelEstBrokerName",     # Broker name
        "TR.AnalystName"                # Analyst name
    ]
    
    # Parameters per IBESGuide.md
    params = {
        "Scale": 6,
        "SDate": start_date,
        "EDate": end_date,
        "FRQ": "D",              # Daily frequency
        "Curn": "USD",
        "RH": "date",
    }
    
    all_data = []
    batch_size = 50
    successful = 0
    failed = 0
    
    total_symbols = len(symbols)
    
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        
        print(f"  Processing batch {i//batch_size + 1}/{(total_symbols-1)//batch_size + 1}")
        
        for symbol in batch:
            try:
                df, err = ek.get_data([symbol], fields, params)
                
                if err:
                    failed += 1
                    continue
                
                if df is None or df.empty:
                    failed += 1
                    continue
                
                # Add symbol column
                df["Instrument"] = symbol
                
                # Rename columns (handle Eikon's actual column names)
                rename_map = {}
                for col in df.columns:
                    if "RecEstValue.date" in col:
                        rename_map[col] = "anndats"
                    elif "RecEstValue" in col and "date" not in col:
                        rename_map[col] = "ireccd"
                    elif "BrkRecLabel" in col or "Broker Rec" in col:
                        rename_map[col] = "itext"
                    elif "Broker Name" in col or "BrokerName" in col:
                        rename_map[col] = "broker_name"
                    elif "Analyst" in col and "Name" in col:
                        rename_map[col] = "analyst_name"
                
                df = df.rename(columns=rename_map)
                
                # Convert recommendation code to numeric
                if "ireccd" in df.columns:
                    df["ireccd"] = pd.to_numeric(df["ireccd"], errors="coerce")
                
                # Parse date
                if "anndats" in df.columns:
                    df["anndats"] = pd.to_datetime(df["anndats"], errors="coerce")
                
                # Create analyst mask code (broker + analyst name)
                if "broker_name" in df.columns and "analyst_name" in df.columns:
                    df["amaskcd"] = df["broker_name"].astype(str) + "_" + df["analyst_name"].astype(str)
                else:
                    df["amaskcd"] = ""
                
                all_data.append(df)
                successful += 1
                
                # Rate limiting
                time.sleep(0.2)
                
            except Exception as e:
                failed += 1
                continue
        
        # Delay between batches
        time.sleep(2)
    
    print(f"✓ Downloaded data for {successful}/{total_symbols} symbols (failed: {failed})")
    
    if not all_data:
        return pd.DataFrame()
    
    # Combine all data
    result = pd.concat(all_data, ignore_index=True)
    
    # Extract ticker from RIC
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
    
    if not EIKON_AVAILABLE:
        print("\n❌ Cannot proceed without eikon package. Please install:")
        print("   pip install eikon")
        sys.exit(1)
    
    # Connect to Eikon
    if not connect_eikon():
        print("\n❌ Eikon connection failed")
        sys.exit(1)
    
    # Get ticker universe
    print(f"\n📋 Loading ticker universe...")
    if DEBUG_MODE:
        symbols = ["AAPL.OQ", "MSFT.OQ", "GOOGL.OQ", "AMZN.OQ", "META.OQ"]
        print(f"  DEBUG MODE: Using {len(symbols)} sample tickers")
    else:
        symbols = get_sp500_tickers_with_exchange()
    
    # Download IBES recommendations
    print(f"\n📥 Downloading IBES recommendations from Eikon...")
    rec_df = bulk_download_ibes_recommendations(symbols, START_DATE, END_DATE)
    
    if rec_df.empty:
        print("\n❌ No recommendations data retrieved")
        sys.exit(1)
    
    # Process data
    final_df = process_recommendations_data(rec_df)
    
    if final_df.empty:
        print("\n❌ Data processing resulted in empty DataFrame")
        sys.exit(1)
    
    # Save output
    print(f"\n💾 Saving output...")
    
    output_file = OUTPUT_DIR / "AP_IBES_Recommendations.parquet"
    final_df.to_parquet(output_file, index=False)
    
    print(f"  ✓ Saved: {output_file}")
    print(f"    Records: {len(final_df):,}")
    print(f"    Size: {output_file.stat().st_size / 1024:.1f} KB")
    
    # Generate summary
    print(f"\n📊 Summary Statistics:")
    print(f"  Total observations: {len(final_df):,}")
    print(f"  Unique tickers: {final_df['tickerIBES'].nunique()}")
    
    if 'time_avail_m' in final_df.columns:
        print(f"  Date range: {final_df['time_avail_m'].min()} to {final_df['time_avail_m'].max()}")
        print(f"  Months covered: {final_df['time_avail_m'].nunique()}")
    
    # Recommendation distribution
    if 'ireccd' in final_df.columns:
        print(f"\n  Recommendation distribution:")
        rec_counts = final_df['ireccd'].value_counts().sort_index()
        rec_labels = {1: 'Strong Buy', 2: 'Buy', 3: 'Hold', 4: 'Sell', 5: 'Strong Sell'}
        for code, count in rec_counts.items():
            label = rec_labels.get(int(code), f'Unknown ({int(code)})')
            pct = count / len(final_df) * 100
            print(f"    {int(code)} ({label}): {count:,} ({pct:.1f}%)")
    
    # Sample data
    print(f"\n  Sample data:")
    sample_cols = ['tickerIBES', 'amaskcd', 'anndats', 'time_avail_m', 'ireccd']
    available_cols = [col for col in sample_cols if col in final_df.columns]
    print(final_df[available_cols].head(10).to_string(index=False))
    
    print("\n" + "=" * 70)
    print("✅ AP_IBESRecommendations.py completed successfully!")
    print("=" * 70)
    print("\nOutputs:")
    print("  1. AP_IBES_Recommendations.parquet - IBES analyst recommendations")
    print("\nNext steps:")
    print("  - Use AP_IBES_Recommendations.parquet in place of IBES_Recommendations.parquet")
    print("  - Use for recommendation-based predictors (ConsRecomm, Recomm_ShortInterest)")
    print("\n💡 Data Source:")
    print("  - Eikon/LSEG API (proprietary, requires subscription)")

if __name__ == "__main__":
    main()

