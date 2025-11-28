# ABOUTME: Downloads IBES EPS estimates (adjusted for splits) from Eikon/LSEG API (live data)
# ABOUTME: Processes data to monthly frequency, removes missing estimates, and applies column standardization
"""
Inputs:
- Eikon/LSEG API (via eikon package)
- EIKON_APP_KEY from .env file (get from Eikon Desktop)
- Universe of symbols (S&P 500 by default)

Outputs:
- ../pyData/Intermediate/AP_IBES_EPS_Adj.parquet

Requirements:
    pip install eikon pandas python-dotenv

How to run: python AP_IBESEPSAdjusted.py

Notes:
- Requires active Eikon/Refinitiv subscription
- Uses Eikon Python API for IBES data access
- IBES data is NOT available from free sources
- Symbols must be in RIC format (e.g., "AAPL.O", "MSFT.OQ")
- Rate limited: ~5 requests/second, 300 requests/minute
"""

import os
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
    print("⚠️  Note: Requires active Eikon/Refinitiv subscription")
    EIKON_AVAILABLE = False

# Print script header
print("=" * 70, flush=True)
print("📊 AP_IBESEPSAdjusted.py - IBES EPS Estimates from Eikon", flush=True)
print("=" * 70, flush=True)

# Load environment variables
load_dotenv()

# Output directory
OUTPUT_DIR = Path("../pyData/Intermediate")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# =============================================================================
# CONFIGURATION
# =============================================================================

# Date range
START_DATE = "2020-01-01"
END_DATE = datetime.now().strftime("%Y-%m-%d")

# Debug mode
DEBUG_MODE = False

if DEBUG_MODE:
    START_DATE = "2023-01-01"
    print(f"🔧 DEBUG MODE: {START_DATE} to {END_DATE}")
else:
    print(f"🚀 PRODUCTION MODE: {START_DATE} to {END_DATE}")

# =============================================================================
# RIC FORMAT CONVERSION
# =============================================================================

def convert_to_ric(symbol: str, exchange: str = "NASDAQ") -> str:
    """
    Convert simple ticker to RIC format.
    
    Exchange suffixes:
    - NYSE: .N
    - NASDAQ: .OQ (or .O alternate)
    - AMEX: .A
    """
    if "." in symbol:
        return symbol  # Already in RIC format
    
    exchange_map = {
        "NYSE": ".N",
        "NASDAQ": ".OQ",
        "AMEX": ".A",
    }
    
    suffix = exchange_map.get(exchange, ".OQ")
    return f"{symbol}{suffix}"

def get_sp500_tickers_with_exchange():
    """Get S&P 500 tickers with exchange information"""
    try:
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        tables = pd.read_html(url)
        df = tables[0]
        
        # Convert to RIC format
        ric_symbols = []
        for _, row in df.iterrows():
            symbol = row['Symbol'].replace('.', '-')
            
            # Determine exchange (most S&P 500 are NYSE or NASDAQ)
            # This is a simplification - in production, use proper exchange mapping
            if 'NYSE' in str(row.get('Exchange', '')):
                ric = convert_to_ric(symbol, "NYSE")
            else:
                ric = convert_to_ric(symbol, "NASDAQ")
            
            ric_symbols.append(ric)
        
        print(f"✓ Retrieved {len(ric_symbols)} S&P 500 tickers (RIC format)")
        return ric_symbols
    except Exception as e:
        print(f"⚠️  Could not fetch S&P 500 list: {e}")
        # Return a few well-known RIC symbols as fallback
        return ["AAPL.OQ", "MSFT.OQ", "GOOGL.OQ", "AMZN.OQ", "META.OQ"]

# =============================================================================
# EIKON CONNECTION
# =============================================================================

def connect_eikon():
    """Connect to Eikon API"""
    if not EIKON_AVAILABLE:
        print("❌ eikon package not available")
        return False
    
    eikon_app_key = os.getenv("EIKON_APP_KEY")
    
    if not eikon_app_key:
        print("\n❌ EIKON_APP_KEY not found in environment variables")
        print("\n📋 To get Eikon App Key:")
        print("   1. Open Eikon Desktop application")
        print("   2. Go to: APPKEY (type in search)")
        print("   3. Generate/copy your App Key")
        print("   4. Add to .env file: EIKON_APP_KEY=your_key_here")
        return False
    
    try:
        ek.set_app_key(eikon_app_key)
        
        # Test connection
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

def bulk_download_ibes_eps_estimates(symbols, start_date, end_date):
    """
    Bulk download IBES EPS estimates from Eikon.
    
    Based on IBESGuide.md specifications.
    """
    
    # Eikon fields for IBES EPS estimates
    fields = [
        "TR.EPSMeanEstimate.date",       # Statement period date (statpers)
        "TR.EPSMeanEstimate.fperiod",    # Forecast period end date (fpedats)
        "TR.EPSMeanEstimate",            # Mean EPS estimate (meanest)
        "TR.EPSNumberOfEstimates",       # Number of analysts (numest)
        "TR.EPSStdDev",                  # Standard deviation (stdev)
        "TR.EPSMedianEstimate",          # Median EPS estimate (medest)
        "TR.EPSActValue",                # Actual EPS value (actual)
    ]
    
    # Parameters per IBESGuide.md
    params = {
        "Scale": 6,              # Millions
        "SDate": start_date,
        "EDate": end_date,
        "FRQ": "M",              # Monthly frequency
        "Curn": "USD",
        "Period": "FY1",         # Next fiscal year
        "RH": "date",            # Return history
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
                
                # Normalize column names to match WRDS format
                df = df.rename(columns={
                    "TR.EPSMeanEstimate.date": "statpers",
                    "TR.EPSMeanEstimate.fperiod": "fpedats",
                    "TR.EPSMeanEstimate": "meanest",
                    "TR.EPSNumberOfEstimates": "numest",
                    "TR.EPSStdDev": "stdev",
                    "TR.EPSMedianEstimate": "medest",
                    "TR.EPSActValue": "actual",
                })
                
                # Parse dates
                df["statpers"] = pd.to_datetime(df["statpers"], errors="coerce")
                df["fpedats"] = pd.to_datetime(df["fpedats"], errors="coerce")
                
                # Add fpi field (forecast period indicator)
                df["fpi"] = "1"  # FY1 equivalent
                
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
    
    # Extract ticker from RIC (e.g., "AAPL.OQ" → "AAPL")
    result["tickerIBES"] = result["Instrument"].str.split(".").str[0]
    
    return result

# =============================================================================
# DATA PROCESSING
# =============================================================================

def process_ibes_data(df):
    """Process IBES data to match original format"""
    
    if df.empty:
        return df
    
    print(f"\n🔄 Processing IBES data...")
    
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
    
    return df

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main execution function"""
    
    if not EIKON_AVAILABLE:
        print("\n❌ Cannot proceed without eikon package. Please install:")
        print("   pip install eikon")
        print("\n⚠️  Note: Requires active Eikon/Refinitiv subscription")
        return
    
    # Connect to Eikon
    if not connect_eikon():
        print("\n❌ Eikon connection failed")
        return
    
    # Get ticker universe
    print(f"\n📋 Loading ticker universe...")
    if DEBUG_MODE:
        symbols = ["AAPL.OQ", "MSFT.OQ", "GOOGL.OQ", "AMZN.OQ", "META.OQ"]
        print(f"  DEBUG MODE: Using {len(symbols)} sample tickers")
    else:
        symbols = get_sp500_tickers_with_exchange()
    
    # Download IBES EPS estimates
    print(f"\n📥 Downloading IBES EPS estimates from Eikon...")
    ibes_df = bulk_download_ibes_eps_estimates(symbols, START_DATE, END_DATE)
    
    if ibes_df.empty:
        print("\n❌ No IBES data retrieved")
        return
    
    # Process data
    final_df = process_ibes_data(ibes_df)
    
    if final_df.empty:
        print("\n❌ Data processing resulted in empty DataFrame")
        return
    
    # Save output
    print(f"\n💾 Saving output...")
    
    output_file = OUTPUT_DIR / "AP_IBES_EPS_Adj.parquet"
    final_df.to_parquet(output_file, index=False)
    
    print(f"  ✓ Saved: {output_file}")
    print(f"    Records: {len(final_df):,}")
    print(f"    Size: {output_file.stat().st_size / 1024:.1f} KB")
    
    # Generate summary
    print(f"\n📊 Summary Statistics:")
    print(f"  Total observations: {len(final_df):,}")
    print(f"  Unique tickers: {final_df['tickerIBES'].nunique()}")
    print(f"  Date range: {final_df['time_avail_m'].min()} to {final_df['time_avail_m'].max()}")
    print(f"  Months covered: {final_df['time_avail_m'].nunique()}")
    
    # Column completeness
    print(f"\n  Data completeness:")
    for col in ['meanest', 'medest', 'numest', 'stdev', 'actual']:
        if col in final_df.columns:
            count = final_df[col].notna().sum()
            pct = count / len(final_df) * 100
            print(f"    {col}: {count:,} / {len(final_df):,} ({pct:.1f}%)")
    
    # Analyst coverage statistics
    if 'numest' in final_df.columns:
        numest_data = final_df['numest'].dropna()
        if len(numest_data) > 0:
            print(f"\n  Analyst coverage (numest):")
            print(f"    Mean: {numest_data.mean():.1f} analysts")
            print(f"    Median: {numest_data.median():.1f} analysts")
            print(f"    Min: {numest_data.min():.0f}, Max: {numest_data.max():.0f}")
    
    # Sample data
    print(f"\n  Sample data:")
    sample_cols = ['tickerIBES', 'fpi', 'time_avail_m', 'meanest', 'numest', 'stdev']
    available_cols = [col for col in sample_cols if col in final_df.columns]
    print(final_df[available_cols].head(10).to_string(index=False))
    
    print("\n" + "=" * 70)
    print("✅ AP_IBESEPSAdjusted.py completed successfully!")
    print("=" * 70)
    print("\nOutputs:")
    print("  1. AP_IBES_EPS_Adj.parquet - IBES EPS estimates")
    print("\nNext steps:")
    print("  - Use AP_IBES_EPS_Adj.parquet in place of IBES_EPS_Adj.parquet")
    print("  - Use for analyst-based predictors (ChNAnalyst, FEPS, REV6, etc.)")
    print("\n💡 Data Source:")
    print("  - Eikon/LSEG API (proprietary, requires subscription)")
    print("  - IBES data NOT available from free sources")

if __name__ == "__main__":
    main()

