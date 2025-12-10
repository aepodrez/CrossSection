# ABOUTME: Downloads IBES unadjusted actual earnings from Eikon/LSEG API and fills time series gaps
# ABOUTME: Creates monthly time series with forward-filled variables for each ticker
"""
Inputs:
- Eikon/LSEG API (via eikon package)
- EIKON_APP_KEY from .env file
- Universe of symbols (S&P 500 by default)

Outputs:
- ../pyData/Intermediate/AP_IBES_UnadjustedActuals.parquet

Requirements:
    pip install eikon pandas python-dotenv

How to run: python AP_IBESUnadjustedActuals.py

Notes:
- Requires active Eikon/Refinitiv subscription
- Downloads actual reported earnings (unadjusted for splits)
- Fills monthly time series gaps with forward-fill
- Per IBESGuide.md: Uses TR.EPSActValue, TR.EPSActReportDate, TR.ISPeriodEndDate
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
print("📊 AP_IBESUnadjustedActuals.py - IBES Actual Earnings from Eikon", flush=True)
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

def bulk_download_ibes_actual_earnings(symbols, start_date, end_date):
    """
    Download IBES actual earnings from Eikon.
    
    Per IBESGuide.md specifications.
    """
    
    # Eikon fields for IBES actual earnings
    fields = [
        "TR.EPSActValue",                # int0a - Actual EPS (unadjusted)
        "TR.EPSActReportDate",           # statpers - Report date
        "TR.ISPeriodEndDate",            # period_end_date - Fiscal period end
        "TR.TickerSymbol",               # ticker symbol
        "TR.CompanyName",                # company name
        "TR.FinancialPeriodAbsolute",    # fiscal period (e.g., FY2025)
        "TR.Currency",                   # currency
        "TR.CommonSharesOutstanding",    # shares outstanding (proxy for shoutIBESUnadj)
    ]
    
    # Parameters per IBESGuide.md
    params = {
        "Scale": 6,              # Millions
        "SDate": start_date,
        "EDate": end_date,
        "FRQ": "Q",              # Quarterly frequency for actuals
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
        
        if i % 250 == 0:
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
                
                # Rename columns to match WRDS IBES format
                rename_map = {}
                for col in df.columns:
                    if "EPSActValue" in col:
                        rename_map[col] = "int0a"
                    elif "EPSActReportDate" in col or ("Act" in col and "Report" in col and "Date" in col):
                        rename_map[col] = "statpers"
                    elif "ISPeriodEndDate" in col or ("Period" in col and "End" in col):
                        rename_map[col] = "fy0edats"
                    elif "TickerSymbol" in col or col == "Instrument":
                        continue  # Will handle separately
                    elif "CompanyName" in col:
                        rename_map[col] = "cname"
                    elif "FinancialPeriodAbsolute" in col:
                        rename_map[col] = "fiscal_period"
                    elif "Currency" in col and "curcode" not in rename_map.values():
                        rename_map[col] = "curcode"
                    elif "CommonSharesOutstanding" in col or ("Shares" in col and "Outstanding" in col):
                        rename_map[col] = "shoutIBESUnadj"
                
                df = df.rename(columns=rename_map)
                
                # Parse dates
                if "statpers" in df.columns:
                    df["statpers"] = pd.to_datetime(df["statpers"], errors="coerce")
                if "fy0edats" in df.columns:
                    df["fy0edats"] = pd.to_datetime(df["fy0edats"], errors="coerce")
                
                # Convert numeric fields
                if "int0a" in df.columns:
                    df["int0a"] = pd.to_numeric(df["int0a"], errors="coerce")
                if "shoutIBESUnadj" in df.columns:
                    df["shoutIBESUnadj"] = pd.to_numeric(df["shoutIBESUnadj"], errors="coerce")
                
                # Add measure field (always EPS for this download)
                df["measure"] = "EPS"
                
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

def process_actual_earnings_data(df):
    """Process actual earnings data with time series filling"""
    
    if df.empty:
        return df
    
    print(f"\n🔄 Processing actual earnings data...")
    
    # Create monthly time variable
    df['time_avail_m'] = df['statpers'].dt.to_period('M').dt.to_timestamp()
    df['time_avail_m'] = pd.to_datetime(df['time_avail_m'])
    
    # Remove within-month duplicates
    initial_count = len(df)
    df = df.drop_duplicates(['tickerIBES', 'time_avail_m'], keep='first')
    print(f"  After removing within-month duplicates: {len(df)} records")
    
    # Fill monthly time series gaps for each ticker
    print(f"  Filling time series gaps...")
    
    def fill_ticker_gaps(group):
        group = group.sort_values('time_avail_m')
        
        if len(group) <= 1:
            return group
        
        # Create full monthly time range
        min_time = group['time_avail_m'].min()
        max_time = group['time_avail_m'].max()
        full_time_range = pd.date_range(start=min_time, end=max_time, freq='MS')
        
        # Reindex to create missing months
        group = group.set_index('time_avail_m').reindex(full_time_range)
        group.index.name = 'time_avail_m'
        group = group.reset_index()
        
        # Forward fill key variables
        fill_vars = ['int0a', 'shoutIBESUnadj', 'tickerIBES', 'fy0edats', 'fiscal_period']
        for var in [v for v in fill_vars if v in group.columns]:
            group[var] = group[var].ffill()
        
        return group
    
    # Apply gap filling to each ticker
    df = df.groupby('tickerIBES', group_keys=False).apply(fill_ticker_gaps).reset_index(drop=True)
    print(f"  After filling time series gaps: {len(df)} records")
    
    # Handle missing values in string columns
    string_columns = ['cname', 'curcode', 'fiscal_period', 'measure']
    for col in [c for c in string_columns if c in df.columns]:
        df[col] = df[col].fillna('')
    
    # Ensure ticker is string
    df['tickerIBES'] = df['tickerIBES'].astype(str)
    
    # Ensure time column remains datetime
    df['time_avail_m'] = pd.to_datetime(df['time_avail_m'])
    
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
    
    print(f"\n📥 Downloading IBES actual earnings from Eikon...")
    actuals_df = bulk_download_ibes_actual_earnings(symbols, START_DATE, END_DATE)
    
    if actuals_df.empty:
        print("\n❌ No actual earnings data retrieved")
        sys.exit(1)
    
    final_df = process_actual_earnings_data(actuals_df)
    
    if final_df.empty:
        print("\n❌ Data processing resulted in empty DataFrame")
        return
    
    print(f"\n💾 Saving output...")
    
    output_file = OUTPUT_DIR / "AP_IBES_UnadjustedActuals.parquet"
    final_df.to_parquet(output_file, index=False)
    
    print(f"  ✓ Saved: {output_file}")
    print(f"    Records: {len(final_df):,}")
    print(f"    Size: {output_file.stat().st_size / 1024:.1f} KB")
    
    print(f"\n📊 Summary Statistics:")
    print(f"  Total observations: {len(final_df):,}")
    print(f"  Unique tickers: {final_df['tickerIBES'].nunique()}")
    print(f"  Date range: {final_df['time_avail_m'].min()} to {final_df['time_avail_m'].max()}")
    
    # Actual earnings statistics
    if 'int0a' in final_df.columns:
        int0a_data = final_df['int0a'].dropna()
        if len(int0a_data) > 0:
            print(f"\n  Actual EPS (int0a):")
            print(f"    Non-null observations: {len(int0a_data):,}")
            print(f"    Mean: ${int0a_data.mean():.2f}")
            print(f"    Median: ${int0a_data.median():.2f}")
            print(f"    Std: ${int0a_data.std():.2f}")
    
    # Sample data
    print(f"\n  Sample data:")
    sample_cols = ['tickerIBES', 'time_avail_m', 'int0a', 'fy0edats', 'shoutIBESUnadj']
    available_cols = [col for col in sample_cols if col in final_df.columns]
    print(final_df[available_cols].head(10).to_string(index=False))
    
    print("\n" + "=" * 70)
    print("✅ AP_IBESUnadjustedActuals.py completed successfully!")
    print("=" * 70)
    print("\nOutputs:")
    print("  1. AP_IBES_UnadjustedActuals.parquet - IBES actual earnings")
    print("\nNext steps:")
    print("  - Use AP_IBES_UnadjustedActuals.parquet in place of IBES_UnadjustedActuals.parquet")

if __name__ == "__main__":
    main()

