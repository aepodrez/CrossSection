# ABOUTME: Downloads daily stock data using yfinance (free alternative to CRSP Daily)
# ABOUTME: Produces CRSP-like price/return/split factors (cfacpr/cfacshr) and shrout approximations
"""
Inputs:
- List of tickers (S&P 500 by default, or user-provided list)
- yfinance API (free, no authentication required)

Outputs:
- ../pyData/Intermediate/AP_dailyCRSP.parquet (full CRSP-like daily data)
- ../pyData/Intermediate/AP_dailyCRSPprc.parquet (price-only version)

Requirements:
    pip install yfinance

How to run: python AP_CRSPDaily.py

Notes:
- Uses ticker symbols instead of CRSP permno (creates numeric ID mapping)
- Historical data typically available from ~2000 onwards (varies by ticker)
- yfinance provides adjusted prices (for splits/dividends)
- Free but has rate limits (~2000 requests/hour)
- Real-time data with ~15-minute delay (upgrade available for real-time)
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Try to import yfinance
try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    print("⚠️  yfinance not installed. Install with: pip install yfinance")
    print("⚠️  Running in demo mode with sample data structure only.")
    YFINANCE_AVAILABLE = False

# Print script header
print("=" * 70, flush=True)
print("📈 AP_CRSPDaily.py - Live Daily Stock Data (yfinance)", flush=True)
print("=" * 70, flush=True)

# =============================================================================
# CONFIGURATION
# =============================================================================

# Date range for download - Last 2 years
END_DATE = datetime.now().strftime('%Y-%m-%d')
START_DATE = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')  # 2 years ago

# Debug mode: download limited tickers and date range
DEBUG_MODE = False  # Set to True for testing with small dataset

if DEBUG_MODE:
    START_DATE = '2023-01-01'
    END_DATE = '2024-12-31'
    print(f"🔧 DEBUG MODE: {START_DATE} to {END_DATE}")
else:
    print(f"🚀 PRODUCTION MODE: {START_DATE} to {END_DATE}")

# Output directory
OUTPUT_DIR = Path("../pyData/Intermediate")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# =============================================================================
# TICKER LISTS
# =============================================================================

def load_sp500_universe():
    """Load S&P 500 ticker universe from pickle file"""
    import pickle
    universe_path = Path("../pyData/Static/sp500_universe.pkl")
    try:
        with open(universe_path, 'rb') as f:
            tickers = pickle.load(f)
        print(f"✓ Loaded {len(tickers)} tickers from sp500_universe.pkl")
        return tickers
    except Exception as e:
        print(f"⚠️  Could not load sp500_universe.pkl: {e}")
        # Fallback to a small list of major stocks
        return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NVDA', 'JPM', 'V', 'JNJ',
                'WMT', 'PG', 'UNH', 'MA', 'HD', 'DIS', 'BAC', 'ADBE', 'NFLX', 'CMCSA']

def get_user_ticker_list():
    """
    User can provide their own ticker list here.
    Options:
    1. Hard-code tickers
    2. Read from CSV file
    3. Use S&P 500 from pickle file (default)
    """
    # Option 1: Hard-coded list (for specific universe)
    # return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
    
    # Option 2: Read from file
    ticker_file = OUTPUT_DIR / "ticker_list.csv"
    if ticker_file.exists():
        df = pd.read_csv(ticker_file)
        if 'ticker' in df.columns:
            return df['ticker'].tolist()
        elif 'symbol' in df.columns:
            return df['symbol'].tolist()
    
    # Option 3: Default to S&P 500 from pickle file
    return load_sp500_universe()

# =============================================================================
# DATA DOWNLOAD FUNCTIONS
# =============================================================================

def create_ticker_to_permno_mapping(tickers):
    """
    Create a mapping from ticker to numeric ID (permno substitute).
    Since we don't have CRSP permno, we create our own numeric IDs.
    """
    ticker_map = pd.DataFrame({
        'ticker': sorted(tickers),
        'permno': range(1, len(tickers) + 1)
    })
    
    # Save mapping for reference
    ticker_map.to_csv(OUTPUT_DIR / "AP_ticker_to_permno.csv", index=False)
    print(f"✓ Created ticker-to-permno mapping for {len(ticker_map)} tickers")
    
    return dict(zip(ticker_map['ticker'], ticker_map['permno']))

def download_ticker_data(ticker, start_date, end_date, permno):
    """
    Download daily data for a single ticker using yfinance.
    
    Returns DataFrame with CRSP-like columns:
    - permno: Numeric identifier (substitute for CRSP permno)
    - time_d: Date
    - ret: Daily return using adjusted prices (CRSP-style)
    - vol: Volume
    - prc: Raw close (unadjusted, like CRSP PRC)
    - cfacpr: Cumulative split-only price factor (from split ratios)
    - shrout: Shares outstanding (approx., in thousands, split-adjusted historically)
    - cfacshr: Cumulative split-only share factor (back-adjusted)
    """
    try:
        # Download data
        stock = yf.Ticker(ticker)
        hist = stock.history(start=start_date, end=end_date, auto_adjust=False)
        
        if hist.empty:
            print(f"  ⚠️  No data for {ticker}")
            return pd.DataFrame()
        # Raw close (CRSP-style prc) and adjusted close (for total return)
        raw_prc = hist['Close']
        adj_prc = hist['Adj Close']

        # Daily total return using adjusted prices (includes splits/dividends)
        ret = adj_prc.pct_change()

        # Split-only price factor: use Stock Splits ratios (new/old); facpr = 1/ratio
        splits = hist.get('Stock Splits', pd.Series(0, index=hist.index)).fillna(0).astype(float)
        facpr = pd.Series(1.0, index=hist.index)
        facpr.loc[splits != 0] = 1.0 / splits.loc[splits != 0]
        cfacpr = facpr.cumprod()

        # Share factor: forward-time factor = ratio, cumulative toward past via reverse cumprod
        facshr = pd.Series(1.0, index=hist.index)
        facshr.loc[splits != 0] = splits.loc[splits != 0]
        cfacshr = facshr.iloc[::-1].cumprod().iloc[::-1]

        # Approximate historical shrout from latest shares outstanding, back-adjusted by cfacshr
        try:
            shares_outstanding = stock.info.get('sharesOutstanding', np.nan)
            if pd.isna(shares_outstanding) or shares_outstanding == 0:
                shares_outstanding = stock.info.get('impliedSharesOutstanding', np.nan)
        except Exception:
            shares_outstanding = np.nan
        shrout_k = np.nan
        if not pd.isna(shares_outstanding) and shares_outstanding > 0:
            shrout_k = shares_outstanding / 1000.0
            shrout_series = shrout_k / cfacshr
        else:
            shrout_series = pd.Series(np.nan, index=hist.index)

        df = pd.DataFrame({
            'permno': permno,
            'time_d': hist.index,
            'ret': ret,
            'vol': hist['Volume'],
            'prc': raw_prc,
            'cfacpr': cfacpr,
            'shrout': shrout_series,
            'cfacshr': cfacshr
        }).reset_index(drop=True)

        return df
        
    except Exception as e:
        print(f"  ❌ Error downloading {ticker}: {e}")
        return pd.DataFrame()

def download_all_tickers(tickers, start_date, end_date, ticker_to_permno):
    """Download data for all tickers with progress tracking"""
    all_data = []
    total = len(tickers)
    
    print(f"\n📥 Downloading data for {total} tickers...")
    
    for i, ticker in enumerate(tickers, 1):
        if i % 50 == 0 or i == 1:
            print(f"  Progress: {i}/{total} ({i/total*100:.1f}%)")
        
        permno = ticker_to_permno.get(ticker)
        if permno is None:
            continue
        
        df = download_ticker_data(ticker, start_date, end_date, permno)
        
        if not df.empty:
            all_data.append(df)
        
        # Rate limiting: small delay to avoid hitting yfinance limits
        if i % 100 == 0:
            import time
            time.sleep(2)  # Pause every 100 tickers
    
    print(f"✓ Successfully downloaded data for {len(all_data)} tickers")
    
    return all_data

# =============================================================================
# DATA PROCESSING FUNCTIONS
# =============================================================================

def process_daily_data(all_data):
    """
    Combine and process all ticker data into CRSP-like format
    """
    if not all_data:
        print("❌ No data to process")
        return pd.DataFrame()
    
    print("\n🔄 Processing data...")
    
    # Combine all dataframes
    combined = pd.concat(all_data, ignore_index=True)
    print(f"  Total records: {len(combined):,}")
    
    # Ensure correct data types
    combined['permno'] = combined['permno'].astype('int32')
    combined['time_d'] = pd.to_datetime(combined['time_d']).dt.floor('D')
    combined['prc'] = combined['prc'].astype('float32')
    combined['ret'] = combined['ret'].astype('float32')
    combined['vol'] = combined['vol'].astype('float64')
    combined['shrout'] = combined['shrout'].astype('float32')
    combined['cfacpr'] = combined['cfacpr'].astype('float32')
    combined['cfacshr'] = combined['cfacshr'].astype('float32')
    
    # Sort by permno and date
    combined = combined.sort_values(['permno', 'time_d']).reset_index(drop=True)
    
    # Basic data quality checks
    print(f"  Date range: {combined['time_d'].min()} to {combined['time_d'].max()}")
    print(f"  Unique tickers (permno): {combined['permno'].nunique()}")
    print(f"  Avg records per ticker: {len(combined) / combined['permno'].nunique():.0f}")
    
    return combined

def save_outputs(combined_data):
    """
    Save two versions of the data:
    1. Full daily file (returns, volume, prices)
    2. Price-only file (prices, adjustment factors, shares)
    """
    if combined_data.empty:
        print("❌ No data to save")
        return
    
    print("\n💾 Saving outputs...")
    
    # 1. Full daily file
    full_columns = ['permno', 'time_d', 'ret', 'vol', 'prc', 'cfacpr', 'shrout', 'cfacshr']
    daily_full = combined_data[full_columns].copy()
    
    output_file = OUTPUT_DIR / "AP_dailyCRSP.parquet"
    daily_full.to_parquet(output_file, index=False)
    print(f"  ✓ Saved: {output_file}")
    print(f"    Records: {len(daily_full):,}")
    print(f"    Size: {output_file.stat().st_size / 1024 / 1024:.1f} MB")
    
    # 2. Price-only file
    price_columns = ['permno', 'time_d', 'prc', 'cfacpr', 'shrout', 'cfacshr']
    daily_prc = combined_data[price_columns].copy()
    
    output_file_prc = OUTPUT_DIR / "AP_dailyCRSPprc.parquet"
    daily_prc.to_parquet(output_file_prc, index=False)
    print(f"  ✓ Saved: {output_file_prc}")
    print(f"    Records: {len(daily_prc):,}")
    print(f"    Size: {output_file_prc.stat().st_size / 1024 / 1024:.1f} MB")

def generate_summary_stats(combined_data):
    """Generate summary statistics"""
    print("\n📊 Summary Statistics:")
    print(f"  Total records: {len(combined_data):,}")
    print(f"  Unique tickers: {combined_data['permno'].nunique()}")
    print(f"  Date range: {combined_data['time_d'].min()} to {combined_data['time_d'].max()}")
    print(f"  Total trading days: {combined_data['time_d'].nunique()}")
    
    # Return statistics
    print(f"\n  Return statistics:")
    print(f"    Mean daily return: {combined_data['ret'].mean()*100:.4f}%")
    print(f"    Median daily return: {combined_data['ret'].median()*100:.4f}%")
    print(f"    Std daily return: {combined_data['ret'].std()*100:.4f}%")
    
    # Volume statistics
    print(f"\n  Volume statistics:")
    print(f"    Mean daily volume: {combined_data['vol'].mean():,.0f}")
    print(f"    Median daily volume: {combined_data['vol'].median():,.0f}")
    
    # Price statistics
    print(f"\n  Price statistics:")
    print(f"    Mean price: ${combined_data['prc'].mean():.2f}")
    print(f"    Median price: ${combined_data['prc'].median():.2f}")
    
    # Data completeness
    print(f"\n  Data completeness:")
    for col in ['ret', 'vol', 'prc', 'shrout', 'cfacpr', 'cfacshr']:
        pct = (1 - combined_data[col].isna().sum() / len(combined_data)) * 100
        print(f"    {col}: {pct:.1f}%")

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main execution function"""
    
    if not YFINANCE_AVAILABLE:
        print("\n❌ Cannot proceed without yfinance. Please install:")
        print("   pip install yfinance")
        return
    
    # Get ticker list
    print("\n📋 Loading ticker list...")
    if DEBUG_MODE:
        tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META', 'JPM', 'V', 'WMT']
        print(f"  DEBUG MODE: Using {len(tickers)} sample tickers")
    else:
        tickers = get_user_ticker_list()
    
    # Create ticker-to-permno mapping
    ticker_to_permno = create_ticker_to_permno_mapping(tickers)
    
    # Download data
    all_data = download_all_tickers(tickers, START_DATE, END_DATE, ticker_to_permno)
    
    if not all_data:
        print("\n❌ No data downloaded. Check your internet connection and ticker list.")
        return
    
    # Process data
    combined_data = process_daily_data(all_data)
    
    if combined_data.empty:
        print("\n❌ Data processing failed.")
        return
    
    # Save outputs
    save_outputs(combined_data)
    
    # Generate summary
    generate_summary_stats(combined_data)
    
    print("\n" + "=" * 70)
    print("✅ AP_CRSPDaily.py completed successfully!")
    print("=" * 70)
    print("\nOutputs:")
    print("  1. AP_dailyCRSP.parquet - Full daily data (returns, volume, prices)")
    print("  2. AP_dailyCRSPprc.parquet - Price-only data")
    print("  3. AP_ticker_to_permno.csv - Ticker mapping")
    print("\nNext steps:")
    print("  - Use AP_dailyCRSP.parquet in place of dailyCRSP.parquet")
    print("  - Update SignalMasterTable.py to use AP files")
    print("  - Set DEBUG_MODE = False for full S&P 500 download")

if __name__ == "__main__":
    main()
