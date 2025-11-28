# ABOUTME: Downloads dividend and distribution data using yfinance (free alternative to CRSP Distributions)
# ABOUTME: Provides dividend amounts, ex-dates, and distribution codes in CRSP-compatible format
"""
Inputs:
- List of tickers (S&P 500 by default, or user-provided list)
- yfinance API (free, no authentication required)

Outputs:
- ../pyData/Intermediate/AP_CRSPdistributions.parquet

Requirements:
    pip install yfinance

How to run: python AP_CRSPDistributions.py

Notes:
- Uses ticker symbols instead of CRSP permno
- Historical dividend data typically available from ~2000 onwards
- Distribution codes approximated (1232=cash div, 5523=split, etc.)
- Record and payment dates estimated from ex-date
- Does NOT include special distributions (spinoffs, rights) comprehensively
- Free but has rate limits (~2000 requests/hour)
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
print("💰 AP_CRSPDistributions.py - Live Dividend & Distribution Data", flush=True)
print("=" * 70, flush=True)

# =============================================================================
# CONFIGURATION
# =============================================================================

# Date range for download - Last 2 years
END_DATE = datetime.now().strftime('%Y-%m-%d')
START_DATE = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')  # 2 years ago

# Debug mode: download limited tickers
DEBUG_MODE = False  # Set to True for testing with small dataset

if DEBUG_MODE:
    START_DATE = '2020-01-01'
    END_DATE = '2024-12-31'
    print(f"🔧 DEBUG MODE: {START_DATE} to {END_DATE}")
else:
    print(f"🚀 PRODUCTION MODE: {START_DATE} to {END_DATE}")

# Output directory
OUTPUT_DIR = Path("../pyData/Intermediate")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# =============================================================================
# CRSP DISTRIBUTION CODES (Approximated)
# =============================================================================

# CRSP uses 4-digit distribution codes
# We approximate based on what we can detect from yfinance

DISTCD_CASH_DIVIDEND = 1232      # Regular cash dividend
DISTCD_SPECIAL_DIVIDEND = 1262   # Special cash dividend (large amount)
DISTCD_STOCK_SPLIT = 5523        # Stock split
DISTCD_REVERSE_SPLIT = 5533      # Reverse stock split
DISTCD_STOCK_DIVIDEND = 5523     # Stock dividend (treated like split)

# Distribution code digit meanings (CRSP documentation):
# 1st digit: Distribution type (1=cash, 2=stock, 3=both, 5=split/spinoff)
# 2nd digit: Amount status (2=amount per share, 3=split ratio)
# 3rd digit: Payment type
# 4th digit: Special circumstances

def classify_distribution(row):
    """
    Classify distribution type based on available information.
    Returns CRSP-like distribution code.
    """
    # For dividends
    if 'Dividends' in row['type']:
        # Heuristic: if dividend > 5% of price, treat as special
        if pd.notna(row.get('divamt')) and pd.notna(row.get('prc')):
            if row['divamt'] / row['prc'] > 0.05:
                return DISTCD_SPECIAL_DIVIDEND
        return DISTCD_CASH_DIVIDEND
    
    # For splits
    elif 'Stock Splits' in row['type']:
        if row.get('facshr', 1.0) > 1.0:
            return DISTCD_STOCK_SPLIT  # Forward split
        elif row.get('facshr', 1.0) < 1.0:
            return DISTCD_REVERSE_SPLIT  # Reverse split
        else:
            return DISTCD_STOCK_SPLIT
    
    return DISTCD_CASH_DIVIDEND  # Default

# =============================================================================
# TICKER LISTS & MAPPINGS
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
        return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NVDA', 'JPM', 'V', 'JNJ',
                'WMT', 'PG', 'UNH', 'MA', 'HD', 'DIS', 'BAC', 'ADBE', 'NFLX', 'CMCSA']

def get_user_ticker_list():
    """User can provide their own ticker list"""
    ticker_file = OUTPUT_DIR / "ticker_list.csv"
    if ticker_file.exists():
        df = pd.read_csv(ticker_file)
        if 'ticker' in df.columns:
            return df['ticker'].tolist()
        elif 'symbol' in df.columns:
            return df['symbol'].tolist()
    
    return load_sp500_universe()

def load_ticker_to_permno_mapping():
    """
    Load ticker-to-permno mapping from daily/monthly files.
    If not available, create new mapping.
    """
    # Try to load from daily mapping first
    mapping_file = OUTPUT_DIR / "AP_ticker_to_permno.csv"
    if not mapping_file.exists():
        mapping_file = OUTPUT_DIR / "AP_ticker_to_permno_monthly.csv"
    
    if mapping_file.exists():
        df = pd.read_csv(mapping_file)
        print(f"✓ Loaded existing ticker mapping from {mapping_file.name}")
        return dict(zip(df['ticker'], df['permno']))
    
    return None

def create_ticker_to_permno_mapping(tickers):
    """Create mapping if it doesn't exist"""
    ticker_map = pd.DataFrame({
        'ticker': sorted(tickers),
        'permno': range(1, len(tickers) + 1)
    })
    
    ticker_map.to_csv(OUTPUT_DIR / "AP_ticker_to_permno_distributions.csv", index=False)
    print(f"✓ Created ticker-to-permno mapping for {len(ticker_map)} tickers")
    
    return dict(zip(ticker_map['ticker'], ticker_map['permno']))

# =============================================================================
# DATA DOWNLOAD FUNCTIONS
# =============================================================================

def download_distributions_for_ticker(ticker, start_date, end_date, permno):
    """
    Download dividend and distribution data for a single ticker.
    
    Returns DataFrame with CRSP-like columns:
    - permno: Numeric identifier
    - divamt: Dividend/distribution amount per share
    - distcd: Distribution code (CRSP-compatible)
    - facshr: Share adjustment factor (for splits)
    - rcrddt: Record date (estimated)
    - exdt: Ex-dividend/distribution date
    - paydt: Payment date (estimated)
    - cd1, cd2, cd3, cd4: Individual digits of distribution code
    """
    try:
        stock = yf.Ticker(ticker)
        
        # Get dividends
        dividends = stock.dividends
        splits = stock.splits
        
        all_distributions = []
        
        # Process dividends
        if not dividends.empty:
            divs = dividends[(dividends.index >= start_date) & (dividends.index <= end_date)]
            
            for date, amount in divs.items():
                all_distributions.append({
                    'permno': permno,
                    'ticker': ticker,
                    'exdt': pd.to_datetime(date),
                    'divamt': amount,
                    'facshr': 1.0,  # No share adjustment for dividends
                    'type': 'Dividends',
                    'prc': None  # Will fill if needed for classification
                })
        
        # Process stock splits
        if not splits.empty:
            splt = splits[(splits.index >= start_date) & (splits.index <= end_date)]
            
            for date, ratio in splt.items():
                all_distributions.append({
                    'permno': permno,
                    'ticker': ticker,
                    'exdt': pd.to_datetime(date),
                    'divamt': 0.0,  # No cash amount for splits
                    'facshr': float(ratio),  # Split ratio
                    'type': 'Stock Splits',
                    'prc': None
                })
        
        if not all_distributions:
            return pd.DataFrame()
        
        df = pd.DataFrame(all_distributions)
        
        # Estimate record and payment dates
        # CRSP convention: record date ~1-2 days before ex-date, payment ~2-4 weeks after
        df['rcrddt'] = df['exdt'] - pd.Timedelta(days=1)  # Record date before ex-date
        df['paydt'] = df['exdt'] + pd.Timedelta(days=14)  # Payment ~2 weeks after
        
        # Assign distribution codes
        df['distcd'] = df.apply(classify_distribution, axis=1)
        
        # Extract distribution code digits
        df['distcd_str'] = df['distcd'].astype(str).str.zfill(4)
        df['cd1'] = pd.to_numeric(df['distcd_str'].str[0], errors='coerce').astype('Int64')
        df['cd2'] = pd.to_numeric(df['distcd_str'].str[1], errors='coerce').astype('Int64')
        df['cd3'] = pd.to_numeric(df['distcd_str'].str[2], errors='coerce').astype('Int64')
        df['cd4'] = pd.to_numeric(df['distcd_str'].str[3], errors='coerce').astype('Int64')
        
        # Clean up
        df = df.drop(['distcd_str', 'type', 'prc', 'ticker'], axis=1, errors='ignore')
        
        return df
        
    except Exception as e:
        print(f"  ❌ Error downloading {ticker}: {e}")
        return pd.DataFrame()

def download_all_distributions(tickers, start_date, end_date, ticker_to_permno):
    """Download distribution data for all tickers with progress tracking"""
    all_data = []
    total = len(tickers)
    
    print(f"\n📥 Downloading distributions for {total} tickers...")
    
    success_count = 0
    for i, ticker in enumerate(tickers, 1):
        if i % 50 == 0 or i == 1:
            print(f"  Progress: {i}/{total} ({i/total*100:.1f}%)")
        
        permno = ticker_to_permno.get(ticker)
        if permno is None:
            continue
        
        df = download_distributions_for_ticker(ticker, start_date, end_date, permno)
        
        if not df.empty:
            all_data.append(df)
            success_count += 1
        
        # Rate limiting
        if i % 100 == 0:
            import time
            time.sleep(2)
    
    print(f"✓ Successfully downloaded distributions for {success_count} tickers")
    
    return all_data

# =============================================================================
# DATA PROCESSING FUNCTIONS
# =============================================================================

def process_distributions(all_data):
    """
    Combine and process all distribution data into CRSP format
    """
    if not all_data:
        print("❌ No distribution data to process")
        return pd.DataFrame()
    
    print("\n🔄 Processing distribution data...")
    
    # Combine all dataframes
    combined = pd.concat(all_data, ignore_index=True)
    print(f"  Total distribution records: {len(combined):,}")
    
    # Ensure correct data types (matching CRSP format)
    combined['permno'] = combined['permno'].astype('Int64')
    combined['divamt'] = combined['divamt'].astype('float64')
    combined['distcd'] = combined['distcd'].astype('Int64')
    combined['facshr'] = combined['facshr'].astype('float64')
    combined['rcrddt'] = pd.to_datetime(combined['rcrddt'])
    combined['exdt'] = pd.to_datetime(combined['exdt'])
    combined['paydt'] = pd.to_datetime(combined['paydt'])
    combined['cd1'] = combined['cd1'].astype('Int64')
    combined['cd2'] = combined['cd2'].astype('Int64')
    combined['cd3'] = combined['cd3'].astype('Int64')
    combined['cd4'] = combined['cd4'].astype('Int64')
    
    # Sort by permno and ex-date
    combined = combined.sort_values(['permno', 'exdt']).reset_index(drop=True)
    
    # Remove duplicates (same logic as original CRSP script)
    id_cols = ['permno', 'rcrddt', 'exdt', 'paydt', 'distcd']
    initial_count = len(combined)
    combined = combined.drop_duplicates(subset=id_cols, keep='first')
    duplicates_removed = initial_count - len(combined)
    
    if duplicates_removed > 0:
        print(f"  Removed {duplicates_removed} duplicate records")
    
    # Summary statistics
    print(f"  Date range: {combined['exdt'].min()} to {combined['exdt'].max()}")
    print(f"  Unique stocks (permno): {combined['permno'].nunique()}")
    print(f"  Avg distributions per stock: {len(combined) / combined['permno'].nunique():.1f}")
    
    return combined

def save_output(combined_data):
    """Save distribution data"""
    if combined_data.empty:
        print("❌ No data to save")
        return
    
    print("\n💾 Saving output...")
    
    # Column order matching CRSP
    columns_order = [
        'permno', 'divamt', 'distcd', 'facshr', 'rcrddt', 'exdt', 'paydt',
        'cd1', 'cd2', 'cd3', 'cd4'
    ]
    
    output_data = combined_data[columns_order]
    
    output_file = OUTPUT_DIR / "AP_CRSPdistributions.parquet"
    output_data.to_parquet(output_file, index=False)
    
    print(f"  ✓ Saved: {output_file}")
    print(f"    Records: {len(output_data):,}")
    print(f"    Size: {output_file.stat().st_size / 1024 / 1024:.1f} MB")

def generate_summary_stats(combined_data):
    """Generate summary statistics"""
    print("\n📊 Summary Statistics:")
    print(f"  Total distribution records: {len(combined_data):,}")
    print(f"  Unique stocks: {combined_data['permno'].nunique()}")
    print(f"  Date range: {combined_data['exdt'].min()} to {combined_data['exdt'].max()}")
    
    # Distribution type breakdown
    print(f"\n  Distribution code breakdown:")
    distcd_names = {
        DISTCD_CASH_DIVIDEND: 'Cash Dividend',
        DISTCD_SPECIAL_DIVIDEND: 'Special Dividend',
        DISTCD_STOCK_SPLIT: 'Stock Split',
        DISTCD_REVERSE_SPLIT: 'Reverse Split',
    }
    
    for distcd, count in combined_data['distcd'].value_counts().sort_index().items():
        name = distcd_names.get(distcd, f'Other ({distcd})')
        pct = count / len(combined_data) * 100
        print(f"    {name} ({distcd}): {count:,} ({pct:.1f}%)")
    
    # Dividend statistics (cash dividends only)
    cash_divs = combined_data[combined_data['distcd'] == DISTCD_CASH_DIVIDEND]
    if len(cash_divs) > 0:
        print(f"\n  Cash dividend statistics:")
        print(f"    Count: {len(cash_divs):,}")
        print(f"    Mean: ${cash_divs['divamt'].mean():.4f}")
        print(f"    Median: ${cash_divs['divamt'].median():.4f}")
        print(f"    Min: ${cash_divs['divamt'].min():.4f}")
        print(f"    Max: ${cash_divs['divamt'].max():.4f}")
    
    # Split statistics
    splits = combined_data[combined_data['distcd'].isin([DISTCD_STOCK_SPLIT, DISTCD_REVERSE_SPLIT])]
    if len(splits) > 0:
        print(f"\n  Stock split statistics:")
        print(f"    Count: {len(splits):,}")
        print(f"    Mean ratio: {splits['facshr'].mean():.4f}")
        print(f"    Median ratio: {splits['facshr'].median():.4f}")
    
    # Frequency analysis
    print(f"\n  Distribution frequency:")
    yearly_counts = combined_data.groupby(combined_data['exdt'].dt.year).size()
    print(f"    Avg per year: {yearly_counts.mean():.0f}")
    print(f"    Recent years:")
    for year in sorted(yearly_counts.index)[-5:]:
        print(f"      {year}: {yearly_counts[year]:,}")

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
    
    # Load or create ticker-to-permno mapping
    ticker_to_permno = load_ticker_to_permno_mapping()
    if ticker_to_permno is None:
        ticker_to_permno = create_ticker_to_permno_mapping(tickers)
    
    # Download distribution data
    all_data = download_all_distributions(tickers, START_DATE, END_DATE, ticker_to_permno)
    
    if not all_data:
        print("\n⚠️  No distribution data downloaded. This could mean:")
        print("   - Tickers don't have dividends/splits in date range")
        print("   - Internet connection issues")
        print("   - Rate limiting from yfinance")
        return
    
    # Process data
    combined_data = process_distributions(all_data)
    
    if combined_data.empty:
        print("\n❌ Data processing failed.")
        return
    
    # Save output
    save_output(combined_data)
    
    # Generate summary
    generate_summary_stats(combined_data)
    
    print("\n" + "=" * 70)
    print("✅ AP_CRSPDistributions.py completed successfully!")
    print("=" * 70)
    print("\nOutputs:")
    print("  1. AP_CRSPdistributions.parquet - Dividend & distribution data")
    print("  2. AP_ticker_to_permno_distributions.csv - Ticker mapping (if created)")
    print("\nNext steps:")
    print("  - Use AP_CRSPdistributions.parquet in place of CRSPdistributions.parquet")
    print("  - Set DEBUG_MODE = False for full S&P 500 download")
    print("\n⚠️  Note:")
    print("  - Distribution codes are approximated (1232=cash, 5523=split)")
    print("  - Record/payment dates are estimated from ex-date")
    print("  - Does NOT include special distributions comprehensively")

if __name__ == "__main__":
    main()

