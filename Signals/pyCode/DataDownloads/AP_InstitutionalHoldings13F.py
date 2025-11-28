# ABOUTME: Downloads and processes institutional holdings from SEC 13F filings using edgartools
# ABOUTME: Calculates institutional ownership metrics at monthly frequency with quarterly 13F data
"""
Inputs:
- List of tickers (S&P 500 by default, or user-provided list)
- SEC 13F-HR filings via edgartools (free, no authentication required)

Outputs:
- ../pyData/Intermediate/AP_TR_13F.parquet

Requirements:
    pip install edgartools pandas

How to run: python AP_InstitutionalHoldings13F.py

Notes:
- Uses edgartools to fetch 13F-HR filings from institutional managers
- 13F data is quarterly (filed 45 days after quarter end)
- Forward-fills quarterly data to monthly frequency
- Calculates institutional ownership metrics:
  * numinstown: Number of institutional owners
  * instown_perc: Institutional ownership percentage
  * dbreadth: Quarterly change in number of institutional owners
- Historical data typically available from ~2000 onwards
- Free but may be slow for large universes (processes all 13F filings)
- Results are cached to avoid re-downloading historical data
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Try to import edgartools
try:
    from edgar import set_identity, get_filings
    EDGARTOOLS_AVAILABLE = True
except ImportError:
    print("⚠️  edgartools not installed. Install with: pip install edgartools")
    print("⚠️  Running in demo mode with sample data structure only.")
    EDGARTOOLS_AVAILABLE = False

# Print script header
print("=" * 70, flush=True)
print("🏦 AP_InstitutionalHoldings13F.py - Live 13F Institutional Holdings", flush=True)
print("=" * 70, flush=True)

# =============================================================================
# CONFIGURATION
# =============================================================================

# Date range for download
START_DATE = '2020-01-01'  # 13F data availability varies
END_DATE = datetime.now().strftime('%Y-%m-%d')

# Debug mode: download limited tickers and filings
DEBUG_MODE = False  # Set to True for testing

if DEBUG_MODE:
    START_DATE = '2023-01-01'
    END_DATE = '2024-12-31'
    print(f"🔧 DEBUG MODE: {START_DATE} to {END_DATE}")
else:
    print(f"🚀 PRODUCTION MODE: {START_DATE} to {END_DATE}")

# Output directory
OUTPUT_DIR = Path("../pyData/Intermediate")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Cache directory
CACHE_DIR = OUTPUT_DIR / ".cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_FILE = CACHE_DIR / "AP_13F_holdings_cache.parquet"

# Edgar identity (required by SEC)
EDGAR_IDENTITY = "apodrez21@gmail.com"  # Change to your email

# =============================================================================
# TICKER LISTS & MAPPINGS
# =============================================================================

def get_sp500_tickers():
    """Get current S&P 500 ticker list from Wikipedia"""
    try:
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        tables = pd.read_html(url)
        df = tables[0]
        tickers = df['Symbol'].str.replace('.', '-', regex=False).tolist()
        print(f"✓ Retrieved {len(tickers)} S&P 500 tickers")
        return tickers
    except Exception as e:
        print(f"⚠️  Could not fetch S&P 500 list: {e}")
        return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NVDA', 'JPM', 'V', 'JNJ']

def get_user_ticker_list():
    """User can provide their own ticker list"""
    ticker_file = OUTPUT_DIR / "ticker_list.csv"
    if ticker_file.exists():
        df = pd.read_csv(ticker_file)
        if 'ticker' in df.columns:
            return df['ticker'].tolist()
        elif 'symbol' in df.columns:
            return df['symbol'].tolist()
    
    return get_sp500_tickers()

def load_ticker_to_permno_mapping():
    """Load ticker-to-permno mapping from existing files"""
    for mapping_file in [
        OUTPUT_DIR / "AP_ticker_to_permno.csv",
        OUTPUT_DIR / "AP_ticker_to_permno_monthly.csv",
        OUTPUT_DIR / "AP_ticker_to_permno_distributions.csv"
    ]:
        if mapping_file.exists():
            df = pd.read_csv(mapping_file)
            print(f"✓ Loaded ticker mapping from {mapping_file.name}")
            return dict(zip(df['ticker'], df['permno']))
    
    return None

def create_ticker_to_permno_mapping(tickers):
    """Create mapping if it doesn't exist"""
    ticker_map = pd.DataFrame({
        'ticker': sorted(tickers),
        'permno': range(1, len(tickers) + 1)
    })
    
    ticker_map.to_csv(OUTPUT_DIR / "AP_ticker_to_permno_13f.csv", index=False)
    print(f"✓ Created ticker-to-permno mapping for {len(ticker_map)} tickers")
    
    return dict(zip(ticker_map['ticker'], ticker_map['permno']))

# =============================================================================
# 13F DATA FETCHING FUNCTIONS
# =============================================================================

def fetch_13f_holdings_data(tickers, ticker_to_permno, start_date):
    """
    Fetch 13F holdings data from SEC using edgartools.
    
    Strategy:
    1. Pull all 13F-HR filings from institutional managers
    2. Extract holdings (infotable) from each filing
    3. Map ticker → permno
    4. Aggregate holdings by (permno, quarter)
    5. Calculate institutional ownership metrics
    
    Returns DataFrame with columns:
    - permno: Stock identifier
    - year: Year
    - quarter: Quarter (1-4)
    - numinstown: Number of institutional owners
    - total_shares: Total shares held by institutions
    - num_managers: Number of unique managers
    """
    
    if not EDGARTOOLS_AVAILABLE:
        print("❌ edgartools not available")
        return pd.DataFrame()
    
    # Set edgartools identity
    try:
        set_identity(EDGAR_IDENTITY)
        print(f"✓ Set edgartools identity to: {EDGAR_IDENTITY}")
    except Exception as e:
        print(f"⚠️  Failed to set edgartools identity: {e}")
    
    # Create uppercase ticker mapping
    ticker_to_permno_upper = {t.upper(): p for t, p in ticker_to_permno.items()}
    target_tickers = set(ticker_to_permno_upper.keys())
    
    print(f"\n📥 Fetching 13F-HR filings from SEC (since {start_date})...")
    
    # Fetch all 13F-HR filings
    try:
        all_filings = get_filings(form="13F-HR")
        print(f"  Found {len(all_filings)} total 13F-HR filings")
        
        # Filter by date
        start_dt = pd.to_datetime(start_date)
        filings = [f for f in all_filings if pd.to_datetime(f.filing_date) >= start_dt]
        print(f"  Filtered to {len(filings)} filings since {start_date}")
        
    except Exception as e:
        print(f"❌ Error fetching 13F filings: {e}")
        return pd.DataFrame()
    
    if not filings:
        print("⚠️  No 13F filings found")
        return pd.DataFrame()
    
    # Process filings and extract holdings
    print(f"\n🔄 Processing {len(filings)} 13F filings (this may take a while)...")
    
    holdings_data = []
    processed_count = 0
    skipped_count = 0
    
    # Limit filings in debug mode
    if DEBUG_MODE:
        filings = filings[:100]  # Only process 100 filings in debug mode
        print(f"  DEBUG: Limited to {len(filings)} filings")
    
    for i, filing in enumerate(filings, 1):
        if i % 100 == 0:
            print(f"  Progress: {i}/{len(filings)} ({i/len(filings)*100:.1f}%) - Processed: {processed_count}, Skipped: {skipped_count}")
        
        try:
            # Get filing date and determine quarter
            filing_date = pd.to_datetime(filing.filing_date)
            year = filing_date.year
            quarter = (filing_date.month - 1) // 3 + 1
            
            # Get manager CIK
            manager_cik = filing.cik
            
            # Parse holdings from infotable
            obj = filing.obj()
            df = obj.infotable
            
            if df is None or (hasattr(df, 'empty') and df.empty):
                skipped_count += 1
                continue
            
            # Ensure it's a DataFrame
            if not isinstance(df, pd.DataFrame):
                if hasattr(df, 'to_pandas'):
                    df = df.to_pandas()
                else:
                    skipped_count += 1
                    continue
            
            # Look for ticker column
            ticker_col = None
            for col in df.columns:
                if col.lower() in ['ticker', 'symbol']:
                    ticker_col = col
                    break
            
            if ticker_col is None:
                skipped_count += 1
                continue
            
            # Standardize tickers
            df['ticker_std'] = df[ticker_col].astype(str).str.upper().str.strip()
            
            # Filter to our universe
            df = df[df['ticker_std'].isin(target_tickers)].copy()
            
            if df.empty:
                skipped_count += 1
                continue
            
            # Map ticker to permno
            df['permno'] = df['ticker_std'].map(ticker_to_permno_upper)
            df = df[df['permno'].notna()]
            
            if df.empty:
                skipped_count += 1
                continue
            
            # Extract shares held
            shares_col = None
            for col in df.columns:
                if col.lower() in ['shares', 'sharesorfprncamt', 'shrs_or_prn_amt']:
                    shares_col = col
                    break
            
            if shares_col:
                df['shares'] = pd.to_numeric(df[shares_col], errors='coerce').fillna(0)
            else:
                df['shares'] = 0
            
            # Add to holdings data
            for _, row in df.iterrows():
                holdings_data.append({
                    'permno': int(row['permno']),
                    'year': year,
                    'quarter': quarter,
                    'manager_cik': str(manager_cik),
                    'ticker': row['ticker_std'],
                    'shares': row['shares']
                })
            
            processed_count += 1
            
        except Exception as e:
            skipped_count += 1
            continue
    
    print(f"✓ Processed {processed_count} filings, skipped {skipped_count}")
    
    if not holdings_data:
        print("⚠️  No holdings data extracted")
        return pd.DataFrame()
    
    # Convert to DataFrame
    holdings_df = pd.DataFrame(holdings_data)
    print(f"✓ Extracted {len(holdings_df)} individual holdings records")
    
    # Aggregate by permno-quarter
    print(f"\n📊 Aggregating holdings by permno-quarter...")
    
    agg_df = holdings_df.groupby(['permno', 'year', 'quarter']).agg({
        'manager_cik': 'nunique',  # Number of unique institutional owners
        'shares': 'sum'  # Total shares held by institutions
    }).reset_index()
    
    agg_df.columns = ['permno', 'year', 'quarter', 'numinstown', 'total_shares']
    
    print(f"✓ Aggregated to {len(agg_df)} permno-quarter observations")
    print(f"  Unique stocks: {agg_df['permno'].nunique()}")
    print(f"  Quarters covered: {agg_df[['year', 'quarter']].drop_duplicates().shape[0]}")
    
    return agg_df

def calculate_institutional_metrics(holdings_df, ticker_to_permno):
    """
    Calculate institutional ownership metrics from aggregated holdings.
    
    Metrics:
    - numinstown: Number of institutional owners (already calculated)
    - dbreadth: Quarterly change in number of institutional owners
    - instown_perc: Institutional ownership percentage (requires shares outstanding)
    """
    
    if holdings_df.empty:
        return holdings_df
    
    print(f"\n📈 Calculating institutional ownership metrics...")
    
    # Sort by permno and date
    holdings_df = holdings_df.sort_values(['permno', 'year', 'quarter']).reset_index(drop=True)
    
    # Calculate dbreadth (quarterly change in institutional owners)
    holdings_df['dbreadth'] = holdings_df.groupby('permno')['numinstown'].diff()
    
    print(f"✓ Calculated dbreadth for {holdings_df['dbreadth'].notna().sum()} observations")
    
    # Try to get shares outstanding from yfinance for ownership percentage
    print(f"\n📊 Fetching shares outstanding from yfinance...")
    
    try:
        import yfinance as yf
        
        # Create reverse mapping (permno to ticker)
        permno_to_ticker = {v: k for k, v in ticker_to_permno.items()}
        
        shares_outstanding_map = {}
        unique_permnos = holdings_df['permno'].unique()
        
        for i, permno in enumerate(unique_permnos, 1):
            if i % 50 == 0:
                print(f"  Progress: {i}/{len(unique_permnos)}")
            
            ticker = permno_to_ticker.get(permno)
            if not ticker:
                continue
            
            try:
                stock = yf.Ticker(ticker)
                info = stock.info
                shares = info.get('sharesOutstanding', 0)
                if shares == 0:
                    shares = info.get('impliedSharesOutstanding', 0)
                
                if shares > 0:
                    shares_outstanding_map[permno] = shares
            except:
                continue
        
        print(f"✓ Retrieved shares outstanding for {len(shares_outstanding_map)} stocks")
        
        # Calculate institutional ownership percentage
        holdings_df['shares_outstanding'] = holdings_df['permno'].map(shares_outstanding_map)
        holdings_df['instown_perc'] = (holdings_df['total_shares'] / holdings_df['shares_outstanding'] * 100).clip(0, 100)
        
        print(f"✓ Calculated instown_perc for {holdings_df['instown_perc'].notna().sum()} observations")
        
    except Exception as e:
        print(f"⚠️  Could not calculate institutional ownership percentage: {e}")
        holdings_df['instown_perc'] = np.nan
    
    return holdings_df

def forward_fill_to_monthly(quarterly_df):
    """
    Forward-fill quarterly 13F data to monthly frequency.
    
    13F data is reported quarterly, but factors may need monthly data.
    We forward-fill each quarter's values for the next 3 months.
    """
    
    if quarterly_df.empty:
        return quarterly_df
    
    print(f"\n📅 Forward-filling quarterly data to monthly frequency...")
    
    # Create monthly data from quarterly
    monthly_data = []
    
    for _, row in quarterly_df.iterrows():
        # Create 3 monthly observations for this quarter
        year = row['year']
        quarter = row['quarter']
        
        # Determine months for this quarter
        months = [(quarter - 1) * 3 + m for m in [1, 2, 3]]
        
        for month in months:
            monthly_row = row.copy()
            monthly_row['time_avail_m'] = pd.Timestamp(year=year, month=month, day=1)
            monthly_data.append(monthly_row)
    
    monthly_df = pd.DataFrame(monthly_data)
    
    # Sort by permno and date
    monthly_df = monthly_df.sort_values(['permno', 'time_avail_m']).reset_index(drop=True)
    
    # Forward-fill missing months for each permno
    filled_data = []
    
    for permno in monthly_df['permno'].unique():
        permno_data = monthly_df[monthly_df['permno'] == permno].copy()
        
        # Get date range
        min_date = permno_data['time_avail_m'].min()
        max_date = permno_data['time_avail_m'].max()
        
        # Create complete monthly range
        monthly_range = pd.date_range(start=min_date, end=max_date, freq='MS')
        
        # Reindex and forward-fill
        permno_data = permno_data.set_index('time_avail_m')
        permno_data = permno_data.reindex(monthly_range)
        permno_data = permno_data.ffill()
        permno_data['permno'] = permno
        
        filled_data.append(permno_data.reset_index())
    
    final_df = pd.concat(filled_data, ignore_index=True)
    final_df = final_df.rename(columns={'index': 'time_avail_m'})
    
    print(f"✓ Expanded to {len(final_df)} monthly observations")
    
    return final_df

# =============================================================================
# CACHING FUNCTIONS
# =============================================================================

def load_cache():
    """Load cached 13F data if available"""
    if CACHE_FILE.exists():
        try:
            cache_df = pd.read_parquet(CACHE_FILE)
            print(f"✓ Loaded cache with {len(cache_df)} observations")
            return cache_df
        except Exception as e:
            print(f"⚠️  Could not load cache: {e}")
    
    return pd.DataFrame()

def save_cache(df):
    """Save 13F data to cache"""
    try:
        df.to_parquet(CACHE_FILE, index=False)
        print(f"✓ Saved cache to {CACHE_FILE}")
    except Exception as e:
        print(f"⚠️  Could not save cache: {e}")

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main execution function"""
    
    if not EDGARTOOLS_AVAILABLE:
        print("\n❌ Cannot proceed without edgartools. Please install:")
        print("   pip install edgartools")
        return
    
    # Get ticker list
    print("\n📋 Loading ticker list...")
    if DEBUG_MODE:
        tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
        print(f"  DEBUG MODE: Using {len(tickers)} sample tickers")
    else:
        tickers = get_user_ticker_list()
    
    # Load or create ticker-to-permno mapping
    ticker_to_permno = load_ticker_to_permno_mapping()
    if ticker_to_permno is None:
        ticker_to_permno = create_ticker_to_permno_mapping(tickers)
    
    # Check cache
    cache_df = load_cache()
    
    if not cache_df.empty:
        print(f"\n✓ Using cached data (delete {CACHE_FILE} to refresh)")
        final_df = cache_df
    else:
        # Fetch 13F holdings data
        holdings_df = fetch_13f_holdings_data(tickers, ticker_to_permno, START_DATE)
        
        if holdings_df.empty:
            print("\n❌ No holdings data retrieved")
            return
        
        # Calculate institutional metrics
        holdings_df = calculate_institutional_metrics(holdings_df, ticker_to_permno)
        
        # Forward-fill to monthly
        final_df = forward_fill_to_monthly(holdings_df)
        
        # Save cache
        save_cache(final_df)
    
    # Prepare final output
    print(f"\n💾 Preparing final output...")
    
    # Select and order columns to match original format
    output_cols = ['permno', 'time_avail_m', 'numinstown', 'dbreadth', 'instown_perc']
    
    # Add additional columns if available
    for col in ['total_shares', 'shares_outstanding']:
        if col in final_df.columns:
            output_cols.append(col)
    
    final_df = final_df[output_cols].copy()
    
    # Ensure correct data types
    final_df['permno'] = final_df['permno'].astype('int64')
    final_df['time_avail_m'] = pd.to_datetime(final_df['time_avail_m'])
    
    # Save output
    output_file = OUTPUT_DIR / "AP_TR_13F.parquet"
    final_df.to_parquet(output_file, index=False)
    
    print(f"  ✓ Saved: {output_file}")
    print(f"    Records: {len(final_df):,}")
    print(f"    Size: {output_file.stat().st_size / 1024 / 1024:.1f} MB")
    
    # Generate summary
    print(f"\n📊 Summary Statistics:")
    print(f"  Total observations: {len(final_df):,}")
    print(f"  Unique stocks: {final_df['permno'].nunique()}")
    print(f"  Date range: {final_df['time_avail_m'].min()} to {final_df['time_avail_m'].max()}")
    print(f"  Months covered: {final_df['time_avail_m'].nunique()}")
    
    print(f"\n  Institutional ownership metrics:")
    print(f"    numinstown: {final_df['numinstown'].notna().sum():,} non-null ({final_df['numinstown'].notna().mean()*100:.1f}%)")
    print(f"    dbreadth: {final_df['dbreadth'].notna().sum():,} non-null ({final_df['dbreadth'].notna().mean()*100:.1f}%)")
    print(f"    instown_perc: {final_df['instown_perc'].notna().sum():,} non-null ({final_df['instown_perc'].notna().mean()*100:.1f}%)")
    
    print(f"\n  Mean values:")
    print(f"    Avg institutional owners: {final_df['numinstown'].mean():.1f}")
    print(f"    Avg institutional ownership: {final_df['instown_perc'].mean():.1f}%")
    
    print("\n" + "=" * 70)
    print("✅ AP_InstitutionalHoldings13F.py completed successfully!")
    print("=" * 70)
    print("\nOutputs:")
    print("  1. AP_TR_13F.parquet - Institutional holdings data")
    print("  2. .cache/AP_13F_holdings_cache.parquet - Cache file")
    print("\nNext steps:")
    print("  - Use AP_TR_13F.parquet in place of TR_13F.parquet")
    print("  - Set DEBUG_MODE = False for full data download")
    print("  - Delete cache file to refresh data")
    print("\n⚠️  Note:")
    print("  - 13F data is quarterly, forward-filled to monthly")
    print("  - First run may take 30-60 minutes (fetches all 13F filings)")
    print("  - Subsequent runs use cached data (instant)")

if __name__ == "__main__":
    main()

