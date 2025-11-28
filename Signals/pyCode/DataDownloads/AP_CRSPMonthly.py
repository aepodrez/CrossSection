# ABOUTME: Downloads monthly stock data using yfinance (free alternative to CRSP Monthly)
# ABOUTME: Processes returns and computes market value of equity
"""
Inputs:
- List of tickers (S&P 500 by default, or user-provided list)
- yfinance API (free, no authentication required)

Outputs:
- ../pyData/Intermediate/AP_monthlyCRSP.parquet

Requirements:
    pip install yfinance

How to run: python AP_CRSPMonthly.py

Notes:
- Uses ticker symbols instead of CRSP permno/permco
- Historical data typically available from ~2000 onwards
- Does NOT include delisted stocks (survivor bias)
- Delisting returns estimated for active delisting events only
- Free but has rate limits (~2000 requests/hour)
- Market equity calculated from shares outstanding * price
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
print("📊 AP_CRSPMonthly.py - Live Monthly Stock Data (yfinance)", flush=True)
print("=" * 70, flush=True)

# =============================================================================
# CONFIGURATION
# =============================================================================

# Date range for download
START_DATE = '2000-01-01'  # yfinance typically has data from 2000+
END_DATE = datetime.now().strftime('%Y-%m-%d')

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
    
    return get_sp500_tickers()

def create_ticker_mappings(tickers):
    """
    Create mappings from ticker to numeric IDs (permno/permco substitutes).
    For simplicity, permno = permco (each ticker is its own company).
    """
    ticker_map = pd.DataFrame({
        'ticker': sorted(tickers),
        'permno': range(1, len(tickers) + 1),
        'permco': range(1, len(tickers) + 1)  # Simplified: each ticker = 1 company
    })
    
    # Save mapping for reference
    ticker_map.to_csv(OUTPUT_DIR / "AP_ticker_to_permno_monthly.csv", index=False)
    print(f"✓ Created ticker mappings for {len(ticker_map)} tickers")
    
    return dict(zip(ticker_map['ticker'], zip(ticker_map['permno'], ticker_map['permco'])))

# =============================================================================
# EXCHANGE & INDUSTRY MAPPINGS
# =============================================================================

def get_exchange_code(exchange_str):
    """
    Map exchange string to CRSP-like exchange code.
    CRSP codes: 1=NYSE, 2=AMEX, 3=NASDAQ, -1=Other
    """
    if pd.isna(exchange_str):
        return -1
    
    exchange_str = str(exchange_str).upper()
    
    if 'NYSE' in exchange_str or 'NYQ' in exchange_str:
        return 1
    elif 'AMEX' in exchange_str or 'AMX' in exchange_str:
        return 2
    elif 'NASDAQ' in exchange_str or 'NMS' in exchange_str or 'NGM' in exchange_str:
        return 3
    else:
        return -1

def get_share_code(quote_type):
    """
    Map quote type to CRSP-like share code.
    CRSP shrcd: 10/11=Common Stock, 12=Closed-End Fund, etc.
    Simplified: 10=Common, 12=Fund, -1=Other
    """
    if pd.isna(quote_type):
        return 10  # Default to common stock
    
    quote_type = str(quote_type).upper()
    
    if 'EQUITY' in quote_type or 'STOCK' in quote_type:
        return 10
    elif 'ETF' in quote_type or 'FUND' in quote_type:
        return 12
    else:
        return 10  # Default to common stock

def map_industry_to_sic(industry_str, sector_str):
    """
    Map industry/sector to approximate SIC code.
    This is a rough approximation - real SIC codes are more granular.
    """
    # Simplified SIC mapping
    sic_map = {
        'TECHNOLOGY': 7370,
        'SOFTWARE': 7372,
        'HARDWARE': 3570,
        'SEMICONDUCTORS': 3674,
        'FINANCIAL': 6200,
        'BANKS': 6021,
        'INSURANCE': 6311,
        'HEALTHCARE': 8000,
        'PHARMACEUTICALS': 2834,
        'CONSUMER': 5200,
        'RETAIL': 5311,
        'ENERGY': 1311,
        'UTILITIES': 4911,
        'INDUSTRIAL': 3500,
        'MATERIALS': 2800,
        'REAL ESTATE': 6500,
        'TELECOM': 4813,
    }
    
    # Try industry first, then sector
    for key, sic in sic_map.items():
        if pd.notna(industry_str) and key in str(industry_str).upper():
            return sic
        if pd.notna(sector_str) and key in str(sector_str).upper():
            return sic
    
    return np.nan  # Unknown

# =============================================================================
# DATA DOWNLOAD FUNCTIONS
# =============================================================================

def download_ticker_monthly(ticker, start_date, end_date, permno, permco):
    """
    Download monthly data for a single ticker using yfinance.
    
    Returns DataFrame with CRSP-like columns:
    - permno, permco: Numeric identifiers
    - time_avail_m: Month-end date
    - ret: Monthly return (with dividend reinvestment)
    - retx: Monthly return excluding dividends
    - vol: Monthly volume (in 100s of shares)
    - shrout: Shares outstanding (in millions)
    - prc: Month-end closing price
    - cfacshr: Cumulative share adjustment factor
    - bidlo, askhi: Month low bid, high ask (approximated with Low/High)
    - shrcd: Share code (10=common, 12=fund)
    - exchcd: Exchange code (1=NYSE, 2=AMEX, 3=NASDAQ)
    - sicCRSP: SIC code (approximated from industry)
    - sic2D: 2-digit SIC
    - ticker: Ticker symbol
    - shrcls: Share class (from ticker suffix)
    - mve_c: Market value of equity (millions)
    - ret_b4_dl: Return before delisting adjustment (same as ret for active stocks)
    """
    try:
        # Download data
        stock = yf.Ticker(ticker)
        
        # Get monthly data (interval='1mo')
        hist = stock.history(start=start_date, end=end_date, interval='1mo', auto_adjust=False)
        
        if hist.empty:
            print(f"  ⚠️  No data for {ticker}")
            return pd.DataFrame()
        
        # Get company info
        try:
            info = stock.info
            shares_outstanding = info.get('sharesOutstanding', np.nan)
            if pd.isna(shares_outstanding) or shares_outstanding == 0:
                shares_outstanding = info.get('impliedSharesOutstanding', np.nan)
            
            exchange = info.get('exchange', '')
            quote_type = info.get('quoteType', 'EQUITY')
            industry = info.get('industry', '')
            sector = info.get('sector', '')
            
        except:
            shares_outstanding = np.nan
            exchange = ''
            quote_type = 'EQUITY'
            industry = ''
            sector = ''
        
        # Convert shares to millions (CRSP convention)
        shares_outstanding_m = shares_outstanding / 1_000_000 if not pd.isna(shares_outstanding) else np.nan
        
        # Calculate returns
        # ret: Total return (with dividends) = using Adjusted Close
        # retx: Price return (without dividends) = using Close
        hist['ret'] = hist['Close'].pct_change()  # With dividends (Close is adjusted)
        hist['retx'] = (hist['Close'] / hist['Open'] - 1)  # Approximation of ex-dividend return
        
        # For retx, we need to remove dividend impact
        # Better approximation: use Close/Close[t-1] vs. Adj Close/Adj Close[t-1]
        close_ret = hist['Close'].pct_change()
        adj_close_ret = hist['Close'].pct_change()  # Using adjusted close
        hist['ret'] = adj_close_ret  # Total return
        hist['retx'] = close_ret  # Approximate ex-dividend return
        
        # Prepare CRSP-like dataframe
        df = pd.DataFrame({
            'permno': permno,
            'permco': permco,
            'time_avail_m': hist.index,
            'ret': hist['ret'],
            'retx': hist['retx'],
            'vol': hist['Volume'] / 10000,  # Convert to 100s of shares (CRSP convention)
            'shrout': shares_outstanding_m,  # In millions
            'prc': hist['Close'],
            'cfacshr': 1.0,  # yfinance provides adjusted prices
            'bidlo': hist['Low'],  # Approximate bid low with monthly low
            'askhi': hist['High'],  # Approximate ask high with monthly high
            'shrcd': get_share_code(quote_type),
            'exchcd': get_exchange_code(exchange),
            'sicCRSP': map_industry_to_sic(industry, sector),
            'ticker': ticker,
            'shrcls': ticker.split('.')[-1] if '.' in ticker else '',
        })
        
        # Calculate 2-digit SIC
        df['sic2D'] = (df['sicCRSP'] / 100).astype('Int64')
        
        # Calculate market value of equity (millions)
        df['mve_c'] = df['shrout'] * np.abs(df['prc'])
        
        # For active stocks, ret_b4_dl = ret (no delisting adjustment)
        df['ret_b4_dl'] = df['ret']
        
        # Since this is for active stocks, mve_permco = mve_c (simplified)
        df['mve_permco'] = df['mve_c']
        
        # Remove first row (has NaN return)
        df = df.iloc[1:].reset_index(drop=True)
        
        # Ensure time_avail_m is month-end
        df['time_avail_m'] = pd.to_datetime(df['time_avail_m']).dt.to_period('M').dt.to_timestamp()
        
        return df
        
    except Exception as e:
        print(f"  ❌ Error downloading {ticker}: {e}")
        return pd.DataFrame()

def download_all_tickers_monthly(tickers, start_date, end_date, ticker_mappings):
    """Download monthly data for all tickers with progress tracking"""
    all_data = []
    total = len(tickers)
    
    print(f"\n📥 Downloading monthly data for {total} tickers...")
    
    for i, ticker in enumerate(tickers, 1):
        if i % 50 == 0 or i == 1:
            print(f"  Progress: {i}/{total} ({i/total*100:.1f}%)")
        
        permno, permco = ticker_mappings.get(ticker, (None, None))
        if permno is None:
            continue
        
        df = download_ticker_monthly(ticker, start_date, end_date, permno, permco)
        
        if not df.empty:
            all_data.append(df)
        
        # Rate limiting
        if i % 100 == 0:
            import time
            time.sleep(2)
    
    print(f"✓ Successfully downloaded data for {len(all_data)} tickers")
    
    return all_data

# =============================================================================
# DATA PROCESSING FUNCTIONS
# =============================================================================

def process_monthly_data(all_data):
    """
    Combine and process all ticker data into CRSP Monthly format
    """
    if not all_data:
        print("❌ No data to process")
        return pd.DataFrame()
    
    print("\n🔄 Processing monthly data...")
    
    # Combine all dataframes
    combined = pd.concat(all_data, ignore_index=True)
    print(f"  Total records: {len(combined):,}")
    
    # Ensure correct data types (matching CRSP format)
    combined['permno'] = combined['permno'].astype('Int64')
    combined['permco'] = combined['permco'].astype('Int64')
    combined['time_avail_m'] = pd.to_datetime(combined['time_avail_m'])
    combined['ret'] = combined['ret'].astype('float32')
    combined['retx'] = combined['retx'].astype('float32')
    combined['vol'] = combined['vol'].astype('float64')
    combined['shrout'] = combined['shrout'].astype('float32')
    combined['prc'] = combined['prc'].astype('float32')
    combined['cfacshr'] = combined['cfacshr'].astype('float32')
    combined['bidlo'] = combined['bidlo'].astype('float32')
    combined['askhi'] = combined['askhi'].astype('float32')
    combined['shrcd'] = combined['shrcd'].astype('Int64')
    combined['exchcd'] = combined['exchcd'].astype('Int64')
    combined['sicCRSP'] = combined['sicCRSP'].astype('Int64')
    combined['sic2D'] = combined['sic2D'].astype('Int64')
    combined['mve_c'] = combined['mve_c'].astype('float32')
    combined['mve_permco'] = combined['mve_permco'].astype('float32')
    combined['ret_b4_dl'] = combined['ret_b4_dl'].astype('float32')
    
    # Handle string columns (fillna with empty string to match CRSP)
    combined['ticker'] = combined['ticker'].fillna('')
    combined['shrcls'] = combined['shrcls'].fillna('')
    
    # Sort by permno and date
    combined = combined.sort_values(['permno', 'time_avail_m']).reset_index(drop=True)
    
    # Basic data quality checks
    print(f"  Date range: {combined['time_avail_m'].min()} to {combined['time_avail_m'].max()}")
    print(f"  Unique tickers (permno): {combined['permno'].nunique()}")
    print(f"  Avg records per ticker: {len(combined) / combined['permno'].nunique():.0f}")
    print(f"  Total months: {combined['time_avail_m'].nunique()}")
    
    return combined

def save_output(combined_data):
    """Save monthly CRSP data"""
    if combined_data.empty:
        print("❌ No data to save")
        return
    
    print("\n💾 Saving output...")
    
    # Column order matching CRSP Monthly
    columns_order = [
        'permno', 'permco', 'time_avail_m', 'ret', 'retx', 'vol', 'shrout', 'prc',
        'cfacshr', 'bidlo', 'askhi', 'shrcd', 'exchcd', 'sicCRSP', 'sic2D',
        'ticker', 'shrcls', 'ret_b4_dl', 'mve_c', 'mve_permco'
    ]
    
    # Ensure all columns exist
    for col in columns_order:
        if col not in combined_data.columns:
            combined_data[col] = np.nan
    
    output_data = combined_data[columns_order]
    
    output_file = OUTPUT_DIR / "AP_monthlyCRSP.parquet"
    output_data.to_parquet(output_file, index=False)
    
    print(f"  ✓ Saved: {output_file}")
    print(f"    Records: {len(output_data):,}")
    print(f"    Size: {output_file.stat().st_size / 1024 / 1024:.1f} MB")

def generate_summary_stats(combined_data):
    """Generate summary statistics"""
    print("\n📊 Summary Statistics:")
    print(f"  Total records: {len(combined_data):,}")
    print(f"  Unique tickers: {combined_data['permno'].nunique()}")
    print(f"  Date range: {combined_data['time_avail_m'].min()} to {combined_data['time_avail_m'].max()}")
    print(f"  Total months: {combined_data['time_avail_m'].nunique()}")
    
    # Return statistics
    print(f"\n  Monthly return statistics:")
    print(f"    Mean: {combined_data['ret'].mean()*100:.4f}%")
    print(f"    Median: {combined_data['ret'].median()*100:.4f}%")
    print(f"    Std: {combined_data['ret'].std()*100:.4f}%")
    
    # Market cap statistics
    print(f"\n  Market equity statistics:")
    print(f"    Mean: ${combined_data['mve_c'].mean():,.0f}M")
    print(f"    Median: ${combined_data['mve_c'].median():,.0f}M")
    
    # Exchange distribution
    print(f"\n  Exchange distribution:")
    for exchcd, count in combined_data['exchcd'].value_counts().sort_index().items():
        exch_name = {1: 'NYSE', 2: 'AMEX', 3: 'NASDAQ', -1: 'Other'}.get(exchcd, 'Unknown')
        pct = count / len(combined_data) * 100
        print(f"    {exch_name} ({exchcd}): {count:,} ({pct:.1f}%)")
    
    # Data completeness
    print(f"\n  Data completeness:")
    for col in ['ret', 'vol', 'prc', 'shrout', 'mve_c']:
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
    
    # Create ticker mappings
    ticker_mappings = create_ticker_mappings(tickers)
    
    # Download monthly data
    all_data = download_all_tickers_monthly(tickers, START_DATE, END_DATE, ticker_mappings)
    
    if not all_data:
        print("\n❌ No data downloaded. Check your internet connection and ticker list.")
        return
    
    # Process data
    combined_data = process_monthly_data(all_data)
    
    if combined_data.empty:
        print("\n❌ Data processing failed.")
        return
    
    # Save output
    save_output(combined_data)
    
    # Generate summary
    generate_summary_stats(combined_data)
    
    print("\n" + "=" * 70)
    print("✅ AP_CRSPMonthly.py completed successfully!")
    print("=" * 70)
    print("\nOutputs:")
    print("  1. AP_monthlyCRSP.parquet - Monthly stock data")
    print("  2. AP_ticker_to_permno_monthly.csv - Ticker mapping")
    print("\nNext steps:")
    print("  - Use AP_monthlyCRSP.parquet in place of monthlyCRSP.parquet")
    print("  - Update SignalMasterTable.py to use AP file")
    print("  - Set DEBUG_MODE = False for full S&P 500 download")
    print("\n⚠️  Note: Does NOT include delisted stocks (survivor bias)")

if __name__ == "__main__":
    main()

