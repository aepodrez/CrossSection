# ABOUTME: Downloads monthly equal- and value-weighted market returns from yfinance (free)
# ABOUTME: Calculates market returns from S&P 500 index and saves as standardized parquet file
"""
Inputs:
- yfinance SPY (S&P 500 ETF) for value-weighted market returns
- S&P 500 constituents for equal-weighted market returns

Outputs:
- ../pyData/Intermediate/AP_monthlyMarket.parquet

Requirements:
    pip install yfinance pandas

How to run: python AP_MarketReturns.py

Notes:
- Uses SPY (S&P 500 ETF) as proxy for value-weighted market returns
- Equal-weighted returns calculated from S&P 500 constituents
- Free alternative to CRSP market summary index
- Historical data available from 1993+ (SPY inception)
- For longer history, could use ^GSPC (S&P 500 index) which has data from 1927+
"""

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
    YFINANCE_AVAILABLE = False

# Print script header
print("=" * 70, flush=True)
print("📈 AP_MarketReturns.py - Market Returns from yfinance", flush=True)
print("=" * 70, flush=True)

# Output directory
OUTPUT_DIR = Path("../pyData/Intermediate")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# =============================================================================
# CONFIGURATION
# =============================================================================

# Use S&P 500 index for longest history
# ^GSPC = S&P 500 Index (1927+)
# SPY = S&P 500 ETF (1993+)
MARKET_TICKER = '^GSPC'  # S&P 500 Index for long history

# Date range - Last 2 years
END_DATE = datetime.now().strftime('%Y-%m-%d')
START_DATE = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')  # 2 years ago

# Debug mode
DEBUG_MODE = False

print(f"📊 Downloading market returns from yfinance")
print(f"  Ticker: {MARKET_TICKER} (S&P 500)")
print(f"  Period: {START_DATE} to {END_DATE}")

# =============================================================================
# DATA DOWNLOAD FUNCTIONS
# =============================================================================

def download_market_returns():
    """
    Download market returns from S&P 500 index.
    
    Returns:
        DataFrame with columns: time_avail_m, vwretd, usdval
    """
    
    if not YFINANCE_AVAILABLE:
        print("❌ yfinance not available")
        return pd.DataFrame()
    
    print(f"\n📥 Downloading {MARKET_TICKER} data from Yahoo Finance...")
    
    try:
        # Download S&P 500 index data
        ticker = yf.Ticker(MARKET_TICKER)
        hist = ticker.history(start=START_DATE, end=END_DATE, interval='1mo')
        
        if hist.empty:
            print(f"❌ No data returned for {MARKET_TICKER}")
            return pd.DataFrame()
        
        print(f"✓ Downloaded {len(hist)} monthly observations")
        print(f"  Date range: {hist.index.min().date()} to {hist.index.max().date()}")
        
        # Calculate monthly returns
        hist_df = hist.reset_index()
        hist_df['time_avail_m'] = pd.to_datetime(hist_df['Date']).dt.to_period('M').dt.to_timestamp()
        
        # Calculate value-weighted return (from S&P 500 price changes)
        hist_df['vwretd'] = hist_df['Close'].pct_change()
        
        # Use market cap as proxy for market value (Volume * Close)
        # For S&P 500 index, we'll use a scaled version
        hist_df['usdval'] = hist_df['Close'] * hist_df['Volume'] / 1e9  # Billions
        
        # Clean data
        result = hist_df[['time_avail_m', 'vwretd', 'usdval']].copy()
        result = result.dropna(subset=['vwretd'])
        
        print(f"✓ Calculated value-weighted market returns")
        print(f"  Records: {len(result)}")
        
        return result
        
    except Exception as e:
        print(f"❌ Error downloading market data: {e}")
        return pd.DataFrame()

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
        # Fallback to Wikipedia
        return get_sp500_tickers()

def get_sp500_tickers():
    """Get current S&P 500 ticker list from Wikipedia (fallback)"""
    try:
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        tables = pd.read_html(url)
        df = tables[0]
        tickers = df['Symbol'].str.replace('.', '-', regex=False).tolist()
        print(f"✓ Retrieved {len(tickers)} S&P 500 tickers")
        return tickers
    except Exception as e:
        print(f"⚠️  Could not fetch S&P 500 list: {e}")
        return []

def calculate_equal_weighted_returns(tickers, start_date, end_date):
    """
    Calculate equal-weighted market returns from S&P 500 constituents.
    
    Note: This is computationally expensive. We'll use a sample in debug mode.
    """
    
    if not YFINANCE_AVAILABLE:
        return pd.DataFrame()
    
    print(f"\n📊 Calculating equal-weighted returns from S&P 500 constituents...")
    
    if DEBUG_MODE:
        tickers = tickers[:20]  # Use only 20 stocks in debug mode
        print(f"  DEBUG: Using sample of {len(tickers)} tickers")
    
    # Download monthly data for all tickers
    all_returns = []
    successful = 0
    failed = 0
    
    for i, ticker in enumerate(tickers, 1):
        if i % 50 == 0:
            print(f"  Progress: {i}/{len(tickers)} ({successful} successful, {failed} failed)")
        
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(start=start_date, end=end_date, interval='1mo')
            
            if hist.empty:
                failed += 1
                continue
            
            hist_df = hist.reset_index()
            hist_df['time_avail_m'] = pd.to_datetime(hist_df['Date']).dt.to_period('M').dt.to_timestamp()
            hist_df['ret'] = hist_df['Close'].pct_change()
            hist_df['ticker'] = ticker
            
            all_returns.append(hist_df[['time_avail_m', 'ticker', 'ret']])
            successful += 1
            
        except:
            failed += 1
            continue
    
    print(f"✓ Downloaded data for {successful}/{len(tickers)} tickers")
    
    if not all_returns:
        return pd.DataFrame()
    
    # Combine all returns
    combined = pd.concat(all_returns, ignore_index=True)
    
    # Calculate equal-weighted average for each month
    ew_returns = combined.groupby('time_avail_m')['ret'].mean().reset_index()
    ew_returns.columns = ['time_avail_m', 'ewretd']
    
    print(f"✓ Calculated equal-weighted returns for {len(ew_returns)} months")
    
    return ew_returns

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main execution function"""
    
    if not YFINANCE_AVAILABLE:
        print("\n❌ Cannot proceed without yfinance. Please install:")
        print("   pip install yfinance")
        return
    
    # Download value-weighted market returns (from S&P 500 index)
    vw_returns = download_market_returns()
    
    if vw_returns.empty:
        print("\n❌ Failed to download market returns")
        return
    
    # Calculate equal-weighted returns from constituents
    # Note: This is optional and slow. We'll make it simple.
    print(f"\n📊 Calculating equal-weighted returns...")
    print(f"  Note: Equal-weighted returns require downloading all S&P 500 stocks")
    print(f"  This may take 10-15 minutes for full history")
    
    # Get S&P 500 tickers
    sp500_tickers = load_sp500_universe()
    
    if sp500_tickers:
        ew_returns = calculate_equal_weighted_returns(sp500_tickers, START_DATE, END_DATE)
        
        if not ew_returns.empty:
            # Merge with value-weighted returns
            final_data = vw_returns.merge(ew_returns, on='time_avail_m', how='left')
        else:
            print("⚠️  Equal-weighted returns calculation failed, using VW only")
            final_data = vw_returns.copy()
            final_data['ewretd'] = np.nan
    else:
        print("⚠️  Could not get S&P 500 tickers, using VW only")
        final_data = vw_returns.copy()
        final_data['ewretd'] = np.nan
    
    # Sort by date
    final_data = final_data.sort_values('time_avail_m').reset_index(drop=True)
    
    # Save output
    print(f"\n💾 Saving output...")
    
    output_file = OUTPUT_DIR / "AP_monthlyMarket.parquet"
    final_data.to_parquet(output_file, index=False)
    
    print(f"  ✓ Saved: {output_file}")
    print(f"    Records: {len(final_data):,}")
    print(f"    Size: {output_file.stat().st_size / 1024:.1f} KB")
    
    # Generate summary
    print(f"\n📊 Summary Statistics:")
    print(f"  Total observations: {len(final_data):,}")
    print(f"  Date range: {final_data['time_avail_m'].min()} to {final_data['time_avail_m'].max()}")
    print(f"  Months covered: {final_data['time_avail_m'].nunique()}")
    
    # Value-weighted return statistics
    vw_data = final_data['vwretd'].dropna()
    if len(vw_data) > 0:
        print(f"\n  Value-weighted return (vwretd):")
        print(f"    Mean: {vw_data.mean():.4f} ({vw_data.mean()*100:.2f}%/month)")
        print(f"    Std: {vw_data.std():.4f} ({vw_data.std()*100:.2f}%/month)")
        print(f"    Min: {vw_data.min():.4f} ({vw_data.min()*100:.2f}%)")
        print(f"    Max: {vw_data.max():.4f} ({vw_data.max()*100:.2f}%)")
        print(f"    Annualized: {(1 + vw_data.mean())**12 - 1:.2%}")
    
    # Equal-weighted return statistics
    ew_data = final_data['ewretd'].dropna()
    if len(ew_data) > 0:
        print(f"\n  Equal-weighted return (ewretd):")
        print(f"    Mean: {ew_data.mean():.4f} ({ew_data.mean()*100:.2f}%/month)")
        print(f"    Std: {ew_data.std():.4f} ({ew_data.std()*100:.2f}%/month)")
        print(f"    Min: {ew_data.min():.4f} ({ew_data.min()*100:.2f}%)")
        print(f"    Max: {ew_data.max():.4f} ({ew_data.max()*100:.2f}%)")
        print(f"    Annualized: {(1 + ew_data.mean())**12 - 1:.2%}")
    else:
        print(f"\n  Equal-weighted return (ewretd): Not available")
    
    # Show recent values
    print(f"\n  Recent monthly returns:")
    recent = final_data.tail(12)
    for _, row in recent.iterrows():
        vw_str = f"{row['vwretd']*100:+.2f}%" if pd.notna(row['vwretd']) else "N/A"
        ew_str = f"{row['ewretd']*100:+.2f}%" if pd.notna(row['ewretd']) else "N/A"
        print(f"    {row['time_avail_m'].strftime('%Y-%m')}: VW={vw_str}, EW={ew_str}")
    
    print("\n" + "=" * 70)
    print("✅ AP_MarketReturns.py completed successfully!")
    print("=" * 70)
    print("\nOutputs:")
    print("  1. AP_monthlyMarket.parquet - Monthly market returns")
    print("\nColumns:")
    print("  - time_avail_m: Month (first day)")
    print("  - vwretd: Value-weighted return (from S&P 500 index)")
    print("  - ewretd: Equal-weighted return (from S&P 500 constituents)")
    print("  - usdval: Market value proxy")
    print("\nNext steps:")
    print("  - Use AP_monthlyMarket.parquet in place of monthlyMarket.parquet")
    print("  - Use for market-adjusted returns and factor calculations")
    print("\n💡 Data Source:")
    print("  - S&P 500 index (^GSPC) from Yahoo Finance")
    print("  - Free, no API key required")
    print("  - Historical data from 1927+")

if __name__ == "__main__":
    main()

