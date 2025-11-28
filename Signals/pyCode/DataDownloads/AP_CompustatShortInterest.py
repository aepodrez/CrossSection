# ABOUTME: Downloads and processes short interest data from FINRA (free alternative to Compustat)
# ABOUTME: Uses FINRA short sale volume data and matches to tickers via CUSIP/ticker mapping
"""
Inputs:
- FINRA short sale volume data (free download)
- AP_CRSPMonthly.parquet or CCMLinkingTable.parquet (for ticker/CUSIP mapping)
- AP_CompustatAnnual.parquet (for gvkey mapping, optional)

Outputs:
- ../pyData/Intermediate/AP_monthlyShortInterest.parquet

Data Sources:
- FINRA Short Sale Volume Data: https://www.finra.org/finra-data/browse-catalog/short-sale-volume-data
- Available from 2010 onwards (daily data)
- Free and publicly available

How to run: python AP_CompustatShortInterest.py

Requirements:
    pip install requests pandas numpy
    # Optional: pip install finra-short-sale (if available)
"""

import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict
from io import StringIO
import time
import warnings
warnings.filterwarnings('ignore')

# Try to import requests for API calls
try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    print("⚠️  requests not installed. Install with: pip install requests")
    REQUESTS_AVAILABLE = False

# Print script header
print("=" * 60, flush=True)
print("📊 AP_CompustatShortInterest.py - Live FINRA Short Interest", flush=True)
print("=" * 60, flush=True)


# =============================================================================
# FINRA DATA DOWNLOAD FUNCTIONS
# =============================================================================

def download_finra_short_interest(date: str) -> Optional[pd.DataFrame]:
    """
    Download FINRA short sale volume data for a specific date.
    
    FINRA provides daily short sale volume data in CSV format.
    URL format: https://cdn.finra.org/equity/regsho/daily/[YYYYMMDD]/CNMSshvol[YYYYMMDD].txt
    
    Args:
        date: Date in YYYYMMDD format
        
    Returns:
        DataFrame with short interest data, or None if download fails
    """
    if not REQUESTS_AVAILABLE:
        return None
    
    url = f"https://cdn.finra.org/equity/regsho/daily/{date}/CNMSshvol{date}.txt"
    
    # FINRA requires proper headers and may have tightened bot detection
    # Use a more recent User-Agent and additional headers to mimic browser behavior
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': 'https://www.finra.org/finra-data/browse-catalog/short-sale-volume-data',
        'Origin': 'https://www.finra.org',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-site',
        'Cache-Control': 'max-age=0'
    }
    
    # Use a session to maintain cookies
    session = requests.Session()
    session.headers.update(headers)
    
    try:
        print(f"  Downloading FINRA data for {date}...", flush=True)
        
        # First, try to access the main FINRA page to establish session
        try:
            session.get('https://www.finra.org/finra-data/browse-catalog/short-sale-volume-data', timeout=10)
        except:
            pass  # Continue even if this fails
        
        # Add small delay to avoid rate limiting
        time.sleep(0.5)
        
        response = session.get(url, timeout=30)
        response.raise_for_status()
        
        # Read from response content instead of URL to avoid double request
        df = pd.read_csv(
            StringIO(response.text),
            sep='|',
            skiprows=1,  # Skip header row
            header=0,
            dtype=str
        )
        
        # Clean column names (remove whitespace)
        df.columns = df.columns.str.strip()
        
        # Expected columns: Date, Symbol, ShortVolume, ShortExemptVolume, TotalVolume
        # Some files may have different formats
        if 'ShortVolume' in df.columns or 'Short Volume' in df.columns:
            return df
        else:
            print(f"    ⚠️  Unexpected column format for {date}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"    ⚠️  Could not download {date}: {e}")
        return None
    except Exception as e:
        print(f"    ⚠️  Error processing {date}: {e}")
        return None


def download_finra_monthly(year: int, month: int) -> pd.DataFrame:
    """
    Download all FINRA short interest data for a given month.
    
    Args:
        year: Year (e.g., 2024)
        month: Month (1-12)
        
    Returns:
        Combined DataFrame for the month
    """
    # Get all dates in the month
    start_date = datetime(year, month, 1)
    if month == 12:
        end_date = datetime(year + 1, 1, 1)
    else:
        end_date = datetime(year, month + 1, 1)
    
    all_data = []
    current_date = start_date
    
    while current_date < end_date:
        # Skip weekends (FINRA data is only for trading days)
        if current_date.weekday() < 5:  # Monday = 0, Friday = 4
            date_str = current_date.strftime('%Y%m%d')
            daily_data = download_finra_short_interest(date_str)
            if daily_data is not None and not daily_data.empty:
                daily_data['date'] = current_date
                all_data.append(daily_data)
        
        current_date += timedelta(days=1)
    
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    else:
        return pd.DataFrame()


def process_finra_data(finra_df: pd.DataFrame) -> pd.DataFrame:
    """
    Process raw FINRA data into standardized format.
    
    Args:
        finra_df: Raw FINRA DataFrame
        
    Returns:
        Processed DataFrame with standardized columns
    """
    if finra_df.empty:
        return pd.DataFrame()
    
    df = finra_df.copy()
    
    # Standardize column names (FINRA format may vary)
    column_mapping = {
        'Symbol': 'ticker',
        'symbol': 'ticker',
        'SYMBOL': 'ticker',
        'ShortVolume': 'short_volume',
        'Short Volume': 'short_volume',
        'SHORT_VOLUME': 'short_volume',
        'TotalVolume': 'total_volume',
        'Total Volume': 'total_volume',
        'TOTAL_VOLUME': 'total_volume',
        'Date': 'date',
        'date': 'date',
        'DATE': 'date',
    }
    
    # Rename columns
    for old_col, new_col in column_mapping.items():
        if old_col in df.columns:
            df = df.rename(columns={old_col: new_col})
    
    # Ensure we have required columns
    if 'ticker' not in df.columns or 'short_volume' not in df.columns:
        print("⚠️  Missing required columns in FINRA data")
        return pd.DataFrame()
    
    # Convert date if it's a string
    if 'date' in df.columns:
        if df['date'].dtype == 'object':
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
    else:
        # Try to extract from other date columns
        date_cols = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
        if date_cols:
            df['date'] = pd.to_datetime(df[date_cols[0]], errors='coerce')
    
    # Convert short_volume to numeric
    df['short_volume'] = pd.to_numeric(df['short_volume'], errors='coerce')
    df['total_volume'] = pd.to_numeric(df['total_volume'], errors='coerce') if 'total_volume' in df.columns else None
    
    # Calculate short interest ratio (if total volume available)
    if 'total_volume' in df.columns and df['total_volume'].notna().any():
        df['short_ratio'] = df['short_volume'] / df['total_volume']
    
    # Clean ticker symbols (remove whitespace, convert to uppercase)
    df['ticker'] = df['ticker'].str.strip().str.upper()
    
    # Drop rows with missing ticker or short_volume
    df = df[df['ticker'].notna() & df['short_volume'].notna()]
    
    return df


# =============================================================================
# TICKER TO GVKEY MAPPING
# =============================================================================

def load_ticker_gvkey_mapping() -> pd.DataFrame:
    """
    Load ticker to gvkey mapping from existing AP files.
    
    Returns:
        DataFrame with columns: ticker, gvkey
    """
    mapping = pd.DataFrame()
    
    # Try to load from AP_CompustatAnnual
    compustat_path = Path("../pyData/Intermediate/AP_CompustatAnnual.parquet")
    if compustat_path.exists():
        try:
            compustat = pd.read_parquet(compustat_path, columns=['ticker', 'gvkey'])
            compustat = compustat.dropna(subset=['ticker', 'gvkey'])
            compustat = compustat.drop_duplicates(subset=['ticker'])
            mapping = compustat[['ticker', 'gvkey']].copy()
            print(f"✓ Loaded {len(mapping)} ticker-gvkey mappings from AP_CompustatAnnual")
        except Exception as e:
            print(f"⚠️  Could not load from AP_CompustatAnnual: {e}")
    
    # Try to load from CCM linking table
    if mapping.empty:
        ccm_path = Path("../pyData/Intermediate/CCMLinkingTable.parquet")
        if ccm_path.exists():
            try:
                ccm = pd.read_parquet(ccm_path, columns=['ticker', 'gvkey'])
                ccm = ccm.dropna(subset=['ticker', 'gvkey'])
                ccm = ccm.drop_duplicates(subset=['ticker'])
                mapping = ccm[['ticker', 'gvkey']].copy()
                print(f"✓ Loaded {len(mapping)} ticker-gvkey mappings from CCMLinkingTable")
            except Exception as e:
                print(f"⚠️  Could not load from CCMLinkingTable: {e}")
    
    return mapping


# =============================================================================
# MAIN PROCESSING FUNCTIONS
# =============================================================================

def aggregate_to_monthly(daily_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate daily short interest data to monthly.
    
    Uses first non-missing value per ticker-month (matching original Compustat logic).
    
    Args:
        daily_df: Daily short interest DataFrame with columns: ticker, date, short_volume
        
    Returns:
        Monthly aggregated DataFrame with columns: ticker, time_avail_m, shortint
    """
    if daily_df.empty:
        return pd.DataFrame()
    
    df = daily_df.copy()
    
    # Convert date to monthly period
    df['time_avail_m'] = df['date'].dt.to_period('M').dt.to_timestamp()
    
    # Sort by ticker, time, and date
    df = df.sort_values(['ticker', 'time_avail_m', 'date'])
    
    # Aggregate: take first non-missing value per ticker-month
    def first_non_missing(series: pd.Series) -> float:
        non_missing = series.dropna()
        return non_missing.iloc[0] if not non_missing.empty else np.nan
    
    monthly = df.groupby(['ticker', 'time_avail_m'], as_index=False).agg(
        shortint=('short_volume', first_non_missing),
        shortintadj=('short_volume', first_non_missing),  # Same as shortint for now
    )
    
    # Convert shortint from shares to millions (matching Compustat format)
    monthly['shortint'] = monthly['shortint'] / 1e6
    monthly['shortintadj'] = monthly['shortintadj'] / 1e6
    
    return monthly


def add_gvkey_to_short_interest(short_df: pd.DataFrame, mapping: pd.DataFrame) -> pd.DataFrame:
    """
    Add gvkey to short interest data using ticker mapping.
    
    Args:
        short_df: Short interest DataFrame with ticker column
        mapping: Ticker to gvkey mapping DataFrame
        
    Returns:
        DataFrame with gvkey added
    """
    if mapping.empty:
        print("⚠️  No ticker-gvkey mapping available. Short interest will not have gvkey.")
        return short_df
    
    # Merge on ticker
    result = short_df.merge(
        mapping,
        on='ticker',
        how='left'
    )
    
    # Report coverage
    coverage = (result['gvkey'].notna().sum() / len(result)) * 100
    print(f"✓ Added gvkey to {result['gvkey'].notna().sum():,} of {len(result):,} records ({coverage:.1f}%)")
    
    return result


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """
    Main execution function.
    """
    
    print("\n" + "="*60)
    print("📋 Loading ticker universe and mapping...")
    print("="*60)
    
    # Load ticker-gvkey mapping
    ticker_gvkey_map = load_ticker_gvkey_mapping()
    
    # Determine date range to download
    # FINRA data available from ~2010 onwards
    # Download last 2 years by default (can be adjusted)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=730)  # ~2 years
    
    print(f"\n📥 Downloading FINRA short interest data...")
    print(f"   Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"   Note: FINRA data is only available for trading days")
    
    # Download data month by month
    all_daily_data = []
    
    current_date = start_date.replace(day=1)  # Start of month
    
    while current_date <= end_date:
        year = current_date.year
        month = current_date.month
        
        print(f"\n📅 Processing {year}-{month:02d}...")
        monthly_data = download_finra_monthly(year, month)
        
        if not monthly_data.empty:
            processed = process_finra_data(monthly_data)
            if not processed.empty:
                all_daily_data.append(processed)
                print(f"  ✓ Collected {len(processed)} daily records for {year}-{month:02d}")
        
        # Move to next month
        if month == 12:
            current_date = datetime(year + 1, 1, 1)
        else:
            current_date = datetime(year, month + 1, 1)
    
    if not all_daily_data:
        print("\n❌ No FINRA data downloaded. Exiting.")
        return
    
    # Combine all daily data
    print("\n" + "="*60)
    print("🔄 Processing and aggregating data...")
    print("="*60)
    
    daily_df = pd.concat(all_daily_data, ignore_index=True)
    print(f"✓ Combined {len(daily_df):,} daily records")
    
    # Aggregate to monthly
    monthly_df = aggregate_to_monthly(daily_df)
    print(f"✓ Aggregated to {len(monthly_df):,} monthly records")
    
    # Add gvkey if mapping available
    if not ticker_gvkey_map.empty:
        monthly_df = add_gvkey_to_short_interest(monthly_df, ticker_gvkey_map)
    else:
        monthly_df['gvkey'] = None
    
    # Convert gvkey to numeric if present
    if 'gvkey' in monthly_df.columns:
        monthly_df['gvkey'] = pd.to_numeric(monthly_df['gvkey'], errors='coerce')
    
    # Sort by gvkey (if available) or ticker, then time
    if 'gvkey' in monthly_df.columns and monthly_df['gvkey'].notna().any():
        monthly_df = monthly_df.sort_values(['gvkey', 'time_avail_m'])
    else:
        monthly_df = monthly_df.sort_values(['ticker', 'time_avail_m'])
    
    # Remove duplicates (keep first)
    if 'gvkey' in monthly_df.columns and monthly_df['gvkey'].notna().any():
        monthly_df = monthly_df.drop_duplicates(subset=['gvkey', 'time_avail_m'], keep='first')
    else:
        monthly_df = monthly_df.drop_duplicates(subset=['ticker', 'time_avail_m'], keep='first')
    
    print(f"✓ After deduplication: {len(monthly_df):,} monthly records")
    
    # Select final columns (matching original format)
    final_columns = ['time_avail_m']
    if 'gvkey' in monthly_df.columns:
        final_columns.insert(0, 'gvkey')
    if 'ticker' in monthly_df.columns:
        final_columns.append('ticker')
    final_columns.extend(['shortint', 'shortintadj'])
    
    monthly_df = monthly_df[[col for col in final_columns if col in monthly_df.columns]]
    
    # Save data
    print("\n" + "="*60)
    print("💾 Saving monthly short interest data...")
    print("="*60)
    
    output_dir = Path("../pyData/Intermediate/")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output_path = output_dir / "AP_monthlyShortInterest.parquet"
    monthly_df.to_parquet(output_path, index=False)
    print(f"✓ Saved: {output_path}")
    
    # Print summary statistics
    print("\n" + "="*60)
    print("📈 Summary Statistics")
    print("="*60)
    
    if 'gvkey' in monthly_df.columns:
        print(f"Total companies (with gvkey): {monthly_df['gvkey'].notna().sum():,}")
        print(f"Total companies (all): {monthly_df['ticker'].nunique() if 'ticker' in monthly_df.columns else 'N/A':,}")
    else:
        print(f"Total companies: {monthly_df['ticker'].nunique() if 'ticker' in monthly_df.columns else 'N/A':,}")
    
    print(f"Total monthly records: {len(monthly_df):,}")
    
    if not monthly_df.empty:
        print(f"Date range: {monthly_df['time_avail_m'].min()} to {monthly_df['time_avail_m'].max()}")
        print(f"Short interest range: {monthly_df['shortint'].min():.2f}M to {monthly_df['shortint'].max():.2f}M shares")
    
    print(f"\nMissing data:")
    print(f"  shortint: {monthly_df['shortint'].isna().sum():,} missing ({monthly_df['shortint'].isna().sum() / len(monthly_df) * 100:.1f}%)")
    print(f"  shortintadj: {monthly_df['shortintadj'].isna().sum():,} missing ({monthly_df['shortintadj'].isna().sum() / len(monthly_df) * 100:.1f}%)")
    
    if 'gvkey' in monthly_df.columns:
        print(f"  gvkey: {monthly_df['gvkey'].isna().sum():,} missing ({monthly_df['gvkey'].isna().sum() / len(monthly_df) * 100:.1f}%)")
    
    print("\n" + "="*60)
    print("✅ AP_CompustatShortInterest.py completed successfully")
    print("="*60)
    
    print("\n📝 Notes:")
    print("  - FINRA data is available from ~2010 onwards")
    print("  - Data is daily, aggregated to monthly (first non-missing value)")
    print("  - Short interest is in millions of shares (matching Compustat format)")
    print("  - gvkey mapping requires AP_CompustatAnnual.parquet or CCMLinkingTable.parquet")


if __name__ == "__main__":
    main()

