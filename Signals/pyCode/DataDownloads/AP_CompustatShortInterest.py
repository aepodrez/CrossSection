# ABOUTME: Downloads and processes equity short interest positions from FINRA (free alternative to Compustat)
# ABOUTME: Uses FINRA bi-monthly short interest positions and matches to gvkeys via AP_CompustatAnnual
"""
Inputs:
- FINRA equity short interest positions (bi-monthly, free download)
- AP_CompustatAnnual.parquet (for ticker/gvkey mapping)

Outputs:
- ../pyData/Intermediate/AP_monthlyShortInterest.parquet

Data Sources:
- FINRA Equity Short Interest Positions: https://www.finra.org/finra-data/browse-catalog/equity-short-interest/files
- Available from Dec 2017 onwards (bi-monthly files such as shrtYYYYMMDD.csv)
- Free and publicly available

How to run: python AP_CompustatShortInterest.py

Requirements:
    pip install requests pandas numpy
    # Optional: pip install finra-short-sale (if available)
"""

import os
import sys
import time
import pandas as pd
import numpy as np
import re
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict
from io import StringIO
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
print("📊 AP_CompustatShortInterest.py - FINRA Equity Short Interest Positions", flush=True)
print("=" * 60, flush=True)


# =============================================================================
# FINRA DATA DOWNLOAD FUNCTIONS
# =============================================================================

FINRA_FILES_PAGE = "https://www.finra.org/finra-data/browse-catalog/equity-short-interest/files?custom_month%5Bmonth%5D=any&custom_year%5Byear%5D=any"


def list_finra_short_interest_files() -> pd.DataFrame:
    """
    Scrape FINRA's Equity Short Interest files page to enumerate available CSVs.
    
    Returns:
        DataFrame with columns: settlement_str (YYYYMMDD), settlementDate (datetime), url
    """
    if not REQUESTS_AVAILABLE:
        return pd.DataFrame()
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    try:
        print(f"  Fetching FINRA files page: {FINRA_FILES_PAGE}", flush=True)
        resp = requests.get(FINRA_FILES_PAGE, headers=headers, timeout=45)
        print(f"  Response status: {resp.status_code}, length: {len(resp.text)} chars", flush=True)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"⚠️  Could not fetch FINRA files list: {e}")
        return pd.DataFrame()
    
    # Pattern captures full URL and settlement date (YYYYMMDD)
    # Primary: explicit CDN links
    pattern_full = r"(https?:)?//cdn\.finra\.org/[\w/.-]*shrt(\d{8})\.csv"
    matches = []
    for m in re.finditer(pattern_full, resp.text):
        full_url = m.group(0)
        if full_url.startswith("//"):
            full_url = "https:" + full_url
        date_str = m.group(2)
        matches.append((full_url, date_str))
    
    # Fallback: find just shrtYYYYMMDD.csv tokens and reconstruct CDN URL
    if not matches:
        dates_only = re.findall(r"shrt(\d{8})\.csv", resp.text)
        if dates_only:
            print(f"  Found {len(set(dates_only))} shrtYYYYMMDD tokens; constructing CDN URLs", flush=True)
            matches = [("https://cdn.finra.org/equity/otcmarket/biweekly/shrt" + d + ".csv", d) for d in set(dates_only)]
        else:
            print("⚠️  No FINRA short interest file links found on the page.")
            snippet = resp.text[:1000] if resp.text else "<empty page>"
            print("⚠️  Page snippet (first 1000 chars) to help debugging:\n")
            print(snippet)
            # Write full page to disk for inspection
            debug_path = Path("../pyData/Intermediate/AP_finra_short_interest_page.html")
            try:
                debug_path.parent.mkdir(parents=True, exist_ok=True)
                debug_path.write_text(resp.text, encoding="utf-8")
                print(f"⚠️  Saved full page HTML to {debug_path} for manual inspection.")
            except Exception as e:
                print(f"⚠️  Could not save debug HTML: {e}")
            return pd.DataFrame()
    
    seen: Dict[str, str] = {}
    for entry in matches:
        url, date_str = entry
        seen[date_str] = url  # keep latest occurrence
    
    records = []
    for date_str, url in seen.items():
        try:
            dt = datetime.strptime(date_str, "%Y%m%d")
        except ValueError:
            dt = pd.NaT
        records.append({"settlement_str": date_str, "settlementDate": dt, "url": url})
    
    file_df = pd.DataFrame(records)
    file_df = file_df.sort_values("settlementDate").reset_index(drop=True)
    
    if not file_df.empty:
        min_date = file_df["settlementDate"].min()
        max_date = file_df["settlementDate"].max()
        print(f"✓ Found {len(file_df)} FINRA short interest files ({min_date.date()} to {max_date.date()})")
    
    return file_df


def download_finra_short_interest_file(url: str, settlement_hint: Optional[str] = None) -> Optional[pd.DataFrame]:
    """
    Download a single FINRA short interest positions file.
    
    Args:
        url: Direct CSV URL (e.g., https://cdn.finra.org/equity/otcmarket/biweekly/shrt20251114.csv)
        settlement_hint: Fallback settlement date string (YYYYMMDD) if column missing
        
    Returns:
        Raw DataFrame or None on failure
    """
    if not REQUESTS_AVAILABLE:
        return None
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    try:
        print(f"  Downloading {url} ...", flush=True)
        time.sleep(0.2)
        resp = requests.get(url, headers=headers, timeout=60)
        if resp.status_code == 404:
            print(f"    ⚠️  File not found: {url}")
            return None
        resp.raise_for_status()
        df = pd.read_csv(StringIO(resp.text), sep="|", dtype=str)
        df.columns = df.columns.str.strip()
        if settlement_hint and "settlementDate" not in df.columns:
            df["settlementDate"] = settlement_hint
        return df
    except requests.exceptions.RequestException as e:
        print(f"    ⚠️  Network error for {url}: {e}")
        return None
    except Exception as e:
        print(f"    ⚠️  Error parsing {url}: {e}")
        return None


def process_finra_data(finra_df: pd.DataFrame) -> pd.DataFrame:
    """
    Process raw FINRA short interest positions into standardized format.
    
    Args:
        finra_df: Raw FINRA DataFrame from CDN
        
    Returns:
        Processed DataFrame with columns: ticker, date, short_interest
    """
    if finra_df.empty:
        return pd.DataFrame()
    
    df = finra_df.copy()
    
    # Standardize column names (FINRA format may vary slightly across vintages)
    column_mapping = {
        'Symbol': 'ticker',
        'symbolCode': 'ticker',
        'symbol': 'ticker',
        'SYMBOL': 'ticker',
        'Ticker': 'ticker',
        'currentShortPositionQuantity': 'short_interest',
        'currentShortPositionQty': 'short_interest',
        'shortPosition': 'short_interest',
        'settlementDate': 'settlementDate',
        'SettlementDate': 'settlementDate',
        'accountingYearMonthNumber': 'accountingYearMonthNumber',
    }
    
    # Rename columns
    for old_col, new_col in column_mapping.items():
        if old_col in df.columns:
            df = df.rename(columns={old_col: new_col})
    
    # Ensure we have required columns
    if 'ticker' not in df.columns or ('short_interest' not in df.columns and 'currentShortPositionQuantity' not in df.columns):
        print("⚠️  Missing required columns in FINRA data")
        return pd.DataFrame()
    
    # Create a unified date column
    date_col = None
    if 'settlementDate' in df.columns:
        date_col = pd.to_datetime(df['settlementDate'], errors='coerce')
    elif 'accountingYearMonthNumber' in df.columns:
        date_col = pd.to_datetime(df['accountingYearMonthNumber'], format='%Y%m%d', errors='coerce')
    else:
        # Try to find any column that includes 'date'
        date_cols = [col for col in df.columns if 'date' in col.lower()]
        if date_cols:
            date_col = pd.to_datetime(df[date_cols[0]], errors='coerce')
    
    if date_col is None:
        print("⚠️  Could not locate a date column in FINRA data")
        return pd.DataFrame()
    
    df['date'] = date_col
    
    # Convert short interest to numeric
    if 'short_interest' not in df.columns and 'currentShortPositionQuantity' in df.columns:
        df['short_interest'] = df['currentShortPositionQuantity']
    df['short_interest'] = pd.to_numeric(df['short_interest'], errors='coerce')
    
    # Clean ticker symbols (remove whitespace, convert to uppercase)
    df['ticker'] = df['ticker'].str.strip().str.upper()
    
    # Drop rows with missing ticker, date, or short interest
    df = df[df['ticker'].notna() & df['date'].notna() & df['short_interest'].notna()]
    
    # Keep only the columns we need downstream
    df = df[['ticker', 'date', 'short_interest']].copy()
    
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
    
    # Load from AP_CompustatAnnual
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
    else:
        print("⚠️  AP_CompustatAnnual.parquet not found. Short interest will not have gvkey.")
    
    return mapping


# =============================================================================
# MAIN PROCESSING FUNCTIONS
# =============================================================================

def aggregate_to_monthly(positions_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate FINRA bi-monthly short interest data to monthly (first non-missing).
    
    Uses first non-missing value per ticker-month (matching Compustat logic).
    
    Args:
        positions_df: Short interest DataFrame with columns: ticker, date, short_interest
        
    Returns:
        Monthly aggregated DataFrame with columns: ticker, time_avail_m, shortint, shortintadj
    """
    if positions_df.empty:
        return pd.DataFrame()
    
    df = positions_df.copy()

    # If there are multiple 'date' columns (duplicate labels), collapse to a single one
    if 'date' in df.columns:
        # Find all indices where column label is exactly 'date'
        date_indices = [i for i, c in enumerate(df.columns) if c == 'date']
        if len(date_indices) > 1:
            # Use the first 'date' column as the canonical one
            primary_date = df.iloc[:, date_indices[0]]
            # Drop all columns named 'date'
            df = df.loc[:, df.columns != 'date']
            # Reattach the canonical 'date' column once
            df['date'] = primary_date
    # Ensure date column exists and is a Series
    if 'date' not in df.columns:
        print("⚠️  'date' column not found in positions_df")
        return pd.DataFrame()
    
    # Safely access the date column
    date_col = df['date']
    if isinstance(date_col, pd.DataFrame):
        # If it's a DataFrame (unlikely but handle it), take first column
        if len(date_col.columns) > 0:
            date_col = date_col.iloc[:, 0]
        else:
            print("⚠️  Date column is empty DataFrame")
            return pd.DataFrame()
    
    # Ensure date is datetime type
    if not pd.api.types.is_datetime64_any_dtype(date_col):
        date_col = pd.to_datetime(date_col, errors='coerce')
    
    # Convert date to monthly period
    df['time_avail_m'] = date_col.dt.to_period('M').dt.to_timestamp()
    
    # Sort by ticker, time, and date
    df = df.sort_values(['ticker', 'time_avail_m', 'date'])
    
    # Aggregate: take first non-missing value per ticker-month
    def first_non_missing(series: pd.Series) -> float:
        non_missing = series.dropna()
        return non_missing.iloc[0] if not non_missing.empty else np.nan
    
    monthly = df.groupby(['ticker', 'time_avail_m'], as_index=False).agg(
        shortint=('short_interest', first_non_missing),
        shortintadj=('short_interest', first_non_missing),  # Same as shortint for now
    )
    
    # Convert short interest from shares to millions (Compustat format)
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
    
    if not REQUESTS_AVAILABLE:
        print("❌ requests library not available. Install with `pip install requests`.")
        return
    
    print("\n" + "="*60)
    print("📋 Loading ticker universe and mapping...")
    print("="*60)
    
    # Load ticker-gvkey mapping
    ticker_gvkey_map = load_ticker_gvkey_mapping()
    
    print("\n" + "="*60)
    print("📥 Discovering FINRA short interest position files...")
    print("="*60)
    available_files = list_finra_short_interest_files()
    if available_files.empty:
        print("\n❌ No FINRA short interest files found. Exiting.")
        return
    
    all_data = []
    for row in available_files.itertuples():
        raw_df = download_finra_short_interest_file(row.url, settlement_hint=row.settlement_str)
        if raw_df is None or raw_df.empty:
            continue
        processed = process_finra_data(raw_df)
        if not processed.empty:
            all_data.append(processed)
            print(f"  ✓ Processed {len(processed)} rows for settlement {row.settlement_str}")
    
    if not all_data:
        print("\n❌ No FINRA data processed. Exiting.")
        return
    
    # Combine all records
    print("\n" + "="*60)
    print("🔄 Processing and aggregating data...")
    print("="*60)
    
    positions_df = pd.concat(all_data, ignore_index=True)
    print(f"✓ Combined {len(positions_df):,} raw records across {len(all_data)} files")
    
    # Deduplicate exact ticker-date combinations
    positions_df = positions_df.drop_duplicates(subset=['ticker', 'date'])
    
    # Aggregate to monthly
    monthly_df = aggregate_to_monthly(positions_df)
    print(f"✓ Aggregated to {len(monthly_df):,} monthly records (first non-missing per ticker-month)")
    
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
    final_columns = ['gvkey', 'ticker', 'time_avail_m', 'shortint', 'shortintadj']
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
    print("  - FINRA equity short interest positions available from Dec 2017 onward (bi-monthly reports)")
    print("  - Aggregation uses first non-missing per ticker-month (Compustat methodology)")
    print("  - Short interest is scaled to millions of shares (shortint and shortintadj match)")
    print("  - gvkey mapping requires AP_CompustatAnnual.parquet")


if __name__ == "__main__":
    main()
