# ABOUTME: Downloads 3-month T-bill rate from FRED API (free) and aggregates to quarterly averages
# ABOUTME: Creates year-quarter-level dataset with TbillRate3M variable for downstream analysis
"""
Inputs:
- FRED API TB3MS series (monthly 3-month Treasury bill rates)
- FRED_API_KEY from .env file (get free key at https://fred.stlouisfed.org/docs/api/api_key.html)

Outputs:
- ../pyData/Intermediate/AP_TBill3M.parquet

Requirements:
    pip install requests pandas python-dotenv

How to run: python AP_TreasuryBill3M.py

Notes:
- FRED API is FREE (requires registration for API key)
- TB3MS = 3-Month Treasury Bill Secondary Market Rate
- Monthly data aggregated to quarterly averages
- Historical data available from 1934 onwards
- Used as risk-free rate in factor calculations
"""

import os
import pandas as pd
import numpy as np
import requests
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import warnings
warnings.filterwarnings('ignore')

# Print script header
print("=" * 70, flush=True)
print("💰 AP_TreasuryBill3M.py - 3-Month Treasury Bill Rate from FRED", flush=True)
print("=" * 70, flush=True)

# Load environment variables
load_dotenv()

# Output directory
OUTPUT_DIR = Path("../pyData/Intermediate")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# =============================================================================
# CONFIGURATION
# =============================================================================

# FRED series ID
SERIES_ID = 'TB3MS'  # 3-Month Treasury Bill Secondary Market Rate

# Date range
START_DATE = '1934-01-01'  # TB3MS available from 1934
END_DATE = datetime.now().strftime('%Y-%m-%d')

print(f"📈 Downloading {SERIES_ID} from {START_DATE} to {END_DATE}")

# =============================================================================
# DATA DOWNLOAD FUNCTION
# =============================================================================

def download_fred_series(series_id, api_key, start_date='1900-01-01'):
    """
    Download time series from FRED API.
    
    Args:
        series_id: FRED series identifier (e.g., 'TB3MS')
        api_key: FRED API key
        start_date: Start date for data download
        
    Returns:
        DataFrame with columns: date, value
    """
    
    print(f"\n📥 Downloading {series_id} from FRED...")
    
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        'series_id': series_id,
        'api_key': api_key,
        'file_type': 'json',
        'observation_start': start_date
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if 'observations' not in data:
            print(f"❌ No observations found in FRED response")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(data['observations'])
        
        if df.empty:
            print(f"❌ No data returned from FRED")
            return pd.DataFrame()
        
        # Process dates and values
        df['date'] = pd.to_datetime(df['date'])
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        
        # Remove missing values
        df = df[['date', 'value']].dropna()
        
        print(f"✓ Downloaded {len(df)} observations")
        print(f"  Date range: {df['date'].min()} to {df['date'].max()}")
        print(f"  Value range: {df['value'].min():.3f}% to {df['value'].max():.3f}%")
        
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Error downloading from FRED: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Error processing FRED data: {e}")
        return pd.DataFrame()

# =============================================================================
# DATA PROCESSING FUNCTIONS
# =============================================================================

def aggregate_to_quarterly(monthly_df):
    """
    Aggregate monthly T-bill rate data to quarterly averages.
    
    Args:
        monthly_df: DataFrame with columns: date, value
        
    Returns:
        DataFrame with columns: year, qtr, TbillRate3M
    """
    
    if monthly_df.empty:
        return monthly_df
    
    print(f"\n📅 Aggregating monthly data to quarterly averages...")
    
    # Set date as index for resampling
    monthly_df = monthly_df.set_index('date')
    
    # Resample to quarterly, taking average of monthly rates
    quarterly_df = monthly_df.resample('QE').mean()
    quarterly_df = quarterly_df.dropna().reset_index()
    
    print(f"✓ Aggregated to {len(quarterly_df)} quarterly observations")
    
    # Convert T-bill rate from percentage to decimal
    quarterly_df['TbillRate3M'] = quarterly_df['value'] / 100.0
    
    # Extract year and quarter
    quarterly_df['year'] = quarterly_df['date'].dt.year
    quarterly_df['qtr'] = quarterly_df['date'].dt.quarter
    
    # Keep only required columns
    final_df = quarterly_df[['year', 'qtr', 'TbillRate3M']].copy()
    
    # Sort by year and quarter
    final_df = final_df.sort_values(['year', 'qtr']).reset_index(drop=True)
    
    return final_df

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main execution function"""
    
    # Check for FRED API key
    fred_api_key = os.getenv("FRED_API_KEY")
    
    if not fred_api_key:
        print("\n❌ FRED_API_KEY not found in environment variables")
        print("\n📋 To get a FREE FRED API key:")
        print("   1. Go to: https://fred.stlouisfed.org/docs/api/api_key.html")
        print("   2. Register for free account")
        print("   3. Generate API key")
        print("   4. Add to .env file: FRED_API_KEY=your_key_here")
        return
    
    print(f"✓ FRED API key found")
    
    # Download data from FRED
    monthly_data = download_fred_series(SERIES_ID, fred_api_key, START_DATE)
    
    if monthly_data.empty:
        print("\n❌ Failed to download data from FRED")
        return
    
    # Process data
    print(f"\n🔄 Processing T-bill rate data...")
    final_data = aggregate_to_quarterly(monthly_data)
    
    if final_data.empty:
        print("\n❌ Data processing failed")
        return
    
    # Save output
    print(f"\n💾 Saving output...")
    
    output_file = OUTPUT_DIR / "AP_TBill3M.parquet"
    final_data.to_parquet(output_file, index=False)
    
    print(f"  ✓ Saved: {output_file}")
    print(f"    Records: {len(final_data):,}")
    print(f"    Size: {output_file.stat().st_size / 1024:.1f} KB")
    
    # Generate summary
    print(f"\n📊 Summary Statistics:")
    print(f"  Total observations: {len(final_data):,}")
    
    # Date range
    date_range_start = f"{final_data['year'].min()}Q{final_data[final_data['year'] == final_data['year'].min()]['qtr'].min()}"
    date_range_end = f"{final_data['year'].max()}Q{final_data[final_data['year'] == final_data['year'].max()]['qtr'].max()}"
    print(f"  Date range: {date_range_start} to {date_range_end}")
    print(f"  Years covered: {final_data['year'].nunique()}")
    print(f"  Quarters covered: {len(final_data)}")
    
    print(f"\n  T-bill rate statistics:")
    print(f"    Mean: {final_data['TbillRate3M'].mean():.6f} ({final_data['TbillRate3M'].mean()*100:.4f}%)")
    print(f"    Median: {final_data['TbillRate3M'].median():.6f} ({final_data['TbillRate3M'].median()*100:.4f}%)")
    print(f"    Std: {final_data['TbillRate3M'].std():.6f} ({final_data['TbillRate3M'].std()*100:.4f}%)")
    print(f"    Min: {final_data['TbillRate3M'].min():.6f} ({final_data['TbillRate3M'].min()*100:.4f}%)")
    print(f"    Max: {final_data['TbillRate3M'].max():.6f} ({final_data['TbillRate3M'].max()*100:.4f}%)")
    
    # Show recent values
    print(f"\n  Recent quarterly values:")
    recent = final_data.tail(8)
    for _, row in recent.iterrows():
        print(f"    {int(row['year'])}Q{int(row['qtr'])}: {row['TbillRate3M']:.6f} ({row['TbillRate3M']*100:.4f}%)")
    
    # Sample data
    print(f"\n  Sample data:")
    print(final_data.head(10).to_string(index=False))
    
    print("\n" + "=" * 70)
    print("✅ AP_TreasuryBill3M.py completed successfully!")
    print("=" * 70)
    print("\nOutputs:")
    print("  1. AP_TBill3M.parquet - Quarterly 3-month T-bill rates")
    print("\nNext steps:")
    print("  - Use AP_TBill3M.parquet in place of TBill3M.parquet")
    print("  - Use as risk-free rate in factor calculations")
    print("\n💡 FRED API Info:")
    print("  - Series: TB3MS (3-Month Treasury Bill Secondary Market Rate)")
    print("  - Frequency: Monthly (aggregated to quarterly)")
    print("  - Free API key: https://fred.stlouisfed.org/docs/api/api_key.html")

if __name__ == "__main__":
    main()

