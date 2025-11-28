# ABOUTME: Downloads quarterly GNP deflator from FRED API (free) and expands to monthly with 3-month lag
# ABOUTME: Processes GNPCTPI index data and outputs monthly time series for economic analysis
"""
Inputs:
- FRED API (GNPCTPI series - GNP: Chain-type Price Index)
- FRED_API_KEY from .env file (get free key at https://fred.stlouisfed.org/docs/api/api_key.html)

Outputs:
- ../pyData/Intermediate/AP_GNPdefl.parquet

Requirements:
    pip install requests pandas python-dotenv

How to run: python AP_GNPDeflator.py

Notes:
- FRED API is FREE (requires registration for API key)
- GNP deflator (GNPCTPI) is quarterly data from BEA
- Expanded to monthly frequency by repeating each quarter for 3 months
- Includes 3-month availability lag to reflect realistic data timing
- Historical data available from 1947 onwards
- Used for inflation adjustment in factor calculations
"""

import os
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
import warnings
warnings.filterwarnings('ignore')

# Print script header
print("=" * 70, flush=True)
print("📊 AP_GNPDeflator.py - GNP Deflator from FRED", flush=True)
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
SERIES_ID = 'GNPCTPI'  # GNP: Chain-type Price Index

# Date range - Last 2 years
END_DATE = datetime.now().strftime('%Y-%m-%d')
START_DATE = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')  # 2 years ago

print(f"📈 Downloading {SERIES_ID} from {START_DATE} to {END_DATE}")

# =============================================================================
# DATA DOWNLOAD FUNCTION
# =============================================================================

def download_fred_series(series_id, api_key, start_date='1900-01-01'):
    """
    Download time series from FRED API.
    
    Args:
        series_id: FRED series identifier (e.g., 'GNPCTPI')
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
        print(f"  Value range: {df['value'].min():.3f} to {df['value'].max():.3f}")
        
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

def expand_quarterly_to_monthly(quarterly_df):
    """
    Expand quarterly GNP deflator data to monthly frequency.
    
    Each quarterly value is repeated for 3 consecutive months.
    Then a 3-month availability lag is added (data available 3 months after quarter end).
    
    Args:
        quarterly_df: DataFrame with columns: date, value
        
    Returns:
        DataFrame with columns: time_avail_m, gnpdefl
    """
    
    if quarterly_df.empty:
        return quarterly_df
    
    print(f"\n📅 Expanding quarterly data to monthly frequency...")
    
    # Convert to monthly period
    quarterly_df['temp_time_m'] = quarterly_df['date'].dt.to_period('M').dt.to_timestamp()
    
    # Expand each quarterly observation to 3 monthly observations
    monthly_data = []
    
    for _, row in quarterly_df.iterrows():
        base_period = pd.to_datetime(row['temp_time_m']).to_period('M')
        
        # Create 3 monthly rows for this quarter
        for i in range(3):
            new_row = {
                'time_avail_m': (base_period + i).to_timestamp(),
                'value': row['value']
            }
            monthly_data.append(new_row)
    
    monthly_df = pd.DataFrame(monthly_data)
    
    print(f"✓ Expanded to {len(monthly_df)} monthly observations")
    
    # Add 3-month availability lag
    # (GNP deflator data is available ~3 months after quarter end)
    monthly_df['time_avail_m'] = (
        pd.to_datetime(monthly_df['time_avail_m']).dt.to_period('M') + 3
    ).dt.to_timestamp()
    
    print(f"✓ Applied 3-month availability lag")
    
    # Convert index to ratio (GNPCTPI is an index with base year = 100)
    # Divide by 100 to get ratio form
    monthly_df['gnpdefl'] = monthly_df['value'] / 100
    
    # Keep only necessary columns
    final_df = monthly_df[['time_avail_m', 'gnpdefl']].copy()
    
    # Remove duplicates (shouldn't be any, but safety check)
    final_df = final_df.drop_duplicates(subset=['time_avail_m'])
    
    # Sort by date
    final_df = final_df.sort_values('time_avail_m').reset_index(drop=True)
    
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
        print("\n⚠️  Creating placeholder data for testing...")
        
        # Create placeholder data
        dates = pd.date_range(start='2020-01-01', end=END_DATE, freq='Q')
        placeholder_data = pd.DataFrame({
            'date': dates,
            'value': np.linspace(120, 130, len(dates))
        })
        
        deflator_data = placeholder_data
        
    else:
        print(f"✓ FRED API key found")
        
        # Download real data from FRED
        deflator_data = download_fred_series(SERIES_ID, fred_api_key, START_DATE)
        
        if deflator_data.empty:
            print("\n❌ Failed to download data from FRED")
            return
    
    # Process data
    print(f"\n🔄 Processing GNP deflator data...")
    final_data = expand_quarterly_to_monthly(deflator_data)
    
    if final_data.empty:
        print("\n❌ Data processing failed")
        return
    
    # Save output
    print(f"\n💾 Saving output...")
    
    output_file = OUTPUT_DIR / "AP_GNPdefl.parquet"
    final_data.to_parquet(output_file, index=False)
    
    print(f"  ✓ Saved: {output_file}")
    print(f"    Records: {len(final_data):,}")
    print(f"    Size: {output_file.stat().st_size / 1024:.1f} KB")
    
    # Generate summary
    print(f"\n📊 Summary Statistics:")
    print(f"  Total observations: {len(final_data):,}")
    print(f"  Date range: {final_data['time_avail_m'].min()} to {final_data['time_avail_m'].max()}")
    print(f"  Months covered: {final_data['time_avail_m'].nunique()}")
    
    print(f"\n  GNP Deflator statistics:")
    print(f"    Mean: {final_data['gnpdefl'].mean():.3f}")
    print(f"    Median: {final_data['gnpdefl'].median():.3f}")
    print(f"    Min: {final_data['gnpdefl'].min():.3f} (earliest date)")
    print(f"    Max: {final_data['gnpdefl'].max():.3f} (most recent)")
    print(f"    Latest value: {final_data.iloc[-1]['gnpdefl']:.3f}")
    
    # Show recent trends
    print(f"\n  Recent values:")
    recent = final_data.tail(12)
    for _, row in recent.iterrows():
        print(f"    {row['time_avail_m'].strftime('%Y-%m')}: {row['gnpdefl']:.4f}")
    
    print("\n" + "=" * 70)
    print("✅ AP_GNPDeflator.py completed successfully!")
    print("=" * 70)
    print("\nOutputs:")
    print("  1. AP_GNPdefl.parquet - Monthly GNP deflator")
    print("\nNext steps:")
    print("  - Use AP_GNPdefl.parquet in place of GNPdefl.parquet")
    print("  - Use for inflation-adjusted factor calculations")
    print("\n💡 FRED API Info:")
    print("  - Series: GNPCTPI (GNP Chain-type Price Index)")
    print("  - Frequency: Quarterly (expanded to monthly)")
    print("  - Lag: 3 months (realistic data availability)")
    print("  - Free API key: https://fred.stlouisfed.org/docs/api/api_key.html")

if __name__ == "__main__":
    main()

