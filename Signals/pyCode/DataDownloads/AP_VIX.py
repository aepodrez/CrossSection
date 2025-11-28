# ABOUTME: Downloads VIX volatility index from FRED API (free) using both VXO and VIX series
# ABOUTME: Creates continuous VIX series and calculates daily VIX changes for market volatility analysis
"""
Inputs:
- FRED API series: VXOCLS (VXO - older volatility series, 1986-2003)
- FRED API series: VIXCLS (VIX - current volatility series, 1990-present)
- FRED_API_KEY from .env file (get free key at https://fred.stlouisfed.org/docs/api/api_key.html)

Outputs:
- ../pyData/Intermediate/AP_d_vix.parquet

Requirements:
    pip install requests pandas python-dotenv

How to run: python AP_VIX.py

Notes:
- FRED API is FREE (requires registration for API key)
- VXO (1986-2003): Original CBOE volatility index (S&P 100)
- VIX (1990-present): Current CBOE volatility index (S&P 500)
- Blends both series for continuous historical coverage
- Includes daily VIX changes (dVIX) for factor calculations
"""

import os
import pandas as pd
import requests
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import warnings
warnings.filterwarnings('ignore')

# Print script header
print("=" * 70, flush=True)
print("📊 AP_VIX.py - VIX Volatility Index from FRED", flush=True)
print("=" * 70, flush=True)

# Load environment variables
load_dotenv()

# Output directory
OUTPUT_DIR = Path("../pyData/Intermediate")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# =============================================================================
# CONFIGURATION
# =============================================================================

# FRED series IDs
SERIES_VXO = 'VXOCLS'  # CBOE VXO (older series, S&P 100)
SERIES_VIX = 'VIXCLS'  # CBOE VIX (current series, S&P 500)

# Cutoff date to switch from VXO to VIX
CUTOFF_DATE = '2021-09-23'  # After this date, use VIX only

print(f"📈 Downloading VIX data from FRED")
print(f"  - {SERIES_VXO}: Older series (1986-2003)")
print(f"  - {SERIES_VIX}: Current series (1990-present)")
print(f"  - Cutoff: {CUTOFF_DATE}")

# =============================================================================
# DATA DOWNLOAD FUNCTION
# =============================================================================

def download_fred_series(series_id, api_key):
    """
    Download time series from FRED API.
    
    Args:
        series_id: FRED series identifier (e.g., 'VIXCLS')
        api_key: FRED API key
        
    Returns:
        DataFrame with columns: date, [series_id]
    """
    
    print(f"\n📥 Downloading {series_id} from FRED...")
    
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        'series_id': series_id,
        'api_key': api_key,
        'file_type': 'json',
        'observation_start': '1900-01-01'
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
        
        # Rename value column to series_id
        df = df.rename(columns={'value': series_id})
        df = df[['date', series_id]]
        
        # Remove missing values
        df = df.dropna()
        
        print(f"✓ Downloaded {len(df)} observations")
        print(f"  Date range: {df['date'].min()} to {df['date'].max()}")
        
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

def blend_vix_series(vxo_df, vix_df, cutoff_date):
    """
    Blend VXO and VIX series to create continuous historical VIX data.
    
    Strategy:
    - Use VXO before cutoff date
    - Use VIX after cutoff date
    - Merge on date to handle overlapping periods
    
    Args:
        vxo_df: DataFrame with VXO data
        vix_df: DataFrame with VIX data
        cutoff_date: Date to switch from VXO to VIX
        
    Returns:
        DataFrame with columns: time_d, vix, dVIX
    """
    
    print(f"\n🔄 Blending VXO and VIX series...")
    
    # Merge both series
    vix_data = vxo_df.merge(vix_df, on='date', how='outer').sort_values('date')
    
    # Create blended VIX column
    # Use VXO first, then fill with VIX after cutoff
    vix_data['vix'] = vix_data[SERIES_VXO]
    
    cutoff = pd.Timestamp(cutoff_date)
    fill_mask = (vix_data['date'] >= cutoff) & vix_data[SERIES_VXO].isna()
    vix_data.loc[fill_mask, 'vix'] = vix_data.loc[fill_mask, SERIES_VIX]
    
    # Also fill any remaining NaNs with VIX (for overlapping period)
    vix_data['vix'] = vix_data['vix'].fillna(vix_data[SERIES_VIX])
    
    # Rename date column
    final_data = vix_data[['date', 'vix']].copy()
    final_data = final_data.rename(columns={'date': 'time_d'})
    
    # Remove any remaining NaNs
    final_data = final_data.dropna(subset=['vix'])
    
    # Calculate daily VIX changes
    final_data['dVIX'] = final_data['vix'].diff()
    
    # Convert to float32 for efficiency
    final_data['vix'] = final_data['vix'].astype('float32')
    final_data['dVIX'] = final_data['dVIX'].astype('float32')
    
    print(f"✓ Created blended VIX series with {len(final_data)} observations")
    print(f"  Date range: {final_data['time_d'].min()} to {final_data['time_d'].max()}")
    
    return final_data

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
    
    # Download both VIX series from FRED
    vxo_data = download_fred_series(SERIES_VXO, fred_api_key)
    vix_data = download_fred_series(SERIES_VIX, fred_api_key)
    
    if vxo_data.empty and vix_data.empty:
        print("\n❌ Failed to download any data from FRED")
        return
    
    if vxo_data.empty:
        print("\n⚠️  VXO data missing, using VIX only")
        final_data = vix_data.rename(columns={'date': 'time_d', SERIES_VIX: 'vix'})
        final_data['dVIX'] = final_data['vix'].diff()
        final_data['vix'] = final_data['vix'].astype('float32')
        final_data['dVIX'] = final_data['dVIX'].astype('float32')
    elif vix_data.empty:
        print("\n⚠️  VIX data missing, using VXO only")
        final_data = vxo_data.rename(columns={'date': 'time_d', SERIES_VXO: 'vix'})
        final_data['dVIX'] = final_data['vix'].diff()
        final_data['vix'] = final_data['vix'].astype('float32')
        final_data['dVIX'] = final_data['dVIX'].astype('float32')
    else:
        # Blend both series
        final_data = blend_vix_series(vxo_data, vix_data, CUTOFF_DATE)
    
    if final_data.empty:
        print("\n❌ Data processing failed")
        return
    
    # Save output
    print(f"\n💾 Saving output...")
    
    output_file = OUTPUT_DIR / "AP_d_vix.parquet"
    final_data.to_parquet(output_file, index=False)
    
    print(f"  ✓ Saved: {output_file}")
    print(f"    Records: {len(final_data):,}")
    print(f"    Size: {output_file.stat().st_size / 1024:.1f} KB")
    
    # Generate summary
    print(f"\n📊 Summary Statistics:")
    print(f"  Total observations: {len(final_data):,}")
    print(f"  Date range: {final_data['time_d'].min().date()} to {final_data['time_d'].max().date()}")
    print(f"  Days covered: {final_data['time_d'].nunique()}")
    
    print(f"\n  VIX statistics:")
    print(f"    Mean: {final_data['vix'].mean():.2f}")
    print(f"    Median: {final_data['vix'].median():.2f}")
    print(f"    Std: {final_data['vix'].std():.2f}")
    print(f"    Min: {final_data['vix'].min():.2f} (date: {final_data.loc[final_data['vix'].idxmin(), 'time_d'].date()})")
    print(f"    Max: {final_data['vix'].max():.2f} (date: {final_data.loc[final_data['vix'].idxmax(), 'time_d'].date()})")
    
    print(f"\n  Daily VIX change (dVIX) statistics:")
    dvix_data = final_data['dVIX'].dropna()
    if len(dvix_data) > 0:
        print(f"    Mean: {dvix_data.mean():.3f}")
        print(f"    Median: {dvix_data.median():.3f}")
        print(f"    Std: {dvix_data.std():.3f}")
        print(f"    Min: {dvix_data.min():.3f}")
        print(f"    Max: {dvix_data.max():.3f}")
    
    # Show recent values
    print(f"\n  Recent daily VIX values:")
    recent = final_data.tail(10)
    for _, row in recent.iterrows():
        dvix_str = f"{row['dVIX']:+.2f}" if pd.notna(row['dVIX']) else "N/A"
        print(f"    {row['time_d'].date()}: VIX={row['vix']:.2f}, dVIX={dvix_str}")
    
    print("\n" + "=" * 70)
    print("✅ AP_VIX.py completed successfully!")
    print("=" * 70)
    print("\nOutputs:")
    print("  1. AP_d_vix.parquet - Daily VIX and changes")
    print("\nColumns:")
    print("  - time_d: Date")
    print("  - vix: VIX level")
    print("  - dVIX: Daily change in VIX")
    print("\nNext steps:")
    print("  - Use AP_d_vix.parquet in place of d_vix.parquet")
    print("  - Use for volatility-based factor calculations")
    print("\n💡 FRED API Info:")
    print("  - Series: VXOCLS (1986-2003), VIXCLS (1990-present)")
    print("  - Blended for continuous historical coverage")
    print("  - Free API key: https://fred.stlouisfed.org/docs/api/api_key.html")

if __name__ == "__main__":
    main()

