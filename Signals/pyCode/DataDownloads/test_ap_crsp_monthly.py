#!/usr/bin/env python3
"""Quick test for AP_CRSPMonthly.py"""

import pandas as pd
import numpy as np
from pathlib import Path

print("=" * 70)
print("Testing AP_CRSPMonthly Outputs")
print("=" * 70)

output_dir = Path("../pyData/Intermediate")

# Check if files exist
files_to_check = [
    "AP_monthlyCRSP.parquet",
    "AP_ticker_to_permno_monthly.csv"
]

print("\n📁 File Check:")
all_exist = True
for file in files_to_check:
    filepath = output_dir / file
    if filepath.exists():
        size_mb = filepath.stat().st_size / 1024 / 1024
        print(f"  ✓ {file} ({size_mb:.1f} MB)")
    else:
        print(f"  ✗ {file} (not found)")
        all_exist = False

if not all_exist:
    print("\n❌ Some files missing. Run AP_CRSPMonthly.py first.")
    exit(1)

# Load and inspect data
print("\n📊 Data Inspection:")

# Monthly data
print("\n1. AP_monthlyCRSP.parquet:")
df = pd.read_parquet(output_dir / "AP_monthlyCRSP.parquet")
print(f"   Records: {len(df):,}")
print(f"   Columns: {list(df.columns)}")
print(f"   Date range: {df['time_avail_m'].min()} to {df['time_avail_m'].max()}")
print(f"   Unique tickers (permno): {df['permno'].nunique()}")
print(f"   Total months: {df['time_avail_m'].nunique()}")
print(f"\n   Sample data:")
print(df.head(10))

# Ticker mapping
print("\n2. AP_ticker_to_permno_monthly.csv:")
ticker_map = pd.read_csv(output_dir / "AP_ticker_to_permno_monthly.csv")
print(f"   Mappings: {len(ticker_map)}")
print(f"\n   Sample mappings:")
print(ticker_map.head(10))

# Data quality checks
print("\n🔍 Data Quality:")
print(f"  Returns completeness: {(1 - df['ret'].isna().mean()) * 100:.1f}%")
print(f"  Volume completeness: {(1 - df['vol'].isna().mean()) * 100:.1f}%")
print(f"  Price completeness: {(1 - df['prc'].isna().mean()) * 100:.1f}%")
print(f"  Shares completeness: {(1 - df['shrout'].isna().mean()) * 100:.1f}%")
print(f"  Market equity completeness: {(1 - df['mve_c'].isna().mean()) * 100:.1f}%")

print(f"\n  Mean monthly return: {df['ret'].mean() * 100:.4f}%")
print(f"  Std monthly return: {df['ret'].std() * 100:.4f}%")
print(f"  Mean volume (100s): {df['vol'].mean():,.0f}")
print(f"  Mean market cap (M): ${df['mve_c'].mean():,.0f}")

# Exchange distribution
print("\n  Exchange distribution:")
exchange_names = {1: 'NYSE', 2: 'AMEX', 3: 'NASDAQ', -1: 'Other'}
for exchcd, count in df['exchcd'].value_counts().sort_index().items():
    pct = count / len(df) * 100
    print(f"    {exchange_names.get(exchcd, 'Unknown')} ({exchcd}): {count:,} ({pct:.1f}%)")

# Check for extreme values
print("\n⚠️  Extreme Value Check:")
extreme_returns = df[df['ret'].abs() > 0.5]
if len(extreme_returns) > 0:
    print(f"  Found {len(extreme_returns)} monthly returns > ±50% (potential errors)")
    print(f"    Sample:")
    print(extreme_returns[['permno', 'ticker', 'time_avail_m', 'ret']].head())
else:
    print(f"  ✓ No extreme returns found")

zero_prices = df[df['prc'] <= 0]
if len(zero_prices) > 0:
    print(f"  Found {len(zero_prices)} zero/negative prices (data errors)")
else:
    print(f"  ✓ No zero/negative prices")

# Check market cap distribution
print("\n💰 Market Cap Distribution:")
df_mkt = df.dropna(subset=['mve_c'])
print(f"  Min: ${df_mkt['mve_c'].min():,.0f}M")
print(f"  25th percentile: ${df_mkt['mve_c'].quantile(0.25):,.0f}M")
print(f"  Median: ${df_mkt['mve_c'].median():,.0f}M")
print(f"  75th percentile: ${df_mkt['mve_c'].quantile(0.75):,.0f}M")
print(f"  Max: ${df_mkt['mve_c'].max():,.0f}M")

# Compare to SignalMasterTable format
print("\n🔄 CRSP Compatibility Check:")
crsp_required_cols = ['permno', 'permco', 'time_avail_m', 'ret', 'retx', 'shrout', 
                       'prc', 'exchcd', 'shrcd', 'mve_c']
missing_cols = [col for col in crsp_required_cols if col not in df.columns]
if missing_cols:
    print(f"  ⚠️  Missing columns: {missing_cols}")
else:
    print(f"  ✓ All required CRSP columns present")

print("\n" + "=" * 70)
print("✅ Test complete!")
print("=" * 70)

