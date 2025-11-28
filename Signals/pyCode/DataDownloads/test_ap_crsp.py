#!/usr/bin/env python3
"""Quick test for AP_CRSPDaily.py"""

import pandas as pd
from pathlib import Path

print("=" * 70)
print("Testing AP_CRSPDaily Outputs")
print("=" * 70)

output_dir = Path("../pyData/Intermediate")

# Check if files exist
files_to_check = [
    "AP_dailyCRSP.parquet",
    "AP_dailyCRSPprc.parquet",
    "AP_ticker_to_permno.csv"
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
    print("\n❌ Some files missing. Run AP_CRSPDaily.py first.")
    exit(1)

# Load and inspect data
print("\n📊 Data Inspection:")

# Full daily data
print("\n1. AP_dailyCRSP.parquet:")
df_full = pd.read_parquet(output_dir / "AP_dailyCRSP.parquet")
print(f"   Records: {len(df_full):,}")
print(f"   Columns: {list(df_full.columns)}")
print(f"   Date range: {df_full['time_d'].min()} to {df_full['time_d'].max()}")
print(f"   Unique tickers (permno): {df_full['permno'].nunique()}")
print(f"\n   Sample data:")
print(df_full.head(10))

# Price-only data
print("\n2. AP_dailyCRSPprc.parquet:")
df_prc = pd.read_parquet(output_dir / "AP_dailyCRSPprc.parquet")
print(f"   Records: {len(df_prc):,}")
print(f"   Columns: {list(df_prc.columns)}")

# Ticker mapping
print("\n3. AP_ticker_to_permno.csv:")
ticker_map = pd.read_csv(output_dir / "AP_ticker_to_permno.csv")
print(f"   Mappings: {len(ticker_map)}")
print(f"\n   Sample mappings:")
print(ticker_map.head(10))

# Data quality checks
print("\n🔍 Data Quality:")
print(f"  Returns completeness: {(1 - df_full['ret'].isna().mean()) * 100:.1f}%")
print(f"  Volume completeness: {(1 - df_full['vol'].isna().mean()) * 100:.1f}%")
print(f"  Price completeness: {(1 - df_full['prc'].isna().mean()) * 100:.1f}%")
print(f"  Shares completeness: {(1 - df_full['shrout'].isna().mean()) * 100:.1f}%")

print(f"\n  Mean daily return: {df_full['ret'].mean() * 100:.4f}%")
print(f"  Std daily return: {df_full['ret'].std() * 100:.4f}%")
print(f"  Mean volume: {df_full['vol'].mean():,.0f}")

# Check for extreme values (potential data errors)
print("\n⚠️  Extreme Value Check:")
extreme_returns = df_full[df_full['ret'].abs() > 0.5]
if len(extreme_returns) > 0:
    print(f"  Found {len(extreme_returns)} returns > ±50% (potential splits/errors)")
else:
    print(f"  ✓ No extreme returns found")

zero_prices = df_full[df_full['prc'] <= 0]
if len(zero_prices) > 0:
    print(f"  Found {len(zero_prices)} zero/negative prices (data errors)")
else:
    print(f"  ✓ No zero/negative prices")

print("\n" + "=" * 70)
print("✅ Test complete!")
print("=" * 70)

