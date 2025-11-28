#!/usr/bin/env python3
"""Quick test for AP_InstitutionalHoldings13F.py"""

import pandas as pd
import numpy as np
from pathlib import Path

print("=" * 70)
print("Testing AP_InstitutionalHoldings13F Outputs")
print("=" * 70)

output_dir = Path("../pyData/Intermediate")

# Check if file exists
filepath = output_dir / "AP_TR_13F.parquet"
cache_file = output_dir / ".cache" / "AP_13F_holdings_cache.parquet"

print("\n📁 File Check:")
if filepath.exists():
    size_mb = filepath.stat().st_size / 1024 / 1024
    print(f"  ✓ AP_TR_13F.parquet ({size_mb:.1f} MB)")
else:
    print(f"  ✗ AP_TR_13F.parquet (not found)")
    print("\n❌ File missing. Run AP_InstitutionalHoldings13F.py first.")
    exit(1)

if cache_file.exists():
    size_mb = cache_file.stat().st_size / 1024 / 1024
    print(f"  ✓ Cache file ({size_mb:.1f} MB)")

# Load and inspect data
print("\n📊 Data Inspection:")
df = pd.read_parquet(filepath)

print(f"   Records: {len(df):,}")
print(f"   Columns: {list(df.columns)}")
print(f"   Date range: {df['time_avail_m'].min()} to {df['time_avail_m'].max()}")
print(f"   Unique stocks (permno): {df['permno'].nunique()}")
print(f"   Months covered: {df['time_avail_m'].nunique()}")

print(f"\n   Sample data:")
print(df.head(15))

# Data completeness
print("\n🔍 Data Completeness:")
for col in ['numinstown', 'dbreadth', 'instown_perc']:
    if col in df.columns:
        count = df[col].notna().sum()
        pct = count / len(df) * 100
        print(f"  {col}: {count:,} / {len(df):,} ({pct:.1f}%)")
    else:
        print(f"  {col}: Column not found")

# Institutional ownership statistics
print("\n🏦 Institutional Ownership Statistics:")

if 'numinstown' in df.columns:
    df_inst = df[df['numinstown'].notna()]
    if len(df_inst) > 0:
        print(f"  Number of institutional owners:")
        print(f"    Mean: {df_inst['numinstown'].mean():.1f}")
        print(f"    Median: {df_inst['numinstown'].median():.1f}")
        print(f"    Min: {df_inst['numinstown'].min():.0f}")
        print(f"    Max: {df_inst['numinstown'].max():.0f}")

if 'instown_perc' in df.columns:
    df_pct = df[df['instown_perc'].notna()]
    if len(df_pct) > 0:
        print(f"\n  Institutional ownership percentage:")
        print(f"    Mean: {df_pct['instown_perc'].mean():.1f}%")
        print(f"    Median: {df_pct['instown_perc'].median():.1f}%")
        print(f"    Min: {df_pct['instown_perc'].min():.1f}%")
        print(f"    Max: {df_pct['instown_perc'].max():.1f}%")

if 'dbreadth' in df.columns:
    df_dbr = df[df['dbreadth'].notna()]
    if len(df_dbr) > 0:
        print(f"\n  Change in breadth (dbreadth):")
        print(f"    Mean: {df_dbr['dbreadth'].mean():.2f}")
        print(f"    Median: {df_dbr['dbreadth'].median():.2f}")
        print(f"    Std: {df_dbr['dbreadth'].std():.2f}")

# Temporal distribution
print("\n📅 Temporal Distribution:")
if 'time_avail_m' in df.columns:
    yearly_counts = df.groupby(df['time_avail_m'].dt.year).size()
    print(f"  Years covered: {yearly_counts.index.min()} to {yearly_counts.index.max()}")
    print(f"  Avg observations per year: {yearly_counts.mean():.0f}")
    print(f"\n  Recent years:")
    for year in sorted(yearly_counts.index)[-5:]:
        print(f"    {year}: {yearly_counts[year]:,} observations")

# Check for extreme values
print("\n⚠️  Data Quality Check:")

if 'numinstown' in df.columns:
    zero_inst = df[(df['numinstown'] == 0) | (df['numinstown'].isna())]
    if len(zero_inst) > 0:
        print(f"  Found {len(zero_inst)} observations with zero/null institutional owners")
    else:
        print(f"  ✓ All observations have institutional ownership data")

if 'instown_perc' in df.columns:
    over_100 = df[df['instown_perc'] > 100]
    if len(over_100) > 0:
        print(f"  Found {len(over_100)} observations with >100% ownership (data errors)")
    else:
        print(f"  ✓ No ownership percentages > 100%")

# Top institutional ownership
print("\n🏆 Top Stocks by Institutional Ownership:")
if 'numinstown' in df.columns and 'time_avail_m' in df.columns:
    # Get most recent month
    latest_month = df['time_avail_m'].max()
    latest_df = df[df['time_avail_m'] == latest_month].sort_values('numinstown', ascending=False)
    
    print(f"  As of {latest_month.strftime('%Y-%m')}:")
    print(f"\n  Top 10 by number of institutional owners:")
    for i, (_, row) in enumerate(latest_df.head(10).iterrows(), 1):
        print(f"    {i}. permno {row['permno']}: {row['numinstown']:.0f} institutions")

# CRSP compatibility check
print("\n🔄 CRSP Compatibility Check:")
required_cols = ['permno', 'time_avail_m', 'numinstown']
missing_cols = [col for col in required_cols if col not in df.columns]
if missing_cols:
    print(f"  ⚠️  Missing columns: {missing_cols}")
else:
    print(f"  ✓ All required columns present")

print("\n" + "=" * 70)
print("✅ Test complete!")
print("=" * 70)
print("\n💡 Notes:")
print("  - 13F data is quarterly, forward-filled to monthly")
print("  - First run may take 30-60 minutes (fetches all 13F filings)")
print("  - Subsequent runs use cached data")
print("  - Delete cache to refresh: rm ../pyData/Intermediate/.cache/AP_13F_holdings_cache.parquet")

