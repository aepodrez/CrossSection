#!/usr/bin/env python3
"""Quick test for AP_CRSPDistributions.py"""

import pandas as pd
import numpy as np
from pathlib import Path

print("=" * 70)
print("Testing AP_CRSPDistributions Outputs")
print("=" * 70)

output_dir = Path("../pyData/Intermediate")

# Check if file exists
filepath = output_dir / "AP_CRSPdistributions.parquet"

print("\n📁 File Check:")
if filepath.exists():
    size_mb = filepath.stat().st_size / 1024 / 1024
    print(f"  ✓ AP_CRSPdistributions.parquet ({size_mb:.1f} MB)")
else:
    print(f"  ✗ AP_CRSPdistributions.parquet (not found)")
    print("\n❌ File missing. Run AP_CRSPDistributions.py first.")
    exit(1)

# Load and inspect data
print("\n📊 Data Inspection:")
df = pd.read_parquet(filepath)

print(f"   Records: {len(df):,}")
print(f"   Columns: {list(df.columns)}")
print(f"   Date range: {df['exdt'].min()} to {df['exdt'].max()}")
print(f"   Unique stocks (permno): {df['permno'].nunique()}")

print(f"\n   Sample data:")
print(df.head(15))

# Distribution code breakdown
print("\n📋 Distribution Code Breakdown:")
distcd_names = {
    1232: 'Cash Dividend',
    1262: 'Special Dividend',
    5523: 'Stock Split',
    5533: 'Reverse Split',
}

for distcd, count in df['distcd'].value_counts().sort_index().items():
    name = distcd_names.get(distcd, f'Other')
    pct = count / len(df) * 100
    print(f"  {name} ({distcd}): {count:,} ({pct:.1f}%)")

# Dividend statistics
print("\n💵 Dividend Statistics:")
cash_divs = df[df['distcd'] == 1232]
if len(cash_divs) > 0:
    print(f"  Cash dividends: {len(cash_divs):,}")
    print(f"  Mean amount: ${cash_divs['divamt'].mean():.4f}")
    print(f"  Median amount: ${cash_divs['divamt'].median():.4f}")
    print(f"  Min amount: ${cash_divs['divamt'].min():.4f}")
    print(f"  Max amount: ${cash_divs['divamt'].max():.4f}")
    
    # Top dividend payers by count
    print(f"\n  Top 10 dividend payers (by frequency):")
    top_payers = cash_divs['permno'].value_counts().head(10)
    for permno, count in top_payers.items():
        print(f"    permno {permno}: {count} dividends")
else:
    print(f"  No cash dividends found")

# Split statistics
print("\n📊 Stock Split Statistics:")
splits = df[df['distcd'].isin([5523, 5533])]
if len(splits) > 0:
    print(f"  Total splits: {len(splits):,}")
    print(f"  Mean split ratio: {splits['facshr'].mean():.4f}")
    print(f"  Median split ratio: {splits['facshr'].median():.4f}")
    print(f"\n  Sample splits:")
    print(splits[['permno', 'exdt', 'facshr', 'distcd']].head(10))
else:
    print(f"  No splits found")

# Data quality checks
print("\n🔍 Data Quality:")
print(f"  Dividend amount completeness: {(1 - df['divamt'].isna().mean()) * 100:.1f}%")
print(f"  Ex-date completeness: {(1 - df['exdt'].isna().mean()) * 100:.1f}%")
print(f"  Record date completeness: {(1 - df['rcrddt'].isna().mean()) * 100:.1f}%")
print(f"  Payment date completeness: {(1 - df['paydt'].isna().mean()) * 100:.1f}%")
print(f"  Distribution code completeness: {(1 - df['distcd'].isna().mean()) * 100:.1f}%")

# Check for data errors
print("\n⚠️  Data Error Check:")
negative_divs = df[(df['divamt'] < 0) & (df['distcd'] == 1232)]
if len(negative_divs) > 0:
    print(f"  Found {len(negative_divs)} negative dividends (possible errors)")
else:
    print(f"  ✓ No negative dividends")

future_dates = df[df['exdt'] > pd.Timestamp.now()]
if len(future_dates) > 0:
    print(f"  Found {len(future_dates)} future ex-dates (possible errors)")
else:
    print(f"  ✓ No future dates")

# Temporal distribution
print("\n📅 Temporal Distribution:")
yearly_counts = df.groupby(df['exdt'].dt.year).size()
print(f"  Years covered: {yearly_counts.index.min()} to {yearly_counts.index.max()}")
print(f"  Avg distributions per year: {yearly_counts.mean():.0f}")
print(f"\n  Recent years:")
for year in sorted(yearly_counts.index)[-5:]:
    print(f"    {year}: {yearly_counts[year]:,} distributions")

# Compare with CRSP format
print("\n🔄 CRSP Compatibility Check:")
crsp_required_cols = ['permno', 'divamt', 'distcd', 'facshr', 'rcrddt', 'exdt', 'paydt']
missing_cols = [col for col in crsp_required_cols if col not in df.columns]
if missing_cols:
    print(f"  ⚠️  Missing columns: {missing_cols}")
else:
    print(f"  ✓ All required CRSP columns present")

# Check distribution code digits
print(f"\n  Distribution code digits:")
for digit_col in ['cd1', 'cd2', 'cd3', 'cd4']:
    if digit_col in df.columns:
        print(f"    {digit_col}: {df[digit_col].notna().sum()} / {len(df)} ({df[digit_col].notna().mean()*100:.1f}%)")
    else:
        print(f"    {digit_col}: Missing")

print("\n" + "=" * 70)
print("✅ Test complete!")
print("=" * 70)
print("\n💡 Notes:")
print("  - Distribution codes are approximated (1232=cash, 5523=split)")
print("  - Record/payment dates are estimated (±1-14 days from ex-date)")
print("  - Special distributions may not be fully captured")

