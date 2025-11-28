#!/usr/bin/env python3
"""Quick test for AP_GNPDeflator.py"""

import pandas as pd
import numpy as np
from pathlib import Path

print("=" * 70)
print("Testing AP_GNPDeflator Outputs")
print("=" * 70)

output_dir = Path("../pyData/Intermediate")

# Check if file exists
filepath = output_dir / "AP_GNPdefl.parquet"

print("\n📁 File Check:")
if filepath.exists():
    size_kb = filepath.stat().st_size / 1024
    print(f"  ✓ AP_GNPdefl.parquet ({size_kb:.1f} KB)")
else:
    print(f"  ✗ AP_GNPdefl.parquet (not found)")
    print("\n❌ File missing. Run AP_GNPDeflator.py first.")
    exit(1)

# Load and inspect data
print("\n📊 Data Inspection:")
df = pd.read_parquet(filepath)

print(f"   Records: {len(df):,}")
print(f"   Columns: {list(df.columns)}")
print(f"   Date range: {df['time_avail_m'].min()} to {df['time_avail_m'].max()}")

print(f"\n   First 10 observations:")
print(df.head(10))

print(f"\n   Last 10 observations:")
print(df.tail(10))

# Statistics
print("\n📈 GNP Deflator Statistics:")
print(f"  Mean: {df['gnpdefl'].mean():.4f}")
print(f"  Median: {df['gnpdefl'].median():.4f}")
print(f"  Std: {df['gnpdefl'].std():.4f}")
print(f"  Min: {df['gnpdefl'].min():.4f} (date: {df.loc[df['gnpdefl'].idxmin(), 'time_avail_m'].strftime('%Y-%m')})")
print(f"  Max: {df['gnpdefl'].max():.4f} (date: {df.loc[df['gnpdefl'].idxmax(), 'time_avail_m'].strftime('%Y-%m')})")

# Check for inflation trends
print("\n📊 Inflation Analysis:")
df_sorted = df.sort_values('time_avail_m')

# Calculate year-over-year inflation
df_sorted['gnpdefl_lag12'] = df_sorted['gnpdefl'].shift(12)
df_sorted['inflation_yoy'] = ((df_sorted['gnpdefl'] / df_sorted['gnpdefl_lag12']) - 1) * 100

recent_inflation = df_sorted.tail(12)
print(f"\n  Recent 12-month inflation rates:")
for _, row in recent_inflation.iterrows():
    if pd.notna(row['inflation_yoy']):
        print(f"    {row['time_avail_m'].strftime('%Y-%m')}: {row['inflation_yoy']:.2f}%")

# Data quality checks
print("\n🔍 Data Quality:")
print(f"  Completeness: {(1 - df['gnpdefl'].isna().mean()) * 100:.1f}%")
print(f"  Missing values: {df['gnpdefl'].isna().sum()}")

# Check for gaps in time series
df_sorted = df.sort_values('time_avail_m').reset_index(drop=True)
df_sorted['month_diff'] = df_sorted['time_avail_m'].diff().dt.days
gaps = df_sorted[df_sorted['month_diff'] > 35]  # More than ~1 month gap

if len(gaps) > 0:
    print(f"  ⚠️  Found {len(gaps)} gaps in time series")
    print(f"      Sample gaps:")
    for _, row in gaps.head(5).iterrows():
        print(f"        Gap at {row['time_avail_m']}: {row['month_diff']} days")
else:
    print(f"  ✓ No gaps in monthly time series")

# Check for extreme values
print("\n⚠️  Extreme Value Check:")

# Check for negative values (should never happen)
negative = df[df['gnpdefl'] <= 0]
if len(negative) > 0:
    print(f"  ❌ Found {len(negative)} negative/zero values (data errors)")
else:
    print(f"  ✓ No negative values")

# Check for unrealistic jumps (>10% month-over-month)
df_sorted['mom_change'] = df_sorted['gnpdefl'].pct_change()
large_jumps = df_sorted[df_sorted['mom_change'].abs() > 0.10]

if len(large_jumps) > 0:
    print(f"  ⚠️  Found {len(large_jumps)} observations with >10% month-over-month change")
    print(f"      (May be legitimate or data errors)")
else:
    print(f"  ✓ No extreme month-over-month changes")

# Historical analysis
print("\n📅 Historical Coverage:")
yearly_avg = df.groupby(df['time_avail_m'].dt.year)['gnpdefl'].mean()
print(f"  Years covered: {yearly_avg.index.min()} to {yearly_avg.index.max()}")
print(f"  Total years: {len(yearly_avg)}")

print(f"\n  Deflator by decade (average):")
decades = df.copy()
decades['decade'] = (df['time_avail_m'].dt.year // 10) * 10
decade_avg = decades.groupby('decade')['gnpdefl'].mean()

for decade, avg_val in decade_avg.items():
    print(f"    {decade}s: {avg_val:.4f}")

# Compare with original format
print("\n🔄 CRSP Compatibility Check:")
required_cols = ['time_avail_m', 'gnpdefl']
missing_cols = [col for col in required_cols if col not in df.columns]

if missing_cols:
    print(f"  ⚠️  Missing columns: {missing_cols}")
else:
    print(f"  ✓ All required columns present")

# Check data types
print(f"\n  Data types:")
print(f"    time_avail_m: {df['time_avail_m'].dtype}")
print(f"    gnpdefl: {df['gnpdefl'].dtype}")

print("\n" + "=" * 70)
print("✅ Test complete!")
print("=" * 70)
print("\n💡 Notes:")
print("  - GNP deflator used for inflation adjustment")
print("  - Base year = 100 (converted to ratio by dividing by 100)")
print("  - Quarterly data expanded to monthly")
print("  - 3-month lag reflects realistic data availability")

