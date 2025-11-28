#!/usr/bin/env python3
"""Test enhanced XBRL mappings"""
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from AP_CompustatAnnual import get_company_financials_from_10k

print("=" * 70)
print("Testing Enhanced XBRL Mappings")
print("=" * 70)

# Test with 2 companies
tickers = ['AAPL', 'MSFT']
all_data = []

for ticker in tickers:
    df = get_company_financials_from_10k(ticker, years=2, debug=False)
    if not df.empty:
        all_data.append(df)
        non_null = df.notna().sum(axis=1).iloc[0]
        print(f"\n✓ {ticker}: {non_null} fields extracted (out of ~108 Compustat fields)")

if all_data:
    combined = pd.concat(all_data, ignore_index=True)
    print(f"\n{'='*70}")
    print(f"Total records: {len(combined)}")
    
    # Count fields with data
    fields_with_any_data = (combined.notna().sum() > 0).sum()
    print(f"Fields with any data: {fields_with_any_data}")
    
    # Show critical fields
    print(f"\n{'='*70}")
    print("Critical Field Check:")
    critical = ['at', 'sale', 'ni', 'che', 'ceq', 'dltt', 'oancf', 
                'capx', 're', 'dp', 'csho', 'epspi', 'emp']
    for field in critical:
        if field in combined.columns:
            count = combined[field].notna().sum()
            pct = count / len(combined) * 100
            status = '✓' if pct > 0 else '✗'
            print(f"  {status} {field:10s}: {count}/{len(combined)} ({pct:5.1f}%)")
        else:
            print(f"  ✗ {field:10s}: NOT EXTRACTED")
    
    print(f"\n✅ Enhancement test complete!")
else:
    print("\n❌ No data extracted")

