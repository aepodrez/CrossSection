# ABOUTME: Creates CRSP-IBES linking table from existing AP files (no WRDS required)
# ABOUTME: Produces a monthly permno/ticker mapping for downstream predictors
"""
Inputs:
- ../pyData/Intermediate/AP_monthlyCRSP.parquet (columns: permno, ticker, time_avail_m)
- ../pyData/Intermediate/AP_IBES_EPS_Adj.parquet (columns: tickerIBES, time_avail_m)

Outputs:
- ../pyData/Intermediate/AP_IBESCRSPLinkingTable.parquet

Requirements:
    pip install pandas

How to run: python AP_IBESCRSPLink.py

Notes:
- Does NOT require Eikon (uses existing AP files)
- Links IBES tickers to CRSP permnos via ticker symbols
- Creates simple 1:1 mapping based on ticker matching
- Score field set to 1 (perfect match) for all matches
- More sophisticated matching could be added later
"""

import os
import pandas as pd
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

print("=" * 70, flush=True)
print("📊 AP_IBESCRSPLink.py - IBES-CRSP Linking Table", flush=True)
print("=" * 70, flush=True)

OUTPUT_DIR = Path("../pyData/Intermediate")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# =============================================================================
# LOAD DATA
# =============================================================================

def load_crsp_monthly():
    """Load CRSP monthly data"""
    crsp_file = OUTPUT_DIR / "AP_monthlyCRSP.parquet"
    
    if not crsp_file.exists():
        print(f"❌ {crsp_file} not found")
        print("   Please run AP_CRSPMonthly.py first")
        return pd.DataFrame()
    
    print(f"\n📥 Loading CRSP monthly data...")
    df = pd.read_parquet(crsp_file, columns=['permno', 'ticker', 'time_avail_m'])
    print(f"  ✓ Loaded {len(df):,} records")
    print(f"    Unique permnos: {df['permno'].nunique()}")
    print(f"    Unique tickers: {df['ticker'].nunique()}")
    
    return df

def load_ibes_data():
    """Load IBES data"""
    ibes_file = OUTPUT_DIR / "AP_IBES_EPS_Adj.parquet"
    
    if not ibes_file.exists():
        print(f"❌ {ibes_file} not found")
        print("   Please run AP_IBESEPSAdjusted.py first")
        return pd.DataFrame()
    
    print(f"\n📥 Loading IBES EPS data...")
    df = pd.read_parquet(ibes_file, columns=['tickerIBES', 'time_avail_m'])
    df = df.drop_duplicates(['tickerIBES', 'time_avail_m'])
    print(f"  ✓ Loaded {len(df):,} records")
    print(f"    Unique IBES tickers: {df['tickerIBES'].nunique()}")
    
    return df

# =============================================================================
# CREATE LINKING TABLE
# =============================================================================

def create_linking_table(crsp_df, ibes_df):
    """
    Create IBES-CRSP linking table based on ticker matching.
    
    Simple approach:
    - Match on ticker symbol (tickerIBES == ticker)
    - For each match, track the time period it's valid
    - Assign score=1 for all matches (perfect match)
    
    More sophisticated approach could include:
    - Fuzzy matching
    - Name matching
    - CUSIP matching (if available)
    - Multiple permnos per ticker handling
    """
    
    if crsp_df.empty or ibes_df.empty:
        return pd.DataFrame()
    
    print(f"\n🔗 Creating IBES-CRSP linking table...")
    
    # Standardize ticker symbols (uppercase, strip)
    crsp_df['ticker_std'] = crsp_df['ticker'].str.upper().str.strip()
    ibes_df['ticker_std'] = ibes_df['tickerIBES'].str.upper().str.strip()
    
    # Merge on ticker and month
    print(f"  Merging on ticker and time_avail_m...")
    merged = crsp_df.merge(
        ibes_df,
        left_on=['ticker_std', 'time_avail_m'],
        right_on=['ticker_std', 'time_avail_m'],
        how='inner'
    )
    
    print(f"  ✓ Matched {len(merged):,} permno-ticker-month combinations")
    print(f"    Unique permnos: {merged['permno'].nunique()}")
    print(f"    Unique IBES tickers: {merged['tickerIBES'].nunique()}")
    
    # Add score field (1 = perfect match)
    merged['score'] = 1
    
    # Select and order columns to match original format
    final_df = merged[['tickerIBES', 'permno', 'time_avail_m', 'score']].copy()
    
    # Remove duplicates (in case of multiple matches)
    initial_count = len(final_df)
    final_df = final_df.drop_duplicates(['permno', 'time_avail_m'], keep='first')
    if len(final_df) < initial_count:
        print(f"  Removed {initial_count - len(final_df)} duplicate permno-month combinations")
    
    # Sort by permno and time
    final_df = final_df.sort_values(['permno', 'time_avail_m']).reset_index(drop=True)
    
    return final_df

# =============================================================================
# VALIDATION
# =============================================================================

def validate_linking_table(df):
    """Validate linking table quality"""
    
    if df.empty:
        return
    
    print(f"\n🔍 Validation:")
    
    # Check for permnos with multiple IBES tickers in same month
    dup_check = df.groupby(['permno', 'time_avail_m'])['tickerIBES'].nunique()
    multi_ticker = (dup_check > 1).sum()
    if multi_ticker > 0:
        print(f"  ⚠️  {multi_ticker} permno-months have multiple IBES tickers")
    else:
        print(f"  ✓ No permno-months with multiple IBES tickers")
    
    # Check for IBES tickers with multiple permnos in same month
    dup_check2 = df.groupby(['tickerIBES', 'time_avail_m'])['permno'].nunique()
    multi_permno = (dup_check2 > 1).sum()
    if multi_permno > 0:
        print(f"  ⚠️  {multi_permno} IBES ticker-months have multiple permnos")
    else:
        print(f"  ✓ No IBES ticker-months with multiple permnos")
    
    # Coverage statistics
    total_permnos = df['permno'].nunique()
    total_tickers = df['tickerIBES'].nunique()
    total_months = df['time_avail_m'].nunique()
    
    print(f"\n  Coverage:")
    print(f"    Unique permnos: {total_permnos}")
    print(f"    Unique IBES tickers: {total_tickers}")
    print(f"    Months with data: {total_months}")
    print(f"    Average observations per permno: {len(df) / total_permnos:.1f}")
    print(f"    Average observations per ticker: {len(df) / total_tickers:.1f}")

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main execution function"""
    
    # Load data
    crsp_df = load_crsp_monthly()
    if crsp_df.empty:
        return
    
    ibes_df = load_ibes_data()
    if ibes_df.empty:
        return
    
    # Create linking table
    link_df = create_linking_table(crsp_df, ibes_df)
    
    if link_df.empty:
        print("\n❌ No matches found between CRSP and IBES")
        print("   This may indicate:")
        print("   - Different ticker symbols used")
        print("   - Non-overlapping time periods")
        print("   - Data issues")
        return
    
    # Validate
    validate_linking_table(link_df)
    
    # Save output
    print(f"\n💾 Saving output...")
    
    output_file = OUTPUT_DIR / "AP_IBESCRSPLinkingTable.parquet"
    link_df.to_parquet(output_file, index=False)
    
    print(f"  ✓ Saved: {output_file}")
    print(f"    Records: {len(link_df):,}")
    print(f"    Size: {output_file.stat().st_size / 1024:.1f} KB")
    
    # Generate summary
    print(f"\n📊 Summary:")
    print(f"  Total observations: {len(link_df):,}")
    print(f"  Unique permnos: {link_df['permno'].nunique()}")
    print(f"  Unique IBES tickers: {link_df['tickerIBES'].nunique()}")
    print(f"  Date range: {link_df['time_avail_m'].min()} to {link_df['time_avail_m'].max()}")
    
    # Sample data
    print(f"\n  Sample data:")
    print(link_df.head(15).to_string(index=False))
    
    print("\n" + "=" * 70)
    print("✅ AP_IBESCRSPLink.py completed successfully!")
    print("=" * 70)
    print("\nOutputs:")
    print("  1. AP_IBESCRSPLinkingTable.parquet - IBES-CRSP linking table")
    print("\nNext steps:")
    print("  - Use AP_IBESCRSPLinkingTable.parquet in place of IBESCRSPLinkingTable.parquet")
    print("  - Use for merging IBES and CRSP data in predictors")
    print("\n💡 Note:")
    print("  - This uses simple ticker matching")
    print("  - For production, consider more sophisticated matching:")
    print("    * CUSIP matching")
    print("    * Name matching")
    print("    * Historical ticker changes")

if __name__ == "__main__":
    main()

