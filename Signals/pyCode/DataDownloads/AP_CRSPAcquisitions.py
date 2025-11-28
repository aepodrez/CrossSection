# ABOUTME: Downloads and processes spinoff company data from SEC 8-K filings (free alternative to CRSP)
# ABOUTME: Identifies companies created in spinoffs by parsing SEC 8-K filings for spinoff events
"""
Inputs:
- SEC EDGAR 8-K filings via edgartools
- AP_CRSPMonthly.parquet or CCMLinkingTable.parquet (for ticker/permno mapping)

Outputs:
- ../pyData/Intermediate/AP_m_CRSPAcquisitions.parquet

Data Sources:
- SEC EDGAR 8-K filings (Item 2.01, 2.02 - acquisitions and spinoffs)
- Available from ~1994 onwards (when 8-K became electronic)
- Free and publicly available

How to run: python AP_CRSPAcquisitions.py

Requirements:
    pip install edgartools pandas numpy
"""

import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import warnings
import re
import requests
import json
warnings.filterwarnings('ignore')

# Try to import edgartools
try:
    from edgar import Company, get_filings, set_identity
    EDGARTOOLS_AVAILABLE = True
except ImportError:
    print("⚠️  edgartools not installed. Install with: pip install edgartools")
    print("⚠️  Running in demo mode with sample data structure only.")
    EDGARTOOLS_AVAILABLE = False

# Print script header
print("=" * 60, flush=True)
print("📊 AP_CRSPAcquisitions.py - Live SEC 8-K Spinoff Data", flush=True)
print("=" * 60, flush=True)

# Set Edgar identity (required by SEC)
if EDGARTOOLS_AVAILABLE:
    # Set your identity - SEC requires this
    set_identity("Your Name your.email@example.com")
    print("✓ Edgar identity set", flush=True)


# =============================================================================
# SPINOFF DETECTION FUNCTIONS
# =============================================================================

def is_spinoff_filing(filing_text: str) -> bool:
    """
    Determine if an 8-K filing contains spinoff information.
    
    Looks for keywords and phrases related to spinoffs, distributions, and divestitures.
    
    Args:
        filing_text: Text content of the 8-K filing
        
    Returns:
        True if filing appears to be a spinoff, False otherwise
    """
    if not filing_text:
        return False
    
    text_lower = filing_text.lower()
    
    # Keywords that indicate spinoff events
    spinoff_keywords = [
        'spinoff',
        'spin-off',
        'spin off',
        'spinout',
        'spin-out',
        'spin out',
        'distribution of shares',
        'distribution of common stock',
        'pro rata distribution',
        'dividend of shares',
        'separation of',
        'divestiture',
        'divest',
        'split-off',
        'split off',
        'carve-out',
        'carve out',
        'carveout',
    ]
    
    # Item 2.01 or 2.02 often contain acquisition/spinoff information
    item_indicators = [
        'item 2.01',
        'item 2.02',
        'completion of acquisition',
        'completion of disposition',
    ]
    
    # Check for spinoff keywords
    has_spinoff_keyword = any(keyword in text_lower for keyword in spinoff_keywords)
    
    # Check for relevant item numbers
    has_relevant_item = any(indicator in text_lower for indicator in item_indicators)
    
    # Additional context: look for distribution language
    distribution_phrases = [
        'distribution to shareholders',
        'distribution to stockholders',
        'distribution to holders',
        'pro rata distribution',
        'tax-free distribution',
        'distribution of',
    ]
    has_distribution = any(phrase in text_lower for phrase in distribution_phrases)
    
    # Return True if we have spinoff keywords and either relevant item or distribution language
    return has_spinoff_keyword and (has_relevant_item or has_distribution)


def extract_spinoff_date(filing_text: str, filing_date: datetime) -> Optional[datetime]:
    """
    Extract the spinoff completion date from filing text.
    
    Args:
        filing_text: Text content of the 8-K filing
        filing_date: Date of the filing (fallback if date not found in text)
        
    Returns:
        Spinoff date if found, otherwise filing date
    """
    if not filing_text:
        return filing_date
    
    text_lower = filing_text.lower()
    
    # Look for date patterns near spinoff keywords
    date_patterns = [
        r'(?:completed|completed on|effective|effective on|occurred on|on)\s+([a-z]+\s+\d{1,2},?\s+\d{4})',
        r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
        r'(\d{4}[/-]\d{1,2}[/-]\d{1,2})',
    ]
    
    for pattern in date_patterns:
        matches = re.finditer(pattern, text_lower, re.IGNORECASE)
        for match in matches:
            try:
                date_str = match.group(1)
                parsed_date = pd.to_datetime(date_str, errors='coerce')
                if pd.notna(parsed_date):
                    # Use date if it's within 90 days of filing date (reasonable range)
                    if abs((parsed_date - filing_date).days) <= 90:
                        return parsed_date
            except:
                continue
    
    # If no date found, use filing date
    return filing_date


def extract_spinoff_company_info(filing_text: str) -> Dict[str, Optional[str]]:
    """
    Extract information about the spinoff company from filing text.
    
    Args:
        filing_text: Text content of the 8-K filing
        
    Returns:
        Dictionary with company information (ticker, name, etc.)
    """
    info = {
        'ticker': None,
        'company_name': None,
    }
    
    if not filing_text:
        return info
    
    # Look for ticker symbols (typically in parentheses or after company name)
    ticker_patterns = [
        r'\(([A-Z]{1,5})\)',  # (AAPL)
        r'\(NYSE:\s*([A-Z]{1,5})\)',  # (NYSE: AAPL)
        r'\(NASDAQ:\s*([A-Z]{1,5})\)',  # (NASDAQ: AAPL)
        r'\(Symbol:\s*([A-Z]{1,5})\)',  # (Symbol: AAPL)
        r'ticker[:\s]+([A-Z]{1,5})',  # ticker: AAPL
    ]
    
    for pattern in ticker_patterns:
        matches = re.finditer(pattern, filing_text, re.IGNORECASE)
        for match in matches:
            ticker = match.group(1).strip().upper()
            if len(ticker) >= 1 and len(ticker) <= 5:
                info['ticker'] = ticker
                break
    
    # Look for company name (often near "new company", "spun-off company", etc.)
    name_patterns = [
        r'(?:new|spun-off|spun off|newly formed)\s+company[:\s]+([A-Z][A-Za-z\s&.,]+?)(?:\.|,|$)',
        r'company[:\s]+([A-Z][A-Za-z\s&.,]+?)\s+(?:will|has|is)',
    ]
    
    for pattern in name_patterns:
        matches = re.finditer(pattern, filing_text, re.IGNORECASE)
        for match in matches:
            name = match.group(1).strip()
            if len(name) > 3 and len(name) < 100:
                info['company_name'] = name
                break
    
    return info


# =============================================================================
# TICKER TO PERMNO MAPPING
# =============================================================================

def load_ticker_permno_mapping() -> pd.DataFrame:
    """
    Load ticker to permno mapping from existing AP files.
    
    Returns:
        DataFrame with columns: ticker, permno
    """
    mapping = pd.DataFrame()
    
    # Try to load from AP_CRSPMonthly
    crsp_path = Path("../pyData/Intermediate/AP_monthlyCRSP.parquet")
    if crsp_path.exists():
        try:
            crsp = pd.read_parquet(crsp_path, columns=['ticker', 'permno'])
            crsp = crsp.dropna(subset=['ticker', 'permno'])
            crsp = crsp.drop_duplicates(subset=['ticker'])
            mapping = crsp[['ticker', 'permno']].copy()
            print(f"✓ Loaded {len(mapping)} ticker-permno mappings from AP_CRSPMonthly")
        except Exception as e:
            print(f"⚠️  Could not load from AP_CRSPMonthly: {e}")
    
    # Try to load from CCM linking table
    if mapping.empty:
        ccm_path = Path("../pyData/Intermediate/CCMLinkingTable.parquet")
        if ccm_path.exists():
            try:
                ccm = pd.read_parquet(ccm_path, columns=['ticker', 'permno'])
                ccm = ccm.dropna(subset=['ticker', 'permno'])
                ccm = ccm.drop_duplicates(subset=['ticker'])
                mapping = ccm[['ticker', 'permno']].copy()
                print(f"✓ Loaded {len(mapping)} ticker-permno mappings from CCMLinkingTable")
            except Exception as e:
                print(f"⚠️  Could not load from CCMLinkingTable: {e}")
    
    return mapping


# =============================================================================
# TICKER TO CIK MAPPING (FALLBACK FOR EDGARTOOLS LIMITATION)
# =============================================================================

_ticker_to_cik_cache = None

def get_ticker_to_cik_mapping() -> Dict[str, str]:
    """
    Get ticker to CIK mapping from SEC company_tickers.json.
    Caches the result to avoid repeated downloads.
    """
    global _ticker_to_cik_cache
    
    if _ticker_to_cik_cache is not None:
        return _ticker_to_cik_cache
    
    url = "https://www.sec.gov/files/company_tickers.json"
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'AP_CRSPAcquisitions/1.0 (test@example.com)',
        'Accept': 'application/json'
    })
    
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        mapping = {}
        items = data.items() if isinstance(data, dict) else enumerate(data)
        
        for _, row in items:
            if not isinstance(row, dict):
                continue
            ticker = row.get("ticker", "").upper().strip()
            cik = row.get("cik_str") or row.get("cik")
            if not ticker or cik is None:
                continue
            try:
                cik_str = str(int(cik)).zfill(10)
                mapping[ticker] = cik_str
            except (ValueError, TypeError):
                continue
        
        _ticker_to_cik_cache = mapping
        return mapping
    except Exception as e:
        print(f"⚠️  Warning: Could not load SEC ticker mapping: {e}")
        return {}


def get_company_by_ticker_or_cik(ticker: str) -> Optional:
    """
    Get Company object by ticker, with CIK fallback.
    
    edgartools Company(ticker) sometimes returns None even for valid SEC-registered
    companies. This function tries ticker first, then falls back to CIK lookup.
    """
    # Try direct ticker lookup first
    company = Company(ticker)
    if company is not None:
        return company
    
    # Fallback: Look up CIK and try that
    ticker_to_cik = get_ticker_to_cik_mapping()
    cik = ticker_to_cik.get(ticker.upper())
    
    if cik:
        try:
            company = Company(cik)
            if company is not None:
                return company
        except Exception:
            pass
    
    return None


# =============================================================================
# MAIN PROCESSING FUNCTIONS
# =============================================================================

def get_spinoff_filings_for_company(ticker: str, years: int = 2) -> pd.DataFrame:
    """
    Fetch 8-K filings for a company and identify spinoff events.
    
    Args:
        ticker: Company ticker symbol
        years: Number of years of history to fetch
        
    Returns:
        DataFrame with spinoff events
    """
    if not EDGARTOOLS_AVAILABLE:
        print(f"⚠️  Skipping {ticker} - edgartools not available")
        return pd.DataFrame()
    
    try:
        print(f"Fetching 8-K filings for {ticker}...", flush=True)
        
        # Get company object (with CIK fallback)
        company = get_company_by_ticker_or_cik(ticker)
        
        # Check if company was found
        if company is None:
            print(f"  ⚠️  Company not found in SEC EDGAR for ticker {ticker} (even with CIK fallback)")
            return pd.DataFrame()
        
        # Get recent 8-K filings
        # Note: amendments parameter removed in newer edgartools versions
        filings = company.get_filings(form='8-K').latest(years * 4)  # More filings for 8-K
        
        results = []
        
        for filing in filings:
            try:
                # Get filing date
                filing_date = pd.to_datetime(filing.filing_date)
                
                # Get filing text
                try:
                    filing_text = filing.text()
                except:
                    # Try alternative method
                    try:
                        filing_text = filing.html()
                    except:
                        filing_text = ""
                
                # Check if this is a spinoff filing
                if not is_spinoff_filing(filing_text):
                    continue
                
                # Extract spinoff information
                spinoff_date = extract_spinoff_date(filing_text, filing_date)
                company_info = extract_spinoff_company_info(filing_text)
                
                # Build record
                record = {
                    'ticker': ticker,
                    'filing_date': filing_date,
                    'spinoff_date': spinoff_date,
                    'spinoff_ticker': company_info.get('ticker'),
                    'spinoff_company_name': company_info.get('company_name'),
                    'filing_text_snippet': filing_text[:500] if filing_text else None,  # First 500 chars for debugging
                }
                
                results.append(record)
                print(f"  ✓ Found spinoff event on {spinoff_date.strftime('%Y-%m-%d')}")
                
            except Exception as e:
                print(f"  ⚠️  Error processing filing: {e}")
                continue
        
        if results:
            df = pd.DataFrame(results)
            print(f"✓ Found {len(df)} spinoff events for {ticker}")
            return df
        else:
            return pd.DataFrame()
            
    except Exception as e:
        print(f"❌ Error fetching data for {ticker}: {e}")
        return pd.DataFrame()


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """
    Main execution function.
    """
    
    print("\n" + "="*60)
    print("📋 Loading ticker universe and mapping...")
    print("="*60)
    
    # Load ticker-permno mapping
    ticker_permno_map = load_ticker_permno_mapping()
    
    # Define universe of tickers to fetch
    # Load from S&P 500 pickle file
    universe_tickers = []
    
    # Load from S&P 500 pickle file
    import pickle
    universe_path = Path("../pyData/Static/sp500_universe.pkl")
    if universe_path.exists():
        try:
            with open(universe_path, 'rb') as f:
                universe_tickers = pickle.load(f)
            print(f"✓ Loaded {len(universe_tickers)} tickers from sp500_universe.pkl")
        except Exception as e:
            print(f"⚠️  Could not load sp500_universe.pkl: {e}")
    
    # Fallback: Try to load from AP_CRSPMonthly
    if not universe_tickers:
        ap_crsp_path = Path("../pyData/Intermediate/AP_monthlyCRSP.parquet")
        if ap_crsp_path.exists():
            print("Loading tickers from AP_monthlyCRSP.parquet...")
            crsp_df = pd.read_parquet(ap_crsp_path, columns=['ticker'])
            universe_tickers = crsp_df['ticker'].dropna().unique().tolist()
            print(f"✓ Found {len(universe_tickers)} unique tickers from AP_CRSPMonthly")
    
    # If still no universe, use a sample
    if not universe_tickers:
        print("⚠️  No universe file found. Using sample tickers for demonstration.")
        universe_tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NVDA', 'JPM', 'V', 'WMT']
    
    # Limit to first N tickers for testing (comment out for full run)
    # universe_tickers = universe_tickers[:50]  # Uncomment to test with 50 tickers
    
    print(f"\n📊 Processing {len(universe_tickers)} tickers...")
    
    # Fetch spinoff data for all companies
    all_spinoff_data = []
    
    for i, ticker in enumerate(universe_tickers, 1):
        print(f"\n[{i}/{len(universe_tickers)}] Processing {ticker}...")
        
        # Fetch 8-K spinoff data
        ticker_data = get_spinoff_filings_for_company(ticker, years=2)
        
        if not ticker_data.empty:
            all_spinoff_data.append(ticker_data)
        
        # Rate limiting - be respectful to SEC servers
        if i % 10 == 0:
            print(f"\n✓ Processed {i}/{len(universe_tickers)} tickers. Pausing briefly...")
            import time
            time.sleep(2)  # 2 second pause every 10 requests
    
    if not all_spinoff_data:
        print("\n❌ No spinoff data extracted. Exiting.")
        return
    
    # Combine all data
    print("\n" + "="*60)
    print("🔄 Processing and combining spinoff data...")
    print("="*60)
    
    df = pd.concat(all_spinoff_data, ignore_index=True)
    print(f"✓ Combined data: {len(df)} spinoff events")
    
    # Convert spinoff_date to monthly
    df['spinoff_date'] = pd.to_datetime(df['spinoff_date'])
    df['time_avail_m'] = df['spinoff_date'].dt.to_period('M').dt.to_timestamp()
    
    # Match spinoff tickers to permnos
    # For spinoff companies, we need to match the spinoff_ticker (the new company)
    # If spinoff_ticker is available, use it; otherwise use parent ticker
    spinoff_companies = []
    
    for _, row in df.iterrows():
        # Try to match spinoff ticker first (the new company)
        spinoff_ticker = row.get('spinoff_ticker')
        if spinoff_ticker and not ticker_permno_map.empty:
            match = ticker_permno_map[ticker_permno_map['ticker'] == spinoff_ticker]
            if not match.empty:
                permno = match.iloc[0]['permno']
                spinoff_companies.append({
                    'permno': permno,
                    'time_avail_m': row['time_avail_m'],
                    'SpinoffCo': 1,
                })
                continue
        
        # Fallback: If spinoff ticker not found, we can't identify the spinoff company
        # In this case, we skip (original CRSP logic only includes companies where we can identify the permno)
        # Note: This is a limitation - we may miss some spinoffs if ticker mapping is incomplete
    
    if not spinoff_companies:
        print("⚠️  No spinoff companies could be matched to permnos.")
        print("   This may be because:")
        print("   - Spinoff tickers were not extracted from filings")
        print("   - Ticker-permno mapping is incomplete")
        print("   - Spinoff companies are not yet in CRSP")
        return
    
    # Create final DataFrame
    spinoff_df = pd.DataFrame(spinoff_companies)
    print(f"✓ Matched {len(spinoff_df)} spinoff companies to permnos")
    
    # Remove duplicates (same permno, same month)
    initial_count = len(spinoff_df)
    spinoff_df = spinoff_df.drop_duplicates(subset=['permno', 'time_avail_m'])
    duplicates_removed = initial_count - len(spinoff_df)
    print(f"✓ Removed {duplicates_removed} duplicate records")
    
    # Ensure SpinoffCo is integer
    spinoff_df['SpinoffCo'] = spinoff_df['SpinoffCo'].astype(int)
    
    # Convert permno to integer
    spinoff_df['permno'] = pd.to_numeric(spinoff_df['permno'], errors='coerce').astype('Int64')
    
    # Sort by permno
    spinoff_df = spinoff_df.sort_values(['permno'])
    
    # Select final columns (matching original format - only permno and SpinoffCo)
    spinoff_df = spinoff_df[['permno', 'SpinoffCo']]
    
    # Save data
    print("\n" + "="*60)
    print("💾 Saving spinoff company data...")
    print("="*60)
    
    output_dir = Path("../pyData/Intermediate/")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output_path = output_dir / "AP_m_CRSPAcquisitions.parquet"
    spinoff_df.to_parquet(output_path, index=False)
    print(f"✓ Saved: {output_path}")
    
    # Print summary statistics
    print("\n" + "="*60)
    print("📈 Summary Statistics")
    print("="*60)
    print(f"Total spinoff companies: {len(spinoff_df)}")
    print(f"Unique spinoff companies: {spinoff_df['permno'].nunique()}")
    
    print("\n" + "="*60)
    print("✅ AP_CRSPAcquisitions.py completed successfully")
    print("="*60)
    
    print("\n📝 Notes:")
    print("  - Spinoff detection relies on text parsing of 8-K filings")
    print("  - Some spinoffs may be missed if not clearly described in filings")
    print("  - Spinoff companies must be matched to permnos via ticker mapping")
    print("  - Coverage may be lower than CRSP for historical data (pre-2000)")


if __name__ == "__main__":
    main()

