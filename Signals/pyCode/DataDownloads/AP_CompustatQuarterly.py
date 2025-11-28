# ABOUTME: Downloads quarterly fundamental data from SEC EDGAR using edgartools (free alternative to Compustat)
# ABOUTME: Extracts XBRL data from 10-Q filings and processes into quarterly/monthly versions
"""
Inputs:
- SEC EDGAR 10-Q filings via edgartools
- List of ticker symbols or CIKs to process
- CCMLinkingTable.parquet (for CRSP linking, optional)

Outputs:
- ../pyData/Intermediate/AP_CompustatQuarterly.csv
- ../pyData/Intermediate/AP_m_QCompustat.parquet
- ../pyData/Intermediate/AP_CompustatQuarterly.parquet

Requirements:
    pip install edgartools

Notes:
- XBRL data only available from ~2009 onwards
- Company-specific XBRL tag mapping required
- Updates available same-day as SEC filing (much faster than Compustat)
- Free but requires more data engineering
- Fetches 10-Q filings (quarterly reports) instead of 10-K (annual)
- Many quarterly items are YTD (year-to-date) and need conversion to quarterly

How to run: python AP_CompustatQuarterly.py
"""

import os
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import warnings
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
print("📊 AP_CompustatQuarterly.py - Live EDGAR Quarterly Fundamentals", flush=True)
print("=" * 60, flush=True)

# Set Edgar identity (required by SEC)
if EDGARTOOLS_AVAILABLE:
    # Set your identity - SEC requires this
    set_identity("Your Name your.email@example.com")
    print("✓ Edgar identity set", flush=True)


# =============================================================================
# XBRL TAG MAPPING - REUSED FROM AP_CompustatAnnual.py
# =============================================================================
# Map Compustat mnemonics to common XBRL tags
# Note: Companies may use different tags - this is a starting point

XBRL_TAG_MAP = {
    # ===== BALANCE SHEET - ASSETS =====
    
    # 🔥 CRITICAL: Total Assets - NO fair value disclosures (footnotes only)
    'atq': ['Assets'],
    
    # 🔥 CRITICAL: Current Assets - NO Abstract tags (they contain no values)
    'actq': ['AssetsCurrent'],
    
    # 🔥 CRITICAL: Cash & Equivalents ONLY - NO short-term investments
    'cheq': ['CashAndCashEquivalentsAtCarryingValue', 'CashAndDueFromBanks', 'Cash'],
    
    # 🔥 CRITICAL: Current AR - NO notes/loans receivables (trade only)
    'rectq': ['AccountsReceivableNetCurrent', 'ReceivablesNetCurrent'],
    
    # 🔥 CRITICAL: Inventory NET only - NO Gross values
    'invtq': ['InventoryNet', 'Inventory'],
    
    # ⚠️ ACO highly unreliable (many custom tags) - USE FALLBACK ONLY, prefer derivation
    # Derive: aco = act - (che + rect + invt + other known current assets)
    'acoq': ['OtherAssetsCurrent', 'PrepaidExpenseAndOtherAssetsCurrent'],
    
    # 🔥 CRITICAL: PP&E NET - NO ROU assets (ASC 842 lease assets separate)
    'ppentq': ['PropertyPlantAndEquipmentNet'],
    # STRICT: PP&E Gross - NO ROU assets or finance lease assets
    'ppegtq': ['PropertyPlantAndEquipmentGross'],
    
    # Intangibles (max 3 tags)
    'intanq': ['IntangibleAssetsNetExcludingGoodwill', 'FiniteLivedIntangibleAssetsNet'],
    'gdwlq': ['Goodwill', 'GoodwillAndIntangibleAssetsGross'],
    
    # Other Assets (max 3 tags)
    'aoq': ['OtherAssetsNoncurrent', 'OtherAssets'],
    
    # Short-term Investments (max 4 tags, no fair value disclosures)
    'ivaoq': ['InvestmentsAndOtherNoncurrentAssets', 'OtherLongTermInvestments', 
              'LongTermInvestments', 'EquityMethodInvestments'],
    
    # ===== BALANCE SHEET - LIABILITIES =====
    
    # 🔥 CRITICAL: Total Liabilities ONLY - NO components (current/noncurrent)
    'ltq': ['Liabilities'],
    
    # Current Liabilities - NO Abstract tags
    'lctq': ['LiabilitiesCurrent'],
    
    # Debt - Current
    'dlcq': ['DebtCurrent', 'ShortTermBorrowings', 'ShortTermDebtAndCapitalLeaseObligations',
             'LongTermDebtCurrent', 'ShortTermBankLoansAndNotesPayable', 'CommercialPaper',
             'LineOfCreditCurrent', 'NotesPayableCurrent'],
    
    # Debt - Long-term
    'dlttq': ['LongTermDebtNoncurrent', 'LongTermDebt', 'LongTermDebtAndCapitalLeaseObligations',
              'LongTermDebtAndCapitalLeaseObligationsNoncurrent', 'LongTermBorrowings',
              'SeniorNotes', 'ConvertibleDebt'],
    
    # Accounts Payable
    'apq': ['AccountsPayableCurrent', 'AccountsPayableAndAccruedLiabilitiesCurrent',
            'AccountsPayableTradeCurrent', 'TradeAndOtherPayablesCurrent'],
    
    # Taxes Payable
    'txpq': ['TaxesPayableCurrent', 'AccruedIncomeTaxesCurrent', 'IncomeTaxesPayable',
             'IncomeTaxesPayableCurrent', 'DeferredTaxLiabilitiesCurrent', 'TaxesPayable'],
    
    # Other Liabilities
    'lcoq': ['OtherLiabilitiesCurrent', 'AccruedLiabilitiesCurrent', 'OtherAccruedLiabilitiesCurrent',
             'AccruedExpensesAndOtherCurrentLiabilities', 'DeferredRevenueAndOtherLiabilitiesCurrent'],
    # Other Liabilities Noncurrent - NO Abstract tags
    'loq': ['OtherLiabilitiesNoncurrent', 'OtherNoncurrentLiabilities'],
    
    # STRICT: Deferred Revenue - ASC 606 dual mapping (old + new GAAP)
    'drcq': ['DeferredRevenueCurrent', 'ContractWithCustomerLiabilityCurrent'],
    'drltq': ['DeferredRevenueNoncurrent', 'ContractWithCustomerLiabilityNoncurrent'],
    
    # STRICT: Minority Interest = Balance sheet carrying amount only (no redemption, no temporary equity)
    'mibq': ['NoncontrollingInterest', 'NoncontrollingInterestInConsolidatedEntity'],
    
    # ===== BALANCE SHEET - EQUITY =====
    
    # Total Equity
    'ceqq': ['StockholdersEquity', 'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest',
             'PartnersCapital', 'CommonStockholdersEquity'],
    'seqq': ['StockholdersEquity', 'StockholdersEquityTotal', 
             'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest'],
    
    # Preferred Stock
    'pstkq': ['PreferredStockValue', 'PreferredStockValueOutstanding', 'PreferredStockCarryingAmount'],
    
    # Retained Earnings
    'req': ['RetainedEarningsAccumulatedDeficit', 'RetainedEarnings', 
            'RetainedEarningsUnappropriated', 'AccumulatedOtherComprehensiveIncomeLossNetOfTax'],
    
    # 🔥 CRITICAL: Shares Outstanding at quarter-end ONLY
    # NO weighted average, NO authorized, NO reserved shares
    'cshoq': ['CommonStockSharesOutstanding', 'CommonStockSharesIssued'],
    'cshprq': ['StockRepurchasedDuringPeriodShares', 'TreasuryStockSharesAcquired',
               'StockRepurchasedAndRetiredDuringPeriodShares', 'SharesRepurchased'],
    
    # ===== INCOME STATEMENT (QUARTERLY, YTD) =====
    
    # Revenue (many are YTD in 10-Q)
    'revtq': ['Revenues', 'RevenueFromContractWithCustomerExcludingAssessedTax', 
              'SalesRevenueNet', 'RevenuesNetOfInterestExpense'],
    'saleq': ['Revenues', 'SalesRevenueNet', 'RevenueFromContractWithCustomerExcludingAssessedTax'],
    
    # Cost of Goods Sold (YTD in 10-Q)
    'cogsq': ['CostOfGoodsAndServicesSold', 'CostOfRevenue', 'CostOfGoodsSold', 
              'CostOfSales', 'CostOfRevenuesAndCostOfGoodsSold'],
    
    # Operating Expenses (YTD in 10-Q)
    'xsgaq': ['SellingGeneralAndAdministrativeExpense', 'OperatingExpenses',
              'GeneralAndAdministrativeExpense', 'SellingAndMarketingExpense'],
    'xrdq': ['ResearchAndDevelopmentExpense', 'ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost'],
    
    # Operating Income (YTD in 10-Q)
    'oibdpq': ['OperatingIncomeLoss', 'IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest'],
    'oiadpq': ['OperatingIncomeLoss'],
    
    # Depreciation & Amortization (YTD in 10-Q)
    'dpq': ['DepreciationDepletionAndAmortization', 'Depreciation', 'DepreciationAndAmortization'],
    
    # Interest Expense (YTD in 10-Q)
    'xintq': ['InterestExpense', 'InterestExpenseDebt', 'InterestIncomeExpenseNet',
              'InterestAndDebtExpense', 'InterestExpenseNet'],
    
    # Income Before Taxes (YTD in 10-Q)
    'piq': ['IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest',
            'IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments'],
    
    # Income Tax Expense (YTD in 10-Q)
    'txtq': ['IncomeTaxExpenseBenefit', 'IncomeTaxExpenseBenefitContinuingOperations'],
    'txdiq': ['DeferredIncomeTaxExpenseBenefit', 'DeferredFederalStateAndLocalTaxExpenseBenefit'],
    
    # Net Income (YTD in 10-Q)
    'niq': ['NetIncomeLoss', 'NetIncomeLossAvailableToCommonStockholdersBasic',
            'ProfitLoss', 'NetIncomeLossAttributableToParent'],
    'ibq': ['IncomeLossFromContinuingOperations', 'NetIncomeLoss'],
    
    # EPS (YTD in 10-Q, but per-share)
    'epspxq': ['EarningsPerShareBasic', 'EarningsPerShareDiluted'],
    'epspiq': ['EarningsPerShareBasic'],
    
    # Dividends (YTD in 10-Q)
    'dvpq': ['DividendsPreferredStock', 'PaymentsOfDividendsPreferredStockAndPreferenceStock'],
    'dvpsxq': ['CommonStockDividendsPerShareDeclared', 'CommonStockDividendsPerShareCashPaid'],
    'dvy': ['DividendYield'],
    
    # ===== CASH FLOW STATEMENT (QUARTERLY, YTD) =====
    
    # Operating Cash Flow (YTD in 10-Q)
    'oancfy': ['NetCashProvidedByUsedInOperatingActivities', 
                'NetCashProvidedByUsedInOperatingActivitiesContinuingOperations'],
    
    # Investing Cash Flow
    'ivaoq': ['PaymentsToAcquireInvestments', 'PaymentsToAcquireMarketableSecurities',
              'PaymentsForProceedsFromOtherInvestingActivities'],
    
    # Financing Cash Flow
    'fopty': ['NetCashProvidedByUsedInFinancingActivities',
              'NetCashProvidedByUsedInFinancingActivitiesContinuingOperations'],
    
    # Capital Expenditures (YTD in 10-Q)
    'capxy': ['PaymentsToAcquirePropertyPlantAndEquipment', 'CapitalExpendituresIncurredButNotYetPaid'],
    
    # Stock Transactions (YTD in 10-Q)
    'prstkcy': ['PaymentsForRepurchaseOfCommonStock', 'TreasuryStockValueAcquiredCostMethod'],
    'sstky': ['ProceedsFromIssuanceOfCommonStock', 'ProceedsFromIssuanceOrSaleOfEquity'],
    
    # Working Capital Items (YTD in 10-Q)
    'xaccq': ['IncreaseDecreaseInAccountsReceivable', 'IncreaseDecreaseInReceivables'],
    
    # ===== TAX ITEMS =====
    'txditcq': ['DeferredTaxAssetsLiabilitiesNet', 'DeferredIncomeTaxLiabilities'],
    
    # ===== MARKET DATA (Limited in XBRL) =====
    # Note: These are sometimes in 10-Q XBRL cover page
    'prccq': ['CommonStockPrice', 'StockPricePerShare', 'SharePrice'],
    
    # ===== QUARTER/FISCAL INFO =====
    # Note: These are in filing metadata, not XBRL
    # We'll extract from filing dates
    # 'fyearq': fiscal year of quarter
    # 'fqtr': fiscal quarter (1-4)
    # 'datacqtr': data quarter (YYYYQ format)
    # 'datafqtr': data fiscal quarter
    # 'datadate': fiscal period end date
    # 'rdq': report date (filing date)
}

# YTD Fields that need conversion to quarterly
# These are reported year-to-date in 10-Q and need to be differenced
# Q1: value = YTD
# Q2-Q4: value = YTD - previous quarter's YTD
YTD_FIELDS = [
    'saleq', 'revtq', 'cogsq', 'xsgaq', 'xrdq', 'oibdpq', 'oiadpq', 'dpq',
    'xintq', 'piq', 'txtq', 'txdiq', 'niq', 'ibq', 'oancfy', 'capxy',
    'prstkcy', 'sstky', 'fopty', 'xaccq', 'dvpq'
]

# Fields to zero-fill (balance sheet and cumulative items)
ZERO_FILL_FIELDS = [
    'acoq', 'actq', 'apq', 'cheq', 'dpq', 'drcq', 'invtq', 'intanq',
    'ivaoq', 'gdwlq', 'lcoq', 'lctq', 'loq', 'mibq', 'prstkcy',
    'rectq', 'sstky', 'txditcq'
]


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def extract_xbrl_value(xbrl_data, tag_list: List[str], period_end=None) -> Optional[float]:
    """
    Extract value from XBRL data using a list of possible tags.
    Returns the first matching tag value found.
    
    Args:
        xbrl_data: XBRL object from edgartools
        tag_list: List of possible XBRL tag names
        period_end: Target period end date for filtering
        
    Returns:
        Float value if found, None otherwise
    """
    if not xbrl_data:
        return None
    
    # Try different ways to access XBRL data based on edgartools API
    for tag in tag_list:
        try:
            # Method 1: Try statements.to_dataframe() (primary method for edgartools)
            if hasattr(xbrl_data, 'statements'):
                statements = xbrl_data.statements
                if hasattr(statements, 'to_dataframe'):
                    try:
                        df = statements.to_dataframe()
                        # Look for tag in columns
                        if tag in df.columns:
                            vals = df[tag].dropna()
                            if len(vals) > 0:
                                return float(vals.iloc[-1])
                        # Try case-insensitive match
                        for col in df.columns:
                            if col.lower() == tag.lower():
                                vals = df[col].dropna()
                                if len(vals) > 0:
                                    return float(vals.iloc[-1])
                    except Exception:
                        pass
            
            # Method 2: Try accessing individual statements
            statement_types = []
            if hasattr(xbrl_data, 'balance_sheet'):
                statement_types.append(xbrl_data.balance_sheet)
            if hasattr(xbrl_data, 'income_statement'):
                statement_types.append(xbrl_data.income_statement)
            if hasattr(xbrl_data, 'cash_flow'):
                statement_types.append(xbrl_data.cash_flow)
            
            for statement in statement_types:
                if statement is not None:
                    # Try to find tag in statement
                    if hasattr(statement, tag):
                        val = getattr(statement, tag)
                        if val is not None:
                            return float(val)
                    # Try as dataframe
                    if hasattr(statement, 'to_dataframe'):
                        try:
                            df = statement.to_dataframe()
                            if tag in df.columns:
                                vals = df[tag].dropna()
                                if len(vals) > 0:
                                    return float(vals.iloc[-1])
                        except Exception:
                            pass
            
            # Method 3: Try get_facts() method
            if hasattr(xbrl_data, 'get_facts'):
                try:
                    facts = xbrl_data.get_facts(tag)
                    if facts:
                        if isinstance(facts, list) and len(facts) > 0:
                            sorted_facts = sorted(facts, key=lambda x: getattr(x, 'end', getattr(x, 'period', '')), reverse=True)
                            if hasattr(sorted_facts[0], 'value'):
                                return float(sorted_facts[0].value)
                        elif hasattr(facts, 'value'):
                            return float(facts.value)
                except Exception:
                    pass
            
            # Method 4: Try direct attribute access
            if hasattr(xbrl_data, tag):
                val = getattr(xbrl_data, tag)
                if val is not None:
                    if hasattr(val, 'value'):
                        return float(val.value)
                    else:
                        try:
                            return float(val)
                        except:
                            pass
                            
        except (AttributeError, KeyError, TypeError, ValueError):
            continue
    
    return None


def get_company_financials_from_10q(ticker: str, years: int = 5, debug: bool = False) -> pd.DataFrame:
    """
    Fetch 10-Q filings for a company and extract financial data using edgartools API.
    
    Args:
        ticker: Company ticker symbol
        years: Number of years of history to fetch
        debug: Print diagnostic information
        
    Returns:
        DataFrame with quarterly financial data
    """
    if not EDGARTOOLS_AVAILABLE:
        print(f"⚠️  Skipping {ticker} - edgartools not available")
        return pd.DataFrame()
    
    try:
        print(f"Fetching 10-Q data for {ticker}...", flush=True)
        
        # Get company object
        company = Company(ticker)
        
        # Get recent 10-Q filings (exclude amendments for cleaner data)
        # Fetch more quarters (years * 4) to get quarterly data
        filings = company.get_filings(form='10-Q', amendments=False).latest(years * 4)
        
        results = []
        
        for filing in filings:
            try:
                # Parse period_of_report as date
                period_end = filing.period_of_report
                if isinstance(period_end, str):
                    period_end = pd.to_datetime(period_end)
                
                # Get XBRL data
                xbrl = filing.xbrl()
                if xbrl is None:
                    print(f"  ⚠️  No XBRL data for {period_end}")
                    continue
                
                # Determine fiscal quarter
                fiscal_year = period_end.year
                fiscal_quarter = ((period_end.month - 1) // 3) + 1  # 1-4
                
                # Build record with basic info
                record = {
                    'ticker': ticker,
                    'cik': company.cik,
                    'filing_date': pd.to_datetime(filing.filing_date),
                    'period_end': period_end,
                    'datadate': period_end,
                    'rdq': pd.to_datetime(filing.filing_date),
                    'fyearq': fiscal_year,
                    'fqtr': fiscal_quarter,
                    'datacqtr': f"{fiscal_year}Q{fiscal_quarter}",
                    'datafqtr': f"Q{fiscal_quarter}",
                }
                
                # Extract financial statement data using edgartools API
                # Try to get consolidated statements
                if hasattr(xbrl, 'statements'):
                    statements = xbrl.statements
                    
                    # Helper function to extract value from statement dataframe
                    def extract_from_statement(df, compustat_field, xbrl_tags):
                        """Extract value from statement dataframe by matching concept column."""
                        if df is None or df.empty:
                            return None
                        
                        # Get the latest date column (skip 'concept', 'label', etc.)
                        date_cols = [col for col in df.columns if isinstance(col, str) and '-' in col and len(col) == 10]
                        if not date_cols:
                            return None
                        latest_col = date_cols[0]  # First date column is usually most recent
                        
                        # Try each XBRL tag
                        if isinstance(xbrl_tags, str):
                            xbrl_tags = [xbrl_tags]
                        
                        for tag in xbrl_tags:
                            # Look for tag in 'concept' column
                            # XBRL tags are like 'us-gaap_Assets', 'us-gaap_StockholdersEquity'
                            pattern = f'us-gaap_{tag}'
                            
                            # Try exact match
                            matches = df[df['concept'] == pattern]
                            if not matches.empty:
                                val = matches.iloc[0][latest_col]
                                if pd.notna(val):
                                    return float(val)
                            
                            # Try case-insensitive partial match
                            matches = df[df['concept'].str.contains(tag, case=False, na=False)]
                            if not matches.empty:
                                # Filter out abstract items (they're just headers)
                                non_abstract = matches[matches['abstract'] != True]
                                if not non_abstract.empty:
                                    val = non_abstract.iloc[0][latest_col]
                                    if pd.notna(val):
                                        return float(val)
                        
                        return None
                    
                    # Get balance sheet
                    try:
                        bs_df = None
                        if hasattr(statements, 'balance_sheet'):
                            bs_df = statements.balance_sheet().to_dataframe()
                        if bs_df is not None and 'concept' in bs_df.columns:
                            for compustat_field, xbrl_tags in XBRL_TAG_MAP.items():
                                if compustat_field not in record or record[compustat_field] is None:
                                    val = extract_from_statement(bs_df, compustat_field, xbrl_tags)
                                    if val is not None:
                                        record[compustat_field] = val
                    except Exception as e:
                        if debug:
                            print(f"    [DEBUG] Balance sheet error: {e}")
                    
                    # Get income statement
                    try:
                        is_df = None
                        if hasattr(statements, 'income_statement'):
                            is_df = statements.income_statement().to_dataframe()
                        if is_df is not None and 'concept' in is_df.columns:
                            for compustat_field, xbrl_tags in XBRL_TAG_MAP.items():
                                if compustat_field not in record or record[compustat_field] is None:
                                    val = extract_from_statement(is_df, compustat_field, xbrl_tags)
                                    if val is not None:
                                        record[compustat_field] = val
                    except Exception as e:
                        if debug:
                            print(f"    [DEBUG] Income statement error: {e}")
                    
                    # Get cash flow statement
                    try:
                        cf_df = None
                        if hasattr(statements, 'cashflow_statement'):
                            cf_df = statements.cashflow_statement().to_dataframe()
                        elif hasattr(statements, 'cash_flow'):
                            cf_df = statements.cash_flow().to_dataframe()
                        if cf_df is not None and 'concept' in cf_df.columns:
                            for compustat_field, xbrl_tags in XBRL_TAG_MAP.items():
                                if compustat_field not in record or record[compustat_field] is None:
                                    val = extract_from_statement(cf_df, compustat_field, xbrl_tags)
                                    if val is not None:
                                        record[compustat_field] = val
                    except Exception as e:
                        if debug:
                            print(f"    [DEBUG] Cash flow error: {e}")
                
                # Fill in any missing fields with None
                for field in XBRL_TAG_MAP.keys():
                    if field not in record:
                        record[field] = None
                
                results.append(record)
                print(f"  ✓ Extracted {period_end} (Q{fiscal_quarter} {fiscal_year})")
                
            except Exception as e:
                print(f"  ⚠️  Error processing filing: {e}")
                continue
        
        if results:
            df = pd.DataFrame(results)
            print(f"✓ Successfully extracted {len(df)} quarterly records for {ticker}")
            return df
        else:
            print(f"⚠️  No quarterly data extracted for {ticker}")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"❌ Error fetching data for {ticker}: {e}")
        return pd.DataFrame()


def convert_ytd_to_quarterly(df: pd.DataFrame, ytd_fields: List[str]) -> pd.DataFrame:
    """
    Convert year-to-date fields to quarterly by taking differences.
    Q1: quarterly = YTD
    Q2-Q4: quarterly = current YTD - previous quarter YTD
    
    Args:
        df: DataFrame with YTD fields
        ytd_fields: List of field names that are YTD
        
    Returns:
        DataFrame with quarterly versions of YTD fields
    """
    df = df.sort_values(['ticker', 'fyearq', 'fqtr']).copy()
    
    for field in ytd_fields:
        if field in df.columns:
            # Create quarterly version with 'q' suffix (if not already there)
            field_q = field if field.endswith('q') else field + 'q'
            
            # For Q1: use full YTD value
            # For Q2-Q4: difference from previous quarter within same fiscal year
            df[field_q] = np.where(
                df['fqtr'] == 1,
                df[field],
                df[field] - df.groupby(['ticker', 'fyearq'])[field].shift(1)
            )
    
    return df


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """
    Main execution function.
    """
    
    # Define universe of tickers to fetch
    # Option 1: Use existing AP files for universe
    print("\n" + "="*60)
    print("📋 Loading ticker universe...")
    print("="*60)
    
    universe_tickers = []
    
    # Try to load from AP_CRSPMonthly
    ap_crsp_path = Path("../pyData/Intermediate/AP_monthlyCRSP.parquet")
    if ap_crsp_path.exists():
        print("Loading tickers from AP_monthlyCRSP.parquet...")
        crsp_df = pd.read_parquet(ap_crsp_path, columns=['ticker'])
        universe_tickers = crsp_df['ticker'].dropna().unique().tolist()
        print(f"✓ Found {len(universe_tickers)} unique tickers from AP_CRSPMonthly")
    
    # If no AP file, try loading from CCM linking table
    if not universe_tickers:
        ccm_path = Path("../pyData/Intermediate/CCMLinkingTable.parquet")
        if ccm_path.exists():
            print("Loading tickers from CCMLinkingTable.parquet...")
            ccm_df = pd.read_parquet(ccm_path)
            if 'ticker' in ccm_df.columns:
                universe_tickers = ccm_df['ticker'].dropna().unique().tolist()
                print(f"✓ Found {len(universe_tickers)} unique tickers from CCM linking")
    
    # If still no universe, use a sample
    if not universe_tickers:
        print("⚠️  No universe file found. Using sample tickers for demonstration.")
        universe_tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NVDA', 'JPM', 'V', 'WMT']
    
    # Limit to first N tickers for testing (comment out for full run)
    # universe_tickers = universe_tickers[:50]  # Uncomment to test with 50 tickers
    
    print(f"\n📊 Processing {len(universe_tickers)} tickers...")
    
    # Fetch quarterly data for all companies
    all_data = []
    
    for i, ticker in enumerate(universe_tickers, 1):
        print(f"\n[{i}/{len(universe_tickers)}] Processing {ticker}...")
        
        # Fetch 10-Q data
        ticker_data = get_company_financials_from_10q(ticker, years=5)
        
        if not ticker_data.empty:
            all_data.append(ticker_data)
        
        # Rate limiting - be respectful to SEC servers
        if i % 10 == 0:
            print(f"\n✓ Processed {i}/{len(universe_tickers)} tickers. Pausing briefly...")
            import time
            time.sleep(1)  # 1 second pause every 10 requests
    
    if not all_data:
        print("\n❌ No data extracted. Exiting.")
        return
    
    # Combine all data
    print("\n" + "="*60)
    print("🔄 Processing and combining quarterly data...")
    print("="*60)
    
    df = pd.concat(all_data, ignore_index=True)
    print(f"✓ Combined data: {len(df)} quarterly records")
    
    # Remove duplicate records (keep most recent filing for each quarter)
    df = df.sort_values(['ticker', 'fyearq', 'fqtr', 'filing_date'])
    df = df.groupby(['ticker', 'fyearq', 'fqtr']).last().reset_index()
    print(f"✓ After deduplication: {len(df)} quarterly records")
    
    # Calculate data availability timing with 3-month lag assumption
    df['datadate'] = pd.to_datetime(df['datadate'])
    df['rdq'] = pd.to_datetime(df['rdq'])
    
    # time_avail_m = datadate + 3 months
    df['time_avail_m'] = (df['datadate'].dt.to_period('M') + 3).dt.to_timestamp()
    
    # Adjust if actual filing date is later
    rdq_monthly = df['rdq'].dt.to_period('M').dt.to_timestamp()
    mask = df['rdq'].notna() & (rdq_monthly > df['time_avail_m'])
    df.loc[mask, 'time_avail_m'] = rdq_monthly[mask]
    
    # Remove records with excessively late filings (> 6 months after quarter end)
    rdq_months = df['rdq'].dt.to_period('M')
    datadate_months = df['datadate'].dt.to_period('M')
    month_diff = (rdq_months - datadate_months).apply(lambda x: x.n if pd.notna(x) else 0)
    drop_mask = df['rdq'].notna() & (month_diff > 6)
    df = df[~drop_mask].copy()
    print(f"✓ After removing late releases: {len(df)} quarterly records")
    
    # Remove duplicates by keeping most recent data for each ticker-month combination
    df = df.sort_values(['ticker', 'time_avail_m', 'datadate'])
    df = df.groupby(['ticker', 'time_avail_m']).last().reset_index()
    print(f"✓ After removing time duplicates: {len(df)} quarterly records")
    
    # Zero-fill specified fields
    print("Filling missing values with zeros for balance sheet items...")
    for field in ZERO_FILL_FIELDS:
        if field in df.columns:
            df[field] = df[field].fillna(0)
    
    # Convert YTD items to quarterly
    print("Converting year-to-date items to quarterly...")
    df = convert_ytd_to_quarterly(df, YTD_FIELDS)
    
    # Try to add gvkey from CCM linking if available
    print("Attempting to add gvkey from CCM linking...")
    ccm_path = Path("../pyData/Intermediate/CCMLinkingTable.parquet")
    if ccm_path.exists():
        ccm_df = pd.read_parquet(ccm_path)
        if 'ticker' in ccm_df.columns and 'gvkey' in ccm_df.columns:
            # Get most recent gvkey for each ticker
            ticker_gvkey = ccm_df.groupby('ticker')['gvkey'].last().reset_index()
            df = df.merge(ticker_gvkey, on='ticker', how='left')
            print(f"✓ Added gvkey for {df['gvkey'].notna().sum()} records")
    
    if 'gvkey' not in df.columns:
        df['gvkey'] = None
        print("⚠️  gvkey not available (no CCM linking table)")
    
    # Save raw quarterly data
    print("\n" + "="*60)
    print("💾 Saving quarterly data...")
    print("="*60)
    
    output_dir = Path("../pyData/Intermediate/")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save CSV version (similar to original)
    csv_path = output_dir / "AP_CompustatQuarterly.csv"
    df.to_csv(csv_path, index=False)
    print(f"✓ Saved: {csv_path}")
    
    # Save parquet version
    parquet_path = output_dir / "AP_CompustatQuarterly.parquet"
    df.to_parquet(parquet_path, index=False)
    print(f"✓ Saved: {parquet_path}")
    
    # Expand quarterly data to monthly (forward-fill for 3 months)
    print("\n" + "="*60)
    print("📅 Expanding quarterly data to monthly...")
    print("="*60)
    
    # Create 3 monthly records per quarter (months 0, 1, 2 after time_avail_m)
    monthly_records = []
    
    for _, row in df.iterrows():
        for month_offset in [0, 1, 2]:
            monthly_row = row.copy()
            monthly_row['time_avail_m'] = (
                pd.to_datetime(row['time_avail_m']).to_period('M') + month_offset
            ).to_timestamp()
            monthly_records.append(monthly_row)
    
    monthly_df = pd.DataFrame(monthly_records)
    print(f"✓ Expanded to {len(monthly_df)} monthly records")
    
    # Keep most recent info for same ticker/time_avail_m after expansion
    monthly_df = monthly_df.sort_values(['ticker', 'time_avail_m', 'datadate'])
    monthly_df = monthly_df.groupby(['ticker', 'time_avail_m']).last().reset_index()
    print(f"✓ After deduplication: {len(monthly_df)} monthly records")
    
    # Rename datadate to datadateq
    if 'datadate' in monthly_df.columns:
        monthly_df = monthly_df.rename(columns={'datadate': 'datadateq'})
    
    # Save monthly version
    monthly_path = output_dir / "AP_m_QCompustat.parquet"
    monthly_df.to_parquet(monthly_path, index=False)
    print(f"✓ Saved: {monthly_path}")
    
    # Print summary statistics
    print("\n" + "="*60)
    print("📈 Summary Statistics")
    print("="*60)
    print(f"Total companies: {df['ticker'].nunique()}")
    print(f"Total quarterly records: {len(df)}")
    print(f"Total monthly records: {len(monthly_df)}")
    print(f"Date range: {df['datadate'].min()} to {df['datadate'].max()}")
    print(f"Availability range: {monthly_df['time_avail_m'].min()} to {monthly_df['time_avail_m'].max()}")
    
    # Field coverage statistics
    print("\n📊 Field Coverage (% non-null):")
    key_fields = ['atq', 'saleq', 'niq', 'cshoq', 'ceqq', 'ltq']
    for field in key_fields:
        if field in df.columns:
            coverage = (df[field].notna().sum() / len(df)) * 100
            print(f"  {field}: {coverage:.1f}%")
    
    print("\n" + "="*60)
    print("✅ AP_CompustatQuarterly.py completed successfully")
    print("="*60)


if __name__ == "__main__":
    main()

