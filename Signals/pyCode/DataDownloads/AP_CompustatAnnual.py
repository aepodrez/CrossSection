# ABOUTME: Downloads annual fundamental data from SEC EDGAR using edgartools (free alternative to Compustat)
# ABOUTME: Extracts XBRL data from 10-K filings and processes into annual/monthly versions
"""
Inputs:
- SEC EDGAR 10-K filings via edgartools
- List of ticker symbols or CIKs to process
- CCMLinkingTable.parquet (for CRSP linking)

Outputs:
- ../pyData/Intermediate/AP_CompustatAnnual.csv
- ../pyData/Intermediate/AP_a_aCompustat.parquet
- ../pyData/Intermediate/AP_m_aCompustat.parquet

Requirements:
    pip install edgartools

Notes:
- XBRL data only available from ~2009 onwards
- Company-specific XBRL tag mapping required
- Updates available same-day as SEC filing (much faster than Compustat)
- Free but requires more data engineering

How to run: python AP_CompustatAnnual.py
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
print("📈 AP_CompustatAnnual.py - Live EDGAR Annual Fundamentals", flush=True)
print("=" * 60, flush=True)

# Set Edgar identity (required by SEC)
if EDGARTOOLS_AVAILABLE:
    # Set your identity - SEC requires this
    set_identity("Your Name your.email@example.com")
    print("✓ Edgar identity set", flush=True)


# =============================================================================
# XBRL TAG MAPPING
# =============================================================================
# Map Compustat mnemonics to common XBRL tags
# Note: Companies may use different tags - this is a starting point

XBRL_TAG_MAP = {
    # ===== BALANCE SHEET - ASSETS =====
    
    # 🔥 CRITICAL: Total Assets - NO fair value disclosures (footnotes only)
    'at': ['Assets'],
    
    # 🔥 CRITICAL: Current Assets - NO Abstract tags (they contain no values)
    'act': ['AssetsCurrent'],
    
    # 🔥 CRITICAL: Cash & Equivalents ONLY - NO short-term investments
    'che': ['CashAndCashEquivalentsAtCarryingValue', 'CashAndDueFromBanks', 'Cash'],
    
    # 🔥 CRITICAL: Current AR - NO notes/loans receivables (trade only)
    'rect': ['AccountsReceivableNetCurrent', 'ReceivablesNetCurrent'],
    # ⚠️ RECTA - Total AR (no Gross, no Loans, no Finance receivables)
    'recta': ['AccountsReceivableNet', 'ReceivablesNet'],
    
    # 🔥 CRITICAL: Inventory NET only - NO Gross values
    'invt': ['InventoryNet', 'Inventory'],
    
    # ⚠️ ACO highly unreliable (many custom tags) - USE FALLBACK ONLY, prefer derivation
    # Derive: aco = act - (che + rect + invt + other known current assets)
    'aco': ['OtherAssetsCurrent', 'PrepaidExpenseAndOtherAssetsCurrent'],
    
    # 🔥 CRITICAL: PP&E NET - NO ROU assets (ASC 842 lease assets separate)
    'ppent': ['PropertyPlantAndEquipmentNet'],
    # STRICT: PP&E Gross - NO ROU assets or finance lease assets
    'ppegt': ['PropertyPlantAndEquipmentGross'],
    # STRICT: Buildings gross only
    'ppenb': ['BuildingsAndImprovementsGross', 'BuildingsAndBuildingImprovements'],
    # STRICT: Finance lease ROU assets only (ASC 842 compliant, old capital lease)
    'ppenls': ['FinanceLeaseRightOfUseAsset', 'PropertyPlantAndEquipmentUnderFinanceLease'],
    
    # Intangibles (max 3 tags)
    'intan': ['IntangibleAssetsNetExcludingGoodwill', 'FiniteLivedIntangibleAssetsNet'],
    'gdwl': ['Goodwill', 'GoodwillAndIntangibleAssetsGross'],
    # ⚠️ Goodwill impairments inconsistent - accept low coverage
    'gdwlia': ['GoodwillImpairmentLoss', 'GoodwillAndIntangibleAssetImpairment'],
    'gdwlip': ['GoodwillImpairmentLoss'],
    # ❌ GWO (goodwill adjustments) basically not reliably extractable
    
    # Other Assets (max 3 tags)
    'ao': ['OtherAssetsNoncurrent', 'OtherAssets'],
    
    # Short-term Investments (max 4 tags, no fair value disclosures)
    'ivst': ['ShortTermInvestments', 'MarketableSecuritiesCurrent', 'AvailableForSaleSecurities'],
    'ivao': ['InvestmentsAndOtherNoncurrentAssets', 'OtherLongTermInvestments', 
             'LongTermInvestments', 'EquityMethodInvestments'],
    
    # Fixed Assets Detail (component-level PP&E)
    # STRICT: Buildings & improvements gross only
    'fatb': ['BuildingsAndImprovementsGross', 'BuildingsAndBuildingImprovements'],
    # STRICT: Land & land improvements only
    'fatl': ['Land', 'LandAndLandImprovements'],
    
    # ===== BALANCE SHEET - LIABILITIES =====
    
    # 🔥 CRITICAL: Total Liabilities ONLY - NO components (current/noncurrent)
    'lt': ['Liabilities'],
    
    # Current Liabilities - NO Abstract tags
    'lct': ['LiabilitiesCurrent'],
    
    # Debt - Current
    'dlc': ['DebtCurrent', 'ShortTermBorrowings', 'ShortTermDebtAndCapitalLeaseObligations',
            'LongTermDebtCurrent', 'ShortTermBankLoansAndNotesPayable', 'CommercialPaper',
            'LineOfCreditCurrent', 'NotesPayableCurrent'],
    
    # Debt - Long-term
    'dltt': ['LongTermDebtNoncurrent', 'LongTermDebt', 'LongTermDebtAndCapitalLeaseObligations',
             'LongTermDebtAndCapitalLeaseObligationsNoncurrent', 'LongTermBorrowings',
             'SeniorNotes', 'ConvertibleDebt'],
    
    # ⚠️ Debt cash flow items highly inconsistent (50-60% coverage) - companies combine/net
    'dlcch': ['IncreaseDecreaseInShortTermBorrowings', 'ProceedsFromRepaymentsOfShortTermDebt'],
    'dltis': ['ProceedsFromIssuanceOfLongTermDebt'],
    'dltr': ['RepaymentsOfLongTermDebt'],
    
    # Accounts Payable
    'ap': ['AccountsPayableCurrent', 'AccountsPayableAndAccruedLiabilitiesCurrent',
           'AccountsPayableTradeCurrent', 'TradeAndOtherPayablesCurrent'],
    
    # Taxes Payable
    'txp': ['TaxesPayableCurrent', 'AccruedIncomeTaxesCurrent', 'IncomeTaxesPayable',
            'IncomeTaxesPayableCurrent', 'DeferredTaxLiabilitiesCurrent', 'TaxesPayable'],
    
    # Other Liabilities
    'lco': ['OtherLiabilitiesCurrent', 'AccruedLiabilitiesCurrent', 'OtherAccruedLiabilitiesCurrent',
            'AccruedExpensesAndOtherCurrentLiabilities', 'DeferredRevenueAndOtherLiabilitiesCurrent'],
    # Other Liabilities Noncurrent - NO Abstract tags
    'lo': ['OtherLiabilitiesNoncurrent', 'OtherNoncurrentLiabilities'],
    
    # STRICT: Deferred Revenue - ASC 606 dual mapping (old + new GAAP)
    'drc': ['DeferredRevenueCurrent', 'ContractWithCustomerLiabilityCurrent'],
    'drlt': ['DeferredRevenueNoncurrent', 'ContractWithCustomerLiabilityNoncurrent'],
    
    # ❌ DM (mortgage debt) - Do NOT map except for REIT-specific analysis (custom tags)
    'dcpstk': ['ConvertibleDebtNoncurrent', 'ConvertiblePreferredStockNoncurrent',
               'ConvertibleNotesPayable', 'ConvertibleSubordinatedDebt'],
    'dcvt': ['ConvertibleDebt', 'ConvertibleLongTermNotesPayable', 'ConvertibleDebtNoncurrent'],
    
    # STRICT: Minority Interest = Balance sheet carrying amount only (no redemption, no temporary equity)
    'mib': ['NoncontrollingInterest', 'NoncontrollingInterestInConsolidatedEntity'],
    # ❌ MSA (minority interest adjustments) - Almost never tagged, not mappable
    
    # ❌ OB (other borrowings) - Not GAAP, not reliably mappable (derive as residual if needed)
    
    # ===== BALANCE SHEET - EQUITY =====
    
    # Total Equity
    'ceq': ['StockholdersEquity', 'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest',
            'PartnersCapital', 'CommonStockholdersEquity'],
    'seq': ['StockholdersEquity', 'StockholdersEquityTotal', 
            'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest'],
    
    # Preferred Stock
    'pstk': ['PreferredStockValue', 'PreferredStockValueOutstanding', 'PreferredStockCarryingAmount'],
    # ⚠️ PSTKL (pref stock liquidation) - Possible but low coverage
    'pstkl': ['PreferredStockCarryingAmount'],
    # ❌ PSTKRV (redemption value) - Not mappable (numeric XBRL fact almost never provided)
    
    # Retained Earnings
    're': ['RetainedEarningsAccumulatedDeficit', 'RetainedEarnings', 
           'RetainedEarningsUnappropriated', 'AccumulatedOtherComprehensiveIncomeLossNetOfTax'],
    
    # STRICT: Treasury stock value (not share count)
    'tstkp': ['TreasuryStockValue', 'TreasuryStockCommonValue', 'TreasuryStockAtCost'],
    
    # ⚠️ CEQT (common equity) - XBRL doesn't distinguish clearly, derive: ceq - pstk - mib
    
    # 🔥 CRITICAL: Shares Outstanding at fiscal year-end ONLY
    # NO weighted average, NO authorized, NO reserved shares
    'csho': ['CommonStockSharesOutstanding', 'CommonStockSharesIssued'],
    'cshrc': ['StockRepurchasedDuringPeriodShares', 'TreasuryStockSharesAcquired',
              'StockRepurchasedAndRetiredDuringPeriodShares', 'SharesRepurchased'],
    
    # Stock Transactions
    'prstkc': ['PaymentsForRepurchaseOfCommonStock', 'TreasuryStockValueAcquiredCostMethod',
               'PaymentsForRepurchaseOfEquity', 'StockRepurchasedDuringPeriodValue'],
    # ❌ PRSTKCC (preferred stock repurchases) - Rarely broken out, not mappable
    # STRICT: Cash proceeds from stock issuance only (no non-cash issuance)
    'scstkc': ['ProceedsFromIssuanceOfCommonStock', 'ProceedsFromStockOptionsExercised'],
    'sstk': ['ProceedsFromIssuanceOfCommonStock', 'StockIssuedDuringPeriodValueNewIssues',
             'ProceedsFromStockPlans'],
    
    # ❌ AJEX (adjustment factor) - NOT in 10-K, Compustat construct for splits/mergers, CANNOT map
    
    # ===== INCOME STATEMENT =====
    
    # 🔥 CRITICAL: Operating Revenue ONLY - NO bank interest income
    'sale': ['Revenues', 'SalesRevenueNet', 'RevenueFromContractWithCustomerExcludingAssessedTax'],
    # Total revenue (max 3 tags, similar to sale)
    'revt': ['Revenues', 'RevenueFromContractWithCustomerExcludingAssessedTax'],
    
    # Cost of Revenue
    'cogs': ['CostOfGoodsAndServicesSold', 'CostOfRevenue', 'CostOfGoodsSold',
             'CostOfGoodsAndServicesSold', 'CostOfSales', 'CostOfGoodsAndServiceExcludingDepreciationDepletionAndAmortization'],
    
    # Operating Expenses (max 3 tags)
    'xsga': ['SellingGeneralAndAdministrativeExpense', 'GeneralAndAdministrativeExpense'],
    'xad': ['AdvertisingExpense', 'MarketingExpense', 'MarketingAndAdvertisingExpense',
            'AdvertisingCost', 'PromotionAndAdvertising'],
    'xrd': ['ResearchAndDevelopmentExpense', 'ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost',
            'ResearchAndDevelopmentExpenseSoftwareExcludingAcquiredInProcessCost'],
    'xint': ['InterestExpense', 'InterestExpenseDebt', 'InterestExpenseOther',
             'InterestExpenseBorrowings', 'InterestAndDebtExpense', 'InterestExpenseLongTermDebt'],
    'xpp': ['PrepaidExpenseAndOtherAssets', 'PrepaidExpense', 'PrepaidExpenseCurrent',
            'PrepaidExpenseAndOtherAssetsCurrent', 'DeferredCosts'],
    'xacc': ['AccruedLiabilitiesCurrent', 'AccruedExpenses', 'AccrualForEnvironmentalLossContingencies',
             'AccruedPayrollTaxesCurrent', 'AccruedBonusesCurrent'],
    
    # Depreciation & Amortization
    'dp': ['Depreciation', 'DepreciationAndAmortization', 'DepreciationDepletionAndAmortization',
           'DepreciationNonproduction', 'CostOfPropertyRepairsAndMaintenance',
           'DepreciationAmortizationAndAccretionNet'],
    # STRICT: Compustat AM = amortization of intangibles only
    'am': ['AmortizationOfIntangibleAssets', 'AmortizationOfIntangibleAssetsExcludingAcquiredInProcess'],
    
    # ===== INCOME STATEMENT - EARNINGS =====
    
    # Operating Income
    'oiadp': ['OperatingIncomeLoss', 'IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest',
              'OperatingIncome', 'IncomeLossFromContinuingOperations'],
    'oibdp': ['OperatingIncomeLoss', 'OperatingIncome', 'EarningsBeforeInterestAndTaxes'],
    
    # EBIT & EBITDA
    'ebit': ['OperatingIncomeLoss', 'EarningsBeforeInterestAndTaxes', 'OperatingIncome'],
    'ebitda': ['OperatingIncomeLoss'],  # Will be calculated: EBIT + D&A
    
    # Net Income
    'ib': ['NetIncomeLoss', 'ProfitLoss', 'IncomeLossFromContinuingOperations',
           'IncomeLossFromContinuingOperationsIncludingPortionAttributableToNoncontrollingInterest',
           'IncomeLossBeforeExtraordinaryItemsAndCumulativeEffectOfChangeInAccountingPrinciple'],
    'ni': ['NetIncomeLoss', 'ProfitLoss', 'NetIncomeLossAvailableToCommonStockholdersBasic',
           'NetIncomeLossIncludingPortionAttributableToNoncontrollingInterest',
           'IncomeLossFromContinuingOperationsIncludingPortionAttributableToNoncontrollingInterest'],
    'ibcom': ['NetIncomeLossAvailableToCommonStockholdersBasic', 'NetIncomeLossAttributableToParent',
              'NetIncomeLossAvailableToCommonStockholdersDiluted', 'NetIncome'],
    
    # Other Income Items
    'nopi': ['NonoperatingIncomeExpense', 'OtherNonoperatingIncomeExpense',
             'OtherIncome', 'NonOperatingIncome', 'OtherComprehensiveIncomeLossNetOfTax'],
    'pi': ['IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments',
           'IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest',
           'IncomeLossFromContinuingOperationsBeforeIncomeTaxesForeign', 'IncomeLossBeforeIncomeTaxes'],
    
    # Special Items
    'spi': ['GainLossOnDispositionOfAssets', 'AssetImpairmentCharges', 'RestructuringCharges',
            'GainLossOnSaleOfBusiness', 'UnusualOrInfrequentItemsDisclosureTextBlock',
            'RestructuringSettlementAndImpairmentProvisions', 'GoodwillAndIntangibleAssetImpairment'],
    
    # Foreign Operations
    'fopt': ['IncomeLossFromContinuingOperationsAttributableToForeignOperations',
             'IncomeLossFromForeignCurrencyTransaction', 'ForeignCurrencyTransactionGainLossRealized'],
    
    # REIT-specific
    'ffo': ['FundsFromOperations', 'FundsFromOperationsPerShare'],
    
    # ===== EARNINGS PER SHARE =====
    'epspi': ['EarningsPerShareBasic', 'IncomeLossFromContinuingOperationsPerBasicShare',
              'EarningsPerShareBasicAndDiluted', 'IncomeLossFromContinuingOperationsPerDilutedShare'],
    'epspx': ['EarningsPerShareDiluted', 'IncomeLossFromContinuingOperationsPerDilutedShare',
              'EarningsPerShareBasicAndDiluted', 'WeightedAverageNumberOfDilutedSharesOutstanding'],
    
    # ===== CASH FLOW STATEMENT =====
    
    # Operating Cash Flow
    'oancf': ['NetCashProvidedByUsedInOperatingActivities', 
              'NetCashProvidedByUsedInOperatingActivitiesContinuingOperations',
              'CashProvidedByUsedInOperatingActivities'],
    
    # Investing Cash Flow
    'ivncf': ['NetCashProvidedByUsedInInvestingActivities',
              'NetCashProvidedByUsedInInvestingActivitiesContinuingOperations',
              'CashProvidedByUsedInInvestingActivities'],
    
    # Financing Cash Flow
    'fincf': ['NetCashProvidedByUsedInFinancingActivities',
              'NetCashProvidedByUsedInFinancingActivitiesContinuingOperations',
              'CashProvidedByUsedInFinancingActivities'],
    
    # Capital Expenditures
    'capx': ['PaymentsToAcquirePropertyPlantAndEquipment', 'CapitalExpendituresIncurredButNotYetPaid',
             'PaymentsForCapitalImprovements', 'PaymentsToAcquireProductiveAssets',
             'CapitalExpenditureDiscontinuedOperations', 'PaymentsForProceedsFromProductiveAssets'],
    
    # Investment Transactions (removed duplicate 'ivao' - balance sheet version is primary)
    
    # Stock Transactions (Cash Flow)
    # STRICT: Cash proceeds from stock issuance only (no non-cash)
    'scstkc': ['ProceedsFromIssuanceOfCommonStock', 'ProceedsFromStockOptionsExercised'],
    'prstkc': ['PaymentsForRepurchaseOfCommonStock', 'TreasuryStockValueAcquiredCostMethod',
               'PaymentsForRepurchaseOfEquity', 'TreasuryStockPurchased'],
    'sstk': ['ProceedsFromIssuanceOfCommonStock', 'ProceedsFromIssuanceOrSaleOfEquity',
             'StockIssuedDuringPeriodValueNewIssues'],
    
    # ⚠️ WCAPCH (working capital change) - Unreliable from XBRL, derive: (act-lct) - lag(act-lct)
    'wcap': ['WorkingCapital'],  # Calculated: Current Assets - Current Liabilities
    
    # ===== DIVIDENDS =====
    'dvc': ['DividendsCommonStock', 'PaymentsOfDividendsCommonStock', 'PaymentsOfOrdinaryDividends',
            'CommonStockDividendsPerShareDeclared', 'DividendsCash'],
    'dvp': ['DividendsPreferredStock', 'PaymentsOfDividendsPreferredStockAndPreferenceStock',
            'PreferredStockDividendsPerShareDeclared', 'DividendsPreferredStockCash'],
    'dv': ['Dividends', 'PaymentsOfDividends', 'PaymentsOfDividendsCommonStock',
           'PaymentsOfDividendsMinorityInterest', 'DividendsPayable'],
    'dvt': ['DividendsPayableCurrentAndNoncurrent', 'DividendsPayable', 'DividendsDeclared'],
    'dvpa': ['CommonStockDividendsPerShareDeclared', 'CommonStockDividendsPerShareCashPaid'],
    'dvpd': ['DividendsCommonStockCash'],
    'dvpsx_c': ['CommonStockDividendsPerShareDeclared'],
    
    # ===== TAX ITEMS =====
    'txditc': ['DeferredTaxAssetsLiabilitiesNet', 'DeferredIncomeTaxLiabilities',
               'DeferredTaxAssetsNet', 'DeferredTaxLiabilitiesNoncurrent',
               'DeferredIncomeTaxAssetsNet', 'DeferredIncomeTaxes'],
    'txp': ['TaxesPayableCurrent', 'AccruedIncomeTaxesCurrent', 'IncomeTaxesPayable',
            'IncomeTaxesPayableCurrent', 'DeferredTaxLiabilitiesCurrent',
            'AccruedTaxes', 'TaxesPayableCurrentAndNoncurrent'],
    'txdb': ['DeferredTaxAssetsNet', 'DeferredTaxLiabilities', 'DeferredTaxAssetsLiabilitiesNet',
             'DeferredTaxAssetsTaxDeferredExpenseReservesAndAccruals', 'DeferredIncomeTaxLiabilities'],
    'txdi': ['DeferredIncomeTaxExpenseBenefit', 'DeferredFederalStateAndLocalTaxExpenseBenefit',
             'IncreaseDeferredIncomeTaxes', 'DeferredIncomeTaxes'],
    'txfo': ['IncomeTaxExpenseBenefitContinuingOperationsForeignIncomeTaxes',
             'CurrentForeignTaxExpenseBenefit', 'ForeignIncomeTaxExpenseBenefitContinuingOperations'],
    'txfed': ['CurrentFederalTaxExpenseBenefit', 'FederalIncomeTaxExpenseBenefitContinuingOperations',
              'CurrentIncomeTaxExpenseBenefitContinuingOperationsFederalNational'],
    # Income tax expense (max 3 tags)
    'txt': ['IncomeTaxExpenseBenefit', 'IncomeTaxExpenseBenefitContinuingOperations'],
    
    # ===== MARKET DATA (Limited in XBRL) =====
    # Note: These are rarely in 10-K XBRL - use yfinance or real-time data vendor
    'prcc_f': ['CommonStockPrice', 'StockPricePerShare', 'SharePrice'],
    'prcc_c': ['StockPrice', 'SharePrice', 'CommonStockMarketPrice'],
    
    # ===== OTHER ITEMS =====
    
    # Employees
    'emp': ['NumberOfEmployees', 'EmployeeRelatedLiabilitiesCurrent', 
            'FullTimeEmployees', 'NumberOfFullTimeEmployees'],
    
    # Working Capital
    'wcap': ['WorkingCapital'],  # Usually calculated: act - lct
    # (wcapch - see earlier definition, marked as unreliable/derive)
    
    # (ajex - see earlier definition, marked as unmappable)
    
    # (ceqt - see earlier definition, marked as derive)
    
    # (dcpstk, dcvt - see earlier definitions, removed duplicates)
}

# Additional derived fields that need calculation
DERIVED_FIELDS = {
    'xint0': 'xint',  # Zero-filled interest expense
    'xsga0': 'xsga',  # Zero-filled SG&A
    'xad0': 'xad',    # Zero-filled advertising
    'dr': ['drc', 'drlt'],  # Total deferred revenue
    'dc': ['dcpstk', 'pstk', 'dcvt'],  # Convertible debt
}

# Fields that should be DERIVED from other fields (not directly mapped - XBRL unreliable)
FIELDS_TO_DERIVE = {
    'aco': 'act - (che + rect + invt + [other known current assets])',  # Other current assets
    'ceqt': 'ceq - pstk - mib',  # Common equity (Compustat construct)
    'wcapch': '(act - lct) - lag(act - lct)',  # Working capital change
    # ajex - CANNOT derive (Compustat-specific for splits/mergers)
}


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


def diagnose_xbrl_structure(xbrl, max_items=5):
    """Debug helper to understand XBRL object structure."""
    print("  [DEBUG] XBRL object type:", type(xbrl))
    print("  [DEBUG] XBRL attributes:", dir(xbrl)[:max_items], "...")
    
    # Try to access statements
    for stmt in ['balance_sheet', 'income_statement', 'cash_flow', 'statements']:
        if hasattr(xbrl, stmt):
            obj = getattr(xbrl, stmt)
            print(f"  [DEBUG] {stmt} type:", type(obj))
            if hasattr(obj, 'to_dataframe'):
                print(f"  [DEBUG] {stmt} has to_dataframe()")


def get_company_financials_from_10k(ticker: str, years: int = 5, debug: bool = False) -> pd.DataFrame:
    """
    Fetch 10-K filings for a company and extract financial data using edgartools API.
    
    Args:
        ticker: Company ticker symbol
        years: Number of years of history to fetch
        debug: Print diagnostic information
        
    Returns:
        DataFrame with annual financial data
    """
    if not EDGARTOOLS_AVAILABLE:
        print(f"⚠️  Skipping {ticker} - edgartools not available")
        return pd.DataFrame()
    
    try:
        print(f"Fetching 10-K data for {ticker}...", flush=True)
        
        # Get company object
        company = Company(ticker)
        
        # Get recent 10-K filings (exclude amendments for cleaner data)
        filings = company.get_filings(form='10-K', amendments=False).latest(years)
        
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
                    print(f"  ⚠️  No XBRL data for {period_end.year}")
                    continue
                
                # Build record with basic info
                record = {
                    'ticker': ticker,
                    'cik': company.cik,
                    'filing_date': pd.to_datetime(filing.filing_date),
                    'period_end': period_end,
                    'fiscal_year': period_end.year,
                }
                
                # Extract financial statement data using edgartools API
                # Try to get consolidated statements
                if hasattr(xbrl, 'statements'):
                    statements = xbrl.statements
                    
                    #Helper function to extract value from statement dataframe
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
                print(f"  ✓ Extracted {period_end.year} data ({len([v for v in record.values() if v is not None])} fields)", flush=True)
                
            except Exception as e:
                import traceback
                print(f"  ⚠️  Error processing filing: {e}")
                if debug:
                    print(f"  [DEBUG] {traceback.format_exc()[:300]}")
                continue
        
        return pd.DataFrame(results)
        
    except Exception as e:
        print(f"❌ Error fetching {ticker}: {e}")
        return pd.DataFrame()


def process_compustat_annual_alternative(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process raw EDGAR data to match Compustat Annual structure.
    Apply same transformations as CompustatAnnual.py
    """
    if df.empty:
        return df
    
    # Sort by ticker and fiscal year
    df = df.sort_values(['ticker', 'fiscal_year'])
    
    # Create 6-digit CUSIP equivalent (we don't have this from EDGAR easily)
    # For now, use CIK as identifier
    df['cnum'] = df['cik'].astype(str).str.zfill(10)[:6]
    
    # Rename columns to match expected format
    df = df.rename(columns={
        'period_end': 'datadate',
        'fiscal_year': 'fyear',
    })
    
    # Create derived variable: deferred revenue (dr)
    df['dr'] = np.nan
    mask1 = df['drc'].notna() & df['drlt'].notna()
    df.loc[mask1, 'dr'] = df.loc[mask1, 'drc'] + df.loc[mask1, 'drlt']
    mask2 = df['drc'].notna() & df['drlt'].isna()
    df.loc[mask2, 'dr'] = df.loc[mask2, 'drc']
    mask3 = df['drc'].isna() & df['drlt'].notna()
    df.loc[mask3, 'dr'] = df.loc[mask3, 'drlt']
    
    # Create zero-filled versions
    df['xint0'] = df['xint'].fillna(0)
    df['xsga0'] = df['xsga'].fillna(0)
    
    # ========== DERIVE UNRELIABLE FIELDS ==========
    # These fields have unreliable XBRL mappings, so we calculate them from other fields
    
    # ACO (other current assets) = Current assets - known components
    # aco = act - (che + rect + invt + other precisely known current assets)
    if 'act' in df.columns and 'che' in df.columns and 'rect' in df.columns and 'invt' in df.columns:
        df['aco_derived'] = df['act'] - df['che'] - df['rect'] - df['invt']
        # If we have direct XBRL mapping, use it; otherwise use derived
        if 'aco' not in df.columns or df['aco'].isna().all():
            df['aco'] = df['aco_derived']
        else:
            # Fill missing with derived values
            df['aco'] = df['aco'].fillna(df['aco_derived'])
        df = df.drop(columns=['aco_derived'], errors='ignore')
    
    # CEQT (common equity) = Total equity - preferred stock - minority interest
    # ceqt = ceq - pstk - mib
    if 'ceq' in df.columns:
        df['ceqt'] = df['ceq']
        if 'pstk' in df.columns:
            df['ceqt'] = df['ceqt'] - df['pstk'].fillna(0)
        if 'mib' in df.columns:
            df['ceqt'] = df['ceqt'] - df['mib'].fillna(0)
    
    # WCAPCH (working capital change) = Δ(Current Assets - Current Liabilities)
    # wcapch = (act - lct) - lag(act - lct)
    if 'act' in df.columns and 'lct' in df.columns:
        df = df.sort_values(['ticker', 'fyear'])
        df['wcap_level'] = df['act'] - df['lct']
        df['wcapch_derived'] = df.groupby('ticker')['wcap_level'].diff()
        # If we have direct XBRL mapping, use it; otherwise use derived
        if 'wcapch' not in df.columns or df['wcapch'].isna().all():
            df['wcapch'] = df['wcapch_derived']
        else:
            df['wcapch'] = df['wcapch'].fillna(df['wcapch_derived'])
        df = df.drop(columns=['wcap_level', 'wcapch_derived'], errors='ignore')
    df['xad0'] = df['xad'].fillna(0)
    
    # Fill missing values with zero for specified items
    zero_fill_vars = ['nopi', 'dvt', 'ob', 'dm', 'dc', 'aco', 'ap', 'intan', 'ao',
                      'lco', 'lo', 'rect', 'invt', 'drc', 'spi', 'gdwl', 'che',
                      'dp', 'act', 'lct', 'tstkp', 'dvpa', 'scstkc', 'sstk',
                      'mib', 'ivao', 'prstkc', 'prstkcc', 'txditc', 'ivst']
    
    for var in zero_fill_vars:
        if var in df.columns:
            df[var] = df[var].fillna(0)
    
    # Add 6-month reporting lag (can be adjusted for live trading)
    df['datadate'] = pd.to_datetime(df['datadate'])
    df['filing_date'] = pd.to_datetime(df['filing_date'])
    
    # Use actual filing date + 1 day as availability date (more realistic for live trading)
    df['time_avail_m'] = (df['filing_date'] + pd.Timedelta(days=1)).dt.to_period('M').dt.to_timestamp()
    
    return df


def link_to_crsp(df: pd.DataFrame) -> pd.DataFrame:
    """
    Link EDGAR data to CRSP using CCM linking table.
    """
    ccm_path = Path("../pyData/Intermediate/CCMLinkingTable.parquet")
    
    if not ccm_path.exists():
        print("⚠️  CCMLinkingTable.parquet not found. Cannot link to CRSP.")
        print("   Run CCMLinkingTable.py first, or data will not have permno/permco.")
        return df
    
    try:
        ccm = pd.read_parquet(ccm_path)
        
        # Link by ticker or CIK
        # Note: This is simplified - proper linking requires more sophisticated matching
        df_linked = df.merge(
            ccm[['ticker', 'permno', 'permco']].drop_duplicates('ticker'),
            on='ticker',
            how='left'
        )
        
        print(f"✓ Linked {df_linked['permno'].notna().sum()} records to CRSP")
        return df_linked
        
    except Exception as e:
        print(f"⚠️  Error linking to CRSP: {e}")
        return df


def create_monthly_version(annual_data: pd.DataFrame) -> pd.DataFrame:
    """
    Create monthly version by expanding annual data (same as Compustat processing).
    """
    monthly_data = annual_data.copy()
    
    # Replicate each annual record 12 times
    monthly_data = pd.concat([monthly_data] * 12, ignore_index=True)
    
    # Add month offsets
    monthly_data = monthly_data.sort_values(['ticker', 'time_avail_m']).reset_index(drop=True)
    monthly_data['tempTime'] = monthly_data['time_avail_m']
    monthly_data['month_offset'] = monthly_data.groupby(['ticker', 'tempTime']).cumcount()
    
    # Apply offsets
    monthly_data['time_avail_m'] = (
        monthly_data['time_avail_m'].dt.to_period('M') + monthly_data['month_offset']
    ).dt.to_timestamp()
    
    monthly_data = monthly_data.drop(['tempTime', 'month_offset'], axis=1)
    
    # Remove duplicates
    if 'permno' in monthly_data.columns:
        monthly_data = monthly_data.sort_values(['permno', 'time_avail_m', 'datadate'])
        monthly_data = monthly_data.drop_duplicates(['permno', 'time_avail_m'], keep='last')
    
    monthly_data = monthly_data.sort_values(['ticker', 'time_avail_m', 'datadate'])
    monthly_data = monthly_data.drop_duplicates(['ticker', 'time_avail_m'], keep='last')
    
    return monthly_data


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """
    Main execution function.
    """
    
    # Example ticker list (you would expand this to full universe)
    # For production, you'd want to:
    # 1. Read from a file of all tickers
    # 2. Or query all companies from EDGAR
    # 3. Or use your existing universe from CRSP
    
    SAMPLE_TICKERS = [
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META',
        'TSLA', 'NVDA', 'JPM', 'V', 'WMT',
        'JNJ', 'PG', 'MA', 'UNH', 'HD',
    ]
    
    print(f"\n📊 Processing {len(SAMPLE_TICKERS)} sample tickers...\n")
    
    if not EDGARTOOLS_AVAILABLE:
        print("=" * 60)
        print("⚠️  DEMO MODE: Creating sample data structure only")
        print("=" * 60)
        print("\nTo use this script with real data:")
        print("1. Install edgartools: pip install edgartools")
        print("2. Set your identity in the script (required by SEC)")
        print("3. Run again to fetch live EDGAR data")
        print("\nCreating empty DataFrames with correct structure...")
        
        # Create sample structure
        all_data = pd.DataFrame(columns=['ticker', 'cik', 'filing_date', 'datadate', 
                                        'fyear', 'time_avail_m'] + list(XBRL_TAG_MAP.keys()))
    else:
        # Fetch data for each ticker
        all_data = []
        for i, ticker in enumerate(SAMPLE_TICKERS):
            # Enable debug for first ticker to see XBRL structure
            debug = (i == 0)
            df = get_company_financials_from_10k(ticker, years=5, debug=debug)
            if not df.empty:
                all_data.append(df)
        
        if not all_data:
            print("❌ No data fetched. Exiting.")
            return
        
        # Combine all ticker data
        all_data = pd.concat(all_data, ignore_index=True)
        print(f"\n✓ Fetched {len(all_data)} annual records total\n")
    
    # Process data
    print("Processing data...")
    processed_data = process_compustat_annual_alternative(all_data)
    
    # Link to CRSP
    print("Linking to CRSP...")
    processed_data = link_to_crsp(processed_data)
    
    # Create output directory
    output_dir = Path("../pyData/Intermediate/")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save raw CSV
    csv_data = processed_data.copy()
    if 'datadate' in csv_data.columns:
        csv_data['datadate'] = pd.to_datetime(csv_data['datadate']).dt.strftime('%d%b%Y').str.lower()
    csv_data.to_csv(output_dir / "AP_CompustatAnnual.csv", index=False)
    print(f"✓ Saved AP_CompustatAnnual.csv ({len(csv_data)} records)")
    
    # Save annual parquet
    if not processed_data.empty:
        processed_data.to_parquet(output_dir / "AP_a_aCompustat.parquet", index=False)
        print(f"✓ Saved AP_a_aCompustat.parquet ({len(processed_data)} records)")
        
        # Create and save monthly version
        monthly_data = create_monthly_version(processed_data)
        monthly_data.to_parquet(output_dir / "AP_m_aCompustat.parquet", index=False)
        print(f"✓ Saved AP_m_aCompustat.parquet ({len(monthly_data)} records)")
    
    print("\n" + "=" * 60)
    print("✅ AP_CompustatAnnual.py completed successfully")
    print("=" * 60)
    print("\n📝 Next Steps:")
    print("1. Expand ticker list to full universe (consider using CRSP universe)")
    print("2. Set up incremental updates (fetch new 10-Ks as filed)")
    print("3. Validate data quality against known benchmarks")
    print("4. Implement company-specific XBRL tag mapping for edge cases")
    print("5. Add error handling and retry logic for API failures")
    print("6. Consider caching to avoid re-fetching unchanged data")


if __name__ == "__main__":
    main()

