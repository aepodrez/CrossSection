# ABOUTME: Downloads annual fundamental data from SEC EDGAR using edgartools (free alternative to Compustat)
# ABOUTME: Extracts XBRL data from 10-K filings and processes into annual/monthly versions
"""
Inputs:
- SEC EDGAR 10-K filings via edgartools
- List of ticker symbols or CIKs to process
- CCMLinkingTable.parquet (for CRSP linking)

Outputs:
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
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import warnings
import requests
import json
from dotenv import load_dotenv
warnings.filterwarnings('ignore')

# Optional free price source for prcc_f
try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False

# Load environment variables from .env file
load_dotenv()

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
    edgar_identity = os.getenv('EDGAR_IDENTITY', 'Your Name your.email@example.com')
    if edgar_identity == 'Your Name your.email@example.com':
        raise RuntimeError(
            "EDGAR_IDENTITY env var not set. Set EDGAR_IDENTITY in your .env to an email / app id for SEC access."
        )
    set_identity(edgar_identity)
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
    
    # Short-term Investments (max 4 tags, no fair value disclosures)
    'ivst': ['ShortTermInvestments', 'MarketableSecuritiesCurrent', 'AvailableForSaleSecurities'],
    'ivao': ['InvestmentsAndOtherNoncurrentAssets', 'OtherLongTermInvestments', 
             'LongTermInvestments', 'EquityMethodInvestments',
             'MarketableSecuritiesNoncurrent', 'AvailableForSaleSecuritiesNoncurrent',
             'LongTermMarketableSecurities', 'InvestmentsAvailableForSaleNoncurrent'],
    
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
            'ShortTermDebt', 'ShortTermDebtAndCurrentMaturitiesOfLongTermDebt',
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
    
    # Taxes Payable (current only - exclude deferred components)
    'txp': ['IncomeTaxesPayable', 'IncomeTaxesPayableCurrent',
            'TaxesPayableCurrent', 'AccruedIncomeTaxesCurrent'],
    
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
           'RetainedEarningsUnappropriated'],
    
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
    
    # EBIT (EBITDA will be derived)
    'ebit': ['OperatingIncomeLoss', 'EarningsBeforeInterestAndTaxes', 'OperatingIncome'],
    
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
    # Taxes Payable (current only - exclude deferred components)
    'txp': ['IncomeTaxesPayable', 'IncomeTaxesPayableCurrent',
            'TaxesPayableCurrent', 'AccruedIncomeTaxesCurrent'],
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

# Fields we always derive (no direct XBRL mapping used)
DERIVED_ONLY_FIELDS = ['ao', 'ebitda', 'che_comp', 'cogs_pre_dp']

# Scope tag mappings to specific statements to avoid cross-statement contamination
BS_FIELDS = {
    key: XBRL_TAG_MAP[key] for key in [
        'at', 'act', 'che', 'rect', 'recta', 'invt', 'aco', 'ppent', 'ppegt',
        'ppenb', 'ppenls', 'intan', 'gdwl', 'gdwlia', 'gdwlip', 'ivst',
        'ivao', 'fatb', 'fatl', 'lt', 'lct', 'dlc', 'dltt', 'ap', 'txp', 'lco',
        'lo', 'drc', 'drlt', 'dcpstk', 'dcvt', 'mib', 'ceq', 'seq', 'pstk',
        'pstkl', 're', 'tstkp', 'csho', 'txditc', 'txdb', 'xpp', 'xacc'
    ] if key in XBRL_TAG_MAP
}

IS_FIELDS = {
    key: XBRL_TAG_MAP[key] for key in [
        'sale', 'revt', 'cogs', 'xsga', 'xad', 'xrd', 'xint', 'dp', 'am',
        'oiadp', 'oibdp', 'ebit', 'ib', 'ni', 'ibcom', 'nopi', 'pi',
        'spi', 'fopt', 'ffo', 'epspi', 'epspx', 'txdi', 'txfo', 'txfed', 'txt'
    ] if key in XBRL_TAG_MAP
}

CF_FIELDS = {
    key: XBRL_TAG_MAP[key] for key in [
        'oancf', 'ivncf', 'fincf', 'capx', 'prstkc', 'scstkc', 'sstk',
        'dvc', 'dvp', 'dv', 'dvt', 'dvpa', 'dvpd', 'dvpsx_c', 'dlcch',
        'dltis', 'dltr', 'cshrc'
    ] if key in XBRL_TAG_MAP
}

OTHER_FIELDS = {
    key: XBRL_TAG_MAP[key] for key in [
        'prcc_f', 'prcc_c'
    ] if key in XBRL_TAG_MAP
}

ALL_FIELDS_MAP = {}
ALL_FIELDS_MAP.update(BS_FIELDS)
ALL_FIELDS_MAP.update(IS_FIELDS)
ALL_FIELDS_MAP.update(CF_FIELDS)
ALL_FIELDS_MAP.update(OTHER_FIELDS)


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


def extract_dei_value(xbrl, tags, debug=False):
    """
    Placeholder retained for compatibility; employee extraction removed.
    """
    return None, None


# =============================================================================
# SIC MAPPING (STATIC FILE)
# =============================================================================

def load_sic_mapping():
    """
    Load ticker -> SIC mapping from static Excel (similar to AP_CRSPMonthly).
    Returns a dict with uppercase ticker keys.
    """
    mapping_path = Path("../pyData/Static/ticker_sic_mapping.xlsx")
    if not mapping_path.exists():
        print("⚠️  ticker_sic_mapping.xlsx not found; SIC columns will be empty.")
        return {}
    try:
        data = pd.read_excel(mapping_path)
        ticker_col = None
        sic_col = None
        for col in data.columns:
            col_lower = str(col).strip().lower()
            if col_lower in ['ticker', 'tic', 'symbol']:
                ticker_col = col
            if col_lower in ['sic', 'siccrsp']:
                sic_col = col
        if ticker_col is None or sic_col is None:
            print("⚠️  ticker_sic_mapping.xlsx missing ticker/sic columns; skipping SIC enrichment.")
            return {}
        mapping = {}
        for _, row in data.iterrows():
            tic = row.get(ticker_col)
            sic = row.get(sic_col)
            if pd.notna(tic) and pd.notna(sic):
                try:
                    mapping[str(tic).upper()] = int(sic)
                except Exception:
                    continue
        print(f"✓ Loaded {len(mapping)} SIC mappings from {mapping_path.name}")
        return mapping
    except Exception as e:
        print(f"⚠️  Could not load {mapping_path.name}: {e}")
        return {}


def add_sic_codes(df: pd.DataFrame, sic_map: Dict[str, int]) -> pd.DataFrame:
    """
    Attach SIC (and sic2D) columns using a ticker->SIC mapping.
    """
    if df.empty:
        return df
    df = df.copy()
    if 'ticker' not in df.columns:
        print("⚠️  Ticker column missing; cannot attach SIC codes.")
        return df
    if not sic_map:
        if 'sic' not in df.columns:
            df['sic'] = pd.Series(pd.NA, index=df.index, dtype='Int64')
        else:
            df['sic'] = pd.to_numeric(df['sic'], errors='coerce').astype('Int64')
        df['sic2D'] = pd.Series(pd.NA, index=df.index, dtype='Int64')
        return df
    prev_non_null = df['sic'].notna().sum() if 'sic' in df.columns else 0
    mapped = df['ticker'].astype(str).str.upper().map(sic_map)
    if 'sic' in df.columns:
        df['sic'] = pd.to_numeric(df['sic'], errors='coerce')
        df['sic'] = df['sic'].fillna(mapped)
    else:
        df['sic'] = mapped
    df['sic'] = pd.to_numeric(df['sic'], errors='coerce').astype('Int64')
    df['sic2D'] = (df['sic'] // 100).astype('Int64')
    added = df['sic'].notna().sum() - prev_non_null
    print(f"✓ Added SIC codes for {max(added, 0)} records (total with SIC: {df['sic'].notna().sum()})")
    return df


# =============================================================================
# TICKER TO CIK MAPPING (FALLBACK FOR EDGARTOOLS LIMITATION)
# =============================================================================

_ticker_to_cik_cache_annual = None

def get_ticker_to_cik_mapping_annual() -> Dict[str, str]:
    """
    Get ticker to CIK mapping from SEC company_tickers.json.
    Caches the result to avoid repeated downloads.
    """
    global _ticker_to_cik_cache_annual
    
    if _ticker_to_cik_cache_annual is not None:
        return _ticker_to_cik_cache_annual
    
    url = "https://www.sec.gov/files/company_tickers.json"
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'AP_CompustatAnnual/1.0 (test@example.com)',
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
        
        _ticker_to_cik_cache_annual = mapping
        return mapping
    except Exception as e:
        print(f"⚠️  Warning: Could not load SEC ticker mapping: {e}")
        return {}


def get_company_by_ticker_or_cik_annual(ticker: str) -> Optional:
    """
    Get Company object by ticker, with CIK fallback.
    
    edgartools Company(ticker) sometimes returns None even for valid SEC-registered
    companies. This function tries ticker first, then falls back to CIK lookup.
    """
    # Try direct ticker lookup first
    try:
        company = Company(ticker)
        if company is not None:
            return company
    except Exception:
        pass
    
    # Fallback: Look up CIK and try that
    ticker_to_cik = get_ticker_to_cik_mapping_annual()
    cik = ticker_to_cik.get(ticker.upper())
    
    if cik:
        try:
            company = Company(cik)
            if company is not None:
                return company
        except Exception:
            pass
    
    return None


def get_company_financials_from_10k(ticker: str, years: int = 2, debug: bool = False) -> pd.DataFrame:
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
        
        # Get company object (with CIK fallback)
        company = get_company_by_ticker_or_cik_annual(ticker)
        
        # Check if company was found
        if company is None:
            print(f"  ⚠️  Company not found in SEC EDGAR for ticker {ticker} (even with CIK fallback)")
            return pd.DataFrame()
        
        # Get recent 10-K filings (exclude amendments for cleaner data)
        # Note: amendments parameter removed in newer edgartools versions
        filings = company.get_filings(form='10-K').latest(years)
        
        results = []
        
        for filing in filings:
            try:
                # Parse report_date as date (EntityFiling uses report_date, not period_of_report)
                period_end = filing.report_date
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
                    bs_df = is_df = cf_df = None
                    
                    #Helper function to extract value from statement dataframe
                    def extract_from_statement(df, compustat_field, xbrl_tags, period_end):
                        """Extract value from statement dataframe by matching concept column."""
                        if df is None or df.empty:
                            return None, None
                        
                        # Identify date columns and select the period closest to but <= period_end
                        date_cols = [col for col in df.columns if isinstance(col, str) and '-' in col and len(col) == 10]
                        if not date_cols:
                            return None, None
                        
                        date_map = {}
                        for col in date_cols:
                            parsed = pd.to_datetime(col, errors='coerce')
                            if isinstance(parsed, pd.Timestamp) and not pd.isna(parsed):
                                date_map[parsed] = col
                        
                        target_col = None
                        if date_map:
                            target_period_end = pd.to_datetime(period_end) if period_end is not None else None
                            if isinstance(target_period_end, pd.Timestamp) and not pd.isna(target_period_end):
                                candidates = [d for d in date_map.keys() if d <= target_period_end]
                                target_date = max(candidates) if candidates else max(date_map.keys())
                            else:
                                target_date = max(date_map.keys())
                            target_col = date_map[target_date]
                        else:
                            # Fallback to prior behavior if parsing fails
                            target_col = date_cols[0]
                        
                        # Try each XBRL tag
                        if isinstance(xbrl_tags, str):
                            xbrl_tags = [xbrl_tags]
                        
                        for tag in xbrl_tags:
                            # Look for tag in 'concept' column
                            # XBRL tags are like 'us-gaap_Assets', 'us-gaap_StockholdersEquity'
                            pattern = f'us-gaap_{tag}'
                            
                            search_sets = [
                                df[df['concept'] == pattern],
                                df[df['concept'].str.contains(tag, case=False, na=False)]
                            ]
                            
                            for matches in search_sets:
                                if matches.empty:
                                    continue
                                non_abstract = matches
                                if 'abstract' in matches.columns:
                                    non_abstract = matches[matches['abstract'] != True]
                                if non_abstract.empty:
                                    continue
                                val = non_abstract.iloc[0][target_col]
                                if pd.notna(val):
                                    used_concept = non_abstract.iloc[0].get('concept', pattern)
                                    return float(val), str(used_concept)
                        
                        return None, None
                    
                    # Get balance sheet
                    try:
                        if hasattr(statements, 'balance_sheet'):
                            bs_df = statements.balance_sheet().to_dataframe()
                        if bs_df is not None and 'concept' in bs_df.columns:
                            for compustat_field, xbrl_tags in BS_FIELDS.items():
                                if compustat_field not in record or record[compustat_field] is None:
                                    val, used_tag = extract_from_statement(bs_df, compustat_field, xbrl_tags, period_end)
                                    if val is not None:
                                        record[compustat_field] = val
                                        if used_tag:
                                            record[f"_src_{compustat_field}"] = used_tag
                    except Exception as e:
                        if debug:
                            print(f"    [DEBUG] Balance sheet error: {e}")
                    
                    # Get income statement
                    try:
                        if hasattr(statements, 'income_statement'):
                            is_df = statements.income_statement().to_dataframe()
                        if is_df is not None and 'concept' in is_df.columns:
                            for compustat_field, xbrl_tags in IS_FIELDS.items():
                                if compustat_field not in record or record[compustat_field] is None:
                                    val, used_tag = extract_from_statement(is_df, compustat_field, xbrl_tags, period_end)
                                    if val is not None:
                                        record[compustat_field] = val
                                        if used_tag:
                                            record[f"_src_{compustat_field}"] = used_tag
                    except Exception as e:
                        if debug:
                            print(f"    [DEBUG] Income statement error: {e}")
                    
                    # Get cash flow statement
                    try:
                        if hasattr(statements, 'cashflow_statement'):
                            cf_df = statements.cashflow_statement().to_dataframe()
                        elif hasattr(statements, 'cash_flow'):
                            cf_df = statements.cash_flow().to_dataframe()
                        if cf_df is not None and 'concept' in cf_df.columns:
                            for compustat_field, xbrl_tags in CF_FIELDS.items():
                                if compustat_field not in record or record[compustat_field] is None:
                                    val, used_tag = extract_from_statement(cf_df, compustat_field, xbrl_tags, period_end)
                                    if val is not None:
                                        record[compustat_field] = val
                                        if used_tag:
                                            record[f"_src_{compustat_field}"] = used_tag
                    except Exception as e:
                        if debug:
                            print(f"    [DEBUG] Cash flow error: {e}")

                    # Fallback: try to pull dp from cash flow statement if missing
                    try:
                        if (record.get('dp') is None) and cf_df is not None and 'concept' in cf_df.columns:
                            val, used_tag = extract_from_statement(cf_df, 'dp', XBRL_TAG_MAP.get('dp', []), period_end)
                            if val is not None:
                                record['dp'] = val
                                if used_tag:
                                    record["_src_dp"] = used_tag
                    except Exception as e:
                        if debug:
                            print(f"    [DEBUG] DP fallback from CF error: {e}")
                
                # Fill in any missing fields with None
                for field in list(ALL_FIELDS_MAP.keys()) + DERIVED_ONLY_FIELDS:
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

    # Create derived variable: convertible debt (dc)
    df['dc'] = np.nan
    mask_dc1 = (
        (df['dcpstk'] > df['pstk']) &
        df['pstk'].notna() &
        df['dcpstk'].notna() &
        df['dcvt'].isna()
    )
    df.loc[mask_dc1, 'dc'] = df.loc[mask_dc1, 'dcpstk'] - df.loc[mask_dc1, 'pstk']
    mask_dc2 = (
        df['pstk'].isna() &
        df['dcpstk'].notna() &
        df['dcvt'].isna()
    )
    df.loc[mask_dc2, 'dc'] = df.loc[mask_dc2, 'dcpstk']
    mask_dc3 = df['dc'].isna()
    df.loc[mask_dc3, 'dc'] = df.loc[mask_dc3, 'dcvt']
    
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
    
    # AO (other noncurrent assets) - derive as residual of noncurrent assets
    # ao = at - act - ppent - ivao - intan - gdwl - fatb - fatl
    if 'at' in df.columns and 'act' in df.columns:
        df['ao'] = df['at'] - df['act']
        for comp in ['ppent', 'ivao', 'intan', 'gdwl', 'fatb', 'fatl']:
            if comp in df.columns:
                df['ao'] = df['ao'] - df[comp].fillna(0)
    
    # CEQT (common equity) = Total equity - preferred stock - minority interest
    # ceqt = ceq - pstk - mib
    if 'ceq' in df.columns:
        df['ceqt'] = df['ceq']
        if 'pstk' in df.columns:
            df['ceqt'] = df['ceqt'] - df['pstk'].fillna(0)
        if 'mib' in df.columns:
            df['ceqt'] = df['ceqt'] - df['mib'].fillna(0)

    # Ensure EBIT exists (fallback to operating income) and derive EBITDA
    if 'ebit' not in df.columns or df['ebit'].isna().all():
        if 'oiadp' in df.columns:
            df['ebit'] = df['oiadp']
    if 'ebit' in df.columns:
        df['ebitda'] = (
            df['ebit'].fillna(0)
            + df.get('dp', 0).fillna(0)
            + df.get('am', 0).fillna(0)
        )
    
    # Working capital and its change are always derived (XBRL working capital is inconsistent)
    # wcap = act - lct
    # wcapch = delta(wcap)
    if 'act' in df.columns and 'lct' in df.columns:
        df = df.sort_values(['ticker', 'fyear'])
        df['wcap'] = df['act'] - df['lct']
        df['wcapch'] = df.groupby('ticker')['wcap'].diff()

    # Compustat-style cash & equivalents (cash + short-term marketable securities)
    if 'che' in df.columns or 'ivst' in df.columns:
        df['che_comp'] = df.get('che', 0).fillna(0) + df.get('ivst', 0).fillna(0)

    # Pre-D&A COGS approximation (Compustat-style)
    if 'cogs' in df.columns:
        df['cogs_pre_dp'] = df['cogs']
        if 'dp' in df.columns:
            df['cogs_pre_dp'] = df['cogs_pre_dp'] - df['dp'].fillna(0)
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


def add_year_end_prices(df: pd.DataFrame) -> pd.DataFrame:
    """
    Populate prcc_f (fiscal year-end price) using free yfinance data.
    """
    if df.empty:
        return df
    if not YFINANCE_AVAILABLE:
        print("⚠️  yfinance not installed; prcc_f will remain empty")
        return df
    
    df = df.copy()
    price_cache: Dict = {}
    
    def fetch_price(ticker: str, datadate) -> Optional[float]:
        if pd.isna(ticker) or pd.isna(datadate):
            return np.nan
        try:
            date = pd.to_datetime(datadate).normalize()
            cache_key = (ticker, date.date())
            if cache_key in price_cache:
                return price_cache[cache_key]
            
            start = date - pd.Timedelta(days=10)
            end = date + pd.Timedelta(days=5)
            hist = yf.download(
                ticker,
                start=start.strftime("%Y-%m-%d"),
                end=(end + pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
                progress=False,
                auto_adjust=False,
                threads=False,
            )
            if hist.empty:
                price_cache[cache_key] = np.nan
                return np.nan
            
            hist.index = pd.to_datetime(hist.index)
            price_col = 'Adj Close' if 'Adj Close' in hist.columns else 'Close'
            on_or_before = hist[hist.index <= date]
            if not on_or_before.empty:
                val = float(on_or_before.iloc[-1][price_col])
            else:
                val = float(hist.iloc[0][price_col])
            price_cache[cache_key] = val
            return val
        except Exception:
            return np.nan
    
    df['prcc_f'] = [
        fetch_price(row['ticker'], row['datadate'])
        for _, row in df.iterrows()
    ]
    
    fetched = df['prcc_f'].notna().sum()
    if fetched:
        print(f"✓ Added prcc_f for {fetched} rows using yfinance", flush=True)
    else:
        print("⚠️  prcc_f could not be fetched from yfinance", flush=True)
    
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
    
    # Load ticker universe from S&P 500 pickle file
    import pickle
    universe_path = Path("../pyData/Static/sp500_universe.pkl")
    SAMPLE_TICKERS = []
    
    if universe_path.exists():
        try:
            with open(universe_path, 'rb') as f:
                SAMPLE_TICKERS = pickle.load(f)
            print(f"\n📊 Processing {len(SAMPLE_TICKERS)} tickers from sp500_universe.pkl...\n")
        except Exception as e:
            print(f"⚠️  Could not load sp500_universe.pkl: {e}")
            # Fallback to sample
            SAMPLE_TICKERS = [
                'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META',
                'TSLA', 'NVDA', 'JPM', 'V', 'WMT',
                'JNJ', 'PG', 'MA', 'UNH', 'HD',
            ]
            print(f"\n📊 Processing {len(SAMPLE_TICKERS)} sample tickers...\n")
    else:
        # Fallback to sample if pickle file doesn't exist
        SAMPLE_TICKERS = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META',
            'TSLA', 'NVDA', 'JPM', 'V', 'WMT',
            'JNJ', 'PG', 'MA', 'UNH', 'HD',
        ]
        print(f"\n📊 Processing {len(SAMPLE_TICKERS)} sample tickers...\n")

    # Optional override via CLI arg or environment variable AP_TICKERS
    override = None
    if len(sys.argv) > 1:
        override = sys.argv[1]
    elif os.getenv("AP_TICKERS"):
        override = os.getenv("AP_TICKERS")
    if override:
        SAMPLE_TICKERS = [t.strip().upper() for t in override.split(',') if t.strip()]
        print(f"\n📊 Override tickers from input: {', '.join(SAMPLE_TICKERS)}\n")
    
    # Load SIC mapping from static file (same approach as AP_monthlyCRSP)
    sic_map = load_sic_mapping()
    
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
                                        'fyear', 'time_avail_m'] + list(ALL_FIELDS_MAP.keys()) + DERIVED_ONLY_FIELDS)
    else:
        # Fetch data for each ticker
        all_data = []
        for i, ticker in enumerate(SAMPLE_TICKERS):
            # Enable debug for first ticker to see XBRL structure
            debug = (i == 0)
            df = get_company_financials_from_10k(ticker, years=2, debug=debug)
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
    
    # Add SIC codes using static mapping
    processed_data = add_sic_codes(processed_data, sic_map)

    # Add fiscal year-end prices from free data
    processed_data = add_year_end_prices(processed_data)
    
    # Create output directory
    output_dir = Path("../pyData/Intermediate/")
    output_dir.mkdir(parents=True, exist_ok=True)
    ticker_suffix = f"_{SAMPLE_TICKERS[0]}" if len(SAMPLE_TICKERS) == 1 else ""
    
    # Save annual parquet
    if not processed_data.empty:
        annual_base = f"AP_a_aCompustat{ticker_suffix}"
        processed_data.to_parquet(output_dir / f"{annual_base}.parquet", index=False)
        processed_data.to_csv(output_dir / f"{annual_base}.csv", index=False)
        print(f"✓ Saved {annual_base}.parquet and .csv ({len(processed_data)} records)")
        
        # Create and save monthly version
        monthly_data = create_monthly_version(processed_data)
        monthly_base = f"AP_m_aCompustat{ticker_suffix}"
        monthly_data.to_parquet(output_dir / f"{monthly_base}.parquet", index=False)
        monthly_data.to_csv(output_dir / f"{monthly_base}.csv", index=False)
        print(f"✓ Saved {monthly_base}.parquet and .csv ({len(monthly_data)} records)")
    
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
