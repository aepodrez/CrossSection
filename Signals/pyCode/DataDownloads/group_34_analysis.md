# Group 34 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 34.

---

## 166. tang.py

### Required Columns:
- **m_aCompustat.parquet**: `permno`, `time_avail_m`, `che`, `rect`, `invt`, `ppegt`, `at`, `sic`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - File exists (from `AP_CompustatAnnual.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `che` (cash and cash equivalents) - mapped from XBRL tags (line 90): `['CashAndCashEquivalentsAtCarryingValue', 'CashAndDueFromBanks', 'Cash']`
  - Contains `rect` (accounts receivable current) - mapped from XBRL tags (line 93): `['AccountsReceivableNetCurrent', 'ReceivablesNetCurrent']`
  - Contains `invt` (inventory net) - mapped from XBRL tags (line 98): `['InventoryNet', 'Inventory']`
  - Contains `ppegt` (PP&E gross) - mapped from XBRL tags (line 107): `['PropertyPlantAndEquipmentGross']`
  - Contains `at` (total assets) - mapped from XBRL tags (line 84): `['Assets']`
  - Contains `sic` (SIC code) - added from static mapping file (line 612-627)
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor calculates tangibility for manufacturing firms (SIC 2000-3999): `tang = (che + 0.715*rect + 0.547*invt + 0.535*ppegt) / at`
- Uses Almeida and Campello (2007) formula for asset tangibility
- Filters for manufacturing firms only (SIC 2000-3999)
- Creates financial constraint measure based on size deciles
- **Note**: `sic` is added from static mapping file (`ticker_sic_mapping.xlsx`) in `AP_CompustatAnnual.py` (line 612-627)

---

## 167. Tax.py

### Required Columns:
- **m_aCompustat.parquet**: `permno`, `time_avail_m`, `txfo`, `txfed`, `ib`, `txt`, `txdi`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - File exists (from `AP_CompustatAnnual.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `txfo` (foreign income tax expense) - mapped from XBRL tags (line 358-359): `['IncomeTaxExpenseBenefitContinuingOperationsForeignIncomeTaxes', ...]`
  - Contains `txfed` (federal income tax expense) - mapped from XBRL tags (line 360-361): `['CurrentFederalTaxExpenseBenefit', 'FederalIncomeTaxExpenseBenefitContinuingOperations', ...]`
  - Contains `ib` (income before extraordinary items) - mapped from XBRL tags (line 269-271): `['NetIncomeLoss', 'ProfitLoss', 'IncomeLossFromContinuingOperations', ...]`
  - Contains `txt` (total income tax expense) - mapped from XBRL tags (line 363): `['IncomeTaxExpenseBenefit', 'IncomeTaxExpenseBenefitContinuingOperations']`
  - Contains `txdi` (deferred income tax expense) - mapped from XBRL tags (line 356-357): `['DeferredIncomeTaxExpenseBenefit', 'DeferredFederalStateAndLocalTaxExpenseBenefit', ...]`
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor calculates taxable income to income ratio: `Tax = ((txfo + txfed) / tr) / ib` or `Tax = ((txt - txdi) / tr) / ib` if foreign/federal taxes missing
- Uses time-varying tax rates (`tr`) based on year (0.48 default, 0.46 for 1979-1986, 0.4 for 1987, 0.34 for 1988-1992, 0.35 for 1993+)
- Handles division by zero cases (sets `Tax = 1` when `ib = 0` and tax activity exists)
- Sets `Tax = 1` when company has tax expense but negative income

---

## 168. TotalAccruals.py

### Required Columns:
- **m_aCompustat.parquet**: `permno`, `time_avail_m`, `ivao`, `ivst`, `dltt`, `dlc`, `pstk`, `sstk`, `prstkc`, `dv`, `act`, `che`, `lct`, `at`, `lt`, `ni`, `oancf`, `ivncf`, `fincf`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - File exists (from `AP_CompustatAnnual.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `ivao` (investments and other noncurrent assets) - mapped from XBRL tags (line 123-126): `['InvestmentsAndOtherNoncurrentAssets', 'OtherLongTermInvestments', ...]`
  - Contains `ivst` (short-term investments) - mapped from XBRL tags (line 122): `['ShortTermInvestments', 'MarketableSecuritiesCurrent', 'AvailableForSaleSecurities']`
  - Contains `dltt` (long-term debt) - mapped from XBRL tags (line 149-151): `['LongTermDebtNoncurrent', 'LongTermDebt', ...]`
  - Contains `dlc` (debt current) - mapped from XBRL tags (line 143-146): `['DebtCurrent', 'ShortTermBorrowings', ...]`
  - Contains `pstk` (preferred stock) - mapped from XBRL tags (line 196): `['PreferredStockValue', 'PreferredStockValueOutstanding', 'PreferredStockCarryingAmount']`
  - Contains `sstk` (proceeds from stock issuance) - mapped from XBRL tags (line 222-223): `['ProceedsFromIssuanceOfCommonStock', 'StockIssuedDuringPeriodValueNewIssues', ...]`
  - Contains `prstkc` (payments for repurchase of common stock) - mapped from XBRL tags (line 217-218): `['PaymentsForRepurchaseOfCommonStock', 'TreasuryStockValueAcquiredCostMethod', ...]`
  - Contains `dv` (dividends) - mapped from XBRL tags (line 340): `['Dividends', 'PaymentsOfDividends', 'PaymentsOfDividendsCommonStock', ...]`
  - Contains `act` (current assets) - mapped from XBRL tags (line 87): `['AssetsCurrent']`
  - Contains `che` (cash) - mapped from XBRL tags (line 90): `['CashAndCashEquivalentsAtCarryingValue', 'CashAndDueFromBanks', 'Cash']`
  - Contains `lct` (current liabilities) - mapped from XBRL tags (line 140): `['LiabilitiesCurrent']`
  - Contains `at` (total assets) - mapped from XBRL tags (line 84): `['Assets']`
  - Contains `lt` (total liabilities) - mapped from XBRL tags (line 137): `['Liabilities']`
  - Contains `ni` (net income) - mapped from XBRL tags (line 272-274): `['NetIncomeLoss', 'ProfitLoss', 'NetIncomeLossAvailableToCommonStockholdersBasic', ...]`
  - Contains `oancf` (operating cash flow) - mapped from XBRL tags (line 306): `['NetCashProvidedByUsedInOperatingActivities', ...]`
  - Contains `ivncf` (investing cash flow) - mapped from XBRL tags (line 311): `['NetCashProvidedByUsedInInvestingActivities', ...]`
  - Contains `fincf` (financing cash flow) - mapped from XBRL tags (line 316): `['NetCashProvidedByUsedInFinancingActivities', ...]`
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor calculates total accruals using balance sheet method (before 1988) or cash flow method (1988+)
- Balance sheet method: `TotalAccruals = (ΔWC + ΔNC + ΔFI) / at_lag12`
- Cash flow method: `TotalAccruals = (ni - (oancf + ivncf + fincf) + (sstk - prstkc - dv)) / at_lag12`
- Uses 12-month lagged assets for scaling
- Missing values set to 0 for temporary variables before calculation

---

## 169. TrendFactor.py

### Required Columns:
- **dailyCRSP.parquet**: `permno`, `time_d`, `prc`, `cfacpr`
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `ret`, `prc`, `exchcd`, `shrcd`, `mve_c`

### AP File Status:
- ✅ **AP_dailyCRSP.parquet**: 
  - File exists (from `AP_CRSPDaily.py`)
  - Contains `permno`, `time_d` (from `AP_CRSPDaily.py` structure)
  - Contains `prc` (stock price) - from `AP_CRSPDaily.py` structure
  - Contains `cfacpr` (cumulative price adjustment factor) - calculated from split factors (line 164 in `AP_CRSPDaily.py`)
  - All required columns are present

- ✅ **AP_SignalMasterTable.parquet**: 
  - File exists (from `AP_SignalMasterTable.py`)
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` structure)
  - Contains `ret` (monthly returns) - comes from `AP_monthlyCRSP.parquet`
  - Contains `prc` (stock price) - comes from `AP_monthlyCRSP.parquet`
  - Contains `exchcd` (exchange code) - from `AP_SignalMasterTable.py` structure
  - Contains `shrcd` (share class code) - from `AP_SignalMasterTable.py` structure
  - Contains `mve_c` (market value of equity) - included in column list (line 127), comes from `AP_monthlyCRSP.parquet`
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor calculates price trend factor using past 20-day returns and volumes
- Computes moving average prices for various lag lengths (3, 5, 10, 20, 50, 100, 200, 400, 600, 800, 1000 days)
- Runs cross-sectional regressions by month: `fRet ~ A_*` (future returns on moving averages)
- Computes 12-month rolling averages of beta coefficients (excludes current month)
- Calculates `TrendFactor` as smoothed regression model predictions: `TrendFactor = sum(EBeta_L * A_L)` for all lag lengths
- Filters for NYSE/AMEX/NASDAQ stocks, common shares only, price >= $5, and size >= NYSE 10th percentile
- **Note**: Uses `asreg_collinear` function for cross-sectional regressions with collinearity handling

---

## 170. UpRecomm.py

### Required Columns:
- **IBES_Recommendations.parquet**: `tickerIBES`, `amaskcd`, `anndats`, `time_avail_m`, `ireccd`
- **SignalMasterTable.parquet**: `permno`, `tickerIBES`, `time_avail_m`

### AP File Status:
- ❌ **AP_IBES_Recommendations.parquet**: **FILE DOES NOT EXIST YET**
  - Script exists: `AP_IBESRecommendations.py` (generates `AP_IBES_Recommendations.parquet`)
  - Would contain `tickerIBES` (IBES ticker) - from `AP_IBESRecommendations.py` structure
  - Would contain `amaskcd` (analyst mask code) - generated from broker_name + analyst_name (line 256-258 in `AP_IBESRecommendations.py`)
  - Would contain `anndats` (announcement date) - mapped from Eikon/LSEG API (line 234-235, 251-252)
  - Would contain `time_avail_m` (monthly availability date) - derived from `anndats` (line 305-306)
  - Would contain `ireccd` (recommendation code) - mapped from Eikon/LSEG API (line 236, 247-248)
  - **Note**: File name mismatch - predictor expects `IBES_Recommendations.parquet`, AP version would be `AP_IBES_Recommendations.parquet`
  - **Note**: Requires Eikon/LSEG API subscription (proprietary data source)

- ✅ **AP_SignalMasterTable.parquet**: 
  - File exists (from `AP_SignalMasterTable.py`)
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` structure)
  - Contains `tickerIBES` (IBES ticker) - populated from `AP_IBESCRSPLinkingTable.parquet` if available (line 99-116)
  - All required columns are present

### Can Be Constructed?
**NO** - Missing required data:
- ❌ `AP_IBES_Recommendations.parquet` does not exist (proprietary IBES recommendations data)

### Additional Work Needed?
**YES** - Missing file:

#### What Needs to Be Done:
1. **Create AP_IBES_Recommendations.parquet**: 
   - Run `AP_IBESRecommendations.py` to generate `AP_IBES_Recommendations.parquet`
   - Requires Eikon/LSEG API subscription (proprietary data source)
   - **Note**: File name mismatch - predictor expects `IBES_Recommendations.parquet`, AP version would be `AP_IBES_Recommendations.parquet`

#### Implementation Notes:
- Predictor creates binary indicator for analyst recommendation upgrades: `UpRecomm = 1` if `ireccd < ireccd_lag`, `0` otherwise
- Aggregates analyst-firm-month recommendations (last non-missing per analyst, then mean across analysts)
- Uses month-over-month changes to identify upgrades (lower values = better recommendations)
- **Note**: Requires `tickerIBES` to be populated in `AP_SignalMasterTable.parquet` (requires `AP_IBESCRSPLinkingTable.parquet`)

---

## Summary

### Overall Status:
**4 out of 5 predictors can be constructed** ✅

### Column Availability:
- ✅ **tang.py**: All required columns are present
- ✅ **Tax.py**: All required columns are present
- ✅ **TotalAccruals.py**: All required columns are present
- ✅ **TrendFactor.py**: All required columns are present
- ❌ **UpRecomm.py**: Missing `AP_IBES_Recommendations.parquet` (proprietary IBES recommendations data)

### Key Notes:
1. **For tang**: 
   - **VERIFICATION**: Test that `che`, `rect`, `invt`, `ppegt`, `at`, and `sic` are correctly populated
   - Uses Almeida and Campello formula for asset tangibility
   - Filters for manufacturing firms only (SIC 2000-3999)
   - **Note**: `sic` is added from static mapping file (`ticker_sic_mapping.xlsx`)

2. **For Tax**: 
   - **VERIFICATION**: Test that `txfo`, `txfed`, `ib`, `txt`, and `txdi` are correctly populated
   - Uses time-varying tax rates based on year
   - Handles division by zero cases and missing tax components
   - **Note**: Complex logic for handling missing foreign/federal taxes

3. **For TotalAccruals**: 
   - **VERIFICATION**: Test that all accrual-related columns are correctly populated
   - Uses balance sheet method (before 1988) or cash flow method (1988+)
   - Requires 12-month lagged assets for scaling
   - **Note**: Many columns required, but all are available in AP data

4. **For TrendFactor**: 
   - **VERIFICATION**: Test that `prc`, `cfacpr`, `ret`, `exchcd`, `shrcd`, and `mve_c` are correctly populated
   - Computes moving averages for 11 different lag lengths
   - Runs cross-sectional regressions by month
   - Computes 12-month rolling averages of beta coefficients
   - **Note**: Complex predictor requiring daily data and cross-sectional regressions

5. **For UpRecomm**: 
   - **VERIFICATION**: Test that `tickerIBES` is correctly populated in `AP_SignalMasterTable.parquet`
   - Requires `AP_IBES_Recommendations.parquet` (does not exist yet)
   - Requires Eikon/LSEG API subscription (proprietary data source)
   - **Note**: File name mismatch - predictor expects `IBES_Recommendations.parquet`, AP version would be `AP_IBES_Recommendations.parquet`

6. **Note on File Names**: 
   - File name mismatches are ignored per user instructions (from Group 19 onwards)
   - Predictors will need to be modified to use AP file names (`AP_dailyCRSP.parquet`, `AP_monthlyCRSP.parquet`, etc.) or AP files can be renamed to match expected names

7. **Note on IBES Recommendations**: 
   - IBES recommendations data is proprietary and requires Eikon/LSEG API subscription
   - `AP_IBESRecommendations.py` script exists but needs to be run to generate `AP_IBES_Recommendations.parquet`
   - This affects `UpRecomm.py` and other predictors that use IBES recommendations data (Groups 5, 8, 12, 29)

