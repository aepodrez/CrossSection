# Group 35 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 35.

---

## 171. VarCF.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `mve_permco`
- **m_aCompustat.parquet**: `permno`, `time_avail_m`, `ib`, `dp`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - File exists (from `AP_SignalMasterTable.py`)
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` structure)
  - Contains `mve_permco` (market value of equity at permco level) - included in column list (line 128), comes from `AP_monthlyCRSP.parquet`
  - All required columns are present

- ✅ **AP_m_aCompustat.parquet**: 
  - File exists (from `AP_CompustatAnnual.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `ib` (income before extraordinary items) - mapped from XBRL tags (line 269-271): `['NetIncomeLoss', 'ProfitLoss', 'IncomeLossFromContinuingOperations', ...]`
  - Contains `dp` (depreciation and amortization) - mapped from XBRL tags (line 252-254): `['Depreciation', 'DepreciationAndAmortization', 'DepreciationDepletionAndAmortization', ...]`
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor calculates rolling variance of cash flow to price ratio: `VarCF = var((ib + dp) / mve_permco)` over 60-month window (min 24 periods)
- Uses `asrol` function for rolling standard deviation, then squares to get variance
- **Note**: `mve_permco` comes from `AP_monthlyCRSP.parquet` where it's calculated as `mve_c` (line 370 in `AP_CRSPMonthly.py`)

---

## 172. VolMkt.py

### Required Columns:
- **monthlyCRSP.parquet**: `permno`, `time_avail_m`, `vol`, `prc`, `shrout`

### AP File Status:
- ✅ **AP_monthlyCRSP.parquet**: 
  - File exists (from `AP_CRSPMonthly.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` structure)
  - Contains `vol` (monthly volume in 100s of shares) - line 347, 438
  - Contains `prc` (month-end closing price) - line 346
  - Contains `shrout` (shares outstanding in millions) - line 348
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor calculates volume to market equity ratio: `VolMkt = (12-month avg dollar volume) / mve_c`
- Dollar volume = `vol * abs(prc)`
- Market value = `shrout * abs(prc)`
- Uses 12-month rolling mean of dollar volume (min 10 periods)
- **Note**: `vol` is in 100s of shares, so dollar volume calculation accounts for this

---

## 173. VolSD.py

### Required Columns:
- **monthlyCRSP.parquet**: `permno`, `time_avail_m`, `vol`

### AP File Status:
- ✅ **AP_monthlyCRSP.parquet**: 
  - File exists (from `AP_CRSPMonthly.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` structure)
  - Contains `vol` (monthly volume in 100s of shares) - line 347, 438
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor calculates rolling standard deviation of monthly trading volume: `VolSD = std(vol)` over 36-month window (min 24 periods)
- Uses Polars `rolling_std` function for efficient calculation
- **Note**: `vol` is in 100s of shares

---

## 174. VolumeTrend.py

### Required Columns:
- **monthlyCRSP.parquet**: `permno`, `time_avail_m`, `vol`

### AP File Status:
- ✅ **AP_monthlyCRSP.parquet**: 
  - File exists (from `AP_CRSPMonthly.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` structure)
  - Contains `vol` (monthly volume in 100s of shares) - line 347, 438
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor calculates rolling coefficient from regressing monthly trading volume on linear time trend: `VolumeTrend = beta / meanX`
- Uses 60-month rolling window regression (min 30 periods)
- Time trend: `time_numeric = (year - 1960) * 12 + month - 1`
- Scales coefficient by 60-month average volume (`meanX`)
- Winsorizes at 1st and 99th percentiles
- **Note**: Uses `polars_ols` for rolling OLS regression
- **Note**: `vol` is in 100s of shares

---

## 175. XFIN.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `sstk`, `dv`, `prstkc`, `dltis`, `dltr`, `dlcch`, `at`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - File exists (from `AP_CompustatAnnual.py`)
  - Contains `gvkey` (surrogate: CIK/permno) - from `AP_CompustatAnnual.py` structure
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `sstk` (proceeds from stock issuance) - mapped from XBRL tags (line 222-223): `['ProceedsFromIssuanceOfCommonStock', 'StockIssuedDuringPeriodValueNewIssues', ...]`
  - Contains `dv` (dividends) - mapped from XBRL tags (line 340): `['Dividends', 'PaymentsOfDividends', 'PaymentsOfDividendsCommonStock', ...]`
  - Contains `prstkc` (payments for repurchase of common stock) - mapped from XBRL tags (line 217-218): `['PaymentsForRepurchaseOfCommonStock', 'TreasuryStockValueAcquiredCostMethod', ...]`
  - Contains `dltis` (proceeds from issuance of long-term debt) - mapped from XBRL tags (line 155): `['ProceedsFromIssuanceOfLongTermDebt']`
  - Contains `dltr` (repayments of long-term debt) - mapped from XBRL tags (line 156): `['RepaymentsOfLongTermDebt']`
  - Contains `dlcch` (change in short-term borrowings) - mapped from XBRL tags (line 154): `['IncreaseDecreaseInShortTermBorrowings', 'ProceedsFromRepaymentsOfShortTermDebt']`
  - Contains `at` (total assets) - mapped from XBRL tags (line 84): `['Assets']`
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor calculates net external financing: `XFIN = (sstk - dv - prstkc + dltis - dltr + dlcch) / at`
- Replaces missing `dlcch` with 0 (line 43)
- Scales by total assets (`at`)
- **Note**: `gvkey` is loaded but not used in calculations (only kept for compatibility)
- **Note**: `gvkey` is a surrogate (CIK/permno) in AP version, but doesn't affect calculations

---

## Summary

### Overall Status:
**5 out of 5 predictors can be constructed** ✅

### Column Availability:
- ✅ All required columns are present in AP data sources
- ✅ No missing columns identified
- ✅ No column-related issues found

### Key Notes:
1. **For VarCF**: 
   - **VERIFICATION**: Test that `ib`, `dp`, and `mve_permco` are correctly populated
   - Calculates rolling variance of cash flow to price ratio over 60-month window
   - Uses `asrol` function for rolling standard deviation
   - **Note**: `mve_permco` comes from `AP_monthlyCRSP.parquet` where it's calculated as `mve_c`

2. **For VolMkt**: 
   - **VERIFICATION**: Test that `vol`, `prc`, and `shrout` are correctly populated
   - Calculates 12-month average dollar volume scaled by market value
   - Uses 12-month rolling mean (min 10 periods)
   - **Note**: `vol` is in 100s of shares

3. **For VolSD**: 
   - **VERIFICATION**: Test that `vol` is correctly populated
   - Calculates rolling standard deviation of monthly trading volume over 36-month window
   - Uses Polars `rolling_std` function
   - **Note**: `vol` is in 100s of shares

4. **For VolumeTrend**: 
   - **VERIFICATION**: Test that `vol` is correctly populated
   - Calculates rolling coefficient from regressing volume on time trend
   - Uses 60-month rolling window regression (min 30 periods)
   - Winsorizes at 1st and 99th percentiles
   - **Note**: Uses `polars_ols` for rolling OLS regression
   - **Note**: `vol` is in 100s of shares

5. **For XFIN**: 
   - **VERIFICATION**: Test that `sstk`, `dv`, `prstkc`, `dltis`, `dltr`, `dlcch`, and `at` are correctly populated
   - Calculates net external financing scaled by total assets
   - Replaces missing `dlcch` with 0
   - **Note**: `gvkey` is loaded but not used in calculations (only kept for compatibility)

6. **Note on gvkey Surrogate**: 
   - `gvkey` is not directly available from EDGAR (Compustat-specific identifier)
   - AP version uses `cik` (SEC identifier) as surrogate, falls back to `permno` if unavailable
   - For `XFIN.py`, `gvkey` is loaded but not used in calculations (only kept for compatibility)

7. **Note on File Names**: 
   - File name mismatches are ignored per user instructions (from Group 19 onwards)
   - Predictors will need to be modified to use AP file names (`AP_dailyCRSP.parquet`, `AP_monthlyCRSP.parquet`, etc.) or AP files can be renamed to match expected names

8. **Note on Volume Units**: 
   - `vol` in `AP_monthlyCRSP.parquet` is in 100s of shares (line 242, 347)
   - This matches the original CRSP format and is accounted for in dollar volume calculations

