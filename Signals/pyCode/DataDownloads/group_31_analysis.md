# Group 31 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 31.

---

## 151. sfe.py

### Required Columns:
- **IBES_EPS_Unadj.parquet**: `fpi`, `statpers`, `fpedats`, `time_avail_m`, `tickerIBES`, `medest`, `numest`
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `tickerIBES`, `prc`
- **m_aCompustat.parquet**: `permno`, `time_avail_m`, `datadate`

### AP File Status:
- ✅ **AP_IBES_EPS_Unadj.parquet**: 
  - File exists (from `AP_IBESEPSUnadjusted.py`)
  - Contains `fpi`, `statpers`, `fpedats`, `time_avail_m`, `tickerIBES` (from `AP_IBESEPSUnadjusted.py` structure)
  - Contains `medest` (median EPS estimate) - mapped from "EPS Median" or "EPS Median Estimate" (line 256-257, 265)
  - Contains `numest` (number of estimates) - mapped from "EPS Number of Estimates" (line 260, 267)
  - All required columns are present

- ✅ **AP_SignalMasterTable.parquet**: 
  - File exists (from `AP_SignalMasterTable.py`)
  - Contains `permno`, `time_avail_m`, `tickerIBES`, `prc` (from `AP_SignalMasterTable.py` structure)
  - All required columns are present

- ✅ **AP_m_aCompustat.parquet**: 
  - File exists (from `AP_CompustatAnnual.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `datadate` (fiscal period end date) - renamed from `period_end` (line 934), included in monthly version (line 1166-1167)
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor filters for `fpi == "1"` (next year forecasts) and March forecasts (`statpers.month == 3`)
- Uses December fiscal year ends only (`datadate.month == 12`)
- Filters for lower analyst coverage (bottom half by `numest`)
- Calculates `sfe = medest / abs(prc)` and holds for 12 months
- **Note**: `tickerIBES` must be populated in `AP_SignalMasterTable.parquet` (requires `AP_IBESCRSPLinkingTable.parquet`)

---

## 152. ShareIss1Y.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`
- **monthlyCRSP.parquet**: `permno`, `time_avail_m`, `shrout`, `cfacshr`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - File exists (from `AP_SignalMasterTable.py`)
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` structure)
  - All required columns are present

- ✅ **AP_monthlyCRSP.parquet**: 
  - File exists (from `AP_CRSPMonthly.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` structure)
  - Contains `shrout` (shares outstanding in millions, back-adjusted by split factor) - line 348
  - Contains `cfacshr` (cumulative share adjustment factor) - line 350
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor calculates growth in adjusted shares outstanding between t-18 and t-6 months
- Formula: `ShareIss1Y = (shrout_6mo * cfacshr_6mo - shrout_18mo * cfacshr_18mo) / (shrout_18mo * cfacshr_18mo)`
- Uses `shrout * cfacshr` to handle stock splits correctly
- **Note**: `cfacshr` is calculated from `facshr` (split factor) in `AP_CRSPMonthly.py` (line 299)

---

## 153. ShareIss5Y.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`
- **monthlyCRSP.parquet**: `permno`, `time_avail_m`, `shrout`, `cfacshr`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - File exists (from `AP_SignalMasterTable.py`)
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` structure)
  - All required columns are present

- ✅ **AP_monthlyCRSP.parquet**: 
  - File exists (from `AP_CRSPMonthly.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` structure)
  - Contains `shrout` (shares outstanding in millions, back-adjusted by split factor) - line 348
  - Contains `cfacshr` (cumulative share adjustment factor) - line 350
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor calculates 5-year growth in adjusted shares outstanding between t-65 and t-5 months
- Formula: `ShareIss5Y = (shrout_5mo * cfacshr_5mo - shrout_65mo * cfacshr_65mo) / (shrout_65mo * cfacshr_65mo)`
- Uses `shrout * cfacshr` to handle stock splits correctly
- **Note**: `cfacshr` is calculated from `facshr` (split factor) in `AP_CRSPMonthly.py` (line 299)

---

## 154. ShareRepurchase.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `prstkc`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - File exists (from `AP_CompustatAnnual.py`)
  - Contains `gvkey` (surrogate: CIK/permno) - from `AP_CompustatAnnual.py` structure
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `prstkc` (payments for repurchase of common stock) - mapped from XBRL tags (line 217, 330)
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor creates binary indicator: `ShareRepurchase = 1` if `prstkc > 0`, `0` otherwise
- Missing `prstkc` values result in missing `ShareRepurchase`
- **Note**: `gvkey` is loaded but not used in calculations (only kept for compatibility)
- **Note**: `prstkc` is extracted from cash flow statement XBRL tags: `PaymentsForRepurchaseOfCommonStock`, `TreasuryStockValueAcquiredCostMethod` (line 217, 330)

---

## 155. ShareVol.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `sicCRSP`, `exchcd`
- **monthlyCRSP.parquet**: `permno`, `time_avail_m`, `shrout`, `vol`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - File exists (from `AP_SignalMasterTable.py`)
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` structure)
  - Contains `sicCRSP` (SIC code from CRSP) - from `AP_SignalMasterTable.py` structure
  - Contains `exchcd` (exchange code) - from `AP_SignalMasterTable.py` structure
  - All required columns are present

- ✅ **AP_monthlyCRSP.parquet**: 
  - File exists (from `AP_CRSPMonthly.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` structure)
  - Contains `shrout` (shares outstanding in millions) - line 348
  - Contains `vol` (monthly volume in 100s of shares) - line 347, 438
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor calculates 3-month rolling share volume: `tempShareVol = (vol + l1_vol + l2_vol) / (3 * shrout) * 100`
- Excludes observations where shares outstanding changed in the past 3 months
- Creates binary signal: `ShareVol = 0` if `tempShareVol < 5`, `1` if `tempShareVol > 10`, missing otherwise
- Missing share volume values are treated as high and assigned `1`
- **Note**: `vol` is extracted from yfinance `Volume` field and converted to 100s of shares (line 321-322)

---

## Summary

### Overall Status:
**5 out of 5 predictors can be constructed** ✅

### Column Availability:
- ✅ All required columns are present in AP data sources
- ✅ No missing columns identified
- ✅ No column-related issues found

### Key Notes:
1. **For sfe**: 
   - **VERIFICATION**: Test that `medest`, `numest`, and `tickerIBES` are correctly populated
   - Requires `AP_IBESCRSPLinkingTable.parquet` to populate `tickerIBES` in `AP_SignalMasterTable.parquet`
   - Filters for December fiscal year ends and lower analyst coverage

2. **For ShareIss1Y and ShareIss5Y**: 
   - **VERIFICATION**: Test that `shrout` and `cfacshr` are correctly calculated
   - Uses adjusted shares (`shrout * cfacshr`) to handle stock splits
   - `cfacshr` is calculated from `facshr` (split factor) in `AP_CRSPMonthly.py`

3. **For ShareRepurchase**: 
   - **VERIFICATION**: Test that `prstkc` is correctly extracted from XBRL cash flow statements
   - Simple binary indicator based on positive `prstkc` values
   - **Note**: `gvkey` is loaded but not used in calculations

4. **For ShareVol**: 
   - **VERIFICATION**: Test that `vol` and `shrout` are correctly populated
   - Calculates 3-month rolling share volume and excludes observations with share changes
   - Creates binary signal based on volume thresholds

5. **Note on gvkey Surrogate**: 
   - `gvkey` is not directly available from EDGAR (Compustat-specific identifier)
   - AP version uses `cik` (SEC identifier) as surrogate, falls back to `permno` if unavailable
   - For `ShareRepurchase.py`, `gvkey` is loaded but not used in calculations (only kept for compatibility)

6. **Note on File Names**: 
   - File name mismatches are ignored per user instructions (from Group 19 onwards)
   - Predictors will need to be modified to use AP file names (`AP_dailyCRSP.parquet`, `AP_monthlyCRSP.parquet`, etc.) or AP files can be renamed to match expected names

