# Group 11 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 11.

---

## 51. DelLTI.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `at`, `ivao`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 77
  - Contains `ivao` (investments and other noncurrent assets) - mapped in XBRL_TAG_MAP line 116-119

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 52. DelNetFin.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `at`, `pstk`, `dltt`, `dlc`, `ivst`, `ivao`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 77
  - Contains `pstk` (preferred stock) - mapped in XBRL_TAG_MAP line 189
  - Contains `dltt` (long-term debt) - mapped in XBRL_TAG_MAP line 142-144
  - Contains `dlc` (debt current) - mapped in XBRL_TAG_MAP line 136-139
  - Contains `ivst` (short-term investments) - mapped in XBRL_TAG_MAP line 115
  - Contains `ivao` (investments and other noncurrent assets) - mapped in XBRL_TAG_MAP line 116-119

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 53. DivInit.py

### Required Columns:
- **AP_CRSPdistributions.parquet**: `permno`, `exdt`, `cd2`, `divamt`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `exchcd`, `shrcd`

### AP File Status:
- ✅ **AP_CRSPdistributions.parquet**: 
  - File exists (from `AP_CRSPDistributions.py`)
  - Contains `permno` (from `AP_CRSPDistributions.py` line 223)
  - Contains `exdt` (ex-dividend date) - from `AP_CRSPDistributions.py` line 225
  - Contains `cd2` (distribution code digit 2) - from `AP_CRSPDistributions.py` line 264
  - Contains `divamt` (dividend amount) - from `AP_CRSPDistributions.py` line 226
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `exchcd` (exchange code) - from `AP_SignalMasterTable.py` line 130
  - Contains `shrcd` (share code) - from `AP_SignalMasterTable.py` line 131

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 54. DivOmit.py

### Required Columns:
- **AP_CRSPdistributions.parquet**: `permno`, `exdt`, `divamt`, `cd2`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `exchcd`, `shrcd`

### AP File Status:
- ✅ **AP_CRSPdistributions.parquet**: 
  - File exists (from `AP_CRSPDistributions.py`)
  - Contains `permno` (from `AP_CRSPDistributions.py` line 223)
  - Contains `exdt` (ex-dividend date) - from `AP_CRSPDistributions.py` line 225
  - Contains `divamt` (dividend amount) - from `AP_CRSPDistributions.py` line 226
  - Contains `cd2` (distribution code digit 2) - from `AP_CRSPDistributions.py` line 264
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `exchcd` (exchange code) - from `AP_SignalMasterTable.py` line 130
  - Contains `shrcd` (share code) - from `AP_SignalMasterTable.py` line 131

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 55. DivSeason.py

### Required Columns:
- **AP_CRSPdistributions.parquet**: `permno`, `cd1`, `cd2`, `cd3`, `divamt`, `exdt`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`

### AP File Status:
- ✅ **AP_CRSPdistributions.parquet**: 
  - File exists (from `AP_CRSPDistributions.py`)
  - Contains `permno` (from `AP_CRSPDistributions.py` line 223)
  - Contains `cd1` (distribution code digit 1) - from `AP_CRSPDistributions.py` line 263
  - Contains `cd2` (distribution code digit 2) - from `AP_CRSPDistributions.py` line 264
  - Contains `cd3` (distribution code digit 3) - from `AP_CRSPDistributions.py` line 265
  - Contains `divamt` (dividend amount) - from `AP_CRSPDistributions.py` line 226
  - Contains `exdt` (ex-dividend date) - from `AP_CRSPDistributions.py` line 225
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## Summary

| Predictor | All Columns Available? | Can Be Constructed? | Additional Work Needed? |
|-----------|----------------------|---------------------|----------------------|
| DelLTI | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |
| DelNetFin | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |
| DivInit | ✅ Yes | ✅ Yes | None |
| DivOmit | ✅ Yes | ✅ Yes | None |
| DivSeason | ✅ Yes | ✅ Yes | None |

### Critical Missing Components:
None - All required columns are available.

### Recommendations:
1. **Verification**: 
   - Test that all AP files actually contain the columns listed above by loading sample data
   - Verify `ivao` (investments and other noncurrent assets) is correctly populated in `AP_m_aCompustat.parquet`
   - Verify `ivst` (short-term investments) is correctly populated in `AP_m_aCompustat.parquet`
   - Verify `AP_CRSPdistributions.parquet` has complete dividend history (may be limited by yfinance data availability)
   - Verify distribution codes (`cd1`, `cd2`, `cd3`) are correctly assigned in `AP_CRSPdistributions.parquet`

2. **Note on gvkey**: 
   - `DelLTI` and `DelNetFin` use surrogate `gvkey` (CIK/permno) instead of true Compustat gvkey
   - This is acceptable as `gvkey` is typically only used for grouping/identification, not calculations

3. **Note on AP_CRSPdistributions.parquet**: 
   - Generated by `AP_CRSPDistributions.py` using yfinance
   - Historical dividend data typically available from ~2000 onwards (yfinance limitation)
   - Distribution codes are approximated based on dividend amounts and split ratios
   - May not include all special distributions (spinoffs, rights) comprehensively
   - **Coverage**: Good for regular dividends and splits, but may miss some special distributions

4. **Data Quality Checks**: 
   - All dividend predictors (`DivInit`, `DivOmit`, `DivSeason`) rely on accurate dividend history
   - Verify that `AP_CRSPdistributions.parquet` has sufficient historical coverage for rolling window calculations
   - `DivInit` uses 24-month rolling windows, `DivOmit` uses 3/6/12/18/24-month windows, `DivSeason` uses 12-month windows
   - Ensure dividend amounts (`divamt`) are correctly extracted from yfinance
   - Verify distribution code digits (`cd1`, `cd2`, `cd3`) correctly classify dividend types

5. **Historical Data Limitations**: 
   - yfinance dividend data may have limited historical coverage compared to CRSP
   - Some older dividend records may be missing
   - This could affect predictors that require long historical windows (e.g., `DivOmit` uses 24-month windows)
   - Consider data availability when interpreting results for early periods
