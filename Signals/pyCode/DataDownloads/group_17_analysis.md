# Group 17 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 17.

---

## 81. GrLTNOA.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `rect`, `invt`, `ppent`, `aco`, `intan`, `ao`, `ap`, `lco`, `lo`, `at`, `dp`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `rect` (accounts receivable) - mapped in XBRL_TAG_MAP line 93
  - Contains `invt` (inventory) - mapped in XBRL_TAG_MAP line 98
  - Contains `ppent` (property, plant, equipment net) - mapped in XBRL_TAG_MAP line 98
  - Contains `aco` (other current assets) - derived in `AP_CompustatAnnual.py` line 972-982
  - Contains `intan` (intangible assets) - mapped in XBRL_TAG_MAP line 114
  - Contains `ao` (other noncurrent assets) - derived in `AP_CompustatAnnual.py` line 984-990
  - Contains `ap` (accounts payable) - mapped in XBRL_TAG_MAP line 152-153
  - Contains `lco` (other current liabilities) - mapped in XBRL_TAG_MAP line 167-168
  - Contains `lo` (other noncurrent liabilities) - mapped in XBRL_TAG_MAP line 170
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 86
  - Contains `dp` (depreciation) - mapped in XBRL_TAG_MAP line 245-247

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate, but predictor only uses it for filtering, not calculation).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 82. GrSaleToGrInv.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `sale`, `invt`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `sale` (sales) - mapped in XBRL_TAG_MAP line 230
  - Contains `invt` (inventory) - mapped in XBRL_TAG_MAP line 98

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 83. GrSaleToGrOverhead.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `sale`, `xsga`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `sale` (sales) - mapped in XBRL_TAG_MAP line 230
  - Contains `xsga` (selling, general, administrative expenses) - mapped in XBRL_TAG_MAP line 239

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 84. Herf.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `permno`, `time_avail_m`, `sale`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `sicCRSP`, `shrcd`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `sale` (sales) - mapped in XBRL_TAG_MAP line 230
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `sicCRSP` (SIC code from CRSP) - from `AP_SignalMasterTable.py` line 134
  - Contains `shrcd` (share code) - from `AP_SignalMasterTable.py` line 131

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 85. HerfAsset.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `permno`, `time_avail_m`, `at`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `sicCRSP`, `shrcd`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 86
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `sicCRSP` (SIC code from CRSP) - from `AP_SignalMasterTable.py` line 134
  - Contains `shrcd` (share code) - from `AP_SignalMasterTable.py` line 131

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## Summary

| Predictor | All Columns Available? | Can Be Constructed? | Additional Work Needed? |
|-----------|----------------------|---------------------|----------------------|
| GrLTNOA | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |
| GrSaleToGrInv | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |
| GrSaleToGrOverhead | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |
| Herf | ✅ Yes | ✅ Yes | None |
| HerfAsset | ✅ Yes | ✅ Yes | None |

### Critical Missing Components:
None. All predictors can be constructed with available AP data.

### Recommendations:
1. **For GrLTNOA**: 
   - **VERIFICATION**: Test that all required columns (`rect`, `invt`, `ppent`, `aco`, `intan`, `ao`, `ap`, `lco`, `lo`, `at`, `dp`) are correctly populated in `AP_m_aCompustat.parquet`
   - Verify `aco` and `ao` derivation logic produces correct values
   - Verify sufficient historical coverage for 12-month lag calculations
   - Note: Predictor calculates growth in long-term net operating assets with working capital adjustment

2. **For GrSaleToGrInv**: 
   - **VERIFICATION**: Test that `sale` and `invt` are correctly populated in `AP_m_aCompustat.parquet`
   - Verify sufficient historical coverage for 12-month and 24-month lag calculations
   - Note: Predictor uses primary formula with 12/24-month average baselines, falls back to 12-month growth if primary unavailable

3. **For GrSaleToGrOverhead**: 
   - **VERIFICATION**: Test that `sale` and `xsga` are correctly populated in `AP_m_aCompustat.parquet`
   - Verify sufficient historical coverage for 12-month and 24-month lag calculations
   - Note: Predictor uses primary formula with 12/24-month average baselines, falls back to 12-month growth if primary unavailable

4. **For Herf**: 
   - **VERIFICATION**: Test that `sale` is correctly populated in `AP_m_aCompustat.parquet`
   - Verify `sicCRSP` and `shrcd` are correctly populated in `AP_SignalMasterTable.parquet`
   - Verify sufficient historical coverage for 36-month rolling average (3-year moving average)
   - Note: Predictor calculates industry concentration (Herfindahl index) based on sales, excludes regulated industries and non-common stock

5. **For HerfAsset**: 
   - **VERIFICATION**: Test that `at` is correctly populated in `AP_m_aCompustat.parquet`
   - Verify `sicCRSP` and `shrcd` are correctly populated in `AP_SignalMasterTable.parquet`
   - Verify sufficient historical coverage for 36-month rolling average (3-year moving average)
   - Note: Predictor calculates industry concentration (Herfindahl index) based on assets, excludes regulated industries and non-common stock

6. **Note on gvkey Surrogate**: 
   - `GrLTNOA.py`, `GrSaleToGrInv.py`, and `GrSaleToGrOverhead.py` use `gvkey` which is not directly available from EDGAR
   - AP version uses `cik` (SEC identifier) as surrogate or falls back to `permno`
   - These predictors only use `gvkey` for filtering/grouping, not for calculations, so surrogate should work fine

7. **Note on Derived Fields**: 
   - `aco` (other current assets) is derived as: `aco = act - (che + rect + invt)`
   - `ao` (other noncurrent assets) is derived as: `ao = at - act - ppent - ivao - intan - gdwl - fatb - fatl`
   - These derivations may not perfectly match Compustat values, but should be sufficient for predictor construction
   - Verify derivation logic produces reasonable values

8. **Note on Historical Coverage**: 
   - `GrLTNOA.py` requires 12-month lags
   - `GrSaleToGrInv.py` and `GrSaleToGrOverhead.py` require 12-month and 24-month lags
   - `Herf.py` and `HerfAsset.py` require 36-month rolling averages (minimum 12 months)
   - Ensure sufficient historical data coverage for these calculations

9. **Note on Industry Classification**: 
   - `Herf.py` and `HerfAsset.py` use 4-digit SIC codes (`sic3D`) from `sicCRSP`
   - Both predictors exclude regulated industries (utilities, transportation, telecommunications) before deregulation dates
   - Both predictors exclude non-common stock (`shrcd > 11`)
   - Verify `sicCRSP` is correctly populated for industry grouping
