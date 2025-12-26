# Group 15 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 15.

---

## 71. ExclExp.py

### Required Columns:
- **AP_SignalMasterTable.parquet**: `permno`, `gvkey`, `time_avail_m`, `tickerIBES`
- **AP_m_QCompustat.parquet**: `gvkey`, `time_avail_m`, `epspiq`
- **AP_IBES_UnadjustedActuals.parquet**: `tickerIBES`, `time_avail_m`, `int0a`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` line 119)
  - Contains `tickerIBES` column - should be populated from `AP_IBESCRSPLinkingTable.parquet` (after fix in Group 2)
- ✅ **AP_m_QCompustat.parquet**: 
  - Contains `gvkey` (surrogate: CIK/permno) - from `AP_CompustatQuarterly.py` structure
  - Contains `time_avail_m` (from `AP_CompustatQuarterly.py` structure)
  - Contains `epspiq` (earnings per share basic quarterly) - mapped in XBRL_TAG_MAP line 206
- ⚠️ **AP_IBES_UnadjustedActuals.parquet**: 
  - Script exists: `AP_IBESUnadjustedActuals.py` (generates `AP_IBES_UnadjustedActuals.parquet`)
  - File may not exist yet - needs to be generated
  - Would contain `tickerIBES` (from `AP_IBESUnadjustedActuals.py` structure)
  - Would contain `time_avail_m` (from `AP_IBESUnadjustedActuals.py` line 301)
  - Would contain `int0a` (actual EPS unadjusted) - from `AP_IBESUnadjustedActuals.py` line 229, 254

### Can Be Constructed?
**PARTIALLY** - Missing `AP_IBES_UnadjustedActuals.parquet` file.

### Additional Work Needed?
**YES** - Need to run `AP_IBESUnadjustedActuals.py` to generate `AP_IBES_UnadjustedActuals.parquet`.

#### What Needs to Be Done:
1. **Run AP_IBESUnadjustedActuals.py**: Execute the script to download IBES unadjusted actual earnings from Eikon/LSEG API
2. **Requirements**:
   - Eikon/LSEG API access (proprietary data source)
   - `eikon` Python package installed
   - Eikon API credentials configured
3. **Output**: Generates `AP_IBES_UnadjustedActuals.parquet` with required columns

#### Implementation Notes:
- The script `AP_IBESUnadjustedActuals.py` exists and is ready to run
- Requires Eikon/LSEG API access (paid subscription)
- If Eikon is not available, this predictor cannot be constructed with AP data
- The predictor calculates excluded expenses as the difference between IBES unadjusted earnings (`int0a`) and Compustat quarterly EPS (`epspiq`)

---

## 72. FEPS.py

### Required Columns:
- **AP_IBES_EPS_Unadj.parquet**: `tickerIBES`, `time_avail_m`, `meanest`, `fpi`
- **AP_SignalMasterTable.parquet**: `permno`, `tickerIBES`, `time_avail_m`

### AP File Status:
- ✅ **AP_IBES_EPS_Unadj.parquet**: 
  - File exists (from `AP_IBESEPSUnadjusted.py`)
  - Contains `tickerIBES` (from `AP_IBESEPSUnadjusted.py` line 253)
  - Contains `time_avail_m` (from `AP_IBESEPSUnadjusted.py` line 277-278)
  - Contains `meanest` (mean estimate) - from `AP_IBESEPSUnadjusted.py` line 298
  - Contains `fpi` (forecast period indicator) - from `AP_IBESEPSUnadjusted.py` line 295
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `tickerIBES` column - should be populated from `AP_IBESCRSPLinkingTable.parquet` (after fix in Group 2)

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 73. fgr5yrLag.py

### Required Columns:
- **AP_IBES_EPS_Unadj.parquet**: `tickerIBES`, `time_avail_m`, `meanest`, `fpi`
- **AP_m_aCompustat.parquet**: `permno`, `time_avail_m`, `ceq`, `ib`, `txdi`, `dv`, `sale`, `ni`, `dp`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `tickerIBES`

### AP File Status:
- ✅ **AP_IBES_EPS_Unadj.parquet**: 
  - File exists (from `AP_IBESEPSUnadjusted.py`)
  - Contains `tickerIBES` (from `AP_IBESEPSUnadjusted.py` line 253)
  - Contains `time_avail_m` (from `AP_IBESEPSUnadjusted.py` line 277-278)
  - Contains `meanest` (mean estimate) - from `AP_IBESEPSUnadjusted.py` line 298
  - Contains `fpi` (forecast period indicator) - from `AP_IBESEPSUnadjusted.py` line 295
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184
  - Contains `ib` (income before extraordinary items) - mapped in XBRL_TAG_MAP line 262-264
  - Contains `txdi` (deferred income tax expense) - mapped in XBRL_TAG_MAP line 349-350, included in IS_FIELDS line 404
  - Contains `dv` (dividends) - mapped in XBRL_TAG_MAP line 333-334, included in CF_FIELDS line 411
  - Contains `sale` (sales) - mapped in XBRL_TAG_MAP line 238-240
  - Contains `ni` (net income) - mapped in XBRL_TAG_MAP line 265-267, included in IS_FIELDS line 403
  - Contains `dp` (depreciation) - mapped in XBRL_TAG_MAP line 245-247
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `tickerIBES` column - should be populated from `AP_IBESCRSPLinkingTable.parquet` (after fix in Group 2)

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 74. FirmAge.py

### Required Columns:
- **AP_SignalMasterTable.parquet**: `gvkey`, `permno`, `time_avail_m`, `exchcd`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` line 119)
  - Contains `exchcd` (exchange code) - from `AP_SignalMasterTable.py` line 130

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate, but predictor only uses it for filtering, not calculation).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 75. FirmAgeMom.py

### Required Columns:
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `ret`, `prc`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `ret` (return) - from `AP_SignalMasterTable.py` line 133
  - Contains `prc` (price) - from `AP_SignalMasterTable.py` line 132

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## Summary

| Predictor | All Columns Available? | Can Be Constructed? | Additional Work Needed? |
|-----------|----------------------|---------------------|----------------------|
| ExclExp | ⚠️ Partial | ⚠️ Partial | **Run AP_IBESUnadjustedActuals.py** |
| FEPS | ✅ Yes | ✅ Yes | None |
| fgr5yrLag | ✅ Yes | ✅ Yes | None |
| FirmAge | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |
| FirmAgeMom | ✅ Yes | ✅ Yes | None |

### Critical Missing Components:
1. **`AP_IBES_UnadjustedActuals.parquet`**: Required for `ExclExp.py`. File does not exist yet - needs to be generated by running `AP_IBESUnadjustedActuals.py`.

### Recommendations:
1. **For ExclExp**: 
   - **CRITICAL**: Run `AP_IBESUnadjustedActuals.py` to generate `AP_IBES_UnadjustedActuals.parquet`
   - Requires Eikon/LSEG API subscription (proprietary data source)
   - Verify `epspiq` is correctly populated in `AP_m_QCompustat.parquet`
   - Verify `tickerIBES` is correctly populated in `AP_SignalMasterTable.parquet` for merging

2. **For FEPS**: 
   - **VERIFICATION**: Test that `meanest` is correctly populated in `AP_IBES_EPS_Unadj.parquet`
   - Verify `tickerIBES` is correctly populated in `AP_SignalMasterTable.parquet`
   - Verify `fpi == "1"` filter works correctly (predictor filters for 1-year ahead forecasts)

3. **For fgr5yrLag**: 
   - **VERIFICATION**: Test that all required columns (`ceq`, `ib`, `txdi`, `dv`, `sale`, `ni`, `dp`) are correctly populated in `AP_m_aCompustat.parquet`
   - Verify `fpi == "0"` filter works correctly (predictor filters for long-term growth forecasts)
   - Verify `tickerIBES` is correctly populated in `AP_SignalMasterTable.parquet`
   - Note: Predictor restricts to June observations and expands to 12 monthly observations

4. **For FirmAge**: 
   - **VERIFICATION**: Test that `exchcd` is correctly populated in `AP_SignalMasterTable.parquet`
   - Verify sufficient historical coverage (predictor calculates months since first appearance)
   - Note: Predictor excludes firms that started trading when CRSP began (July 1926)

5. **For FirmAgeMom**: 
   - **VERIFICATION**: Test that `ret` and `prc` are correctly populated in `AP_SignalMasterTable.parquet`
   - Verify sufficient historical coverage for 6-month momentum calculation (requires at least 12 months of history)
   - Note: Predictor filters for stocks with price >= $5 and restricts to youngest quintile (bottom 20%) by age

6. **Note on gvkey Surrogate**: 
   - `ExclExp.py` and `FirmAge.py` use `gvkey` which is not directly available from EDGAR
   - AP version uses `cik` (SEC identifier) as surrogate or falls back to `permno`
   - This should work for most predictors, but verify linking logic if issues arise

7. **Note on IBES Data**: 
   - `ExclExp.py` and `FEPS.py` require IBES data
   - `AP_IBES_EPS_Unadj.parquet` exists and can be used for `FEPS.py`
   - `AP_IBES_UnadjustedActuals.parquet` needs to be generated for `ExclExp.py`
   - Both require `tickerIBES` to be populated in `AP_SignalMasterTable.parquet` (from `AP_IBESCRSPLinkingTable.parquet`)

8. **Note on Forecast Period Indicators**: 
   - `FEPS.py` filters for `fpi == "1"` (1-year ahead forecast)
   - `fgr5yrLag.py` filters for `fpi == "0"` (long-term growth forecast)
   - Verify `AP_IBES_EPS_Unadj.parquet` contains both forecast periods
