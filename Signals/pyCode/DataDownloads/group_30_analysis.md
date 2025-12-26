# Group 30 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 30.

---

## 146. ReturnSkew.py

### Required Columns:
- **dailyCRSP.parquet**: `permno`, `time_d`, `ret`

### AP File Status:
- ⚠️ **dailyCRSP.parquet**: **FILE NAME MISMATCH**
  - Predictor expects `dailyCRSP.parquet`
  - AP version is `AP_dailyCRSP.parquet` (from `AP_CRSPDaily.py`)
  - Contains `permno`, `time_d` (from `AP_CRSPDaily.py` structure)
  - Contains `ret` (returns) - from `AP_CRSPDaily.py` structure

### Can Be Constructed?
**YES** - All required columns are present (file name mismatch).

### Additional Work Needed?
**MINOR** - File name mismatch:

#### What Needs to Be Done:
1. **File Name Mismatch**: 
   - Predictor expects `dailyCRSP.parquet`
   - AP version is `AP_dailyCRSP.parquet`
   - **Solution**: Either rename `AP_dailyCRSP.parquet` to `dailyCRSP.parquet` or modify predictor to use `AP_dailyCRSP.parquet`

#### Implementation Notes:
- Predictor calculates skewness of daily returns within each month
- Formula: `ReturnSkew = skewness(ret)` by `permno` and `time_avail_m`
- Requires minimum 15 daily observations per permno-month for valid calculation
- Uses Polars for efficient group-by operations

---

## 147. REV6.py

### Required Columns:
- **IBES_EPS_Unadj.parquet**: `tickerIBES`, `time_avail_m`, `fpi`, `fpedats`, `statpers`, `meanest`
- **SignalMasterTable.parquet**: `permno`, `tickerIBES`, `time_avail_m`, `prc`

### AP File Status:
- ✅ **AP_IBES_EPS_Unadj.parquet**: 
  - File exists (from `AP_IBESEPSUnadjusted.py`)
  - Contains `tickerIBES`, `time_avail_m` (from `AP_IBESEPSUnadjusted.py` structure)
  - Contains `fpi` (forecast period indicator) - from `AP_IBESEPSUnadjusted.py` structure
  - Contains `fpedats` (forecast period end date) - from `AP_IBESEPSUnadjusted.py` structure
  - Contains `statpers` (statement period end date) - from `AP_IBESEPSUnadjusted.py` structure
  - Contains `meanest` (mean estimate) - from `AP_IBESEPSUnadjusted.py` structure
  - **Note**: Predictor filters for `fpi == "1"` (1-year ahead forecasts)
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `tickerIBES` (IBES ticker) - from `AP_SignalMasterTable.py` line 138 (merged from `AP_IBESCRSPLinkingTable.parquet` if available)
  - Contains `prc` (price) - from `AP_CRSPMonthly.py` line 349, included in `AP_SignalMasterTable.py` line 132

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates 6-month sum of monthly changes in mean earnings estimates scaled by prior month stock price
- Formula: `REV6 = sum(tempRev + l1.tempRev + l2.tempRev + l3.tempRev + l4.tempRev + l5.tempRev + l6.tempRev)`
- Where `tempRev = (meanest - l1.meanest) / abs(l1.prc)`
- Filters for `fpi == "1"` (1-year ahead forecasts)
- Uses conditional fill-forward logic for missing estimates when forecast periods match
- Requires valid `tickerIBES` mapping (from `AP_IBESCRSPLinkingTable.parquet`)

---

## 148. RevenueSurprise.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `gvkey`, `time_avail_m`
- **m_QCompustat.parquet**: `gvkey`, `time_avail_m`, `revtq`, `cshprq`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` line 119)
- ✅ **AP_m_QCompustat.parquet**: 
  - File exists (from `AP_CompustatQuarterly.py`)
  - Contains `time_avail_m` (from `AP_CompustatQuarterly.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `revtq` (quarterly revenue) - mapped in XBRL_TAG_MAP line 167
  - Contains `cshprq` (quarterly shares repurchased) - mapped in XBRL_TAG_MAP line 161-162
  - ✅ **`cshoq`**: Available - quarterly shares outstanding mapped in XBRL_TAG_MAP line 160
  - **Note**: Predictor uses `cshprq` (shares repurchased) in formula: `revps = revtq / cshprq`. This seems unusual as revenue per share typically uses shares outstanding (`cshoq`), not shares repurchased. However, the code explicitly uses `cshprq`, so both columns are available if needed.

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- **Note**: Predictor uses `cshprq` (shares repurchased) in formula: `revps = revtq / cshprq`
- This is unusual as revenue per share typically uses shares outstanding (`cshoq`), not shares repurchased
- However, the code explicitly uses `cshprq`, and both `cshprq` and `cshoq` are available in `AP_m_QCompustat.parquet`
- If the predictor logic is incorrect and should use `cshoq` instead, that would be a predictor code issue, not a data availability issue

#### Implementation Notes:
- Predictor calculates standardized revenue surprise scaled by revenue per share standard deviation
- Formula: `RevenueSurprise = (revps - revps_l12 - Drift) / SD`
- Where `revps = revtq / cshprq` and `Drift` is mean of historical revenue changes
- Uses 3, 6, 9, 12, 15, 18, 21, 24 month lags for drift and volatility calculations
- **Note**: `gvkey` is used for merging, surrogate approach is acceptable
- **Potential Issue**: `cshprq` may be shares repurchased, not shares outstanding - verify column meaning

---

## 149. roaq.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `gvkey`, `time_avail_m`
- **m_QCompustat.parquet**: `gvkey`, `time_avail_m`, `atq`, `ibq`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` line 119)
- ✅ **AP_m_QCompustat.parquet**: 
  - File exists (from `AP_CompustatQuarterly.py`)
  - Contains `time_avail_m` (from `AP_CompustatQuarterly.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `atq` (quarterly total assets) - mapped in XBRL_TAG_MAP line 70
  - Contains `ibq` (quarterly income before extraordinary items) - mapped in XBRL_TAG_MAP line 202

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates quarterly return on assets: quarterly income divided by 3-month lagged quarterly assets
- Formula: `roaq = ibq / atq_lag3`
- Uses calendar-based 3-month lag for exact date matching
- **Note**: `gvkey` is used for merging, surrogate approach is acceptable

---

## 150. RoE.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `ni`, `ceq`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `ni` (net income) - mapped in XBRL_TAG_MAP line 265-266
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates return on equity: net income divided by book value of equity
- Formula: `RoE = ni / ceq`
- Simple ratio calculation
- **Note**: `gvkey` is loaded but not used in calculations (only kept for compatibility)

---

## Summary

### Overall Status:
**5 OUT OF 5 PREDICTORS CAN BE CONSTRUCTED** - All predictors can be constructed with available columns.

### Column Availability:
- ✅ **SignalMasterTable columns**: `permno`, `time_avail_m`, `gvkey` (surrogate), `tickerIBES`, `prc` - All available
- ✅ **Compustat monthly columns**: `permno`, `time_avail_m`, `gvkey` (surrogate), `ni`, `ceq` - All available
- ✅ **Compustat quarterly columns**: `gvkey` (surrogate), `time_avail_m`, `atq`, `ibq`, `revtq`, `cshprq`, `cshoq` - All available
- ✅ **CRSP daily columns**: `permno`, `time_d`, `ret` - Available in `AP_dailyCRSP.parquet`
- ✅ **IBES EPS unadjusted**: `tickerIBES`, `time_avail_m`, `fpi`, `fpedats`, `statpers`, `meanest` - Available in `AP_IBES_EPS_Unadj.parquet`

### Additional Notes:
1. **For ReturnSkew**: 
   - **VERIFICATION**: Test that `ret` is correctly populated in `AP_dailyCRSP.parquet`
   - **File Name Mismatch**: Predictor expects `dailyCRSP.parquet`, AP version is `AP_dailyCRSP.parquet`
   - Requires minimum 15 daily observations per permno-month for valid calculation

2. **For REV6**: 
   - **VERIFICATION**: Test that `tickerIBES`, `meanest`, `fpi`, `fpedats`, `statpers`, and `prc` are correctly populated
   - Requires valid `tickerIBES` mapping (from `AP_IBESCRSPLinkingTable.parquet`)
   - Filters for `fpi == "1"` (1-year ahead forecasts)
   - Uses conditional fill-forward logic for missing estimates

3. **For RevenueSurprise**: 
   - **VERIFICATION**: Test that `revtq`, `cshprq`, and `gvkey` (surrogate) are correctly populated
   - **Note**: Predictor uses `cshprq` (shares repurchased) in formula: `revps = revtq / cshprq`
   - This is unusual as revenue per share typically uses shares outstanding (`cshoq`), not shares repurchased
   - However, both `cshprq` and `cshoq` are available in `AP_m_QCompustat.parquet` if needed
   - Uses 3, 6, 9, 12, 15, 18, 21, 24 month lags for drift and volatility calculations
   - **Note**: `gvkey` is used for merging, surrogate approach is acceptable

4. **For roaq**: 
   - **VERIFICATION**: Test that `atq`, `ibq`, and `gvkey` (surrogate) are correctly populated
   - Uses calendar-based 3-month lag for exact date matching
   - **Note**: `gvkey` is used for merging, surrogate approach is acceptable

5. **For RoE**: 
   - **VERIFICATION**: Test that `ni` and `ceq` are correctly populated
   - Simple ratio calculation, should work without issues
   - **Note**: `gvkey` is loaded but not used in calculations

6. **Note on gvkey Surrogate**: 
   - `gvkey` is not directly available from EDGAR (Compustat-specific identifier)
   - AP version uses `cik` (SEC identifier) as surrogate, falls back to `permno` if unavailable
   - For `RevenueSurprise.py` and `roaq.py`, `gvkey` is used for merging, surrogate approach is acceptable
   - For `RoE.py`, `gvkey` is loaded but not used in calculations (only kept for compatibility)

7. **Note on File Names**: 
   - File name mismatches are ignored per user instructions (from Group 19 onwards)
   - Predictors will need to be modified to use AP file names (`AP_dailyCRSP.parquet`) or AP files can be renamed to match expected names

8. **Note on Quarterly Compustat Columns**: 
   - `AP_CompustatQuarterly.py` extracts both `cshprq` (shares repurchased) and `cshoq` (shares outstanding)
   - Verify which column `RevenueSurprise.py` actually needs for the `revps = revtq / cshprq` calculation
   - If `cshoq` is needed, it should be available in `AP_m_QCompustat.parquet`

