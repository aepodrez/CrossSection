# Group 13 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 13.

---

## 61. dVolPut.py

### Required Columns:
- **OptionMetricsVolSurf.csv**: `secid`, `time_avail_m`, `days`, `delta`, `cp_flag`, `impl_vol`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `secid`

### AP File Status:
- ❌ **OptionMetricsVolSurf.csv**: **FILE DOES NOT EXIST**
  - This file is typically generated from OptionMetrics data (proprietary database)
  - Contains implied volatility surface data for options
  - Required columns: `secid`, `time_avail_m`, `days`, `delta`, `cp_flag` (C/P), `impl_vol`
- ⚠️ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - ⚠️ **`secid`**: Set to `np.nan` (from `AP_SignalMasterTable.py` line 118) - **NOT POPULATED**

### Can Be Constructed?
**NO** - Missing `OptionMetricsVolSurf.csv` and `secid` is not populated in `AP_SignalMasterTable.parquet`.

### Additional Work Needed?
**YES** - Same issues as `dVolCall.py` (Group 12):

#### What Needs to Be Done:
1. **Create OptionMetricsVolSurf.csv**: 
   - This requires OptionMetrics data (proprietary database)
   - OptionMetrics provides implied volatility surface data for options
   - Need to process OptionMetrics data to create volatility surface with required dimensions
   - Script would need to:
     - Download OptionMetrics implied volatility data
     - Filter to specific `days` (30) and `delta` (50) for ATM options
     - Filter to put options (`cp_flag == "P"`)
     - Aggregate by `secid`, `time_avail_m`, `days`, `delta`, and `cp_flag`

2. **Populate `secid` in AP_SignalMasterTable.parquet**:
   - `secid` is OptionMetrics security identifier
   - Need to create linking table between CRSP `permno` and OptionMetrics `secid`
   - This typically requires OptionMetrics-CRSP linking table
   - Without `secid`, cannot merge options data with stock data

#### Implementation Notes:
- **For OptionMetricsVolSurf**: OptionMetrics is a proprietary database that requires subscription
- The predictor calculates change in put implied volatility: `dVolPut = impl_vol - l1_impl_vol`
- Without OptionMetrics access, this predictor cannot be constructed
- **Alternative**: Could potentially use free options data sources (e.g., CBOE, Yahoo Finance options), but would need significant adaptation of the data processing logic

- **For secid**: OptionMetrics uses `secid` as its primary identifier, which maps to CRSP `permno` via a linking table
- Without OptionMetrics-CRSP linking table, cannot populate `secid` in `AP_SignalMasterTable.parquet`
- Even if `OptionMetricsVolSurf.csv` exists, cannot merge without `secid`

---

## 62. EarningsConsistency.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `permno`, `time_avail_m`, `epspx`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `epspx` (earnings per share diluted) - mapped in XBRL_TAG_MAP line 293

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 63. EarningsForecastDisparity.py

### Required Columns:
- **AP_IBES_EPS_Unadj.parquet**: `tickerIBES`, `fpi`, `fpedats`, `statpers`, `meanest`
- **AP_IBES_UnadjustedActuals.parquet**: `tickerIBES`, `time_avail_m`, `fy0a`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `tickerIBES`

### AP File Status:
- ✅ **AP_IBES_EPS_Unadj.parquet**: 
  - File exists (from `AP_IBESEPSUnadjusted.py`)
  - Contains `tickerIBES` (from `AP_IBESEPSUnadjusted.py` line 253)
  - Contains `fpi` (forecast period indicator) - from `AP_IBESEPSUnadjusted.py` line 295
  - Contains `fpedats` (forecast period end date) - from `AP_IBESEPSUnadjusted.py` line 300
  - Contains `statpers` (statistical period) - from `AP_IBESEPSUnadjusted.py` line 294
  - Contains `meanest` (mean estimate) - from `AP_IBESEPSUnadjusted.py` line 298
- ⚠️ **AP_IBES_UnadjustedActuals.parquet**: 
  - Script exists: `AP_IBESUnadjustedActuals.py` (generates `AP_IBES_UnadjustedActuals.parquet`)
  - File may not exist yet - needs to be generated
  - Would contain `tickerIBES` (from `AP_IBESUnadjustedActuals.py` structure)
  - Would contain `time_avail_m` (from `AP_IBESUnadjustedActuals.py` line 301)
  - ⚠️ **`fy0a`**: **NOT EXTRACTED** - AP version only extracts `int0a` (actual EPS unadjusted), but original WRDS version has both `int0a` and `fy0a` columns. Predictor expects `fy0a`.
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `tickerIBES` column - should be populated from `AP_IBESCRSPLinkingTable.parquet` (after fix in Group 2)

### Can Be Constructed?
**PARTIALLY** - Missing `AP_IBES_UnadjustedActuals.parquet` and column name mismatch (`int0a` vs `fy0a`).

### Additional Work Needed?
**YES** - Multiple issues:

#### What Needs to Be Done:
1. **Run AP_IBESUnadjustedActuals.py**: Execute the script to generate `AP_IBES_UnadjustedActuals.parquet`
   - Requires Eikon/LSEG API subscription (proprietary data source)
   - `eikon` Python package installed
   - Eikon API credentials configured

2. **Column Name Mapping**: 
   - AP version only extracts `int0a` (actual EPS unadjusted) from Eikon
   - Original WRDS version has both `int0a` and `fy0a` columns (both appear to be actual EPS, possibly different fiscal periods)
   - Predictor expects `fy0a`
   - **Solution Options**:
     a. Check Eikon API for `fy0a` equivalent field and extract it
     b. Use `int0a` as substitute for `fy0a` if they're equivalent
     c. Modify `EarningsForecastDisparity.py` to use `int0a` instead of `fy0a`
   - **Recommended**: Check Eikon API documentation for `fy0a` equivalent, or verify if `int0a` can be used as substitute

#### Implementation Notes:
- **For AP_IBES_UnadjustedActuals**: The script exists and is ready to run
- Requires Eikon/LSEG API access (paid subscription)
- If Eikon is not available, this predictor cannot be constructed with AP data
- **Column Name Issue**: `int0a` and `fy0a` appear to be the same thing (actual EPS unadjusted), but the predictor expects `fy0a`
- Check original `IBES_UnadjustedActuals.py` to see if `fy0a` is derived from `int0a` or if they're the same

---

## 64. EarningsStreak.py

### Required Columns:
- **AP_IBES_EPS_Adj.parquet**: `tickerIBES`, `anndats_act`, `time_avail_m`, `fpi`, `actual`, `meanest`, `price`, `statpers`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `tickerIBES`

### AP File Status:
- ❌ **AP_IBES_EPS_Adj.parquet**: **FILE DOES NOT EXIST YET**
  - Script exists: `AP_IBESEPSAdjusted.py` (generates `AP_IBES_EPS_Adj.parquet`)
  - Would contain `tickerIBES` (from `AP_IBESEPSAdjusted.py` line 220)
  - Would contain `anndats_act` (actual announcement date) - from `AP_IBESEPSAdjusted.py` line 226
  - Would contain `time_avail_m` (from `AP_IBESEPSAdjusted.py` line 240)
  - ⚠️ **`fpi`**: Set to `1` (from `AP_IBESEPSAdjusted.py` line 243), but predictor filters for `fpi == "6"` (6-month ahead forecast). Script uses `Period="FQ1"` (next fiscal quarter) which corresponds to `fpi = 1`, not `fpi = 6`.
  - Would contain `actual` (actual earnings) - from `AP_IBESEPSAdjusted.py` line 225
  - Would contain `meanest` (mean estimate) - from `AP_IBESEPSAdjusted.py` line 221
  - Would contain `price` (price) - from `AP_IBESEPSAdjusted.py` line 229
  - Would contain `statpers` (statistical period) - from `AP_IBESEPSAdjusted.py` line 228
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `tickerIBES` column - should be populated from `AP_IBESCRSPLinkingTable.parquet` (after fix in Group 2)

### Can Be Constructed?
**NO** - Missing `AP_IBES_EPS_Adj.parquet` file.

### Additional Work Needed?
**YES** - Need to run `AP_IBESEPSAdjusted.py` to generate `AP_IBES_EPS_Adj.parquet`.

#### What Needs to Be Done:
1. **Run AP_IBESEPSAdjusted.py**: Execute the script to download IBES adjusted EPS data from Refinitiv Platform
2. **Requirements**:
   - Refinitiv Platform API access (proprietary data source)
   - `refinitiv-dataplatform` Python package installed
   - Refinitiv API credentials configured
3. **Output**: Generates `AP_IBES_EPS_Adj.parquet` with required columns
4. **Note on fpi**: The script sets `fpi = 1` (line 243), but the predictor filters for `fpi == "6"` (6-month forecast). May need to modify script to download multiple forecast periods or modify predictor to use `fpi == 1`.

#### Implementation Notes:
- The script `AP_IBESEPSAdjusted.py` exists and is ready to run
- Requires Refinitiv Platform API access (paid subscription)
- If Refinitiv is not available, this predictor cannot be constructed with AP data
- **Forecast Period Issue**: Script sets `fpi = 1` (1-year ahead), but predictor filters for `fpi == "6"` (6-month ahead). Need to verify if script can download multiple forecast periods or if predictor needs modification.

---

## 65. EarningsSurprise.py

### Required Columns:
- **AP_SignalMasterTable.parquet**: `permno`, `gvkey`, `time_avail_m`
- **AP_m_QCompustat.parquet**: `gvkey`, `time_avail_m`, `epspxq`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` line 119)
- ✅ **AP_m_QCompustat.parquet**: 
  - Contains `gvkey` (surrogate: CIK/permno) - from `AP_CompustatQuarterly.py` structure
  - Contains `time_avail_m` (from `AP_CompustatQuarterly.py` structure)
  - Contains `epspxq` (earnings per share quarterly) - mapped in XBRL_TAG_MAP line 205

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## Summary

| Predictor | All Columns Available? | Can Be Constructed? | Additional Work Needed? |
|-----------|----------------------|---------------------|----------------------|
| dVolPut | ❌ No | ❌ No | **Create OptionMetricsVolSurf.csv, populate secid** |
| EarningsConsistency | ✅ Yes | ✅ Yes | None |
| EarningsForecastDisparity | ⚠️ Partial | ⚠️ Partial | **Run AP_IBESUnadjustedActuals.py, fix column name (int0a→fy0a)** |
| EarningsStreak | ❌ No | ❌ No | **Run AP_IBESEPSAdjusted.py, verify fpi** |
| EarningsSurprise | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |

### Critical Missing Components:
1. **`OptionMetricsVolSurf.csv`**: Required for `dVolPut.py`. File does not exist - needs to be generated from OptionMetrics data (proprietary database).
2. **`secid` column in AP_SignalMasterTable.parquet**: Required for `dVolPut.py`. Currently set to `np.nan` - needs OptionMetrics-CRSP linking table.
3. **`AP_IBES_UnadjustedActuals.parquet`**: Required for `EarningsForecastDisparity.py`. File does not exist yet - needs to be generated by running `AP_IBESUnadjustedActuals.py`.
4. **`AP_IBES_EPS_Adj.parquet`**: Required for `EarningsStreak.py`. File does not exist yet - needs to be generated by running `AP_IBESEPSAdjusted.py`.

### Recommendations:
1. **For dVolPut**: 
   - **CRITICAL**: Requires OptionMetrics data access (proprietary database)
   - Need to create `OptionMetricsVolSurf.csv` from OptionMetrics implied volatility data
   - Need to populate `secid` in `AP_SignalMasterTable.parquet` using OptionMetrics-CRSP linking table
   - Without OptionMetrics access, this predictor cannot be constructed
   - **Alternative**: Could use free options data sources, but would require significant code adaptation

2. **For EarningsConsistency**: 
   - **VERIFICATION**: Test that `epspx` is correctly populated in `AP_m_aCompustat.parquet`
   - Verify sufficient historical coverage for 48-month rolling windows

3. **For EarningsForecastDisparity**: 
   - **CRITICAL**: Run `AP_IBESUnadjustedActuals.py` to generate `AP_IBES_UnadjustedActuals.parquet`
   - Requires Eikon/LSEG API subscription (proprietary data source)
   - **CRITICAL**: Fix column name mismatch - add `fy0a` column (or rename `int0a` to `fy0a`) in `AP_IBESUnadjustedActuals.py`
   - Verify `AP_IBES_EPS_Unadj.parquet` has both `fpi == "0"` (long-term growth) and `fpi == "1"` (1-year ahead) forecasts

4. **For EarningsStreak**: 
   - **CRITICAL**: Run `AP_IBESEPSAdjusted.py` to generate `AP_IBES_EPS_Adj.parquet`
   - Requires Refinitiv Platform API access (proprietary data source)
   - **IMPORTANT**: Forecast period mismatch:
     - Script sets `fpi = 1` and uses `Period="FQ1"` (next fiscal quarter) in Refinitiv API
     - Predictor filters for `fpi == "6"` (6-month ahead forecast)
     - **Solution Options**:
       a. Modify script to download multiple forecast periods (FQ0, FQ1, FQ2, etc.) and map to appropriate `fpi` values
       b. Modify predictor to use `fpi == 1` instead of `fpi == "6"`
       c. Check Refinitiv API for 6-month ahead forecast period equivalent
     - **Recommended**: Check Refinitiv API documentation for 6-month ahead forecast period, or modify predictor to use available `fpi` values

5. **For EarningsSurprise**: 
   - **VERIFICATION**: Test that `epspxq` is correctly populated in `AP_m_QCompustat.parquet`
   - Verify sufficient historical coverage for 24-month rolling windows (predictor uses lags up to 24 months)

6. **Note on Proprietary Data Sources**: 
   - OptionMetrics, Eikon/LSEG, and Refinitiv Platform are proprietary databases requiring paid subscriptions
   - Without access to these databases, `dVolPut`, `EarningsForecastDisparity`, and `EarningsStreak` cannot be constructed
   - Consider alternative free data sources or skip these predictors if proprietary data is unavailable

7. **Note on Column Name Mismatches**: 
   - `AP_IBES_UnadjustedActuals.parquet` only extracts `int0a` (actual EPS unadjusted)
   - Original WRDS version has both `int0a` and `fy0a` columns
   - Predictor expects `fy0a` - need to verify if `int0a` can be used as substitute or if `fy0a` needs to be extracted from Eikon
   - Check Eikon API documentation for `fy0a` equivalent field (may be fiscal year 0 actuals vs interim actuals)

8. **Note on Forecast Period Indicators**: 
   - `AP_IBESEPSAdjusted.py` sets `fpi = 1` and uses `Period="FQ1"` (next fiscal quarter) in Refinitiv API
   - `EarningsStreak.py` filters for `fpi == "6"` (6-month ahead forecast)
   - These may not be equivalent - need to verify Refinitiv API forecast period mapping or modify predictor/script accordingly
