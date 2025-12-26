# Group 12 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 12.

---

## 56. DivYieldST.py

### Required Columns:
- **AP_CRSPdistributions.parquet**: `permno`, `cd1`, `cd2`, `cd3`, `divamt`, `exdt`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `prc`
- **AP_monthlyCRSP.parquet**: `permno`, `time_avail_m`, `ret`, `retx`

### AP File Status:
- ✅ **AP_CRSPdistributions.parquet**: 
  - File exists (from `AP_CRSPDistributions.py`)
  - Contains `permno` (from `AP_CRSPDistributions.py` line 223)
  - Contains `cd1`, `cd2`, `cd3` (distribution code digits) - from `AP_CRSPDistributions.py` lines 263-265
  - Contains `divamt` (dividend amount) - from `AP_CRSPDistributions.py` line 226
  - Contains `exdt` (ex-dividend date) - from `AP_CRSPDistributions.py` line 225
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `prc` (price) - from `AP_SignalMasterTable.py` line 132
- ✅ **AP_monthlyCRSP.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` structure)
  - Contains `ret` (returns) - from `AP_CRSPMonthly.py` line 345
  - Contains `retx` (returns excluding dividends) - from `AP_CRSPMonthly.py` line 346

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 57. dNoa.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `permno`, `time_avail_m`, `at`, `che`, `dltt`, `dlc`, `mib`, `pstk`, `ceq`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 77
  - Contains `che` (cash) - mapped in XBRL_TAG_MAP line 83
  - Contains `dltt` (long-term debt) - mapped in XBRL_TAG_MAP line 142-144
  - Contains `dlc` (debt current) - mapped in XBRL_TAG_MAP line 136-139
  - Contains `mib` (minority interest) - mapped in XBRL_TAG_MAP line 175
  - Contains `pstk` (preferred stock) - mapped in XBRL_TAG_MAP line 189
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 58. DolVol.py

### Required Columns:
- **AP_monthlyCRSP.parquet**: `permno`, `time_avail_m`, `vol`, `prc`

### AP File Status:
- ✅ **AP_monthlyCRSP.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` structure)
  - Contains `vol` (volume) - from `AP_CRSPMonthly.py` line 347
  - Contains `prc` (price) - from `AP_CRSPMonthly.py` line 349

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 59. DownRecomm.py

### Required Columns:
- **AP_IBES_Recommendations.parquet**: `tickerIBES`, `amaskcd`, `anndats`, `time_avail_m`, `ireccd`
- **AP_SignalMasterTable.parquet**: `permno`, `tickerIBES`, `time_avail_m`

### AP File Status:
- ❌ **AP_IBES_Recommendations.parquet**: **FILE DOES NOT EXIST YET**
  - Script exists: `AP_IBESRecommendations.py` (generates `AP_IBES_Recommendations.parquet`)
  - Would contain `tickerIBES` (from `AP_IBESRecommendations.py` line 282)
  - Would contain `amaskcd` (analyst mask code) - from `AP_IBESRecommendations.py` line 254-280
  - Would contain `anndats` (announcement date) - from `AP_IBESRecommendations.py` line 252
  - Would contain `time_avail_m` (monthly availability) - from `AP_IBESRecommendations.py` line 306-307
  - Would contain `ireccd` (recommendation code) - from `AP_IBESRecommendations.py` line 248
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `tickerIBES` column - should be populated from `AP_IBESCRSPLinkingTable.parquet` (after fix in Group 2)

### Can Be Constructed?
**NO** - Missing `AP_IBES_Recommendations.parquet` file.

### Additional Work Needed?
**YES** - Need to run `AP_IBESRecommendations.py` to generate `AP_IBES_Recommendations.parquet`.

#### What Needs to Be Done:
1. **Run AP_IBESRecommendations.py**: Execute the script to download IBES recommendations from Eikon/LSEG API
2. **Requirements**:
   - Eikon/LSEG API subscription (proprietary data source)
   - `eikon` Python package installed
   - Eikon API credentials configured
3. **Output**: Generates `AP_IBES_Recommendations.parquet` with required columns

#### Implementation Notes:
- The script `AP_IBESRecommendations.py` exists and is ready to run
- Requires Eikon/LSEG API access (paid subscription)
- If Eikon is not available, this predictor cannot be constructed with AP data
- Alternative: Could potentially use other free sources (e.g., Yahoo Finance analyst recommendations), but format would need adaptation

---

## 60. dVolCall.py

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
**YES** - Same issues as `dCPVolSpread.py` (Group 8):

#### What Needs to Be Done:
1. **Create OptionMetricsVolSurf.csv**: 
   - This requires OptionMetrics data (proprietary database)
   - OptionMetrics provides implied volatility surface data for options
   - Need to process OptionMetrics data to create volatility surface with required dimensions
   - Script would need to:
     - Download OptionMetrics implied volatility data
     - Filter to specific `days` (30) and `delta` (50) for ATM options
     - Aggregate by `secid`, `time_avail_m`, `days`, `delta`, and `cp_flag` (call/put)

2. **Populate `secid` in AP_SignalMasterTable.parquet**:
   - `secid` is OptionMetrics security identifier
   - Need to create linking table between CRSP `permno` and OptionMetrics `secid`
   - This typically requires OptionMetrics-CRSP linking table
   - Without `secid`, cannot merge options data with stock data

#### Implementation Notes:
- **For OptionMetricsVolSurf**: OptionMetrics is a proprietary database that requires subscription
- The predictor calculates change in call implied volatility: `dVolCall = impl_vol - l1_impl_vol`
- Without OptionMetrics access, this predictor cannot be constructed
- **Alternative**: Could potentially use free options data sources (e.g., CBOE, Yahoo Finance options), but would need significant adaptation of the data processing logic

- **For secid**: OptionMetrics uses `secid` as its primary identifier, which maps to CRSP `permno` via a linking table
- Without OptionMetrics-CRSP linking table, cannot populate `secid` in `AP_SignalMasterTable.parquet`
- Even if `OptionMetricsVolSurf.csv` exists, cannot merge without `secid`

---

## Summary

| Predictor | All Columns Available? | Can Be Constructed? | Additional Work Needed? |
|-----------|----------------------|---------------------|----------------------|
| DivYieldST | ✅ Yes | ✅ Yes | None |
| dNoa | ✅ Yes | ✅ Yes | None |
| DolVol | ✅ Yes | ✅ Yes | None |
| DownRecomm | ❌ No | ❌ No | **Run AP_IBESRecommendations.py** |
| dVolCall | ❌ No | ❌ No | **Create OptionMetricsVolSurf.csv, populate secid** |

### Critical Missing Components:
1. **`AP_IBES_Recommendations.parquet`**: Required for `DownRecomm.py`. File does not exist yet - needs to be generated by running `AP_IBESRecommendations.py` (requires Eikon/LSEG API subscription).
2. **`OptionMetricsVolSurf.csv`**: Required for `dVolCall.py`. File does not exist - needs to be generated from OptionMetrics data (proprietary database).
3. **`secid` column in AP_SignalMasterTable.parquet**: Required for `dVolCall.py`. Currently set to `np.nan` - needs OptionMetrics-CRSP linking table to populate.

### Recommendations:
1. **For DivYieldST**: 
   - **VERIFICATION**: Test that all AP files actually contain the columns listed above by loading sample data
   - Verify `retx` (returns excluding dividends) is correctly calculated in `AP_monthlyCRSP.parquet`
   - Verify `prc` (price) is correctly populated in `AP_SignalMasterTable.parquet`
   - Verify distribution codes (`cd1`, `cd2`, `cd3`) are correctly assigned in `AP_CRSPdistributions.parquet`

2. **For dNoa**: 
   - **VERIFICATION**: Test that all AP files actually contain the columns listed above by loading sample data
   - Verify `mib` (minority interest) is correctly populated in `AP_m_aCompustat.parquet`
   - Verify all balance sheet components (`at`, `che`, `dltt`, `dlc`, `pstk`, `ceq`) are correctly populated

3. **For DolVol**: 
   - **VERIFICATION**: Test that `vol` (volume) and `prc` (price) are correctly populated in `AP_monthlyCRSP.parquet`
   - Verify volume data is available for the required historical period

4. **For DownRecomm**: 
   - **CRITICAL**: Run `AP_IBESRecommendations.py` to generate `AP_IBES_Recommendations.parquet`
   - Requires Eikon/LSEG API subscription (proprietary data source)
   - If Eikon is not available, this predictor cannot be constructed with AP data

5. **For dVolCall**: 
   - **CRITICAL**: Requires OptionMetrics data access (proprietary database)
   - Need to create `OptionMetricsVolSurf.csv` from OptionMetrics implied volatility data
   - Need to populate `secid` in `AP_SignalMasterTable.parquet` using OptionMetrics-CRSP linking table
   - Without OptionMetrics access, this predictor cannot be constructed
   - **Alternative**: Could potentially use free options data sources, but would require significant code adaptation

6. **Note on Proprietary Data Sources**: 
   - IBES recommendations (Eikon/LSEG) and OptionMetrics are proprietary databases requiring paid subscriptions
   - Without access to these databases, `DownRecomm` and `dVolCall` cannot be constructed with AP data
   - Consider alternative free data sources or skip these predictors if proprietary data is unavailable

7. **Note on Historical Data**: 
   - All predictors that can be constructed use basic balance sheet or market data that should be available
   - Verify sufficient historical coverage for predictors that use rolling windows or lags
   - `DivYieldST` uses 12-month rolling windows, `dNoa` uses 12-month lags, `DolVol` uses 2-month lags
