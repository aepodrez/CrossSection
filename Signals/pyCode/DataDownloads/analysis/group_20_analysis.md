# Group 20 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 20.

---

## 96. InvGrowth.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `invt`, `sic`, `ppent`, `at`
- **GNPdefl.parquet**: `time_avail_m`, `gnpdefl`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `invt` (inventory) - mapped in XBRL_TAG_MAP line 98
  - ✅ **`sic`**: **ADDED** - SIC code is added via `add_sic_codes()` function (from `AP_CompustatAnnual.py` line 1265)
  - Contains `ppent` (property, plant, equipment net) - mapped in XBRL_TAG_MAP line 105
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 84
- ✅ **AP_GNPdefl.parquet**: 
  - File exists (from `AP_GNPDeflator.py`)
  - Contains `time_avail_m` (from `AP_GNPDeflator.py` line 181)
  - Contains `gnpdefl` (GNP deflator) - from `AP_GNPDeflator.py` line 178

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate, sic has been added).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates year-over-year inventory growth using GNP deflator for inflation adjustment
- Excludes utilities (SIC 4xxx) and financial firms (SIC 6xxx)
- Excludes firms with non-positive total assets or property, plant & equipment
- Uses 12-month calendar-based lag for inventory values
- **Note**: `gvkey` is loaded but not used in calculations (only kept for compatibility)

---

## 97. IO_ShortInterest.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `gvkey`, `time_avail_m`
- **TR_13F.parquet**: `permno`, `time_avail_m`, `instown_perc`
- **monthlyCRSP.parquet**: `permno`, `time_avail_m`, `shrout`
- **monthlyShortInterest.parquet**: `gvkey`, `time_avail_m`, `shortint`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` line 119)
- ✅ **AP_TR_13F.parquet**: 
  - File exists (from `AP_InstitutionalHoldings13F.py`)
  - Contains `permno` (from `AP_InstitutionalHoldings13F.py` line 532)
  - Contains `time_avail_m` (from `AP_InstitutionalHoldings13F.py` line 532)
  - Contains `instown_perc` (institutional ownership percentage) - from `AP_InstitutionalHoldings13F.py` line 384, 532
- ✅ **AP_monthlyCRSP.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` structure)
  - Contains `shrout` (shares outstanding) - from `AP_CRSPMonthly.py` line 303
- ✅ **AP_monthlyShortInterest.parquet**: 
  - File exists (from `AP_CompustatShortInterest.py`)
  - Contains `gvkey` (surrogate: CIK/permno) - from `AP_CompustatShortInterest.py` line 442-448
  - Contains `time_avail_m` (from `AP_CompustatShortInterest.py` line 465)
  - Contains `shortint` (short interest) - from `AP_CompustatShortInterest.py` line 465

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor excludes stocks below 99th percentile of short interest, calculates institutional ownership for remaining stocks
- Calculates short ratio: `tempshortratio = shortint / shrout`
- Calculates 99th percentile of short ratio by month
- Sets `IO_ShortInterest` to `instown_perc` if `tempshortratio >= temps99`, otherwise NaN
- **Note**: `gvkey` is used for merging with short interest data, surrogate approach is acceptable

---

## 98. Leverage.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `lt`
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `mve_permco`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `lt` (total liabilities) - mapped in XBRL_TAG_MAP line 137
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `mve_permco` (market value of equity per company) - from `AP_SignalMasterTable.py` line 128

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates market leverage as total liabilities divided by market value of equity
- Formula: `Leverage = lt / mve_permco`
- **Note**: `gvkey` is loaded but not used in calculations (only kept for compatibility)

---

## 99. LRreversal.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `ret`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno` (from `AP_SignalMasterTable.py` line 124)
  - Contains `time_avail_m` (from `AP_SignalMasterTable.py` line 126)
  - Contains `ret` (return) - from `AP_SignalMasterTable.py` line 133

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates long-run reversal: stock return between months t-36 and t-13
- Compounds monthly returns over months t-36 to t-13 (24 months)
- Uses position-based lag (not calendar-based) via `groupby().shift()`
- Missing lagged values result in missing `LRreversal`

---

## 100. MaxRet.py

### Required Columns:
- **dailyCRSP.parquet**: `permno`, `time_d`, `ret`

### AP File Status:
- ✅ **AP_dailyCRSP.parquet**: 
  - Contains `permno` (from `AP_CRSPDaily.py` line 250)
  - Contains `time_d` (date) - from `AP_CRSPDaily.py` line 251
  - Contains `ret` (return) - from `AP_CRSPDaily.py` line 253

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates maximum of daily returns over the previous month
- Groups by `permno` and `time_avail_m` (monthly), takes maximum of `ret`
- Converts daily date to monthly using `to_period('M').to_timestamp()`

---

## Summary

### Overall Status:
**ALL 5 PREDICTORS CAN BE CONSTRUCTED** - All required columns are available in AP data sources.

### Column Availability:
- ✅ **SignalMasterTable columns**: `permno`, `time_avail_m`, `ret`, `gvkey` (surrogate), `mve_permco` - All available
- ✅ **Compustat columns**: `gvkey` (surrogate), `permno`, `time_avail_m`, `invt`, `sic` (added), `ppent`, `at`, `lt` - All available
- ✅ **GNP deflator**: `time_avail_m`, `gnpdefl` - Available in `AP_GNPdefl.parquet`
- ✅ **13F data**: `permno`, `time_avail_m`, `instown_perc` - Available in `AP_TR_13F.parquet`
- ✅ **Monthly CRSP**: `permno`, `time_avail_m`, `shrout` - Available in `AP_monthlyCRSP.parquet`
- ✅ **Short interest**: `gvkey` (surrogate), `time_avail_m`, `shortint` - Available in `AP_monthlyShortInterest.parquet`
- ✅ **Daily CRSP**: `permno`, `time_d`, `ret` - Available in `AP_dailyCRSP.parquet`

### Additional Notes:
1. **For InvGrowth**: 
   - **VERIFICATION**: Test that `sic` is correctly populated in `AP_m_aCompustat.parquet`
   - Verify that `GNPdefl.parquet` is available (or use `AP_GNPdefl.parquet`)
   - Verify sufficient historical coverage for 12-month lag calculation
   - Note: Predictor excludes utilities (SIC 4xxx) and financial firms (SIC 6xxx)

2. **For IO_ShortInterest**: 
   - **VERIFICATION**: Test that `instown_perc` is correctly populated in `AP_TR_13F.parquet`
   - Verify that `shortint` and `shrout` are correctly populated
   - Verify sufficient historical coverage (13F data typically available from ~2000 onwards, FINRA short interest from Dec 2017 onwards)
   - Note: Predictor calculates 99th percentile of short ratio by month

3. **For Leverage**: 
   - **VERIFICATION**: Test that `lt` and `mve_permco` are correctly populated
   - Verify sufficient historical coverage

4. **For LRreversal**: 
   - **VERIFICATION**: Test that `ret` is correctly populated in `AP_SignalMasterTable.parquet`
   - Verify sufficient historical coverage for 36-month lag calculation (requires at least 36 months of data)

5. **For MaxRet**: 
   - **VERIFICATION**: Test that `ret` is correctly populated in `AP_dailyCRSP.parquet`
   - Verify sufficient historical coverage for monthly maximum calculation

6. **Note on gvkey Surrogate**: 
   - `gvkey` is not directly available from EDGAR (Compustat-specific identifier)
   - AP version uses `cik` (SEC identifier) as surrogate, falls back to `permno` if unavailable
   - For `InvGrowth.py` and `Leverage.py`, `gvkey` is loaded but not used in calculations (only kept for compatibility)
   - For `IO_ShortInterest.py`, `gvkey` is used for merging with short interest data, surrogate approach is acceptable

7. **Note on SIC Codes**: 
   - `sic` is added to `AP_m_aCompustat.parquet` via `add_sic_codes()` function
   - SIC codes come from static mapping file (`ticker_sic_mapping.xlsx`) or heuristic mapping
   - May be approximate rather than official CRSP SIC codes, but should be sufficient for industry filtering
