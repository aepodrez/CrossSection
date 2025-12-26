# Group 22 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 22.

---

## 106. MomOffSeason.py

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
- Predictor calculates off-season momentum (years 2-5): averages returns from years 2-5 before predicted month, excluding same calendar month
- Uses lags 12-59 months, excluding same calendar month as predicted month
- A lag represents same month when `(lag + 1) % 12 == 0`
- Fills date gaps and missing returns with 0
- Uses calendar-aware lag function (`stata_multi_lag`) for proper time-series handling

---

## 107. MomOffSeason06YrPlus.py

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
- Predictor calculates off-season momentum (years 6-10): averages returns from years 6-10 before predicted month, excluding same calendar month
- Uses lags 60-119 months, excluding same calendar month as predicted month
- A lag represents same month when `(lag + 1) % 12 == 0`
- Fills date gaps and missing returns with 0
- Uses calendar-aware lag function (`stata_multi_lag`) for proper time-series handling

---

## 108. MomOffSeason11YrPlus.py

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
- Predictor calculates off-season momentum (years 11-15): averages returns from years 11-15 before predicted month, excluding same calendar month
- Uses lags 120-179 months, excluding same calendar month as predicted month
- A lag represents same month when `(lag + 1) % 12 == 0`
- Fills date gaps and missing returns with 0
- Uses calendar-aware lag function (`stata_multi_lag`) for proper time-series handling

---

## 109. MomOffSeason16YrPlus.py

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
- Predictor calculates off-season momentum (years 16-20): averages returns from years 16-20 before predicted month, excluding same calendar month
- Uses lags 180-239 months, excluding same calendar month as predicted month
- A lag represents same month when `(lag + 1) % 12 == 0`
- Fills date gaps and missing returns with 0
- Uses calendar-aware lag function (`stata_multi_lag`) for proper time-series handling

---

## 110. MomRev.py

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
- Predictor combines 6-month momentum with 36-month reversal patterns
- Calculates `Mom6m`: compounds returns over months t-5 to t-1 (lags 1-5)
- Calculates `Mom36m`: compounds returns over months t-36 to t-13 (lags 13-36, skips recent 12 months)
- Ranks stocks into quintiles (1-5) within each month based on `Mom6m` and `Mom36m`
- Goes long (`MomRev = 1`): top quintile for 6m momentum AND bottom quintile for 36m momentum
- Goes short (`MomRev = 0`): bottom quintile for 6m momentum AND top quintile for 36m momentum
- Uses calendar-aware lag function (`stata_multi_lag`) for proper time-series handling

---

## Summary

### Overall Status:
**ALL 5 PREDICTORS CAN BE CONSTRUCTED** - All required columns are available in AP data sources.

### Column Availability:
- ✅ **SignalMasterTable columns**: `permno`, `time_avail_m`, `ret` - All available

### Additional Notes:
1. **For MomOffSeason, MomOffSeason06YrPlus, MomOffSeason11YrPlus, MomOffSeason16YrPlus**: 
   - **VERIFICATION**: Test that `ret` is correctly populated in `AP_SignalMasterTable.parquet`
   - Verify sufficient historical coverage for long-term lag calculations:
     - `MomOffSeason`: requires at least 5 years of data (60 months)
     - `MomOffSeason06YrPlus`: requires at least 10 years of data (120 months)
     - `MomOffSeason11YrPlus`: requires at least 15 years of data (180 months)
     - `MomOffSeason16YrPlus`: requires at least 20 years of data (240 months)
   - Note: All use calendar-aware lag functions and exclude same calendar month returns

2. **For MomRev**: 
   - **VERIFICATION**: Test that `ret` is correctly populated in `AP_SignalMasterTable.parquet`
   - Verify sufficient historical coverage for 36-month lag calculation (requires at least 36 months of data)
   - Note: Predictor uses quintile rankings within each month, so needs sufficient cross-sectional coverage

3. **Note on Historical Coverage**: 
   - Predictors requiring very long historical windows (10-20 years) may have limited coverage in AP data sources
   - yfinance data typically has good coverage from ~2000 onwards, but may be incomplete for earlier periods
   - **Recommended**: Verify date range coverage for predictors requiring long historical windows

4. **Note on Off-Season Momentum**: 
   - All off-season momentum predictors exclude same calendar month returns to avoid seasonal effects
   - This is important for capturing true momentum vs. calendar effects
   - Uses `(lag + 1) % 12 == 0` to identify same calendar month

