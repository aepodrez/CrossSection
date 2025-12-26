# Group 23 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 23.

---

## 111. MomSeason.py

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
- Predictor calculates seasonal momentum (years 2-5): averages returns from 23, 35, 47, 59 months ago
- Uses specific lag periods: 23, 35, 47, 59 months (same calendar month from previous years)
- Calculates average: `sum(ret_lag23, ret_lag35, ret_lag47, ret_lag59) / count(non-missing)`
- Missing returns are filled with 0 for momentum calculations
- Uses calendar-aware lag function (`stata_multi_lag`) for proper time-series handling

---

## 112. MomSeason06YrPlus.py

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
- Predictor calculates seasonal momentum (years 6-10): averages returns from 71, 83, 95, 107, 119 months ago
- Uses specific lag periods: 71, 83, 95, 107, 119 months (same calendar month from previous years)
- Calculates average: `sum(ret_lag71, ret_lag83, ret_lag95, ret_lag107, ret_lag119) / count(non-missing)`
- Missing returns are filled with 0 for momentum calculations
- Uses calendar-aware lag function (`stata_multi_lag`) for proper time-series handling

---

## 113. MomSeason11YrPlus.py

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
- Predictor calculates seasonal momentum (years 11-15): averages returns from 131, 143, 155, 167, 179 months ago
- Uses specific lag periods: 131, 143, 155, 167, 179 months (same calendar month from previous years)
- Calculates average: `sum(ret_lag131, ret_lag143, ret_lag155, ret_lag167, ret_lag179) / count(non-missing)`
- Missing returns are filled with 0 for momentum calculations
- Uses calendar-aware lag function (`stata_multi_lag`) for proper time-series handling

---

## 114. MomSeason16YrPlus.py

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
- Predictor calculates seasonal momentum (years 16-20): averages returns from 191, 203, 215, 227, 239 months ago
- Uses specific lag periods: 191, 203, 215, 227, 239 months (same calendar month from previous years)
- Calculates average: `sum(ret_lag191, ret_lag203, ret_lag215, ret_lag227, ret_lag239) / count(non-missing)`
- Missing returns are filled with 0 for momentum calculations
- Uses calendar-aware lag function (`stata_multi_lag`) for proper time-series handling

---

## 115. MomSeasonShort.py

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
- Predictor calculates short-term seasonal momentum: uses 11-month lagged return
- Uses lag period: 11 months (same calendar month from previous year)
- Simply assigns: `MomSeasonShort = ret_lag11`
- Missing returns are filled with 0 for momentum calculations
- Uses calendar-aware lag function (`stata_multi_lag`) for proper time-series handling

---

## Summary

### Overall Status:
**ALL 5 PREDICTORS CAN BE CONSTRUCTED** - All required columns are available in AP data sources.

### Column Availability:
- ✅ **SignalMasterTable columns**: `permno`, `time_avail_m`, `ret` - All available

### Additional Notes:
1. **For MomSeason, MomSeason06YrPlus, MomSeason11YrPlus, MomSeason16YrPlus**: 
   - **VERIFICATION**: Test that `ret` is correctly populated in `AP_SignalMasterTable.parquet`
   - Verify sufficient historical coverage for long-term lag calculations:
     - `MomSeason`: requires at least 59 months (~5 years) of data
     - `MomSeason06YrPlus`: requires at least 119 months (~10 years) of data
     - `MomSeason11YrPlus`: requires at least 179 months (~15 years) of data
     - `MomSeason16YrPlus`: requires at least 239 months (~20 years) of data
   - Note: All use specific lag periods that correspond to same calendar month from previous years (seasonal pattern)

2. **For MomSeasonShort**: 
   - **VERIFICATION**: Test that `ret` is correctly populated in `AP_SignalMasterTable.parquet`
   - Verify sufficient historical coverage for 11-month lag calculation (requires at least 11 months of data)
   - Note: Uses single 11-month lag (same calendar month from previous year)

3. **Note on Historical Coverage**: 
   - Predictors requiring very long historical windows (10-20 years) may have limited coverage in AP data sources
   - yfinance data typically has good coverage from ~2000 onwards, but may be incomplete for earlier periods
   - **Recommended**: Verify date range coverage for predictors requiring long historical windows

4. **Note on Seasonal Momentum**: 
   - All seasonal momentum predictors use returns from the same calendar month in previous years
   - This captures calendar/seasonal effects (e.g., January effect, year-end effects)
   - Lag periods are multiples of 12 months (plus small offset) to capture same calendar month
   - Uses average of multiple years to reduce noise and capture persistent seasonal patterns

5. **Note on Calculation Method**: 
   - Seasonal momentum predictors calculate average as: `sum(lagged_returns) / count(non-missing)`
   - This handles missing values gracefully by only averaging available returns
   - If all lagged returns are missing, the result is NaN (not 0)

