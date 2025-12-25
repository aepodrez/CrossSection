# Group 21 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 21.

---

## 101. MeanRankRevGrowth.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `revt`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `revt` (total revenue) - mapped in XBRL_TAG_MAP line 232

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates weighted average of revenue growth ranks over past 5 years
- Calculates 12-month revenue growth: `log(revt) - log(revt_lag12)`
- Creates monthly rankings of revenue growth
- Calculates weighted average rank using lags 12, 24, 36, 48, 60 months (weights: 5, 4, 3, 2, 1)
- **Note**: `gvkey` is loaded but not used in calculations (only kept for compatibility)

---

## 102. Mom12m.py

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
- Predictor calculates 12-month momentum: compounds monthly returns over months t-11 to t-1 (skipping current month t)
- Uses calendar-aware lag function (`stata_multi_lag`) to create lags 1-11
- Compounds returns: `(1 + ret_lag1) * (1 + ret_lag2) * ... * (1 + ret_lag11) - 1`
- Missing returns are filled with 0 for momentum calculations

---

## 103. Mom12mOffSeason.py

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
- Predictor calculates 12-month momentum excluding same calendar month (off-season momentum)
- Uses lags 1-10, excluding same calendar month as predicted month
- Averages returns from off-season lags: `mean(ret_lag1, ret_lag2, ..., ret_lag10)` excluding same calendar month
- Fills date gaps and missing returns with 0

---

## 104. Mom6m.py

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
- Predictor calculates 6-month momentum: compounds monthly returns from t-5 to t-1 (excludes current month t)
- Uses calendar-aware lag function (`stata_multi_lag`) to create lags 1-5
- Compounds returns: `(1 + ret_lag1) * (1 + ret_lag2) * ... * (1 + ret_lag5) - 1`
- Missing returns are filled with 0 for momentum calculations

---

## 105. Mom6mJunk.py

### Required Columns:
- **m_CIQ_creditratings.parquet**: `gvkey`, `ratingdate`, `source`, `currentratingnum`
- **SignalMasterTable.parquet**: `gvkey`, `permno`, `time_avail_m`, `ret`
- **m_SP_creditratings.parquet**: `gvkey`, `time_avail_m`, `credrat`

### AP File Status:
- ❌ **AP_m_CIQ_creditratings.parquet**: **FILE DOES NOT EXIST YET**
  - Script exists: `CIQCreditRatings.py` (non-AP version downloads from WRDS)
  - AP version would need to be created to download CIQ credit ratings data
  - CIQ credit ratings are typically available from proprietary sources (S&P Capital IQ, WRDS)
  - Would contain `gvkey`, `ratingdate`, `source`, `currentratingnum` (from `CIQCreditRatings.py` lines 165-179)
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `ret` (return) - from `AP_SignalMasterTable.py` line 133
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` line 119)
- ❌ **AP_m_SP_creditratings.parquet**: **FILE DOES NOT EXIST YET**
  - Script exists: `SPCreditRatings.py` (non-AP version downloads from WRDS)
  - AP version would need to be created to download S&P credit ratings data
  - S&P credit ratings are typically available from proprietary sources (S&P Capital IQ, WRDS)
  - Would contain `gvkey`, `time_avail_m`, `credrat` (from `SPCreditRatings.py` lines 78-79)

### Can Be Constructed?
**NO** - Missing both credit ratings files (`AP_m_CIQ_creditratings.parquet` and `AP_m_SP_creditratings.parquet`).

### Additional Work Needed?
**YES** - Need to create scripts to download credit ratings data.

#### What Needs to Be Done:
1. **Create AP_CIQ_creditratings.py**: 
   - Download CIQ credit ratings from proprietary source (S&P Capital IQ, WRDS)
   - Process to create monthly time series with `gvkey`, `ratingdate`, `source`, `currentratingnum`
   - Requires proprietary data access
   - Non-AP version (`CIQCreditRatings.py`) downloads from WRDS `ciq.wrds_erating`, `ciq.wrds_irating`, `ciq.wrds_srating` tables

2. **Create AP_SP_creditratings.py**: 
   - Download S&P credit ratings from proprietary source (S&P Capital IQ, WRDS)
   - Process to create monthly time series with `gvkey`, `time_avail_m`, `credrat`
   - Requires proprietary data access
   - Non-AP version (`SPCreditRatings.py`) downloads from WRDS `comp.adsprate` table

#### Implementation Notes:
- **For Credit Ratings**: S&P and CIQ credit ratings are proprietary data sources that require paid subscriptions
- Without access to these databases, this predictor cannot be constructed
- **Alternative**: Could potentially use free credit rating sources (e.g., SEC filings, company websites), but would require significant data engineering and may have lower coverage/quality
- **Note on gvkey**: Credit ratings data typically uses `gvkey` as identifier. The AP version uses surrogate `gvkey` (CIK/permno), which may not match exactly with credit ratings databases
- Predictor calculates 6-month momentum for junk-rated stocks (credit rating <= 14)
- Uses SP ratings by default, CIQ as fallback
- Forward-fills missing credit ratings with most recent rating

---

## Summary

### Overall Status:
**4 OUT OF 5 PREDICTORS CAN BE CONSTRUCTED** - One predictor (`Mom6mJunk.py`) requires credit ratings data that is not available in AP versions.

### Column Availability:
- ✅ **SignalMasterTable columns**: `permno`, `time_avail_m`, `ret`, `gvkey` (surrogate) - All available
- ✅ **Compustat columns**: `gvkey` (surrogate), `permno`, `time_avail_m`, `revt` - All available
- ❌ **Credit ratings**: `AP_m_CIQ_creditratings.parquet` and `AP_m_SP_creditratings.parquet` - **NOT AVAILABLE** (proprietary data sources)

### Additional Notes:
1. **For MeanRankRevGrowth**: 
   - **VERIFICATION**: Test that `revt` is correctly populated in `AP_m_aCompustat.parquet`
   - Verify sufficient historical coverage for 60-month lag calculation (requires at least 5 years of data)
   - Note: Predictor calculates weighted average rank over past 5 years

2. **For Mom12m, Mom12mOffSeason, Mom6m**: 
   - **VERIFICATION**: Test that `ret` is correctly populated in `AP_SignalMasterTable.parquet`
   - Verify sufficient historical coverage for lag calculations (6-month for Mom6m, 12-month for Mom12m)
   - Note: All use calendar-aware lag functions for proper time-series handling

3. **For Mom6mJunk**: 
   - **CRITICAL**: Cannot be constructed without credit ratings data
   - Requires proprietary access to S&P Capital IQ or WRDS credit ratings databases
   - **Alternative**: Could potentially scrape credit ratings from SEC filings or company websites, but would require significant data engineering

4. **Note on gvkey Surrogate**: 
   - `gvkey` is not directly available from EDGAR (Compustat-specific identifier)
   - AP version uses `cik` (SEC identifier) as surrogate, falls back to `permno` if unavailable
   - For `MeanRankRevGrowth.py`, `gvkey` is loaded but not used in calculations (only kept for compatibility)
   - For `Mom6mJunk.py`, `gvkey` is used for merging with credit ratings data, but surrogate approach may not match credit ratings databases exactly

5. **Note on Credit Ratings Data**: 
   - Credit ratings are proprietary data typically available from S&P Capital IQ or WRDS
   - Free alternatives (SEC filings, company websites) may have lower coverage/quality
   - Would require significant data engineering to create AP versions

