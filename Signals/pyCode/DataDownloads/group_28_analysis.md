# Group 28 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 28.

---

## 136. ProbInformedTrading.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `gvkey`, `time_avail_m`, `mve_c`
- **pin_monthly.parquet**: `permno`, `time_avail_m`, `a`, `u`, `es`, `eb`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` line 119)
  - Contains `mve_c` (market value of equity) - from `AP_SignalMasterTable.py` line 127
- ❌ **pin_monthly.parquet**: **FILE MAY NOT EXIST**
  - Script exists: `PINData.py` (generates `pin_monthly.parquet`)
  - Would contain `permno`, `time_avail_m`, `a`, `u`, `es`, `eb` (PIN microstructure parameters)
  - Downloads from Dropbox (Easley et al. PIN data)
  - **Note**: This is NOT an AP script - it downloads from a proprietary source (Dropbox)

### Can Be Constructed?
**PARTIALLY** - Missing `pin_monthly.parquet`.

### Additional Work Needed?
**YES** - Missing PIN data:

#### What Needs to Be Done:
1. **Create pin_monthly.parquet**: 
   - Run `PINData.py` to generate `pin_monthly.parquet`
   - Requires Dropbox access to Easley et al. PIN data
   - Converts yearly PIN parameters to monthly data with 11-month availability lag
   - **Note**: This is NOT an AP script - it downloads from a proprietary source

2. **Alternative**: 
   - If PIN data is not available, this predictor cannot be constructed
   - Would require implementing PIN estimation from trade-level data (complex and computationally intensive)

#### Implementation Notes:
- Predictor calculates probability of informed trading from microstructure parameters
- Formula: `ProbInformedTrading = (a * u) / (a * u + es + eb)`
- Sets to missing for large cap stocks (top 50% by market value)
- **Note**: `gvkey` is loaded but not used in calculations (only kept for compatibility)

---

## 137. PS.py

### Required Columns:
- **m_aCompustat.parquet**: `permno`, `time_avail_m`, `fopt`, `oancf`, `ib`, `at`, `dltt`, `act`, `lct`, `txt`, `xint`, `sale`, `ceq`
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `mve_permco`
- **monthlyCRSP.parquet**: `permno`, `time_avail_m`, `shrout`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `fopt` (foreign operations income) - mapped in XBRL_TAG_MAP line 291
  - Contains `oancf` (operating cash flow) - mapped in XBRL_TAG_MAP line 280-282
  - Contains `ib` (income before extraordinary items) - mapped in XBRL_TAG_MAP line 262-264
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 84
  - Contains `dltt` (long-term debt) - mapped in XBRL_TAG_MAP line 142-144
  - Contains `act` (current assets) - mapped in XBRL_TAG_MAP line 90
  - Contains `lct` (current liabilities) - mapped in XBRL_TAG_MAP line 133
  - Contains `txt` (total income taxes) - mapped in XBRL_TAG_MAP line 270-272
  - Contains `xint` (interest expense) - mapped in XBRL_TAG_MAP line 245-246
  - Contains `sale` (sales) - mapped in XBRL_TAG_MAP line 230
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `mve_permco` (market value of equity by permco) - from `AP_SignalMasterTable.py` line 128
- ✅ **AP_monthlyCRSP.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` line 344)
  - Contains `shrout` (shares outstanding) - from `AP_CRSPMonthly.py` line 348

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates Piotroski F-score: nine-factor profitability, efficiency, and leverage score
- Components: p1 (positive net income), p2 (positive operating cash flow), p3 (improvement in ROA), p4 (cash flow exceeds net income), p5 (reduction in leverage), p6 (improvement in current ratio), p7 (improvement in gross margin), p8 (improvement in asset turnover), p9 (no increase in shares outstanding)
- Restricted to highest book-to-market quintile
- Uses 12-month lags for comparison
- Replaces missing `fopt` with `oancf`

---

## 138. RD.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `gvkey`, `time_avail_m`, `mve_permco`
- **m_aCompustat.parquet**: `gvkey`, `time_avail_m`, `xrd`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` line 119)
  - Contains `mve_permco` (market value of equity by permco) - from `AP_SignalMasterTable.py` line 128
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `xrd` (R&D expenses) - mapped in XBRL_TAG_MAP line 250-252

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates R&D intensity: R&D expenditure divided by market value of equity
- Formula: `RD = xrd / mve_permco`
- Simple ratio calculation

---

## 139. RDAbility.py

### Required Columns:
- **a_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `fyear`, `datadate`, `xrd`, `sale`

### AP File Status:
- ✅ **AP_a_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - ✅ **`fyear`**: Available - from `AP_CompustatAnnual.py` line 765 (extracted from `fiscal_year`) and line 935 (renamed to `fyear`)
  - ✅ **`datadate`**: Available - from `AP_CompustatAnnual.py` line 934 (set from `period_end`)
  - Contains `xrd` (R&D expenses) - mapped in XBRL_TAG_MAP line 250-252
  - Contains `sale` (sales) - mapped in XBRL_TAG_MAP line 230

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates R&D ability by regressing sales growth on lagged R&D intensity
- Uses rolling regression with 8-year window, minimum 6 observations
- Calculates R&D ability for lags 1-5 years
- Keeps R&D ability only for firms in highest R&D intensity tercile
- Expands annual observations to monthly frequency
- **Note**: Requires `fyear` for proper sorting and lagging operations

---

## 140. RDcap.py

### Required Columns:
- **m_aCompustat.parquet**: `permno`, `time_avail_m`, `at`, `xrd`
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `mve_c`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 84
  - Contains `xrd` (R&D expenses) - mapped in XBRL_TAG_MAP line 250-252
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `mve_c` (market value of equity) - from `AP_SignalMasterTable.py` line 127

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates R&D capital-to-assets: weighted sum of current and lagged R&D expenditures scaled by total assets
- Formula: `RDcap = (xrd + 0.8*xrd_lag12 + 0.6*xrd_lag24 + 0.4*xrd_lag36 + 0.2*xrd_lag48) / at`
- Uses calendar-based lags (12, 24, 36, 48 months)
- Excludes observations before 1980
- Restricted to small firms only (bottom size tertile)
- Assumes missing R&D values are 0

---

## Summary

### Overall Status:
**4 OUT OF 5 PREDICTORS CAN BE CONSTRUCTED** - One predictor (`ProbInformedTrading.py`) has missing PIN data file.

### Column Availability:
- ✅ **SignalMasterTable columns**: `permno`, `time_avail_m`, `gvkey` (surrogate), `mve_c`, `mve_permco` - All available
- ✅ **Compustat monthly columns**: `permno`, `time_avail_m`, `fopt`, `oancf`, `ib`, `at`, `dltt`, `act`, `lct`, `txt`, `xint`, `sale`, `ceq`, `xrd` - All available
- ✅ **Compustat annual columns**: `permno`, `time_avail_m`, `gvkey` (surrogate), `datadate`, `xrd`, `sale` - All available (with `fyear` verification needed)
- ✅ **CRSP monthly columns**: `permno`, `time_avail_m`, `shrout` - Available in `AP_monthlyCRSP.parquet`
- ❌ **pin_monthly.parquet**: **FILE MAY NOT EXIST** - Requires running `PINData.py` (downloads from Dropbox)

### Additional Notes:
1. **For ProbInformedTrading**: 
   - **CRITICAL**: Run `PINData.py` to generate `pin_monthly.parquet`
   - Requires Dropbox access to Easley et al. PIN data
   - **Note**: This is NOT an AP script - it downloads from a proprietary source
   - If PIN data is not available, this predictor cannot be constructed

2. **For PS**: 
   - **VERIFICATION**: Test that all financial statement columns (`fopt`, `oancf`, `ib`, `at`, `dltt`, `act`, `lct`, `txt`, `xint`, `sale`, `ceq`), `mve_permco`, and `shrout` are correctly populated
   - Verify sufficient historical coverage for 12-month lag calculation

3. **For RD**: 
   - **VERIFICATION**: Test that `xrd` and `mve_permco` are correctly populated
   - Simple ratio calculation, should work without issues

4. **For RDAbility**: 
   - **VERIFICATION**: Test that `gvkey` (surrogate), `permno`, `time_avail_m`, `fyear`, `datadate`, `xrd`, `sale` are correctly populated
   - Verify sufficient historical coverage for rolling regression (8-year window, minimum 6 observations)
   - **Note**: `fyear` is available from `AP_CompustatAnnual.py` (extracted from `fiscal_year`)

5. **For RDcap**: 
   - **VERIFICATION**: Test that `at`, `xrd`, and `mve_c` are correctly populated
   - Verify sufficient historical coverage for 48-month lag calculation
   - Restricted to small firms only (bottom size tertile)

6. **Note on gvkey Surrogate**: 
   - `gvkey` is not directly available from EDGAR (Compustat-specific identifier)
   - AP version uses `cik` (SEC identifier) as surrogate, falls back to `permno` if unavailable
   - For `ProbInformedTrading.py` and `RD.py`, `gvkey` is loaded but not used in calculations (only kept for compatibility)
   - For `RDAbility.py`, `gvkey` is used for grouping and lagging operations, surrogate approach is acceptable

7. **Note on File Names**: 
   - File name mismatches are ignored per user instructions (from Group 19 onwards)
   - Predictors will need to be modified to use AP file names (`AP_monthlyCRSP.parquet`, `AP_a_aCompustat.parquet`) or AP files can be renamed to match expected names

