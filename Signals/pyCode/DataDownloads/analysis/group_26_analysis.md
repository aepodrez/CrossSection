# Group 26 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 26.

---

## 126. OperProfRD.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `gvkey`, `time_avail_m`, `exchcd`, `sicCRSP`, `mve_c`, `shrcd`
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `xrd`, `revt`, `cogs`, `xsga`, `at`, `ceq`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` line 119)
  - Contains `exchcd` (exchange code) - from `AP_SignalMasterTable.py` line 130
  - Contains `sicCRSP` (SIC code from CRSP) - from `AP_SignalMasterTable.py` line 134
  - Contains `mve_c` (market value of equity) - from `AP_SignalMasterTable.py` line 127
  - Contains `shrcd` (share code) - from `AP_CRSPMonthly.py` line 353, included in `AP_SignalMasterTable.py` line 131
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `gvkey` (surrogate: CIK/permno) - from `AP_CompustatAnnual.py` structure
  - Contains `xrd` (R&D expenses) - mapped in XBRL_TAG_MAP line 250-252
  - Contains `revt` (total revenue) - mapped in XBRL_TAG_MAP line 232
  - Contains `cogs` (cost of goods sold) - mapped in XBRL_TAG_MAP line 235-236
  - Contains `xsga` (selling, general, administrative expenses) - mapped in XBRL_TAG_MAP line 239
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 84
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates R&D-adjusted operating profitability scaled by total assets
- Formula: `OperProfRD = (revt - cogs - xsga + xrd) / at`
- Handles missing R&D by setting to zero
- Filters: `shrcd <= 11`, excludes financial firms (SIC 6000-6999), requires non-missing `mve_c`, `ceq`, `at`

---

## 127. OPLeverage.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `xsga`, `cogs`, `at`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `xsga` (selling, general, administrative expenses) - mapped in XBRL_TAG_MAP line 239
  - Contains `cogs` (cost of goods sold) - mapped in XBRL_TAG_MAP line 235-236
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 84

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates operating leverage: sum of administrative expenses and cost of goods sold, scaled by total assets
- Formula: `OPLeverage = (xsga + cogs) / at`
- Sets missing SGA expenses to zero
- **Note**: `gvkey` is loaded but not used in calculations (only kept for compatibility)

---

## 128. OrderBacklog.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `ob`, `at`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - ⚠️ **`ob`**: **ZERO-FILLED** - Order backlog is not GAAP, not reliably mappable from XBRL (line 185: "Not GAAP, not reliably mappable"), currently zero-filled if missing (from `zero_fill_vars` list line 1032)
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 84

### Can Be Constructed?
**PARTIALLY** - `ob` is zero-filled, which will result in `OrderBacklog = 0` for all observations (since `ob / (0.5 * (at + l12_at))` with `ob = 0` gives 0, and predictor sets to missing if `ob == 0`).

### Additional Work Needed?
**YES** - `ob` (order backlog) is zero-filled, which means the predictor will have no valid observations.

#### What Needs to Be Done:
1. **Extract ob from XBRL**: 
   - Order backlog is not a standard GAAP item and may not be reliably mappable from XBRL
   - May need to search for custom XBRL tags or extract from footnotes/narrative sections
   - Alternative: Extract from MD&A (Management Discussion & Analysis) text sections of 10-K filings

2. **Alternative Data Source**: 
   - Order backlog may be reported in company earnings calls or press releases
   - Would require text mining or manual data collection
   - May have limited historical coverage

#### Implementation Notes:
- Predictor calculates order backlog scaled by average total assets
- Formula: `OrderBacklog = ob / (0.5 * (at + l12_at))`
- Sets to missing if `ob == 0`
- **Current Issue**: Since `ob` is zero-filled, all `OrderBacklog` values will be set to missing (predictor filters out `ob == 0`)
- **Note**: `gvkey` is loaded but not used in calculations (only kept for compatibility)

---

## 129. OrderBacklogChg.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `ob`, `at`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - ⚠️ **`ob`**: **ZERO-FILLED** - Order backlog is not GAAP, not reliably mappable from XBRL (line 185: "Not GAAP, not reliably mappable"), currently zero-filled if missing (from `zero_fill_vars` list line 1032)
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 84

### Can Be Constructed?
**PARTIALLY** - `ob` is zero-filled, which will result in `OrderBacklogChg = 0` for all observations (since `OrderBacklog` will be missing when `ob == 0`).

### Additional Work Needed?
**YES** - `ob` (order backlog) is zero-filled, which means the predictor will have no valid observations.

#### What Needs to Be Done:
1. **Extract ob from XBRL**: 
   - Order backlog is not a standard GAAP item and may not be reliably mappable from XBRL
   - May need to search for custom XBRL tags or extract from footnotes/narrative sections
   - Alternative: Extract from MD&A (Management Discussion & Analysis) text sections of 10-K filings

2. **Alternative Data Source**: 
   - Order backlog may be reported in company earnings calls or press releases
   - Would require text mining or manual data collection
   - May have limited historical coverage

#### Implementation Notes:
- Predictor calculates change in normalized order backlog
- Formula: `OrderBacklogChg = OrderBacklog - OrderBacklog_lag12`
- Where `OrderBacklog = ob / (0.5 * (at + l12_at))`
- Sets to missing if `ob == 0`
- **Current Issue**: Since `ob` is zero-filled, all `OrderBacklog` values will be set to missing, resulting in no valid `OrderBacklogChg` observations
- **Note**: `gvkey` is loaded but not used in calculations (only kept for compatibility)

---

## 130. OScore.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `fopt`, `at`, `lt`, `act`, `lct`, `ib`, `oancf`, `sic`
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `prc`
- **GNPdefl.parquet**: `time_avail_m`, `gnpdefl`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `fopt` (foreign operations income) - mapped in XBRL_TAG_MAP line 291
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 84
  - Contains `lt` (total liabilities) - mapped in XBRL_TAG_MAP line 137
  - Contains `act` (current assets) - mapped in XBRL_TAG_MAP line 90
  - Contains `lct` (current liabilities) - mapped in XBRL_TAG_MAP line 133
  - Contains `ib` (income before extraordinary items) - mapped in XBRL_TAG_MAP line 262-264
  - Contains `oancf` (operating cash flow) - mapped in XBRL_TAG_MAP line 280-282
  - ✅ **`sic`**: **ADDED** - SIC code has been added to `AP_m_aCompustat.parquet`
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `prc` (price) - from `AP_CRSPMonthly.py` line 349, included in `AP_SignalMasterTable.py` line 132
- ✅ **AP_GNPdefl.parquet**: 
  - File exists (from `AP_GNPDeflator.py`)
  - Contains `time_avail_m` (from `AP_GNPDeflator.py` line 181)
  - Contains `gnpdefl` (GNP deflator) - from `AP_GNPDeflator.py` line 178

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates O-Score bankruptcy predictor using Dichev formula
- Uses multiple financial ratios: log(assets/GNP deflator), liabilities/assets, working capital/assets, current ratio, leverage indicator, income/assets, operating income/liabilities, loss indicators
- Replaces missing `fopt` with `oancf`
- Uses 12-month calendar-based lag of `ib`
- Filters out utilities (SIC 4000-4999) and financial firms (SIC 6000+)
- Creates deciles and forms binary signal: long (OScore=1) for top decile, short (OScore=0) for bottom 7 deciles

---

## Summary

### Overall Status:
**3 OUT OF 5 PREDICTORS CAN BE CONSTRUCTED** - Two predictors (`OrderBacklog.py` and `OrderBacklogChg.py`) require `ob` (order backlog) which is zero-filled.

### Column Availability:
- ✅ **SignalMasterTable columns**: `permno`, `time_avail_m`, `gvkey` (surrogate), `exchcd`, `sicCRSP`, `mve_c`, `shrcd`, `prc` - All available
- ✅ **Compustat annual columns**: `gvkey` (surrogate), `permno`, `time_avail_m`, `xrd`, `revt`, `cogs`, `xsga`, `at`, `ceq`, `fopt`, `lt`, `act`, `lct`, `ib`, `oancf`, `sic` (added) - All available
- ⚠️ **`ob`**: **ZERO-FILLED** - Order backlog is not GAAP, not reliably mappable from XBRL, currently zero-filled
- ✅ **GNP deflator**: `time_avail_m`, `gnpdefl` - Available in `AP_GNPdefl.parquet`

### Additional Notes:
1. **For OperProfRD**: 
   - **VERIFICATION**: Test that `xrd`, `revt`, `cogs`, `xsga`, `at`, `ceq`, `mve_c`, `shrcd` are correctly populated
   - Verify sufficient cross-sectional coverage for filtering

2. **For OPLeverage**: 
   - **VERIFICATION**: Test that `xsga`, `cogs`, `at` are correctly populated
   - Note: Missing SGA expenses are set to zero

3. **For OrderBacklog and OrderBacklogChg**: 
   - **CRITICAL**: `ob` (order backlog) is zero-filled, which means these predictors will have no valid observations
   - Order backlog is not a standard GAAP item and is not reliably mappable from XBRL
   - **Recommended**: Extract from MD&A text sections or alternative data sources (earnings calls, press releases)
   - Would require text mining or manual data collection
   - May have limited historical coverage

4. **For OScore**: 
   - **VERIFICATION**: Test that all financial statement columns (`fopt`, `at`, `lt`, `act`, `lct`, `ib`, `oancf`, `sic`), `prc`, and `gnpdefl` are correctly populated
   - Verify sufficient historical coverage for 12-month lag calculation
   - Note: Predictor uses complex formula with multiple financial ratios

5. **Note on gvkey Surrogate**: 
   - `gvkey` is not directly available from EDGAR (Compustat-specific identifier)
   - AP version uses `cik` (SEC identifier) as surrogate, falls back to `permno` if unavailable
   - For `OPLeverage.py`, `OrderBacklog.py`, and `OrderBacklogChg.py`, `gvkey` is loaded but not used in calculations (only kept for compatibility)
   - For `OperProfRD.py` and `OScore.py`, `gvkey` is used for merging with Compustat data, surrogate approach is acceptable

6. **Note on ob (Order Backlog)**: 
   - Order backlog (`ob`) is not a standard GAAP financial statement item
   - It's typically reported in MD&A sections or earnings calls, not in XBRL financial statements
   - Currently zero-filled in `AP_CompustatAnnual.py` (line 1032)
   - **Impact**: `OrderBacklog.py` and `OrderBacklogChg.py` will have no valid observations since predictor sets to missing when `ob == 0`
   - **Recommended**: Extract from MD&A text sections using NLP/text mining, or use alternative data sources
   - May require significant data engineering and may have limited historical coverage

