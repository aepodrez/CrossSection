# Group 27 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 27.

---

## 131. PatentsRD.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `gvkey`, `time_avail_m`, `mve_c`, `sicCRSP`, `exchcd`
- **m_aCompustat.parquet**: `permno`, `time_avail_m`, `xrd`, `sich`, `datadate`, `ceq`
- **PatentDataProcessed.parquet**: `gvkey`, `year`, `npat`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` line 119)
  - Contains `mve_c` (market value of equity) - from `AP_SignalMasterTable.py` line 127
  - Contains `sicCRSP` (SIC code from CRSP) - from `AP_SignalMasterTable.py` line 134
  - Contains `exchcd` (exchange code) - from `AP_SignalMasterTable.py` line 130
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `xrd` (R&D expenses) - mapped in XBRL_TAG_MAP line 250-252
  - ⚠️ **`sich`**: **NOT EXTRACTED** - Historical SIC code is not extracted from XBRL in `AP_CompustatAnnual.py`. However, `sich` is loaded but **NOT USED** in the predictor logic (only `sicCRSP` is used for filtering).
  - ✅ **`datadate`**: Available - from `AP_CompustatAnnual.py` line 934 (set from `period_end`)
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184
- ❌ **PatentDataProcessed.parquet**: **FILE MAY NOT EXIST**
  - Script exists: `PatentCitations.py` (generates `PatentDataProcessed.parquet`)
  - Would contain `gvkey`, `year`, `npat` (number of patents)
  - Requires patent citation data from USPTO/patent databases

### Can Be Constructed?
**PARTIALLY** - Missing `PatentDataProcessed.parquet`. Note: `sich` is loaded but not used in calculations.

### Additional Work Needed?
**YES** - Missing patent data:

#### What Needs to Be Done:
1. **Create PatentDataProcessed.parquet**: 
   - Run `PatentCitations.py` to generate `PatentDataProcessed.parquet`
   - Requires patent citation database access (USPTO or proprietary sources)
   - Must contain `gvkey`, `year`, `npat` columns

2. **Note on `sich`**: 
   - `sich` is loaded in the predictor but **NOT USED** in the actual signal construction
   - Only `sicCRSP` from `SignalMasterTable` is used for filtering financial firms (line 119)
   - Can be set to empty/NaN or omitted from the data load without affecting results

#### Implementation Notes:
- Predictor calculates patent efficiency scaled by R&D capital with double-sorted portfolio approach
- Formula: `PatentsRD = npat / RDcap` where `RDcap` is sum of depreciated past R&D
- Portfolios are computed from July of year t to June of year t+1
- Filters: Excludes financial firms (SIC 6000-6999), requires positive `ceq`
- **Note**: `sich` is loaded but not used - can be omitted or set to NaN

---

## 132. PayoutYield.py

### Required Columns:
- **m_aCompustat.parquet**: `permno`, `time_avail_m`, `dvc`, `prstkc`, `pstkrv`, `sstk`, `sic`, `ceq`, `datadate`
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `mve_permco`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `dvc` (cash dividends) - mapped in XBRL_TAG_MAP line 285-286
  - Contains `prstkc` (purchase of common stock) - mapped in XBRL_TAG_MAP line 217-218
  - ❌ **`pstkrv`**: **NOT MAPPABLE** - Preferred stock redemption value is not mappable from XBRL (line 199: "Not mappable (numeric XBRL fact almost never provided)"). Not included in zero_fill_vars, so likely missing from output.
  - Contains `sstk` (sale of common stock) - mapped in XBRL_TAG_MAP line 222-223
  - ✅ **`sic`**: **ADDED** - SIC code has been added to `AP_m_aCompustat.parquet`
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184
  - ✅ **`datadate`**: Available - from `AP_CompustatAnnual.py` line 934 (set from `period_end`)
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `mve_permco` (market value of equity by permco) - from `AP_SignalMasterTable.py` line 128

### Can Be Constructed?
**PARTIALLY** - Missing `pstkrv` (preferred stock redemption value).

### Additional Work Needed?
**YES** - Missing `pstkrv`:

#### What Needs to Be Done:
1. **Handle `pstkrv`**: 
   - `pstkrv` is not mappable from XBRL (line 199: "Not mappable (numeric XBRL fact almost never provided)")
   - **Option 1**: Zero-fill `pstkrv` (add to `zero_fill_vars` list in `AP_CompustatAnnual.py`)
   - **Option 2**: Use `pstkl` (preferred stock liquidation value) as proxy if available
   - **Option 3**: Use `pstk` (preferred stock carrying amount) as proxy
   - **Impact**: Formula is `PayoutYield = (dvc + prstkc + pstkrv) / mve_permco_l6`. If `pstkrv` is zero-filled, it will underestimate payout yield for firms with preferred stock redemptions.

#### Implementation Notes:
- Predictor calculates payout yield scaled by lagged market value of equity
- Formula: `PayoutYield = (dvc + prstkc + pstkrv) / mve_permco_l6`
- Uses 6-month calendar-based lag of `mve_permco`
- Sets negative or zero payout yields to missing
- Filters: Excludes financial companies (SIC 6000-6999), requires positive `ceq`, requires at least 24 historical observations per company
- **Current Issue**: `pstkrv` is missing, which will underestimate payout yield for firms with preferred stock redemptions

---

## 133. PctAcc.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `ib`, `oancf`, `dp`, `act`, `che`, `lct`, `txp`, `dlc`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `ib` (income before extraordinary items) - mapped in XBRL_TAG_MAP line 262-264
  - Contains `oancf` (operating cash flow) - mapped in XBRL_TAG_MAP line 280-282
  - Contains `dp` (depreciation and amortization) - mapped in XBRL_TAG_MAP line 238
  - Contains `act` (current assets) - mapped in XBRL_TAG_MAP line 90
  - Contains `che` (cash and cash equivalents) - mapped in XBRL_TAG_MAP line 88
  - Contains `lct` (current liabilities) - mapped in XBRL_TAG_MAP line 133
  - Contains `txp` (income taxes payable) - mapped in XBRL_TAG_MAP line 163-164
  - Contains `dlc` (debt current) - mapped in XBRL_TAG_MAP line 136-139

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates percent operating accruals: income before extraordinary items minus net cash flow, scaled by absolute income
- Formula: `PctAcc = (ib - oancf) / abs(ib)`
- Uses balance sheet approach when cash flow data is unavailable: `PctAcc = (delta(act) - delta(che) - delta(lct) + delta(dlc) + delta(txp) + dp) / abs(ib)`
- Handles case when `ib == 0` by dividing by 0.01
- Uses 12-month lagged values for balance sheet approach

---

## 134. PctTotAcc.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `ni`, `prstkcc`, `sstk`, `dvt`, `oancf`, `fincf`, `ivncf`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `ni` (net income) - mapped in XBRL_TAG_MAP line 265-266
  - ⚠️ **`prstkcc`**: **ZERO-FILLED** - Preferred stock repurchases are rarely broken out, not mappable from XBRL (line 219: "Rarely broken out, not mappable"), currently zero-filled if missing (from `zero_fill_vars` list line 1035)
  - Contains `sstk` (sale of common stock) - mapped in XBRL_TAG_MAP line 222-223
  - Contains `dvt` (total dividends) - mapped in XBRL_TAG_MAP line 287-288
  - Contains `oancf` (operating cash flow) - mapped in XBRL_TAG_MAP line 280-282
  - Contains `fincf` (financing cash flow) - mapped in XBRL_TAG_MAP line 316
  - Contains `ivncf` (investing cash flow) - mapped in XBRL_TAG_MAP line 311

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate, `prstkcc` is zero-filled).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates percent total accruals: net income minus cash flows from operations, financing and investment, scaled by absolute net income
- Formula: `PctTotAcc = (ni - (prstkcc - sstk + dvt + oancf + fincf + ivncf)) / abs(ni)`
- **Note**: `prstkcc` is zero-filled, which may underestimate total accruals for firms with preferred stock repurchases. However, this is a known limitation and does not prevent construction.
- **Note**: `gvkey` is loaded but not used in calculations (only kept for compatibility)

---

## 135. Price.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `prc`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `prc` (price) - from `AP_CRSPMonthly.py` line 349, included in `AP_SignalMasterTable.py` line 132

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates log of absolute value of stock price
- Formula: `Price = log(abs(prc))`
- Simple transformation of price data from SignalMasterTable

---

## Summary

### Overall Status:
**3 OUT OF 5 PREDICTORS CAN BE CONSTRUCTED** - Two predictors (`PatentsRD.py` and `PayoutYield.py`) have missing columns.

### Column Availability:
- ✅ **SignalMasterTable columns**: `permno`, `time_avail_m`, `gvkey` (surrogate), `mve_c`, `sicCRSP`, `exchcd`, `mve_permco`, `prc` - All available
- ✅ **Compustat annual columns**: `gvkey` (surrogate), `permno`, `time_avail_m`, `xrd`, `datadate`, `ceq`, `dvc`, `prstkc`, `sstk`, `sic` (added), `ib`, `oancf`, `dp`, `act`, `che`, `lct`, `txp`, `dlc`, `ni`, `dvt`, `fincf`, `ivncf` - All available
- ⚠️ **`sich`**: **NOT EXTRACTED** - Historical SIC code is not extracted from XBRL, but is not used in `PatentsRD.py` calculations
- ⚠️ **`prstkcc`**: **ZERO-FILLED** - Preferred stock repurchases are rarely broken out, zero-filled if missing
- ❌ **`pstkrv`**: **NOT MAPPABLE** - Preferred stock redemption value is not mappable from XBRL, likely missing from output
- ❌ **PatentDataProcessed.parquet**: **FILE MAY NOT EXIST** - Requires patent citation database access

### Additional Notes:
1. **For PatentsRD**: 
   - **CRITICAL**: Run `PatentCitations.py` to generate `PatentDataProcessed.parquet`
   - Requires patent citation database access (USPTO, NBER Patent Database, or proprietary sources)
   - **Note on `sich`**: While `sich` is loaded, it is not used in calculations. Can be set to NaN/empty or omitted from data load.
   - If patent data is not available, this predictor cannot be constructed with AP data sources

2. **For PayoutYield**: 
   - **CRITICAL**: Missing `pstkrv` (preferred stock redemption value)
   - **Recommended**: Zero-fill `pstkrv` (add to `zero_fill_vars` list in `AP_CompustatAnnual.py`)
   - **Alternative**: Use `pstkl` (preferred stock liquidation value) or `pstk` (preferred stock carrying amount) as proxy
   - **Impact**: If `pstkrv` is zero-filled, it will underestimate payout yield for firms with preferred stock redemptions

3. **For PctAcc**: 
   - **VERIFICATION**: Test that all financial statement columns (`ib`, `oancf`, `dp`, `act`, `che`, `lct`, `txp`, `dlc`) are correctly populated
   - Verify sufficient historical coverage for 12-month lag calculation

4. **For PctTotAcc**: 
   - **VERIFICATION**: Test that all financial statement columns (`ni`, `prstkcc`, `sstk`, `dvt`, `oancf`, `fincf`, `ivncf`) are correctly populated
   - **Note**: `prstkcc` is zero-filled, which may underestimate total accruals for firms with preferred stock repurchases

5. **For Price**: 
   - **VERIFICATION**: Test that `prc` is correctly populated in `AP_SignalMasterTable.parquet`
   - Simple transformation, should work without issues

6. **Note on gvkey Surrogate**: 
   - `gvkey` is not directly available from EDGAR (Compustat-specific identifier)
   - AP version uses `cik` (SEC identifier) as surrogate, falls back to `permno` if unavailable
   - For `PctAcc.py` and `PctTotAcc.py`, `gvkey` is loaded but not used in calculations (only kept for compatibility)
   - For `PatentsRD.py`, `gvkey` is used for merging with patent data, surrogate approach is acceptable

7. **Note on Preferred Stock Variables**: 
   - `pstkrv` (preferred stock redemption value) is not mappable from XBRL and likely missing from output
   - `prstkcc` (preferred stock repurchases) is zero-filled
   - `pstkl` (preferred stock liquidation value) has low coverage but may be available
   - `pstk` (preferred stock carrying amount) is available
   - **Impact**: Predictors that use preferred stock variables may have limited accuracy for firms with preferred stock

