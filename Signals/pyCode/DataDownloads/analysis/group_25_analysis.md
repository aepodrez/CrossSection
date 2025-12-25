# Group 25 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 25.

---

## 121. NetEquityFinance.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `sstk`, `prstkc`, `at`, `dv`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `sstk` (stock issuance) - mapped in XBRL_TAG_MAP line 222-223
  - Contains `prstkc` (stock repurchases) - mapped in XBRL_TAG_MAP line 217-218
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 84
  - Contains `dv` (dividends) - mapped in XBRL_TAG_MAP line 340

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates net equity financing scaled by average total assets
- Formula: `NetEquityFinance = (sstk - prstkc - dv) / (0.5 * (at + l12_at))`
- Uses 12-month lag of total assets
- Removes extreme values (absolute value greater than 1)
- **Note**: `gvkey` is loaded but not used in calculations (only kept for compatibility)

---

## 122. NetPayoutYield.py

### Required Columns:
- **m_aCompustat.parquet**: `permno`, `time_avail_m`, `dvc`, `prstkc`, `sstk`, `sic`, `ceq`
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `mve_permco`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `dvc` (common dividends) - mapped in XBRL_TAG_MAP line 336
  - Contains `prstkc` (stock repurchases) - mapped in XBRL_TAG_MAP line 217-218
  - Contains `sstk` (stock issuance) - mapped in XBRL_TAG_MAP line 222-223
  - ✅ **`sic`**: **ADDED** - SIC code has been added to `AP_m_aCompustat.parquet`
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `mve_permco` (market value of equity per company) - from `AP_SignalMasterTable.py` line 128

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates net payout yield scaled by lagged market value of equity
- Formula: `NetPayoutYield = (dvc + prstkc - sstk) / mve_permco_l6`
- Uses 6-month calendar-based lag of market value
- Filters out financial firms (SIC 6000-6999) and requires positive book equity
- Requires at least 24 observations per firm for stability

---

## 123. NOA.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `at`, `che`, `dltt`, `mib`, `dc`, `ceq`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 84
  - Contains `che` (cash) - mapped in XBRL_TAG_MAP line 88
  - Contains `dltt` (long-term debt) - mapped in XBRL_TAG_MAP line 142-144
  - Contains `mib` (minority interest) - mapped in XBRL_TAG_MAP line 182
  - ⚠️ **`dc`**: **ZERO-FILLED** - Deferred charges is listed as derived field (line 382: `['dcpstk', 'pstk', 'dcvt']`) but not implemented, currently zero-filled if missing
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate, dc is zero-filled).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates Net Operating Assets: `NOA = (OA - OL) / l12_at` where:
  - `OA = at - che` (Operating Assets)
  - `OL = at - dltt - mib - dc - ceq` (Operating Liabilities)
- Uses 12-month lag of total assets
- **Note**: `dc` is zero-filled, which may affect accuracy for firms with convertible debt, but predictor can still be constructed
- **Note**: `gvkey` is loaded but not used in calculations (only kept for compatibility)

---

## 124. NumEarnIncrease.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `gvkey`, `time_avail_m`
- **m_QCompustat.parquet**: `gvkey`, `time_avail_m`, `ibq`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` line 119)
- ✅ **AP_m_QCompustat.parquet**: 
  - Contains `gvkey` (surrogate: CIK/permno) - from `AP_CompustatQuarterly.py` structure
  - Contains `time_avail_m` (from `AP_CompustatQuarterly.py` structure)
  - Contains `ibq` (income before extraordinary items quarterly) - mapped in XBRL_TAG_MAP line 202

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor counts consecutive quarters with positive earnings growth, up to 8 quarters
- Calculates earnings change: `chearn = ibq - l12_ibq` (year-over-year change)
- Uses calendar-based lags for quarterly data (3, 6, 9, 12, 15, 18, 21, 24 months)
- Sets `NumEarnIncrease` to 1-8 based on consecutive positive earnings growth quarters
- Missing earnings growth is treated as positive (conservative approach)

---

## 125. OperProf.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `gvkey`, `time_avail_m`, `mve_c`
- **m_aCompustat.parquet**: `gvkey`, `time_avail_m`, `revt`, `cogs`, `xsga`, `xint`, `ceq`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `mve_c` (market value of equity) - from `AP_SignalMasterTable.py` line 127
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` line 119)
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `gvkey` (surrogate: CIK/permno) - from `AP_CompustatAnnual.py` structure
  - Contains `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `revt` (total revenue) - mapped in XBRL_TAG_MAP line 232
  - Contains `cogs` (cost of goods sold) - mapped in XBRL_TAG_MAP line 235-236
  - Contains `xsga` (selling, general, administrative expenses) - mapped in XBRL_TAG_MAP line 239
  - Contains `xint` (interest expense) - mapped in XBRL_TAG_MAP line 244-245
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates operating profitability scaled by book equity
- Formula: `OperProf = (revt - cogs - xsga - xint) / ceq`
- Excludes smallest size tercile (simulates NYSE size breakpoints)
- Creates size terciles by `time_avail_m` and sets `OperProf` to missing for smallest tercile

---

## Summary

### Overall Status:
**ALL 5 PREDICTORS CAN BE CONSTRUCTED** - All required columns are available in AP data sources.

### Column Availability:
- ✅ **SignalMasterTable columns**: `permno`, `time_avail_m`, `gvkey` (surrogate), `mve_permco`, `mve_c` - All available
- ✅ **Compustat annual columns**: `gvkey` (surrogate), `permno`, `time_avail_m`, `sstk`, `prstkc`, `at`, `dv`, `dvc`, `sic` (added), `ceq`, `che`, `dltt`, `mib`, `dc` (zero-filled), `revt`, `cogs`, `xsga`, `xint` - All available
- ✅ **Compustat quarterly columns**: `gvkey` (surrogate), `time_avail_m`, `ibq` - All available

### Additional Notes:
1. **For NetEquityFinance**: 
   - **VERIFICATION**: Test that `sstk`, `prstkc`, `dv`, `at` are correctly populated
   - Verify sufficient historical coverage for 12-month lag calculation

2. **For NetPayoutYield**: 
   - **VERIFICATION**: Test that `dvc`, `prstkc`, `sstk`, `sic`, `ceq`, `mve_permco` are correctly populated
   - Verify sufficient historical coverage for 6-month lag calculation
   - Note: Requires at least 24 observations per firm

3. **For NOA**: 
   - **VERIFICATION**: Test that `at`, `che`, `dltt`, `mib`, `dc`, `ceq` are correctly populated
   - Verify sufficient historical coverage for 12-month lag calculation
   - **Note**: `dc` is zero-filled, which may affect accuracy for firms with convertible debt, but predictor can still be constructed

4. **For NumEarnIncrease**: 
   - **VERIFICATION**: Test that `ibq` is correctly populated in `AP_m_QCompustat.parquet`
   - Verify sufficient historical coverage for 24-month lag calculation (requires at least 2 years of quarterly data)
   - Note: Uses quarterly data with calendar-based lags

5. **For OperProf**: 
   - **VERIFICATION**: Test that `revt`, `cogs`, `xsga`, `xint`, `ceq`, `mve_c` are correctly populated
   - Verify sufficient cross-sectional coverage for size tercile calculation

6. **Note on gvkey Surrogate**: 
   - `gvkey` is not directly available from EDGAR (Compustat-specific identifier)
   - AP version uses `cik` (SEC identifier) as surrogate, falls back to `permno` if unavailable
   - For `NetEquityFinance.py` and `NOA.py`, `gvkey` is loaded but not used in calculations (only kept for compatibility)
   - For `NumEarnIncrease.py` and `OperProf.py`, `gvkey` is used for merging with Compustat data, surrogate approach is acceptable

7. **Note on dc (Deferred Charges)**: 
   - `dc` is listed as a derived field in `AP_CompustatAnnual.py` (line 382: `['dcpstk', 'pstk', 'dcvt']`) but derivation logic is not implemented
   - Currently zero-filled if missing (from `zero_fill_vars` list)
   - For `NOA.py`, `dc` is used in calculation: `OL = at - dltt - mib - dc - ceq`
   - Zero-filling `dc` may affect accuracy for firms with convertible debt, but predictor can still be constructed
   - **Optional Enhancement**: Implement derivation logic for `dc` from `dcpstk`, `pstk`, and `dcvt` if available

