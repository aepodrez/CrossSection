# Group 4 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 4.

---

## 16. BookLeverage.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `permno`, `time_avail_m`, `at`, `lt`, `txditc`, `pstk`, `pstkrv`, `pstkl`, `seq`, `ceq`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 86
  - Contains `lt` (total liabilities) - mapped in XBRL_TAG_MAP line 130
  - Contains `txditc` (deferred tax) - mapped in XBRL_TAG_MAP line 341-343
  - Contains `pstk` (preferred stock) - mapped in XBRL_TAG_MAP line 189
  - ⚠️ **`pstkrv`** (preferred stock redemption value) - marked as "not mappable" in XBRL_TAG_MAP line 192
  - Contains `pstkl` (preferred stock liquidation) - mapped in XBRL_TAG_MAP line 191
  - Contains `seq` (stockholders equity) - mapped in XBRL_TAG_MAP line 185-186
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184

### Can Be Constructed?
**YES** - All required columns are present, though `pstkrv` will be missing (has fallback logic).

### Additional Work Needed?
None. The predictor has fallback logic that handles missing `pstkrv`:
```python
df["tempPS"] = df["pstk"].copy()
df["tempPS"] = df["tempPS"].fillna(df["pstkrv"])  # Will be NaN
df["tempPS"] = df["tempPS"].fillna(df["pstkl"])    # Will use this
```

---

## 17. BrandInvest.py

### Required Columns:
- **AP_a_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `fyear`, `datadate`, `xad`, `xad0`, `at`, `sic`

### AP File Status:
- ✅ **AP_a_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` lines 64-66)
  - Contains `fyear` (fiscal year) - renamed from `fiscal_year` in `AP_CompustatAnnual.py` line 855
  - Contains `datadate` (period end date) - renamed from `period_end` in `AP_CompustatAnnual.py` line 854
  - Contains `xad` (advertising expense) - mapped in XBRL_TAG_MAP line 233-234
  - Contains `xad0` (zero-filled advertising) - created in `AP_CompustatAnnual.py` line 931
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 86
  - ✅ **`sic`**: **ADDED** - SIC code has been added to `AP_a_aCompustat.parquet`

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate, sic has been added).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 18. Cash.py

### Required Columns:
- **AP_m_QCompustat.parquet**: `gvkey`, `rdq`, `cheq`, `atq`
- **AP_SignalMasterTable.parquet**: `permno`, `gvkey`, `time_avail_m`

### AP File Status:
- ✅ **AP_m_QCompustat.parquet**: 
  - Contains `gvkey` (surrogate: CIK/permno) - from `AP_CompustatQuarterly.py` structure
  - Contains `rdq` (report date/filing date) - from `AP_CompustatQuarterly.py` line 550
  - Contains `cheq` (cash quarterly) - mapped in XBRL_TAG_MAP (quarterly version)
  - Contains `atq` (total assets quarterly) - mapped in XBRL_TAG_MAP (quarterly version)
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 100-103)
  - Contains `gvkey` (surrogate: CIK/permno) - from `AP_SignalMasterTable.py` line 96

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 19. CashProd.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `permno`, `time_avail_m`, `at`, `che`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `mve_permco`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 86
  - Contains `che` (cash) - mapped in XBRL_TAG_MAP line 88
- ✅ **AP_SignalMasterTable.parquet**: Contains `mve_permco` (from `AP_SignalMasterTable.py` line 105)

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 20. CBOperProf.py

### Required Columns:
- **AP_SignalMasterTable.parquet**: `permno`, `gvkey`, `time_avail_m`, `exchcd`, `sicCRSP`, `shrcd`, `mve_permco`, `mve_c`
- **AP_m_aCompustat.parquet**: `permno`, `time_avail_m`, `revt`, `cogs`, `xsga`, `xrd`, `rect`, `invt`, `xpp`, `drc`, `drlt`, `ap`, `xacc`, `at`, `ceq`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 100-103)
  - Contains `gvkey` (surrogate: CIK/permno) - from `AP_SignalMasterTable.py` line 96
  - Contains `exchcd` (exchange code) - from `AP_SignalMasterTable.py` line 107
  - Contains `sicCRSP` (SIC code) - from `AP_SignalMasterTable.py` line 111
  - Contains `shrcd` (share code) - from `AP_SignalMasterTable.py` line 108
  - Contains `mve_permco` (market value) - from `AP_SignalMasterTable.py` line 105
  - Contains `mve_c` (market value) - from `AP_SignalMasterTable.py` line 104
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m`
  - Contains `revt` (revenue) - mapped in XBRL_TAG_MAP line 225
  - Contains `cogs` (cost of goods sold) - mapped in XBRL_TAG_MAP line 228-229
  - Contains `xsga` (SG&A) - mapped in XBRL_TAG_MAP line 232
  - Contains `xrd` (R&D) - mapped in XBRL_TAG_MAP line 235-236
  - Contains `rect` (receivables) - mapped in XBRL_TAG_MAP line 92
  - Contains `invt` (inventory) - mapped in XBRL_TAG_MAP line 94
  - Contains `xpp` (prepaid expenses) - mapped in XBRL_TAG_MAP line 239-240
  - Contains `drc` (deferred revenue current) - mapped in XBRL_TAG_MAP line 166
  - Contains `drlt` (deferred revenue noncurrent) - mapped in XBRL_TAG_MAP line 167
  - Contains `ap` (accounts payable) - mapped in XBRL_TAG_MAP line 152-153
  - Contains `xacc` (accrued expenses) - mapped in XBRL_TAG_MAP line 241-242
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 86
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## Summary

| Predictor | All Columns Available? | Can Be Constructed? | Additional Work Needed? |
|-----------|----------------------|---------------------|----------------------|
| BookLeverage | ⚠️ Partial* | ✅ Yes | None (*pstkrv missing but has fallback) |
| BrandInvest | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate, sic added) |
| Cash | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |
| CashProd | ✅ Yes | ✅ Yes | None |
| CBOperProf | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |

### Critical Missing Component:
None - All required columns are now available.

### Recommendations:
1. **Verification**: 
   - Test that all AP files actually contain the columns listed above by loading sample data
   - Verify `xad0` is correctly populated in `AP_a_aCompustat.parquet`
   - Verify `rdq` is correctly populated in `AP_m_QCompustat.parquet`
   - Verify `sic` is correctly populated in `AP_a_aCompustat.parquet` (recently added)

2. **Note**: SIC codes in AP data may be approximate (from heuristic mapping or ticker mapping file) rather than official CRSP SIC codes, but should be sufficient for industry filtering purposes.
