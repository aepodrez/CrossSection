# Group 7 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 7.

---

## 31. ChNWC.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `permno`, `time_avail_m`, `act`, `che`, `lct`, `dlc`, `at`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `act` (current assets) - mapped in XBRL_TAG_MAP line 90
  - Contains `che` (cash) - mapped in XBRL_TAG_MAP line 88
  - Contains `lct` (current liabilities) - mapped in XBRL_TAG_MAP line 133
  - Contains `dlc` (debt current) - mapped in XBRL_TAG_MAP line 136-139
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 86

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 32. ChTax.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `permno`, `gvkey`, `time_avail_m`, `at`
- **AP_m_QCompustat.parquet**: `gvkey`, `time_avail_m`, `txtq`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 86
- ✅ **AP_m_QCompustat.parquet**: 
  - Contains `gvkey` (surrogate: CIK/permno) - from `AP_CompustatQuarterly.py` structure
  - Contains `time_avail_m` (from `AP_CompustatQuarterly.py` structure)
  - Contains `txtq` (total taxes quarterly) - mapped in XBRL_TAG_MAP line 196

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 33. CitationsRD.py

### Required Columns:
- **AP_SignalMasterTable.parquet**: `permno`, `gvkey`, `time_avail_m`, `mve_c`, `sicCRSP`, `exchcd`
- **AP_m_aCompustat.parquet**: `permno`, `time_avail_m`, `xrd`, `sich`, `datadate`, `ceq`
- **PatentDataProcessed.parquet**: `gvkey`, `year`, `ncitscale`

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
  - ✅ **`datadate`**: Available - from `AP_CompustatAnnual.py` line 1017 (set from `period_end`)
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184
- ❌ **PatentDataProcessed.parquet**: **FILE DOES NOT EXIST YET**
  - Script exists: `PatentCitations.py` (generates `PatentDataProcessed.parquet`)
  - Would contain `gvkey`, `year`, `ncitscale` (scaled patent citations)
  - Requires patent citation data from USPTO/patent databases

### Can Be Constructed?
**NO** - Missing `PatentDataProcessed.parquet`. Note: `sich` is loaded but not used in calculations.

### Additional Work Needed?
**YES** - Missing patent data:

#### What Needs to Be Done:
1. **Create PatentDataProcessed.parquet**: 
   - Run `PatentCitations.py` to generate patent citation data
   - Requires patent citation database access (USPTO or proprietary sources)
   - Note: This is NOT an AP script - it uses WRDS/patent databases
   - Must contain `gvkey`, `year`, `ncitscale` columns

2. **Note on `sich`**: 
   - `sich` is loaded in the predictor but **NOT USED** in the actual signal construction
   - Only `sicCRSP` from `SignalMasterTable` is used for filtering financial firms (line 140)
   - Can be set to empty/NaN or omitted from the data load without affecting results

#### Implementation Notes:
- **For PatentDataProcessed**: This predictor requires patent citation data which is typically sourced from proprietary databases (USPTO, NBER Patent Database, etc.). The `PatentCitations.py` script processes patent data to create citation counts (`ncitscale`). Without this data, the predictor cannot be constructed.
- **For sich**: While `sich` is listed as a required column, it is not actually used in the predictor logic. The predictor only uses `sicCRSP` from `SignalMasterTable` for filtering financial firms. However, to match the original code exactly, `sich` should still be included in the data load (can be NaN/empty).
- **Alternative**: If patent data is not available, this predictor cannot be constructed with AP data sources.

---

## 34. CompEquIss.py

### Required Columns:
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `ret`, `mve_c`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `ret` (returns) - from `AP_SignalMasterTable.py` line 133
  - Contains `mve_c` (market value of equity) - from `AP_SignalMasterTable.py` line 127

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 35. CompositeDebtIssuance.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `dltt`, `dlc`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `dltt` (long-term debt) - mapped in XBRL_TAG_MAP line 142-144
  - Contains `dlc` (debt current) - mapped in XBRL_TAG_MAP line 136-139

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## Summary

| Predictor | All Columns Available? | Can Be Constructed? | Additional Work Needed? |
|-----------|----------------------|---------------------|----------------------|
| ChNWC | ✅ Yes | ✅ Yes | None |
| ChTax | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |
| CitationsRD | ❌ No | ❌ No | **Create PatentDataProcessed.parquet, add sich column** |
| CompEquIss | ✅ Yes | ✅ Yes | None |
| CompositeDebtIssuance | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |

### Critical Missing Component:
1. **`PatentDataProcessed.parquet`**: Required for `CitationsRD.py`. File does not exist yet - needs to be generated by running `PatentCitations.py` (requires patent citation database access).

### Recommendations:
1. **For CitationsRD**: 
   - **CRITICAL**: Run `PatentCitations.py` to generate `PatentDataProcessed.parquet`
   - Requires patent citation database access (USPTO, NBER Patent Database, or proprietary sources)
   - **Note on `sich`**: While `sich` is loaded, it is not used in calculations. Can be set to NaN/empty or omitted from data load. If needed for exact code matching, can merge from `AP_monthlyCRSP.parquet` using `sicCRSP` column.
   - If patent data is not available, this predictor cannot be constructed with AP data sources

2. **Verification**: 
   - Test that all AP files actually contain the columns listed above by loading sample data
   - Verify `txtq` (total taxes quarterly) is correctly populated in `AP_m_QCompustat.parquet`
   - Verify `dltt` and `dlc` (debt columns) are correctly populated in `AP_m_aCompustat.parquet`

3. **Note on Patent Data**: 
   - Patent citation data is typically sourced from proprietary databases
   - The `PatentCitations.py` script processes patent data to create citation counts
   - Without access to patent databases, this predictor cannot be constructed
   - Consider alternative data sources or skip this predictor if patent data is unavailable

4. **Note on sich**: 
   - `sich` is loaded but not used in the predictor logic
   - Only `sicCRSP` from `SignalMasterTable` is used for filtering financial firms
   - Can be safely omitted or set to NaN/empty without affecting results
