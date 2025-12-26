# Group 5 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 5.

---

## 21. CF.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `ib`, `dp`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `mve_permco`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` lines 64-66)
  - Contains `ib` (net income) - mapped in XBRL_TAG_MAP line 262-264
  - Contains `dp` (depreciation) - mapped in XBRL_TAG_MAP line 245-247
- ✅ **AP_SignalMasterTable.parquet**: Contains `mve_permco` (from `AP_SignalMasterTable.py` line 105)

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 22. cfp.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `act`, `che`, `lct`, `dlc`, `txp`, `dp`, `ib`, `oancf`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `mve_permco`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `act` (current assets) - mapped in XBRL_TAG_MAP line 90
  - Contains `che` (cash) - mapped in XBRL_TAG_MAP line 88
  - Contains `lct` (current liabilities) - mapped in XBRL_TAG_MAP line 133
  - Contains `dlc` (debt current) - mapped in XBRL_TAG_MAP line 136-139
  - Contains `txp` (taxes payable) - mapped in XBRL_TAG_MAP line 156-157
  - Contains `dp` (depreciation) - mapped in XBRL_TAG_MAP line 245-247
  - Contains `ib` (net income) - mapped in XBRL_TAG_MAP line 262-264
  - Contains `oancf` (operating cash flow) - mapped in XBRL_TAG_MAP line 299-301
- ✅ **AP_SignalMasterTable.parquet**: Contains `mve_permco` (from `AP_SignalMasterTable.py` line 105)

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 23. ChangeInRecommendation.py

### Required Columns:
- **AP_IBES_Recommendations.parquet**: `tickerIBES`, `amaskcd`, `anndats`, `time_avail_m`, `ireccd`
- **AP_SignalMasterTable.parquet**: `permno`, `tickerIBES`, `time_avail_m`

### AP File Status:
- ❌ **AP_IBES_Recommendations.parquet**: **FILE DOES NOT EXIST YET**
  - Script exists: `AP_IBESRecommendations.py` (generates `AP_IBES_Recommendations.parquet`)
  - Would contain `tickerIBES` (from `AP_IBESRecommendations.py` line 282)
  - Would contain `amaskcd` (analyst mask code) - from `AP_IBESRecommendations.py` line 254-280
  - Would contain `anndats` (announcement date) - from `AP_IBESRecommendations.py` line 252
  - Would contain `time_avail_m` (monthly availability) - from `AP_IBESRecommendations.py` line 306-307
  - Would contain `ireccd` (recommendation code) - from `AP_IBESRecommendations.py` line 248
- ⚠️ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 100-103)
  - Contains `tickerIBES` column - should be populated from `AP_IBESCRSPLinkingTable.parquet` (after fix in Group 2)

### Can Be Constructed?
**NO** - Missing `AP_IBES_Recommendations.parquet` file.

### Additional Work Needed?
**YES** - Need to run `AP_IBESRecommendations.py` to generate `AP_IBES_Recommendations.parquet`.

#### What Needs to Be Done:
1. **Run AP_IBESRecommendations.py**: Execute the script to download IBES recommendations from Eikon/LSEG API
2. **Requirements**:
   - Eikon/LSEG API subscription (proprietary data source)
   - `eikon` Python package installed
   - Eikon API credentials configured
3. **Output**: Generates `AP_IBES_Recommendations.parquet` with required columns

#### Implementation Notes:
- The script `AP_IBESRecommendations.py` exists and is ready to run
- Requires Eikon/LSEG API access (paid subscription)
- If Eikon is not available, this predictor cannot be constructed with AP data
- Alternative: Could potentially use other free sources (e.g., Yahoo Finance analyst recommendations), but format would need adaptation

---

## 24. ChAssetTurnover.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `rect`, `invt`, `aco`, `ppent`, `intan`, `ap`, `lco`, `lo`, `sale`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `rect` (receivables) - mapped in XBRL_TAG_MAP line 92
  - Contains `invt` (inventory) - mapped in XBRL_TAG_MAP line 91
  - ⚠️ **`aco`** (other current assets) - mapped in XBRL_TAG_MAP line 95, but marked as "highly unreliable" and prefers derivation
  - Contains `ppent` (PP&E net) - mapped in XBRL_TAG_MAP line 98
  - Contains `intan` (intangibles) - mapped in XBRL_TAG_MAP line 107
  - Contains `ap` (accounts payable) - mapped in XBRL_TAG_MAP line 152-153
  - Contains `lco` (other current liabilities) - mapped in XBRL_TAG_MAP line 160-161
  - Contains `lo` (other noncurrent liabilities) - mapped in XBRL_TAG_MAP line 163
  - Contains `sale` (sales revenue) - mapped in XBRL_TAG_MAP line 223

### Can Be Constructed?
**YES** - All required columns are present, though `aco` may be derived rather than directly mapped.

### Additional Work Needed?
None. The predictor can be constructed with the available columns. Note that `aco` is marked as "highly unreliable" in XBRL mapping and may be derived from `act - (che + rect + invt)` if direct mapping fails.

---

## 25. ChEQ.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `ceq`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## Summary

| Predictor | All Columns Available? | Can Be Constructed? | Additional Work Needed? |
|-----------|----------------------|---------------------|----------------------|
| CF | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |
| cfp | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |
| ChangeInRecommendation | ❌ No | ❌ No | **Run AP_IBESRecommendations.py** |
| ChAssetTurnover | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate, aco may be derived) |
| ChEQ | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |

### Critical Missing Component:
- **`AP_IBES_Recommendations.parquet`**: Required for `ChangeInRecommendation.py`. File does not exist yet - needs to be generated by running `AP_IBESRecommendations.py`.

### Recommendations:
1. **Verification**: 
   - Test that all AP files actually contain the columns listed above by loading sample data
   - Verify `oancf` (operating cash flow) is correctly populated in `AP_m_aCompustat.parquet`
   - Verify `aco` (other current assets) is populated (may be derived rather than directly mapped)
   - Verify `AP_IBES_Recommendations.parquet` exists and has required columns

2. **For ChangeInRecommendation**: 
   - **CRITICAL**: Run `AP_IBESRecommendations.py` to generate `AP_IBES_Recommendations.parquet`
   - Requires Eikon/LSEG API subscription (proprietary data source)
   - Ensure `tickerIBES` is populated in `AP_SignalMasterTable.parquet` (fix from Group 2)
   - If Eikon is not available, this predictor cannot be constructed with AP data

3. **Note on aco (Other Current Assets)**: 
   - Marked as "highly unreliable" in XBRL mapping
   - May be derived as: `aco = act - (che + rect + invt + other known current assets)`
   - This derivation is acceptable for predictor construction

4. **Note on gvkey**: 
   - All predictors use surrogate `gvkey` (CIK/permno) instead of true Compustat gvkey
   - This is acceptable as `gvkey` is typically only used for grouping/identification, not calculations
