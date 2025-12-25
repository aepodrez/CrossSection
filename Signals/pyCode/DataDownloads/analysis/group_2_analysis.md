# Group 2 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 2.

---

## 6. AM.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `permno`, `time_avail_m`, `at`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `mve_permco`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 86
- ✅ **AP_SignalMasterTable.parquet**: Contains `mve_permco` (from `AP_SignalMasterTable.py` line 105)

### Can Be Constructed?
**YES** - All required columns are present in the AP files.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 7. AnalystRevision.py

### Required Columns:
- **AP_IBES_EPS_Unadj.parquet**: `tickerIBES`, `time_avail_m`, `meanest`, `fpi`
- **AP_SignalMasterTable.parquet**: `permno`, `tickerIBES`, `time_avail_m`

### AP File Status:
- ✅ **AP_IBES_EPS_Unadj.parquet**: 
  - Contains `tickerIBES` (from `AP_IBESEPSUnadjusted.py` line 253)
  - Contains `time_avail_m` (from `AP_IBESEPSUnadjusted.py` line 277-278)
  - Contains `meanest` (from `AP_IBESEPSUnadjusted.py` line 254)
  - Contains `fpi` (forecast period indicator) - present in output columns (line 295)
- ⚠️ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 100-103)
  - Contains `tickerIBES` column BUT it's set to empty string `""` (from `AP_SignalMasterTable.py` line 93)

### Can Be Constructed?
**PARTIALLY** - The `tickerIBES` column exists in AP_SignalMasterTable but is empty. The predictor needs this column populated to merge with IBES data.

### Additional Work Needed?
**YES** - Need to populate `tickerIBES` in `AP_SignalMasterTable.parquet`.

#### What Needs to Be Done:
1. **Modify AP_SignalMasterTable.py**: Currently sets `tickerIBES = ""` on line 93
2. **Merge from AP_IBESCRSPLinkingTable**: The linking table already exists (`AP_IBESCRSPLink.py` creates `AP_IBESCRSPLinkingTable.parquet`)

#### Implementation Notes:
- ✅ **AP_IBESCRSPLink.py exists** and creates `AP_IBESCRSPLinkingTable.parquet` with columns: `tickerIBES`, `permno`, `time_avail_m`, `score`
- **Modify AP_SignalMasterTable.py** line 93 to:
  1. Load `AP_IBESCRSPLinkingTable.parquet` if it exists
  2. Merge: `df = df.merge(ibes_link[['permno', 'time_avail_m', 'tickerIBES']], on=['permno', 'time_avail_m'], how='left')`
  3. Fill missing: `df['tickerIBES'] = df['tickerIBES'].fillna('')`
- **Execution Order**: Ensure `AP_IBESCRSPLink.py` runs before `AP_SignalMasterTable.py` in the pipeline

---

## 8. AP_BetaTailRisk.py

### Required Columns:
- **AP_dailyCRSP.parquet**: `permno`, `time_d`, `ret`
- **AP_monthlyCRSP.parquet**: `permno`, `time_avail_m`, `ret`, `shrcd`

### AP File Status:
- ✅ **AP_dailyCRSP.parquet**: Contains `permno`, `time_d`, `ret` (from `AP_CRSPDaily.py` lines 282-283)
- ✅ **AP_monthlyCRSP.parquet**: 
  - Contains `permno`, `time_avail_m`, `ret` (from `AP_CRSPMonthly.py` lines 476-480)
  - Contains `shrcd` (share code) - from `AP_CRSPMonthly.py` line 353

### Can Be Constructed?
**YES** - All required columns are present. This predictor is specifically designed for AP data.

### Additional Work Needed?
None. This predictor already uses AP files by design.

---

## 9. AssetGrowth.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `at`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 86
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` lines 64-66)

### Can Be Constructed?
**YES** - All required columns are present, though `gvkey` is a surrogate (CIK/permno).

### Additional Work Needed?
None. The predictor doesn't actually use `gvkey` in calculations (it's only kept for compatibility), so the surrogate is acceptable.

---

## 10. Beta.py

### Required Columns:
- **AP_monthlyCRSP.parquet**: `permno`, `time_avail_m`, `ret`
- **AP_monthlyFF.parquet**: `time_avail_m`, `rf`
- **AP_monthlyMarket.parquet**: `time_avail_m`, `ewretd`

### AP File Status:
- ✅ **AP_monthlyCRSP.parquet**: Contains `permno`, `time_avail_m`, `ret` (from `AP_CRSPMonthly.py` lines 476-480)
- ✅ **AP_monthlyFF.parquet**: 
  - Contains `time_avail_m` (from `AP_FamaFrenchMonthly.py` line 490)
  - Contains `rf` (risk-free rate) - from `AP_FamaFrenchMonthly.py` line 494
- ✅ **AP_monthlyMarket.parquet**: 
  - Contains `time_avail_m` (from `AP_MarketReturns.py` line 233)
  - Contains `ewretd` (equal-weighted return) - from `AP_MarketReturns.py` line 233

### Can Be Constructed?
**YES** - All required columns are present in the AP files.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## Summary

| Predictor | All Columns Available? | Can Be Constructed? | Additional Work Needed? |
|-----------|----------------------|---------------------|----------------------|
| AM | ✅ Yes | ✅ Yes | None |
| AnalystRevision | ⚠️ Partial | ❌ No | **Populate tickerIBES in AP_SignalMasterTable** |
| AP_BetaTailRisk | ✅ Yes | ✅ Yes | None (designed for AP) |
| AssetGrowth | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |
| Beta | ✅ Yes | ✅ Yes | None |

### Critical Missing Component:
- **tickerIBES in AP_SignalMasterTable**: The column exists but is empty. Needs to be populated from IBES linking table.

### Recommendations:
1. **Immediate**: 
   - Check if `AP_IBESCRSPLink.py` exists and creates `AP_IBESCRSPLink.parquet`
   - Modify `AP_SignalMasterTable.py` to merge IBES linking data instead of setting `tickerIBES = ""`
   - The merge should populate `tickerIBES` from the linking table

2. **Verification**: 
   - Test that all AP files actually contain the columns listed above by loading sample data
   - Verify that `AP_IBES_EPS_Unadj.parquet` has sufficient coverage for your universe

3. **Documentation**: 
   - Note that `gvkey` in AP files is a surrogate (CIK/permno) rather than true Compustat gvkey
   - Document that `tickerIBES` requires IBES linking table to be populated

### Implementation Details for AnalystRevision:
The `AP_SignalMasterTable.py` currently sets `tickerIBES = ""` on line 93. To fix this:

**Solution**: Modify `AP_SignalMasterTable.py` to merge from `AP_IBESCRSPLinkingTable.parquet`:

```python
# After line 90 (after merging comp data)
# Load IBES linking table if it exists
ibes_link_path = Path("../pyData/Intermediate/AP_IBESCRSPLinkingTable.parquet")
if ibes_link_path.exists():
    ibes_link = pd.read_parquet(ibes_link_path)
    df = df.merge(ibes_link[['permno', 'time_avail_m', 'tickerIBES']], 
                  on=['permno', 'time_avail_m'], how='left')
    df['tickerIBES'] = df['tickerIBES'].fillna('')
else:
    df['tickerIBES'] = ''  # Fallback if linking table doesn't exist
```

**Execution Order**: Ensure `AP_IBESCRSPLink.py` runs before `AP_SignalMasterTable.py` in your pipeline.

