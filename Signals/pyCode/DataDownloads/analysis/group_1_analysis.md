# Group 1 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 1.

---

## 1. ZZ2_PriceDelaySlope_PriceDelayRsq_PriceDelayTstat.py

### Required Columns:
- **AP_dailyCRSP.parquet**: `permno`, `time_d`, `ret`
- **AP_dailyFF.parquet**: `time_d`, `mktrf`, `rf`

### AP File Status:
- ✅ **AP_dailyCRSP.parquet**: Contains `permno`, `time_d`, `ret` (from `AP_CRSPDaily.py` lines 282-283)
- ✅ **AP_dailyFF.parquet**: Contains `time_d`, `mktrf`, `rf` (from `AP_FamaFrenchDaily.py` lines 538-546)

### Can Be Constructed?
**YES** - All required columns are present in the AP files.

### Additional Work Needed?
None. The AP versions have all required columns.

---

## 2. Accruals.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `txp`, `act`, `che`, `lct`, `dlc`, `at`, `dp`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `txp` (taxes payable) - mapped in XBRL_TAG_MAP line 156-157
  - Contains `act` (current assets) - mapped in XBRL_TAG_MAP line 90
  - Contains `che` (cash) - mapped in XBRL_TAG_MAP line 88
  - Contains `lct` (current liabilities) - mapped in XBRL_TAG_MAP line 133
  - Contains `dlc` (debt current) - mapped in XBRL_TAG_MAP line 136-139
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 86
  - Contains `dp` (depreciation) - mapped in XBRL_TAG_MAP line 245-247
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` lines 64-66)

### Can Be Constructed?
**YES** - All required columns are present, though `gvkey` is a surrogate (CIK/permno).

### Additional Work Needed?
None. The predictor doesn't actually use `gvkey` in calculations (it's only kept for compatibility), so the surrogate is acceptable.

---

## 3. AccrualsBM.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `ceq`, `act`, `che`, `lct`, `dlc`, `txp`, `at`
- **AP_SignalMasterTable.parquet**: `mve_permco`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains all required columns (same as Accruals.py above)
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184
- ✅ **AP_SignalMasterTable.parquet**: Contains `mve_permco` (from `AP_SignalMasterTable.py` line 105)
- ⚠️ **`gvkey`**: Same surrogate issue as Accruals.py (not used in calculations)

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 4. AdExp.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `permno`, `time_avail_m`, `xad`
- **AP_SignalMasterTable.parquet**: `mve_permco`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m`
  - Contains `xad` (advertising expense) - mapped in XBRL_TAG_MAP line 233-234
- ✅ **AP_SignalMasterTable.parquet**: Contains `mve_permco` (from `AP_SignalMasterTable.py` line 105)

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 5. AgeIPO.py

### Required Columns:
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`
- **IPODates.parquet**: `permno`, `IPOdate`, `FoundingYear`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 100-103)
- ❌ **IPODates.parquet**: **NO AP VERSION EXISTS**

### Can Be Constructed?
**PARTIALLY** - Missing AP version of IPODates.

### Additional Work Needed?
**YES** - Need to create `AP_IPODates.parquet` or `AP_IPODates.csv`.

#### What Needs to Be Done:
1. **Create AP_IPODates.py script** in `DataDownloads/` directory
2. **Data Source**: The original `IPODates.py` downloads from Ritter's website (https://site.warrington.ufl.edu/ritter/files/IPO-age.xlsx)
3. **Required Columns**: `permno`, `IPOdate`, `FoundingYear`
4. **Linking Challenge**: The original script uses CRSP permno. For AP version, you'll need to:
   - Download the same Excel file from Ritter's website
   - Map tickers/CUSIPs to AP permnos using the `AP_ticker_to_permno.csv` mapping
   - Or use the same permno if the mapping is consistent

#### Implementation Notes:
- The original `IPODates.py` (lines 27-91) downloads from Ritter's website
- It extracts `permno`, `FoundingYear`, and `IPOdate` columns
- For AP version, you may need to add ticker-to-permno mapping logic similar to other AP scripts
- The file should be saved as `AP_IPODates.parquet` in `../pyData/Intermediate/`

---

## Summary

| Predictor | All Columns Available? | Can Be Constructed? | Additional Work Needed? |
|-----------|----------------------|---------------------|----------------------|
| ZZ2_PriceDelaySlope_PriceDelayRsq_PriceDelayTstat | ✅ Yes | ✅ Yes | None |
| Accruals | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |
| AccrualsBM | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |
| AdExp | ✅ Yes | ✅ Yes | None |
| AgeIPO | ❌ No | ❌ No | **Create AP_IPODates.py** |

### Critical Missing Component:
- **AP_IPODates.parquet**: This file is required for `AgeIPO.py` but does not exist. An AP version needs to be created.

### Recommendations:
1. **Immediate**: Create `AP_IPODates.py` script to generate `AP_IPODates.parquet`
2. **Verification**: Test that all AP files actually contain the columns listed above by loading sample data
3. **Documentation**: Note that `gvkey` in AP files is a surrogate (CIK/permno) rather than true Compustat gvkey
