# Group 16 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 16.

---

## 76. ForecastDispersion.py

### Required Columns:
- **AP_IBES_EPS_Unadj.parquet**: `tickerIBES`, `time_avail_m`, `stdev`, `meanest`, `fpi`, `fpedats`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `tickerIBES`

### AP File Status:
- ✅ **AP_IBES_EPS_Unadj.parquet**: 
  - File exists (from `AP_IBESEPSUnadjusted.py`)
  - Contains `tickerIBES` (from `AP_IBESEPSUnadjusted.py` line 253)
  - Contains `time_avail_m` (from `AP_IBESEPSUnadjusted.py` line 277-278)
  - Contains `stdev` (standard deviation of estimates) - from `AP_IBESEPSUnadjusted.py` line 259, 299
  - Contains `meanest` (mean estimate) - from `AP_IBESEPSUnadjusted.py` line 298
  - Contains `fpi` (forecast period indicator) - from `AP_IBESEPSUnadjusted.py` line 295
  - Contains `fpedats` (forecast period end date) - from `AP_IBESEPSUnadjusted.py` line 262, 300
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `tickerIBES` column - should be populated from `AP_IBESCRSPLinkingTable.parquet` (after fix in Group 2)

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 77. Frontier.py

### Required Columns:
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `mve_permco`, `sicCRSP`
- **AP_m_aCompustat.parquet**: `permno`, `time_avail_m`, `at`, `ceq`, `dltt`, `capx`, `sale`, `xrd`, `xad`, `ppent`, `ebitda`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `mve_permco` (market value per company) - from `AP_SignalMasterTable.py` line 128
  - Contains `sicCRSP` (SIC code from CRSP) - from `AP_SignalMasterTable.py` line 134
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 86
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184
  - Contains `dltt` (long-term debt) - mapped in XBRL_TAG_MAP line 142-144
  - Contains `capx` (capital expenditures) - mapped in XBRL_TAG_MAP line 314-316, included in CF_FIELDS line 410
  - Contains `sale` (sales) - mapped in XBRL_TAG_MAP line 230
  - Contains `xrd` (R&D expenses) - mapped in XBRL_TAG_MAP line 235-236
  - Contains `xad` (advertising expenses) - mapped in XBRL_TAG_MAP line 233-234
  - Contains `ppent` (property, plant, equipment net) - mapped in XBRL_TAG_MAP line 98
  - Contains `ebitda` (earnings before interest, taxes, depreciation, amortization) - derived in `AP_CompustatAnnual.py` line 999-1005

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 78. Governance.py

### Required Columns:
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `ticker`, `exchcd`
- **GovIndex.parquet**: `ticker`, `time_avail_m`, `G`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `ticker` (ticker symbol) - from `AP_SignalMasterTable.py` line 129
  - Contains `exchcd` (exchange code) - from `AP_SignalMasterTable.py` line 130
- ❌ **GovIndex.parquet**: **FILE DOES NOT EXIST**
  - No script found to generate this file
  - Would contain governance index scores (`G`) from Gompers-Ishii-Metrick (2003) dataset
  - This is a proprietary dataset that requires manual compilation or purchase

### Can Be Constructed?
**NO** - Missing `GovIndex.parquet` file.

### Additional Work Needed?
**YES** - Need to create or obtain `GovIndex.parquet`.

#### What Needs to Be Done:
1. **Obtain Governance Index Data**: 
   - The governance index (`G`) comes from Gompers, Ishii, and Metrick (2003) "Corporate Governance and Equity Prices"
   - This is a proprietary dataset that measures corporate governance quality
   - The index ranges from 5 (best governance) to 14 (worst governance)
   - Requires manual compilation from SEC proxy statements or purchase from data vendors

2. **Create GovIndex.parquet**: 
   - File should contain columns: `ticker`, `time_avail_m`, `G`
   - `ticker`: Stock ticker symbol
   - `time_avail_m`: Monthly availability date
   - `G`: Governance index score (integer, typically 5-14)
   - Data should be merged with `AP_SignalMasterTable.parquet` on `ticker` and `time_avail_m`

#### Implementation Notes:
- The governance index is based on 24 governance provisions from corporate charters and bylaws
- Original data covers 1990-1999 period, but may have been extended by other researchers
- Without access to this proprietary dataset, this predictor cannot be constructed
- **Alternative**: Could potentially construct a simplified governance index from publicly available proxy statement data, but would not match the original methodology

---

## 79. GP.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `revt`, `cogs`, `at`, `sic`, `datadate`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `revt` (total revenue) - mapped in XBRL_TAG_MAP line 232
  - Contains `cogs` (cost of goods sold) - mapped in XBRL_TAG_MAP line 235-236
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 86
  - ⚠️ **`sic`**: **NOT DIRECTLY EXTRACTED** from XBRL in `AP_CompustatAnnual.py`. However, `sicCRSP` is available in `AP_monthlyCRSP.parquet` and can be merged.
  - Contains `datadate` (data date) - renamed from `period_end` (from `AP_CompustatAnnual.py` line 927)

### Can Be Constructed?
**PARTIALLY** - Missing `sic` column in `AP_m_aCompustat.parquet`.

### Additional Work Needed?
**YES** - Need to merge `sic` from `AP_monthlyCRSP.parquet` or add `sic` extraction to `AP_CompustatAnnual.py`.

#### What Needs to Be Done:
1. **Option 1: Merge sic from AP_monthlyCRSP.parquet**:
   - `AP_monthlyCRSP.parquet` contains `sicCRSP` (from `AP_CRSPMonthly.py`)
   - Merge `sicCRSP` from `AP_monthlyCRSP.parquet` into `AP_m_aCompustat.parquet` based on `permno` and `time_avail_m`
   - Rename `sicCRSP` to `sic` for compatibility with predictor

2. **Option 2: Extract sic from XBRL**:
   - Add SIC code extraction to `AP_CompustatAnnual.py` from XBRL filings
   - SIC codes may be available in XBRL cover page or company facts
   - However, XBRL may not always have SIC codes, so merging from CRSP is more reliable

#### Implementation Notes:
- The predictor filters for non-financial firms: `sic < 6000 or sic >= 7000`
- `sicCRSP` from `AP_monthlyCRSP.parquet` should work as a substitute for `sic`
- **Recommended**: Merge `sicCRSP` from `AP_monthlyCRSP.parquet` into `AP_m_aCompustat.parquet` during monthly expansion process

---

## 80. GrAdExp.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `permno`, `time_avail_m`, `at`, `xad`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `mve_c`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 86
  - Contains `xad` (advertising expenses) - mapped in XBRL_TAG_MAP line 233-234
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `mve_c` (market value of equity) - from `AP_SignalMasterTable.py` line 127

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## Summary

| Predictor | All Columns Available? | Can Be Constructed? | Additional Work Needed? |
|-----------|----------------------|---------------------|----------------------|
| ForecastDispersion | ✅ Yes | ✅ Yes | None |
| Frontier | ✅ Yes | ✅ Yes | None |
| Governance | ❌ No | ❌ No | **Create/obtain GovIndex.parquet** |
| GP | ⚠️ Partial | ⚠️ Partial | **Merge sic from AP_monthlyCRSP** |
| GrAdExp | ✅ Yes | ✅ Yes | None |

### Critical Missing Components:
1. **`GovIndex.parquet`**: Required for `Governance.py`. File does not exist - requires proprietary governance index dataset from Gompers-Ishii-Metrick (2003).
2. **`sic` column in AP_m_aCompustat.parquet**: Required for `GP.py`. Not directly extracted from XBRL, but `sicCRSP` is available in `AP_monthlyCRSP.parquet` and can be merged.

### Recommendations:
1. **For ForecastDispersion**: 
   - **VERIFICATION**: Test that `stdev` and `meanest` are correctly populated in `AP_IBES_EPS_Unadj.parquet`
   - Verify `tickerIBES` is correctly populated in `AP_SignalMasterTable.parquet`
   - Verify `fpi == "1"` filter works correctly (predictor filters for 1-year ahead forecasts)
   - Verify `fpedats` is not null (predictor filters for records with valid forecast period end dates)

2. **For Frontier**: 
   - **VERIFICATION**: Test that all required columns (`at`, `ceq`, `dltt`, `capx`, `sale`, `xrd`, `xad`, `ppent`, `ebitda`) are correctly populated in `AP_m_aCompustat.parquet`
   - Verify `mve_permco` and `sicCRSP` are correctly populated in `AP_SignalMasterTable.parquet`
   - Note: Predictor uses 60-month rolling window regressions, requires sufficient historical coverage
   - Note: Predictor is computationally intensive - uses sklearn LinearRegression with industry dummies

3. **For Governance**: 
   - **CRITICAL**: Obtain or create `GovIndex.parquet` with governance index scores
   - Requires proprietary dataset from Gompers-Ishii-Metrick (2003)
   - File should contain: `ticker`, `time_avail_m`, `G` (governance index score, typically 5-14)
   - Without this dataset, predictor cannot be constructed
   - **Alternative**: Could construct simplified governance index from proxy statements, but would not match original methodology

4. **For GP**: 
   - **CRITICAL**: Merge `sic` (or `sicCRSP`) into `AP_m_aCompustat.parquet`
   - **Recommended**: Merge `sicCRSP` from `AP_monthlyCRSP.parquet` during monthly expansion process
   - Verify `revt`, `cogs`, and `at` are correctly populated
   - Note: Predictor filters for non-financial firms (`sic < 6000 or sic >= 7000`)

5. **For GrAdExp**: 
   - **VERIFICATION**: Test that `xad` and `at` are correctly populated in `AP_m_aCompustat.parquet`
   - Verify `mve_c` is correctly populated in `AP_SignalMasterTable.parquet`
   - Verify sufficient historical coverage for 12-month lag calculation
   - Note: Predictor filters for `xad >= 0.1` and excludes smallest size decile

6. **Note on gvkey Surrogate**: 
   - `GP.py` uses `gvkey` which is not directly available from EDGAR
   - AP version uses `cik` (SEC identifier) as surrogate or falls back to `permno`
   - This should work for most predictors, but verify linking logic if issues arise

7. **Note on sic vs sicCRSP**: 
   - `GP.py` expects `sic` from Compustat
   - AP version has `sicCRSP` from CRSP data
   - These should be equivalent for most firms, but may differ for some companies
   - **Recommended**: Use `sicCRSP` as substitute for `sic` in `GP.py`

8. **Note on ebitda**: 
   - `Frontier.py` requires `ebitda` which is derived in `AP_CompustatAnnual.py` (line 999-1005)
   - Derived as: `ebitda = ebit + dp + am` (with fallbacks)
   - Verify derivation logic produces correct values

9. **Note on Governance Index**: 
   - The governance index (`G`) is a proprietary measure from Gompers, Ishii, and Metrick (2003)
   - Based on 24 governance provisions from corporate charters and bylaws
   - Original dataset covers 1990-1999, but may have been extended
   - Without access to this dataset, `Governance.py` cannot be constructed
   - Consider alternative approaches if governance data is needed
