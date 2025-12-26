# Group 19 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 19.

---

## 91. IndMom.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `ret`, `sicCRSP`, `mve_c`

### AP File Status:
- ⚠️ **SignalMasterTable.parquet**: **FILE NAME MISMATCH**
  - Predictor expects: `SignalMasterTable.parquet`
  - AP version exists: `AP_SignalMasterTable.parquet` (from `AP_SignalMasterTable.py`)
  - Contains `permno` (from `AP_SignalMasterTable.py` line 124)
  - Contains `time_avail_m` (from `AP_SignalMasterTable.py` line 126)
  - Contains `ret` (return) - from `AP_SignalMasterTable.py` line 133
  - Contains `sicCRSP` (SIC code from CRSP) - from `AP_SignalMasterTable.py` line 134
  - Contains `mve_c` (market value of equity) - from `AP_SignalMasterTable.py` line 127

### Can Be Constructed?
**YES** - All required columns are available, but file name needs to match.

### Additional Work Needed?
**YES** - File name mismatch needs to be resolved.

#### What Needs to Be Done:
1. **Option 1**: Rename `AP_SignalMasterTable.parquet` to `SignalMasterTable.parquet` (or create symlink/copy)
2. **Option 2**: Modify `IndMom.py` to use `AP_SignalMasterTable.parquet` instead of `SignalMasterTable.parquet`
3. **Option 3**: Create a wrapper script that copies/renames AP files to match expected names

#### Implementation Notes:
- Predictor calculates industry momentum as market-cap weighted average within 2-digit SIC groups
- Uses 6-month momentum (months t-5 to t-1) for individual stocks
- Then calculates weighted average by industry-month using `mve_c` as weights
- **Recommended**: Create symlink or copy `AP_SignalMasterTable.parquet` to `SignalMasterTable.parquet` for compatibility

---

## 92. IndRetBig.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `ret`, `mve_c`, `sicCRSP`

### AP File Status:
- ⚠️ **SignalMasterTable.parquet**: **FILE NAME MISMATCH**
  - Predictor expects: `SignalMasterTable.parquet`
  - AP version exists: `AP_SignalMasterTable.parquet` (from `AP_SignalMasterTable.py`)
  - Contains `permno` (from `AP_SignalMasterTable.py` line 124)
  - Contains `time_avail_m` (from `AP_SignalMasterTable.py` line 126)
  - Contains `ret` (return) - from `AP_SignalMasterTable.py` line 133
  - Contains `mve_c` (market value of equity) - from `AP_SignalMasterTable.py` line 127
  - Contains `sicCRSP` (SIC code from CRSP) - from `AP_SignalMasterTable.py` line 134

### Can Be Constructed?
**YES** - All required columns are available, but file name needs to match.

### Additional Work Needed?
**YES** - File name mismatch needs to be resolved.

#### What Needs to Be Done:
1. **Option 1**: Rename `AP_SignalMasterTable.parquet` to `SignalMasterTable.parquet` (or create symlink/copy)
2. **Option 2**: Modify `IndRetBig.py` to use `AP_SignalMasterTable.parquet` instead of `SignalMasterTable.parquet`
3. **Option 3**: Create a wrapper script that copies/renames AP files to match expected names

#### Implementation Notes:
- Predictor calculates average monthly return of 30% largest companies in same FF48 industry
- Uses Fama-French 48 industry classification from SIC codes (via `sicff` utility)
- Calculates relative rank of market value within industry-month groups
- Keeps only large companies (market value rank > 70th percentile)
- Sets `IndRetBig` to missing for companies that are themselves large (>= 70th percentile)
- **Recommended**: Create symlink or copy `AP_SignalMasterTable.parquet` to `SignalMasterTable.parquet` for compatibility

---

## 93. IntMom.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `ret`

### AP File Status:
- ⚠️ **SignalMasterTable.parquet**: **FILE NAME MISMATCH**
  - Predictor expects: `SignalMasterTable.parquet`
  - AP version exists: `AP_SignalMasterTable.parquet` (from `AP_SignalMasterTable.py`)
  - Contains `permno` (from `AP_SignalMasterTable.py` line 124)
  - Contains `time_avail_m` (from `AP_SignalMasterTable.py` line 126)
  - Contains `ret` (return) - from `AP_SignalMasterTable.py` line 133

### Can Be Constructed?
**YES** - All required columns are available, but file name needs to match.

### Additional Work Needed?
**YES** - File name mismatch needs to be resolved.

#### What Needs to Be Done:
1. **Option 1**: Rename `AP_SignalMasterTable.parquet` to `SignalMasterTable.parquet` (or create symlink/copy)
2. **Option 2**: Modify `IntMom.py` to use `AP_SignalMasterTable.parquet` instead of `SignalMasterTable.parquet`
3. **Option 3**: Create a wrapper script that copies/renames AP files to match expected names

#### Implementation Notes:
- Predictor calculates intermediate momentum: stock returns between months t-12 and t-6
- Compounds monthly returns over months t-12 to t-6 (intermediate horizon)
- Missing lagged values result in missing `IntMom` (consistent with methodology)
- **Recommended**: Create symlink or copy `AP_SignalMasterTable.parquet` to `SignalMasterTable.parquet` for compatibility

---

## 94. Investment.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `capx`, `revt`

### AP File Status:
- ⚠️ **m_aCompustat.parquet**: **FILE NAME MISMATCH**
  - Predictor expects: `m_aCompustat.parquet`
  - AP version exists: `AP_m_aCompustat.parquet` (from `AP_CompustatAnnual.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` line 119)
  - Contains `capx` (capital expenditures) - mapped in XBRL_TAG_MAP line 321-323
  - Contains `revt` (total revenue) - mapped in XBRL_TAG_MAP line 232

### Can Be Constructed?
**YES** - All required columns are available (gvkey is surrogate), but file name needs to match.

### Additional Work Needed?
**YES** - File name mismatch needs to be resolved.

#### What Needs to Be Done:
1. **Option 1**: Rename `AP_m_aCompustat.parquet` to `m_aCompustat.parquet` (or create symlink/copy)
2. **Option 2**: Modify `Investment.py` to use `AP_m_aCompustat.parquet` instead of `m_aCompustat.parquet`
3. **Option 3**: Create a wrapper script that copies/renames AP files to match expected names

#### Implementation Notes:
- Predictor calculates ratio of capital investment to revenue divided by firm-specific 36-month rolling mean
- Calculates investment ratio: `Investment = capx / revt`
- Calculates 36-month rolling historical average of investment ratio (minimum 24 observations required)
- Normalizes current investment ratio by its historical average
- Excludes firms with revenue below $10 million
- **Note**: `gvkey` is loaded but not used in calculations (only kept for compatibility)
- **Recommended**: Create symlink or copy `AP_m_aCompustat.parquet` to `m_aCompustat.parquet` for compatibility

---

## 95. InvestPPEInv.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `ppegt`, `invt`, `at`

### AP File Status:
- ⚠️ **m_aCompustat.parquet**: **FILE NAME MISMATCH**
  - Predictor expects: `m_aCompustat.parquet`
  - AP version exists: `AP_m_aCompustat.parquet` (from `AP_CompustatAnnual.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` line 119)
  - Contains `ppegt` (property, plant, equipment gross) - mapped in XBRL_TAG_MAP line 107
  - Contains `invt` (inventory) - mapped in XBRL_TAG_MAP line 98
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 84

### Can Be Constructed?
**YES** - All required columns are available (gvkey is surrogate), but file name needs to match.

### Additional Work Needed?
**YES** - File name mismatch needs to be resolved.

#### What Needs to Be Done:
1. **Option 1**: Rename `AP_m_aCompustat.parquet` to `m_aCompustat.parquet` (or create symlink/copy)
2. **Option 2**: Modify `InvestPPEInv.py` to use `AP_m_aCompustat.parquet` instead of `m_aCompustat.parquet`
3. **Option 3**: Create a wrapper script that copies/renames AP files to match expected names

#### Implementation Notes:
- Predictor calculates one-year change in PPE plus one-year change in inventory scaled by lagged assets
- Formula: `InvestPPEInv = (tempPPE + tempInv) / l12_at` where:
  - `tempPPE = ppegt - l12_ppegt`
  - `tempInv = invt - l12_invt`
- Uses 12-month lagged values for PPE, inventory, and total assets
- Sets to missing if `l12_at == 0` (division by zero)
- **Note**: `gvkey` is loaded but not used in calculations (only kept for compatibility)
- **Recommended**: Create symlink or copy `AP_m_aCompustat.parquet` to `m_aCompustat.parquet` for compatibility

---

## Summary

### Overall Status:
**ALL 5 PREDICTORS CAN BE CONSTRUCTED** - All required columns are available in AP data sources.

### File Name Mismatches:
- **ALL predictors** expect original file names (`SignalMasterTable.parquet`, `m_aCompustat.parquet`)
- **ALL AP versions** use `AP_` prefix (`AP_SignalMasterTable.parquet`, `AP_m_aCompustat.parquet`)
- **Recommended Solution**: Create symlinks or copies of AP files with original names for compatibility

### Column Availability:
- ✅ **SignalMasterTable columns**: `permno`, `time_avail_m`, `ret`, `sicCRSP`, `mve_c` - All available
- ✅ **Compustat columns**: `gvkey` (surrogate), `permno`, `time_avail_m`, `capx`, `revt`, `ppegt`, `invt`, `at` - All available

### Additional Notes:
1. **For IndMom, IndRetBig, IntMom**: 
   - **CRITICAL**: Resolve file name mismatch - rename `AP_SignalMasterTable.parquet` to `SignalMasterTable.parquet` or modify predictors
   - **VERIFICATION**: Test that `ret`, `sicCRSP`, `mve_c` are correctly populated
   - Verify sufficient historical coverage for lag calculations (6-month for IndMom, 12-month for IntMom)

2. **For Investment**: 
   - **CRITICAL**: Resolve file name mismatch - rename `AP_m_aCompustat.parquet` to `m_aCompustat.parquet` or modify predictor
   - **VERIFICATION**: Test that `capx` and `revt` are correctly populated
   - Verify sufficient historical coverage for 36-month rolling mean calculation (minimum 24 observations required)
   - Note: Predictor excludes firms with revenue below $10 million

3. **For InvestPPEInv**: 
   - **CRITICAL**: Resolve file name mismatch - rename `AP_m_aCompustat.parquet` to `m_aCompustat.parquet` or modify predictor
   - **VERIFICATION**: Test that `ppegt`, `invt`, `at` are correctly populated
   - Verify sufficient historical coverage for 12-month lag calculation
   - Note: Predictor sets to missing if lagged assets is zero

4. **Note on File Name Mismatches**: 
   - **ALL predictors use original file names** (`SignalMasterTable.parquet`, `m_aCompustat.parquet`)
   - **ALL AP versions use `AP_` prefix** (`AP_SignalMasterTable.parquet`, `AP_m_aCompustat.parquet`)
   - **Recommended Solution**: Create symlinks or copies of AP files with original names for compatibility
   - **Alternative**: Modify predictors to use AP file names, but this requires changing multiple predictor files
   - **Best Practice**: Create a setup script that creates symlinks: `AP_SignalMasterTable.parquet` → `SignalMasterTable.parquet`, `AP_m_aCompustat.parquet` → `m_aCompustat.parquet`, etc.

5. **Note on gvkey Surrogate**: 
   - `gvkey` is not directly available from EDGAR (Compustat-specific identifier)
   - AP version uses `cik` (SEC identifier) as surrogate, falls back to `permno` if unavailable
   - For `Investment.py` and `InvestPPEInv.py`, `gvkey` is loaded but not used in calculations (only kept for compatibility)
   - This surrogate approach is acceptable for these predictors
