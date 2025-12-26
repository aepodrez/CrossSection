# Group 18 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 18.

---

## 86. HerfBE.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `permno`, `time_avail_m`, `txditc`, `pstk`, `pstkrv`, `pstkl`, `seq`, `ceq`, `at`, `lt`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `sicCRSP`, `shrcd`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `txditc` (deferred tax) - mapped in XBRL_TAG_MAP line 341-343
  - Contains `pstk` (preferred stock) - mapped in XBRL_TAG_MAP line 189
  - ⚠️ **`pstkrv`**: **NOT MAPPABLE** - Preferred stock redemption value is marked as "not mappable" in XBRL_TAG_MAP line 192. Predictor has fallback logic.
  - Contains `pstkl` (preferred stock liquidation) - mapped in XBRL_TAG_MAP line 191
  - Contains `seq` (stockholders equity) - mapped in XBRL_TAG_MAP line 192-193
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 190-191
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 86
  - Contains `lt` (total liabilities) - mapped in XBRL_TAG_MAP line 137
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `sicCRSP` (SIC code from CRSP) - from `AP_SignalMasterTable.py` line 134
  - Contains `shrcd` (share code) - from `AP_SignalMasterTable.py` line 131

### Can Be Constructed?
**YES** - All required columns are present. `pstkrv` is missing but predictor has fallback logic.

### Additional Work Needed?
None. The predictor has fallback logic that handles missing `pstkrv`:
```python
df["tempPS"] = df["pstk"]
df["tempPS"] = df["tempPS"].fillna(df["pstkrv"])  # Will be NaN
df["tempPS"] = df["tempPS"].fillna(df["pstkl"])    # Will use this
```

---

## 87. High52.py

### Required Columns:
- **dailyCRSP.parquet**: `permno`, `time_d`, `prc`

### AP File Status:
- ⚠️ **dailyCRSP.parquet**: **FILE NAME MISMATCH**
  - Predictor expects: `dailyCRSP.parquet`
  - AP version exists: `AP_dailyCRSP.parquet` (from `AP_CRSPDaily.py`)
  - Contains `permno` (from `AP_CRSPDaily.py` line 250)
  - Contains `time_d` (date) - from `AP_CRSPDaily.py` line 251
  - Contains `prc` (price) - from `AP_CRSPDaily.py` line 252

### Can Be Constructed?
**YES** - All required columns are available, but file name needs to match.

### Additional Work Needed?
**YES** - File name mismatch needs to be resolved.

#### What Needs to Be Done:
1. **Option 1**: Rename `AP_dailyCRSP.parquet` to `dailyCRSP.parquet` (or create symlink/copy)
2. **Option 2**: Modify `High52.py` to use `AP_dailyCRSP.parquet` instead of `dailyCRSP.parquet`
3. **Option 3**: Create a wrapper script that copies/renames AP files to match expected names

#### Implementation Notes:
- `AP_CRSPDaily.py` generates `AP_dailyCRSP.parquet` with all required columns (`permno`, `time_d`, `prc`, `ret`, `vol`)
- The predictor uses absolute price (`prcadj = prc.abs()`) and calculates 52-week high from 12-month rolling maximum
- **Recommended**: Create symlink or copy `AP_dailyCRSP.parquet` to `dailyCRSP.parquet` for compatibility

---

## 88. hire.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `permno`, `time_avail_m`, `emp`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ❌ **`emp`**: **NOT EXTRACTED** - Employee extraction was removed from `AP_CompustatAnnual.py` (line 554: "Placeholder retained for compatibility; employee extraction removed")

### Can Be Constructed?
**NO** - Missing `emp` (number of employees) column.

### Additional Work Needed?
**YES** - Need to extract `emp` from XBRL data or find alternative data source.

#### What Needs to Be Done:
1. **Extract emp from XBRL DEI Section**: 
   - Employee count is in the **DEI (Document and Entity Information)** section, NOT in financial statements
   - XBRL tag: `EntityEmployeeCount` or `dei:EntityEmployeeCount`
   - In edgartools, access via `xbrl.dei` or `xbrl.dei()` method
   - DEI section contains metadata: entity name, CIK, employee count, etc.
   - Add extraction logic to `AP_CompustatAnnual.py` using DEI section (separate from balance sheet/income statement extraction)

2. **Alternative Data Source**: 
   - Use yfinance or other free data sources for employee count
   - May have limited historical coverage
   - Need to merge based on `permno` and `time_avail_m`

#### Implementation Notes:
- Employee count (`emp`) is in the **DEI section** of XBRL filings (metadata, not financial statements)
- XBRL tag: `EntityEmployeeCount` (typically `dei:EntityEmployeeCount` namespace)
- In edgartools: `xbrl.dei()` returns DEI data as DataFrame or dict
- The predictor calculates employment growth: `hire = (emp - l12_emp) / (0.5 * (emp + l12_emp))`
- Sets `hire = 0` if `emp` or `l12_emp` is missing
- Filters out data before 1965
- **Recommended**: Implement `extract_dei_value()` function in `AP_CompustatAnnual.py` to extract `EntityEmployeeCount` from DEI section

---

## 89. Illiquidity.py

### Required Columns:
- **dailyCRSP.parquet**: `permno`, `time_d`, `ret`, `prc`, `vol`

### AP File Status:
- ⚠️ **dailyCRSP.parquet**: **FILE NAME MISMATCH**
  - Predictor expects: `dailyCRSP.parquet`
  - AP version exists: `AP_dailyCRSP.parquet` (from `AP_CRSPDaily.py`)
  - Contains `permno` (from `AP_CRSPDaily.py` line 250)
  - Contains `time_d` (date) - from `AP_CRSPDaily.py` line 251
  - Contains `ret` (return) - from `AP_CRSPDaily.py` line 253
  - Contains `prc` (price) - from `AP_CRSPDaily.py` line 252
  - Contains `vol` (volume) - from `AP_CRSPDaily.py` line 254

### Can Be Constructed?
**YES** - All required columns are available, but file name needs to match.

### Additional Work Needed?
**YES** - File name mismatch needs to be resolved.

#### What Needs to Be Done:
1. **Option 1**: Rename `AP_dailyCRSP.parquet` to `dailyCRSP.parquet` (or create symlink/copy)
2. **Option 2**: Modify `Illiquidity.py` to use `AP_dailyCRSP.parquet` instead of `dailyCRSP.parquet`
3. **Option 3**: Create a wrapper script that copies/renames AP files to match expected names

#### Implementation Notes:
- `AP_CRSPDaily.py` generates `AP_dailyCRSP.parquet` with all required columns
- The predictor calculates Amihud's illiquidity: `ill = abs(ret) / (abs(prc) * vol)`
- Then calculates 12-month rolling mean (requires all 12 months to be non-missing)
- **Recommended**: Create symlink or copy `AP_dailyCRSP.parquet` to `dailyCRSP.parquet` for compatibility

---

## 90. IndIPO.py

### Required Columns:
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`
- **IPODates.parquet**: `permno`, `IPOdate`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
- ⚠️ **IPODates.parquet**: **FILE NAME MISMATCH**
  - Predictor expects: `IPODates.parquet`
  - AP version exists: `AP_IPODates.parquet` (from `AP_IPODates.py`)
  - Contains `permno` (from `AP_IPODates.py` structure)
  - Contains `IPOdate` (IPO date) - from `AP_IPODates.py` line 213

### Can Be Constructed?
**YES** - All required columns are available, but file name needs to match.

### Additional Work Needed?
**YES** - File name mismatch needs to be resolved.

#### What Needs to Be Done:
1. **Option 1**: Rename `AP_IPODates.parquet` to `IPODates.parquet` (or create symlink/copy)
2. **Option 2**: Modify `IndIPO.py` to use `AP_IPODates.parquet` instead of `IPODates.parquet`
3. **Option 3**: Create a wrapper script that copies/renames AP files to match expected names

#### Implementation Notes:
- `AP_IPODates.py` generates `AP_IPODates.parquet` with required columns (`permno`, `IPOdate`, `FoundingYear`)
- Combines Ritter IPO data with live IPO calendars (Nasdaq/NYSE)
- The predictor calculates months since IPO and sets indicator for 3-36 months after IPO
- **Recommended**: Create symlink or copy `AP_IPODates.parquet` to `IPODates.parquet` for compatibility

---

## Summary

| Predictor | All Columns Available? | Can Be Constructed? | Additional Work Needed? |
|-----------|----------------------|---------------------|----------------------|
| HerfBE | ✅ Yes* | ✅ Yes | None (*pstkrv has fallback) |
| High52 | ⚠️ Partial | ⚠️ Partial | **Rename AP_dailyCRSP to dailyCRSP** |
| hire | ❌ No | ❌ No | **Extract emp from XBRL** |
| Illiquidity | ⚠️ Partial | ⚠️ Partial | **Rename AP_dailyCRSP to dailyCRSP** |
| IndIPO | ⚠️ Partial | ⚠️ Partial | **Rename AP_IPODates to IPODates** |

### Critical Missing Components:
1. **`emp` (number of employees) in AP_m_aCompustat.parquet**: Required for `hire.py`. **NOT EXTRACTED** - Employee extraction was removed from `AP_CompustatAnnual.py`.

### File Name Mismatches:
1. **`dailyCRSP.parquet`**: Predictors expect this name, but AP version is `AP_dailyCRSP.parquet`
   - Affects: `High52.py`, `Illiquidity.py`
2. **`IPODates.parquet`**: Predictor expects this name, but AP version is `AP_IPODates.parquet`
   - Affects: `IndIPO.py`

### Recommendations:
1. **For HerfBE**: 
   - **VERIFICATION**: Test that all required columns (`txditc`, `pstk`, `pstkl`, `seq`, `ceq`, `at`, `lt`) are correctly populated
   - Verify `pstkrv` fallback logic works correctly (uses `pstkl` when `pstkrv` is missing)
   - Verify `sicCRSP` and `shrcd` are correctly populated in `AP_SignalMasterTable.parquet`
   - Verify sufficient historical coverage for 36-month rolling average (3-year moving average)
   - Note: Predictor calculates book equity from stockholders equity, deferred tax, and preferred stock

2. **For High52**: 
   - **CRITICAL**: Resolve file name mismatch - rename `AP_dailyCRSP.parquet` to `dailyCRSP.parquet` or modify predictor
   - **VERIFICATION**: Test that `prc` is correctly populated in daily CRSP data
   - Verify sufficient historical coverage for 12-month rolling maximum calculation
   - Note: Predictor calculates 52-week high ratio: current price / maximum price over previous 12 months

3. **For hire**: 
   - **CRITICAL**: Extract `emp` (number of employees) from XBRL data
   - **Implementation**: Add extraction logic to `AP_CompustatAnnual.py`:
     - XBRL tag: `EntityEmployeeCount` or similar
     - Extract from annual 10-K filings
     - Add to balance sheet or income statement extraction logic
   - **VERIFICATION**: Test that `emp` is correctly populated
   - Verify sufficient historical coverage for 12-month lag calculation
   - Note: Predictor sets `hire = 0` if `emp` is missing, filters out data before 1965

4. **For Illiquidity**: 
   - **CRITICAL**: Resolve file name mismatch - rename `AP_dailyCRSP.parquet` to `dailyCRSP.parquet` or modify predictor
   - **VERIFICATION**: Test that `ret`, `prc`, and `vol` are correctly populated in daily CRSP data
   - Verify sufficient historical coverage for 12-month rolling mean (requires all 12 months to be non-missing)
   - Note: Predictor calculates Amihud's illiquidity: `ill = abs(ret) / (abs(prc) * vol)`, then 12-month rolling mean

5. **For IndIPO**: 
   - **CRITICAL**: Resolve file name mismatch - rename `AP_IPODates.parquet` to `IPODates.parquet` or modify predictor
   - **VERIFICATION**: Test that `IPOdate` is correctly populated in IPO dates data
   - Verify `permno` linking is correct (AP_IPODates.py attaches permno from ticker mapping)
   - Note: Predictor calculates months since IPO and sets indicator for 3-36 months after IPO

6. **Note on File Name Mismatches**: 
   - **ALL predictors use original file names** (`SignalMasterTable.parquet`, `m_aCompustat.parquet`, `dailyCRSP.parquet`, `IPODates.parquet`, etc.)
   - **ALL AP versions use `AP_` prefix** (`AP_SignalMasterTable.parquet`, `AP_m_aCompustat.parquet`, `AP_dailyCRSP.parquet`, `AP_IPODates.parquet`, etc.)
   - **119 predictor files** use original file names, **0 predictor files** use AP_ prefix
   - **Recommended Solution**: Create symlinks or copies of AP files with original names for compatibility
   - **Alternative**: Modify all predictors to use AP file names, but this requires changing 119+ predictor files
   - **Best Practice**: Create a setup script that creates symlinks: `AP_SignalMasterTable.parquet` → `SignalMasterTable.parquet`, etc.

7. **Note on Employee Data (`emp`)**: 
   - Employee count (`emp`) is in the **DEI (Document and Entity Information) section** of XBRL filings
   - **NOT in financial statements** (balance sheet, income statement, cash flow)
   - DEI section contains metadata: entity name, CIK, employee count, fiscal year end, etc.
   - XBRL tag: `EntityEmployeeCount` (typically `dei:EntityEmployeeCount` with DEI namespace)
   - In edgartools: Access via `xbrl.dei()` method (separate from `xbrl.statements`)
   - **Current Status**: `extract_dei_value()` function exists but is a placeholder (returns None)
   - **Recommended**: Implement DEI extraction in `AP_CompustatAnnual.py`:
     ```python
     # After extracting financial statements, extract DEI data:
     if hasattr(xbrl, 'dei'):
         dei_data = xbrl.dei()  # or xbrl.dei().to_dataframe()
         # Extract EntityEmployeeCount from dei_data
     ```
   - **Location**: Should be extracted in `get_company_financials_from_10k()` function after financial statement extraction

8. **Note on Daily CRSP Data**: 
   - `AP_CRSPDaily.py` generates `AP_dailyCRSP.parquet` with all required columns
   - Data comes from yfinance (free alternative to CRSP)
   - May have limited historical coverage compared to CRSP
   - **Recommended**: Verify date range coverage for predictors requiring long historical windows

9. **Note on IPO Dates**: 
   - `AP_IPODates.py` combines Ritter IPO data with live IPO calendars
   - May have incomplete `permno` coverage if ticker-to-permno mapping is unavailable
   - **Recommended**: Verify `permno` coverage in `AP_IPODates.parquet` for predictor construction
