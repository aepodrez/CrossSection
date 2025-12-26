# Group 32 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 32.

---

## 156. ShortInterest.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `gvkey`, `time_avail_m`
- **monthlyCRSP.parquet**: `permno`, `time_avail_m`, `shrout`
- **monthlyShortInterest.parquet**: `gvkey`, `time_avail_m`, `shortint`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - File exists (from `AP_SignalMasterTable.py`)
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` structure)
  - Contains `gvkey` (surrogate: CIK/permno) - line 119, filled from `permno` if missing
  - All required columns are present

- ✅ **AP_monthlyCRSP.parquet**: 
  - File exists (from `AP_CRSPMonthly.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` structure)
  - Contains `shrout` (shares outstanding in millions, back-adjusted by split factor) - line 348
  - All required columns are present

- ✅ **AP_monthlyShortInterest.parquet**: 
  - File exists (from `AP_CompustatShortInterest.py`)
  - Contains `gvkey` (surrogate: CIK/permno) - line 442-448, mapped from `AP_CompustatAnnual.parquet`
  - Contains `time_avail_m` (from `AP_CompustatShortInterest.py` structure)
  - Contains `shortint` (short interest in millions of shares) - line 348, 465
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor filters for non-null `gvkey` (line 33) - AP version uses surrogate `gvkey` (CIK/permno)
- Merges on `gvkey` between `SignalMasterTable.parquet` and `monthlyShortInterest.parquet`
- Calculates `ShortInterest = shortint / shrout`
- **Note**: `gvkey` is a surrogate (CIK/permno) in AP version, but should work for merging as long as both files use the same surrogate approach
- **Note**: `shortint` is scaled to millions of shares in `AP_CompustatShortInterest.py` (line 348)

---

## 157. sinAlgo.py

### Required Columns:
- **CompustatSegments.parquet**: `gvkey`, `sics1`, `naicsh`, `datadate`
- **SignalMasterTable.parquet**: `permno`, `gvkey`, `time_avail_m`, `sicCRSP`, `shrcd`
- **m_aCompustat.parquet**: `permno`, `time_avail_m`, `naicsh`

### AP File Status:
- ⚠️ **CompustatSegments.parquet**: **FILE EXISTS** (WRDS version)
  - File exists: `CompustatSegments.parquet` (from `CompustatBusinessSegments.py`)
  - Contains `gvkey`, `sics1`, `naicsh`, `datadate` (business segment data)
  - **Note**: This is NOT an AP script - it downloads from WRDS
  - **Issue**: Uses real Compustat `gvkey` values, while AP Compustat data uses surrogate `gvkey` (CIK/permno)
  - **Can be used IF**: Using WRDS segment data (real gvkey)
  - **Cannot be used IF**: Using AP segment data (surrogate gvkey)

- ✅ **AP_SignalMasterTable.parquet**: 
  - File exists (from `AP_SignalMasterTable.py`)
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` structure)
  - Contains `gvkey` (surrogate: CIK/permno) - line 119
  - Contains `sicCRSP` (SIC code from CRSP) - from `AP_SignalMasterTable.py` structure
  - Contains `shrcd` (share class code) - from `AP_SignalMasterTable.py` structure
  - All required columns are present

- ❌ **AP_m_aCompustat.parquet**: **MISSING COLUMN**
  - File exists (from `AP_CompustatAnnual.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - **Missing**: `naicsh` (NAICS code) - NOT extracted from XBRL filings
  - **Note**: `naicsh` is used to identify gaming segments (line 52-54, 134-135)

### Can Be Constructed?
**PARTIALLY** - Multiple issues:
- ✅ `CompustatSegments.parquet` exists (WRDS version) but has `gvkey` mismatch
- ❌ `naicsh` missing from `AP_m_aCompustat.parquet`
- ⚠️ **Issue**: `CompustatSegments.parquet` uses real Compustat `gvkey`, while AP Compustat data uses surrogate `gvkey` (CIK/permno)
- **Can be constructed IF**: Using WRDS segment data (real gvkey) AND extracting `naicsh` from XBRL filings
- **Cannot be constructed IF**: Using AP segment data (surrogate gvkey) OR missing `naicsh`

### Additional Work Needed?
**YES** - Multiple issues:

#### What Needs to Be Done:
1. **Extract `naicsh` from XBRL Filings**: 
   - `naicsh` (NAICS code) is not currently extracted from XBRL filings in `AP_CompustatAnnual.py`
   - NAICS codes are typically found in the DEI (Document and Entity Information) section of XBRL filings
   - XBRL tag: `EntityCentralIndexKey` or similar entity identification tags
   - **Alternative**: Could use a static mapping file (ticker -> NAICS) if available

2. **Option 1: Use WRDS Segment Data** (Not AP version):
   - Use existing `CompustatSegments.parquet` (WRDS version) ✓
   - Extract `naicsh` from XBRL filings for `AP_m_aCompustat.parquet`
   - **Result**: Predictor can be constructed, but NOT fully using AP data sources

3. **Option 2: Create AP Version of Segments** (True AP version):
   - Create `AP_CompustatSegments.parquet` from SEC 10-K filings (extracts segments with surrogate `gvkey`)
   - Extract `naicsh` from XBRL filings for `AP_m_aCompustat.parquet`
   - **Result**: Fully AP version, but requires significant text parsing of 10-K segment disclosures

#### Implementation Notes:
- Predictor identifies sin stocks by SIC codes (tobacco: 2100-2199, beer: 2080-2085) and NAICS codes (gaming: 7132, 71312, 713210, 71329, 713290, 72112, 721120)
- Uses segment-level data to identify sin firms even if firm-level SIC/NAICS doesn't match
- Applies historical backfill: stocks identified as sinful remain sinful throughout history
- Excludes non-standard share classes (`shrcd > 11`)
- **Note**: `naicsh` is critical for identifying gaming segments (line 52-54, 134-135)

---

## 158. Size.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `mve_c`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - File exists (from `AP_SignalMasterTable.py`)
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` structure)
  - Contains `mve_c` (market value of equity in millions) - included in column list (line 127), comes from `AP_monthlyCRSP.parquet`
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor calculates `Size = log(mve_c)`
- Simple log transformation of market value of equity
- **Note**: `mve_c` comes from `AP_monthlyCRSP.parquet` where it's calculated as `shrout * abs(prc)` (line 364 in `AP_CRSPMonthly.py`)

---

## 159. skew1.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `secid`
- **OptionMetricsXZZ.csv**: `secid`, `time_avail_m`, `Skew1`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - File exists (from `AP_SignalMasterTable.py`)
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` structure)
  - Contains `secid` (OptionMetrics security identifier) - set to `np.nan` (line 118)
  - **Issue**: `secid` is not populated in AP version (set to NaN)

- ❌ **OptionMetricsXZZ.csv**: **FILE DOES NOT EXIST YET**
  - Predictor expects `OptionMetricsXZZ.csv` in `../pyData/Prep/` directory
  - Contains pre-calculated `Skew1` values from OptionMetrics data
  - **Note**: OptionMetrics data is proprietary and requires subscription
  - **Note**: Predictor has a patch option (`PATCH_OPTIONM_IV`) that uses `openassetpricing` library (2023 vintage) as alternative

### Can Be Constructed?
**NO** - Missing required data:
- ❌ `secid` is not populated in `AP_SignalMasterTable.parquet` (set to NaN)
- ❌ `OptionMetricsXZZ.csv` does not exist (proprietary OptionMetrics data)

### Additional Work Needed?
**YES** - Multiple issues:

#### What Needs to Be Done:
1. **Populate `secid` in AP_SignalMasterTable.parquet**: 
   - `secid` (OptionMetrics security identifier) is currently set to `np.nan` (line 118)
   - OptionMetrics `secid` is a proprietary identifier that maps to CRSP `permno`
   - **Solution**: Would need OptionMetrics data access or a mapping file (permno -> secid)
   - **Alternative**: Use `PATCH_OPTIONM_IV` flag to use `openassetpricing` library (2023 vintage)

2. **Obtain OptionMetricsXZZ.csv**: 
   - `OptionMetricsXZZ.csv` contains pre-calculated `Skew1` values from OptionMetrics data
   - OptionMetrics data is proprietary and requires subscription
   - **Alternative**: Use `PATCH_OPTIONM_IV` flag to use `openassetpricing` library (2023 vintage)

#### Implementation Notes:
- Predictor calculates volatility smirk near the money using OptionMetrics implied volatility differences
- Uses `secid` to merge with OptionMetrics data
- Has a patch option (`PATCH_OPTIONM_IV`) that uses `openassetpricing` library (2023 vintage) as alternative
- **Note**: If `PATCH_OPTIONM_IV` is True, predictor uses `openassetpricing` library and exits early (line 32-41)

---

## 160. SmileSlope.py

### Required Columns:
- **OptionMetricsVolSurf.csv**: `secid`, `time_avail_m`, `days`, `delta`, `cp_flag`, `impl_vol`
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `secid`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - File exists (from `AP_SignalMasterTable.py`)
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` structure)
  - Contains `secid` (OptionMetrics security identifier) - set to `np.nan` (line 118)
  - **Issue**: `secid` is not populated in AP version (set to NaN)

- ❌ **OptionMetricsVolSurf.csv**: **FILE DOES NOT EXIST YET**
  - Predictor expects `OptionMetricsVolSurf.csv` in `../pyData/Prep/` directory
  - Contains OptionMetrics volatility surface data with columns: `secid`, `time_avail_m`, `days`, `delta`, `cp_flag`, `impl_vol`
  - **Note**: OptionMetrics data is proprietary and requires subscription
  - **Note**: Predictor has a patch option (`PATCH_OPTIONM_IV`) that uses `openassetpricing` library (2023 vintage) as alternative

### Can Be Constructed?
**NO** - Missing required data:
- ❌ `secid` is not populated in `AP_SignalMasterTable.parquet` (set to NaN)
- ❌ `OptionMetricsVolSurf.csv` does not exist (proprietary OptionMetrics data)

### Additional Work Needed?
**YES** - Multiple issues:

#### What Needs to Be Done:
1. **Populate `secid` in AP_SignalMasterTable.parquet**: 
   - `secid` (OptionMetrics security identifier) is currently set to `np.nan` (line 118)
   - OptionMetrics `secid` is a proprietary identifier that maps to CRSP `permno`
   - **Solution**: Would need OptionMetrics data access or a mapping file (permno -> secid)
   - **Alternative**: Use `PATCH_OPTIONM_IV` flag to use `openassetpricing` library (2023 vintage)

2. **Obtain OptionMetricsVolSurf.csv**: 
   - `OptionMetricsVolSurf.csv` contains OptionMetrics volatility surface data
   - OptionMetrics data is proprietary and requires subscription
   - **Alternative**: Use `PATCH_OPTIONM_IV` flag to use `openassetpricing` library (2023 vintage)

#### Implementation Notes:
- Predictor calculates implied volatility smile slope: `SmileSlope = Put IV - Call IV` for 30-day 50-delta options
- Filters to 30-day options with 50 delta (line 50)
- Pivots data to get separate columns for put and call implied volatilities
- Has a patch option (`PATCH_OPTIONM_IV`) that uses `openassetpricing` library (2023 vintage) as alternative
- **Note**: If `PATCH_OPTIONM_IV` is True, predictor uses `openassetpricing` library and exits early (line 32-41)

---

## Summary

### Overall Status:
**2 out of 5 predictors can be constructed** ✅

### Column Availability:
- ✅ **ShortInterest.py**: All required columns are present
- ✅ **Size.py**: All required columns are present
- ⚠️ **sinAlgo.py**: `CompustatSegments.parquet` exists (WRDS version) but has `gvkey` mismatch; `naicsh` missing from `AP_m_aCompustat.parquet`
- ❌ **skew1.py**: `secid` not populated; `OptionMetricsXZZ.csv` missing (proprietary)
- ❌ **SmileSlope.py**: `secid` not populated; `OptionMetricsVolSurf.csv` missing (proprietary)

### Key Notes:
1. **For ShortInterest**: 
   - **VERIFICATION**: Test that `gvkey` (surrogate) matches between `AP_SignalMasterTable.parquet` and `AP_monthlyShortInterest.parquet`
   - Uses surrogate `gvkey` (CIK/permno) for merging
   - **Note**: `shortint` is scaled to millions of shares

2. **For sinAlgo**: 
   - **VERIFICATION**: Test that `naicsh` is correctly extracted from XBRL filings
   - Requires `CompustatSegments.parquet` (WRDS version) OR AP version with surrogate `gvkey`
   - Requires `naicsh` from `AP_m_aCompustat.parquet` (currently missing)
   - **Note**: `CompustatSegments.parquet` exists (WRDS version) but uses real Compustat `gvkey`, while AP Compustat data uses surrogate `gvkey` (CIK/permno)

3. **For Size**: 
   - **VERIFICATION**: Test that `mve_c` is correctly populated
   - Simple log transformation of market value of equity
   - **Note**: `mve_c` comes from `AP_monthlyCRSP.parquet` where it's calculated as `shrout * abs(prc)`

4. **For skew1 and SmileSlope**: 
   - **VERIFICATION**: Test that `secid` is correctly populated (if OptionMetrics mapping is available)
   - Both predictors require OptionMetrics data (proprietary)
   - Both predictors have a patch option (`PATCH_OPTIONM_IV`) that uses `openassetpricing` library (2023 vintage) as alternative
   - **Note**: `secid` is currently set to `np.nan` in `AP_SignalMasterTable.parquet` (line 118)

5. **Note on gvkey Surrogate**: 
   - `gvkey` is not directly available from EDGAR (Compustat-specific identifier)
   - AP version uses `cik` (SEC identifier) as surrogate, falls back to `permno` if unavailable
   - For `ShortInterest.py` and `sinAlgo.py`, `gvkey` is used for merging, surrogate approach is acceptable IF both files use the same surrogate

6. **Note on File Names**: 
   - File name mismatches are ignored per user instructions (from Group 19 onwards)
   - Predictors will need to be modified to use AP file names (`AP_dailyCRSP.parquet`, `AP_monthlyCRSP.parquet`, etc.) or AP files can be renamed to match expected names

7. **Note on OptionMetrics Data**: 
   - OptionMetrics data is proprietary and requires subscription
   - Both `skew1.py` and `SmileSlope.py` have a patch option (`PATCH_OPTIONM_IV`) that uses `openassetpricing` library (2023 vintage) as alternative
   - If `PATCH_OPTIONM_IV` is True, predictors use `openassetpricing` library and exit early (bypassing OptionMetrics data requirements)

