# Group 9 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 9.

---

## 41. CredRatDG.py

### Required Columns:
- **AP_m_SP_creditratings.parquet**: `gvkey`, `time_avail_m`, `credrat`
- **AP_m_CIQ_creditratings.parquet**: `gvkey`, `ratingdate`, `source`, `anydowngrade`
- **AP_SignalMasterTable.parquet**: `gvkey`, `permno`, `time_avail_m`

### AP File Status:
- ❌ **AP_m_SP_creditratings.parquet**: **FILE DOES NOT EXIST YET**
  - Script would need to be created to download S&P credit ratings data
  - S&P credit ratings are typically available from proprietary sources (S&P Capital IQ, WRDS)
  - Would contain `gvkey`, `time_avail_m`, `credrat` (credit rating code)
- ❌ **AP_m_CIQ_creditratings.parquet**: **FILE DOES NOT EXIST YET**
  - Script would need to be created to download CIQ (Capital IQ) credit ratings data
  - CIQ credit ratings are typically available from proprietary sources (S&P Capital IQ, WRDS)
  - Would contain `gvkey`, `ratingdate`, `source`, `anydowngrade` (downgrade indicator)
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` line 119)

### Can Be Constructed?
**NO** - Missing both credit ratings files.

### Additional Work Needed?
**YES** - Need to create scripts to download credit ratings data.

#### What Needs to Be Done:
1. **Create AP_SP_creditratings.py**: 
   - Download S&P credit ratings from proprietary source (S&P Capital IQ, WRDS)
   - Process to create monthly time series with `gvkey`, `time_avail_m`, `credrat`
   - Requires proprietary data access

2. **Create AP_CIQ_creditratings.py**: 
   - Download CIQ credit ratings from proprietary source (S&P Capital IQ, WRDS)
   - Process to create monthly time series with `gvkey`, `ratingdate`, `source`, `anydowngrade`
   - Requires proprietary data access

#### Implementation Notes:
- **For Credit Ratings**: S&P and CIQ credit ratings are proprietary data sources that require paid subscriptions
- Without access to these databases, this predictor cannot be constructed
- **Alternative**: Could potentially use free credit rating sources (e.g., SEC filings, company websites), but would require significant data engineering and may have lower coverage/quality
- **Note on gvkey**: Credit ratings data typically uses `gvkey` as identifier. The AP version uses surrogate `gvkey` (CIK/permno), which may not match exactly with credit ratings databases

---

## 42. CustomerMomentum.py

### Required Columns:
- **CompustatSegmentDataCustomers.csv**: `gvkey`, `datadate`, `ctype`, `cnms`
- **CCMLinkingTable.parquet**: `gvkey`, `timeLinkStart_d`, `timeLinkEnd_d`, `permno`, `conm`
- **AP_monthlyCRSP.parquet**: `permno`, `time_avail_m`, `ret_b4_dl`

### AP File Status:
- ❌ **CompustatSegmentDataCustomers.csv**: **FILE DOES NOT EXIST YET**
  - Script exists: `CompustatBusinessSegments.py` (generates `CompustatSegments.parquet`)
  - Customer segment data is a subset of segment data where `stype == "CUSTOMER"`
  - Would need to extract customer segments from segment data or create separate download script
- ⚠️ **CCMLinkingTable.parquet**: **FILE EXISTS** (WRDS version)
  - File exists: `CCMLinkingTable.parquet` (from `CCMLinkingTable.py`)
  - Contains `gvkey`, `timeLinkStart_d`, `timeLinkEnd_d`, `permno`, `conm` (company name)
  - **Note**: This is NOT an AP script - it downloads from WRDS
  - **Issue**: Uses real Compustat `gvkey` values, while AP customer segment data would use surrogate `gvkey` (CIK/permno)
  - **Can be used IF**: Using WRDS customer segment data (real gvkey)
  - **Cannot be used IF**: Using AP customer segment data (surrogate gvkey)
- ✅ **AP_monthlyCRSP.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` structure)
  - Contains `ret_b4_dl` (returns before delisting adjustment) - from `AP_CRSPMonthly.py` line 367

### Can Be Constructed?
**PARTIALLY** - `CCMLinkingTable.parquet` exists but has `gvkey` mismatch issues:
- ✅ `CCMLinkingTable.parquet` exists (WRDS version)
- ❌ `CompustatSegmentDataCustomers.csv` missing
- ⚠️ **Issue**: CCMLinkingTable uses real Compustat `gvkey` values, while AP customer segment data would use surrogate `gvkey` (CIK/permno)
- **Can be constructed IF**: Using WRDS customer segment data (real gvkey) - segments will match CCMLinkingTable on `gvkey` (line 154)
- **Cannot be constructed IF**: Using AP customer segment data (surrogate gvkey) - segments won't match CCMLinkingTable

### Additional Work Needed?
**YES** - Multiple issues:

#### What Needs to Be Done:
1. **Create CompustatSegmentDataCustomers.csv**: 
   - Extract customer segments from Compustat segment data
   - Customer segments are identified by `stype == "CUSTOMER"` in segment data
   - Requires Compustat segment data access (proprietary)
   - **Alternative**: Could potentially extract from SEC filings (10-K segment disclosures), but would require significant text parsing

2. **Option 1: Use WRDS Customer Segment Data** (Not AP version):
   - Use existing `CCMLinkingTable.parquet` (WRDS version) ✓
   - Use WRDS `CompustatSegmentDataCustomers.csv` (real gvkey)
   - **Result**: Predictor can be constructed, but NOT using AP data sources

3. **Option 2: Create AP Versions** (True AP version):
   - Create `AP_CCMLinkingTable.parquet` from AP data (links surrogate `gvkey` to `permno`)
   - Create `AP_CompustatSegmentDataCustomers.csv` from SEC 10-K filings (extracts customer segments with surrogate `gvkey`)
   - **Result**: Predictor can be constructed using AP data sources, but requires significant work

3. **Note on ret_b4_dl**: 
   - `ret_b4_dl` is available in `AP_monthlyCRSP.parquet` (from `AP_CRSPMonthly.py` line 367)
   - For active stocks, `ret_b4_dl = ret` (no delisting adjustment)

#### Implementation Notes:
- **For Customer Segments**: Compustat segment data is proprietary and requires WRDS access
- Customer segment data contains customer names (`cnms`) which are matched to company names in CCM linking table
- **Alternative**: Could extract customer information from SEC 10-K filings (segment disclosures), but would require:
  - Text parsing of segment tables
  - Name standardization and matching
  - Significant data engineering effort

- **For CCM Linking Table**: The linking table connects Compustat `gvkey` to CRSP `permno` with temporal validity ranges
- AP version would need to create this from ticker mappings and company name matching
- Company names (`conm`) are critical for matching customer names to companies

---

## 43. dCPVolSpread.py

### Required Columns:
- **OptionMetricsVolSurf.csv**: `secid`, `time_avail_m`, `days`, `delta`, `cp_flag`, `impl_vol`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `secid`

### AP File Status:
- ❌ **OptionMetricsVolSurf.csv**: **FILE DOES NOT EXIST**
  - This file is typically generated from OptionMetrics data (proprietary database)
  - Contains implied volatility surface data for options
  - Required columns: `secid`, `time_avail_m`, `days`, `delta`, `cp_flag` (C/P), `impl_vol`
- ⚠️ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - ⚠️ **`secid`**: Set to `np.nan` (from `AP_SignalMasterTable.py` line 118) - **NOT POPULATED**

### Can Be Constructed?
**NO** - Missing `OptionMetricsVolSurf.csv` and `secid` is not populated in `AP_SignalMasterTable.parquet`.

### Additional Work Needed?
**YES** - Same issues as `CPVolSpread.py` (Group 8):

#### What Needs to Be Done:
1. **Create OptionMetricsVolSurf.csv**: 
   - This requires OptionMetrics data (proprietary database)
   - OptionMetrics provides implied volatility surface data for options
   - Need to process OptionMetrics data to create volatility surface with required dimensions
   - Script would need to:
     - Download OptionMetrics implied volatility data
     - Filter to specific `days` (30) and `delta` (50) for ATM options
     - Aggregate by `secid`, `time_avail_m`, and `cp_flag` (call/put)

2. **Populate `secid` in AP_SignalMasterTable.parquet**:
   - `secid` is OptionMetrics security identifier
   - Need to create linking table between CRSP `permno` and OptionMetrics `secid`
   - This typically requires OptionMetrics-CRSP linking table
   - Without `secid`, cannot merge options data with stock data

#### Implementation Notes:
- **For OptionMetricsVolSurf**: OptionMetrics is a proprietary database that requires subscription
- The predictor calculates changes in call-put volatility spread: `dCPVolSpread = dVolPut - dVolCall`
- Without OptionMetrics access, this predictor cannot be constructed
- **Alternative**: Could potentially use free options data sources (e.g., CBOE, Yahoo Finance options), but would need significant adaptation of the data processing logic

- **For secid**: OptionMetrics uses `secid` as its primary identifier, which maps to CRSP `permno` via a linking table
- Without OptionMetrics-CRSP linking table, cannot populate `secid` in `AP_SignalMasterTable.parquet`
- Even if `OptionMetricsVolSurf.csv` exists, cannot merge without `secid`

---

## 44. DebtIssuance.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `permno`, `time_avail_m`, `ceq`, `dltis`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `mve_permco`, `shrcd`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184
  - Contains `dltis` (debt issuance) - mapped in XBRL_TAG_MAP line 148
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `mve_permco` (from `AP_SignalMasterTable.py` line 128)
  - Contains `shrcd` (share code) - from `AP_SignalMasterTable.py` line 131

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 45. DelBreadth.py

### Required Columns:
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `exchcd`, `mve_c`
- **AP_TR_13F.parquet**: `permno`, `time_avail_m`, `dbreadth`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `exchcd` (exchange code) - from `AP_SignalMasterTable.py` line 130
  - Contains `mve_c` (market value of equity) - from `AP_SignalMasterTable.py` line 127
- ❌ **AP_TR_13F.parquet**: **FILE DOES NOT EXIST YET**
  - Script would need to be created to download 13F data
  - 13F filings are publicly available from SEC but require processing
  - Would contain `permno`, `time_avail_m`, `dbreadth` (change in breadth of ownership)
  - `dbreadth` is calculated from 13F holdings data (number of institutional owners)

### Can Be Constructed?
**NO** - Missing `AP_TR_13F.parquet` file.

### Additional Work Needed?
**YES** - Need to create script to download and process 13F data.

#### What Needs to Be Done:
1. **Create AP_TR_13F.py**: 
   - Download 13F filings from SEC EDGAR (publicly available)
   - Process 13F XML filings to extract holdings data
   - Calculate `dbreadth` (change in number of institutional owners) for each stock
   - Match to CRSP `permno` using CUSIP or ticker matching
   - Aggregate to monthly frequency

#### Implementation Notes:
- **For 13F Data**: 13F filings are publicly available from SEC EDGAR
- 13F filings are required for institutional investment managers with >$100M in assets
- Filings are in XML format and require parsing
- Need to:
  - Download 13F filings from SEC EDGAR
  - Parse XML to extract holdings (CUSIP, shares, value)
  - Match CUSIPs to CRSP `permno` using CUSIP mapping
  - Count number of unique institutional owners per stock per quarter
  - Calculate `dbreadth` as change in number of owners
  - Convert quarterly data to monthly (forward-fill)

- **Data Source**: SEC EDGAR (free, publicly available)
- **Complexity**: Moderate - requires XML parsing and CUSIP-to-permno matching
- **Coverage**: Good - 13F filings cover most institutional holdings

---

## Summary

| Predictor | All Columns Available? | Can Be Constructed? | Additional Work Needed? |
|-----------|----------------------|---------------------|----------------------|
| CredRatDG | ❌ No | ❌ No | **Create AP_SP_creditratings.py, AP_CIQ_creditratings.py** |
| CustomerMomentum | ❌ No | ❌ No | **Create CompustatSegmentDataCustomers.csv, AP_CCMLinkingTable.parquet** |
| dCPVolSpread | ❌ No | ❌ No | **Create OptionMetricsVolSurf.csv, populate secid** |
| DebtIssuance | ✅ Yes | ✅ Yes | None |
| DelBreadth | ❌ No | ❌ No | **Create AP_TR_13F.py** |

### Critical Missing Components:
1. **`AP_m_SP_creditratings.parquet` and `AP_m_CIQ_creditratings.parquet`**: Required for `CredRatDG.py`. Files do not exist - need to be created from proprietary credit ratings databases.
2. **`CompustatSegmentDataCustomers.csv`**: Required for `CustomerMomentum.py`. File does not exist - needs to be extracted from Compustat segment data or SEC filings.
3. **`CCMLinkingTable.parquet`**: Required for `CustomerMomentum.py`. Exists but uses WRDS - need AP version using ticker mappings.
4. **`OptionMetricsVolSurf.csv`**: Required for `dCPVolSpread.py`. File does not exist - needs to be generated from OptionMetrics data (proprietary database).
5. **`secid` column in AP_SignalMasterTable.parquet**: Required for `dCPVolSpread.py`. Currently set to `np.nan` - needs OptionMetrics-CRSP linking table.
6. **`AP_TR_13F.parquet`**: Required for `DelBreadth.py`. File does not exist - needs to be created from SEC 13F filings.

### Recommendations:
1. **For CredRatDG**: 
   - **CRITICAL**: Create scripts to download S&P and CIQ credit ratings
   - Requires proprietary data access (S&P Capital IQ, WRDS)
   - If proprietary data is not available, this predictor cannot be constructed
   - **Alternative**: Could use free sources (SEC filings, company websites), but coverage/quality may be lower

2. **For CustomerMomentum**: 
   - **CRITICAL**: Extract customer segment data from Compustat or SEC filings
   - Requires Compustat segment data access (proprietary) OR significant text parsing of SEC 10-K filings
   - **CRITICAL**: Create AP version of CCM linking table using ticker mappings and company name matching
   - `ret_b4_dl` is available in `AP_monthlyCRSP.parquet` (confirmed)

3. **For dCPVolSpread**: 
   - **CRITICAL**: Requires OptionMetrics data access (proprietary database)
   - Need to create `OptionMetricsVolSurf.csv` from OptionMetrics implied volatility data
   - Need to populate `secid` in `AP_SignalMasterTable.parquet` using OptionMetrics-CRSP linking table
   - Without OptionMetrics access, this predictor cannot be constructed
   - **Alternative**: Could use free options data sources, but would require significant code adaptation

4. **For DebtIssuance**: 
   - **VERIFICATION**: Test that all AP files actually contain the columns listed above by loading sample data
   - Verify `dltis` (debt issuance) is correctly populated in `AP_m_aCompustat.parquet`
   - Verify `ceq` (common equity) is correctly populated in `AP_m_aCompustat.parquet`

5. **For DelBreadth**: 
   - **CRITICAL**: Create `AP_TR_13F.py` to download and process 13F filings from SEC EDGAR
   - 13F data is publicly available (free) but requires XML parsing and CUSIP-to-permno matching
   - Calculate `dbreadth` (change in number of institutional owners) from 13F holdings data
   - This is feasible with free data sources but requires significant data engineering

6. **Note on Proprietary Data Sources**: 
   - Credit ratings (S&P, CIQ) and OptionMetrics are proprietary databases requiring paid subscriptions
   - Without access to these databases, `CredRatDG` and `dCPVolSpread` cannot be constructed
   - Consider alternative free data sources or skip these predictors if proprietary data is unavailable

7. **Note on Free Data Sources**: 
   - 13F filings are publicly available from SEC EDGAR (free)
   - Customer segment data could potentially be extracted from SEC 10-K filings (free but requires text parsing)
   - These predictors are feasible with free data but require significant data engineering effort
