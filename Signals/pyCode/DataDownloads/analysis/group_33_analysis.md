# Group 33 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 33.

---

## 161. SP.py

### Required Columns:
- **m_aCompustat.parquet**: `permno`, `time_avail_m`, `sale`
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `mve_permco`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - File exists (from `AP_CompustatAnnual.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `sale` (sales revenue) - mapped from XBRL tags (line 230): `['Revenues', 'SalesRevenueNet', 'RevenueFromContractWithCustomerExcludingAssessedTax']`
  - All required columns are present

- ✅ **AP_SignalMasterTable.parquet**: 
  - File exists (from `AP_SignalMasterTable.py`)
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` structure)
  - Contains `mve_permco` (market value of equity at permco level) - included in column list (line 128), comes from `AP_monthlyCRSP.parquet`
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor calculates `SP = sale / mve_permco`
- Simple ratio calculation of sales to market value of equity
- **Note**: `mve_permco` comes from `AP_monthlyCRSP.parquet` where it's calculated as `mve_c` (line 370 in `AP_CRSPMonthly.py`)

---

## 162. Spinoff.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`
- **m_CRSPAcquisitions.parquet**: `permno`, `SpinoffCo`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - File exists (from `AP_SignalMasterTable.py`)
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` structure)
  - All required columns are present

- ✅ **AP_m_CRSPAcquisitions.parquet**: 
  - File exists (from `AP_CRSPAcquisitions.py`)
  - Contains `permno` (from `AP_CRSPAcquisitions.py` structure)
  - Contains `SpinoffCo` (spinoff company indicator) - set to `1` for spinoff companies (line 526, 553, 562)
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor creates spinoff indicator: `Spinoff = 1` if `SpinoffCo == 1` and `FirmAgeNoScreen <= 24`, `0` otherwise
- Uses firm age (number of observations) to identify newly spun-off companies
- **Note**: `AP_CRSPAcquisitions.py` identifies spinoff companies by parsing SEC 8-K filings for spinoff events (Item 2.01, 2.02)
- **Note**: Coverage may be lower than CRSP for historical data (pre-2000) as noted in `AP_CRSPAcquisitions.py` (line 591)

---

## 163. std_turn.py

### Required Columns:
- **monthlyCRSP.parquet**: `permno`, `time_avail_m`, `vol`, `shrout`, `prc`

### AP File Status:
- ✅ **AP_monthlyCRSP.parquet**: 
  - File exists (from `AP_CRSPMonthly.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` structure)
  - Contains `vol` (monthly volume in 100s of shares) - line 347, 438
  - Contains `shrout` (shares outstanding in millions) - line 348
  - Contains `prc` (stock price) - line 346
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor calculates standard deviation of turnover over past 36 months: `std_turn = std(vol / shrout)` over 36-month window (min 24 observations)
- Sets `std_turn` to missing for size quintiles 4-5 (tiny spread per original paper)
- Calculates turnover as `vol / shrout` and market value as `shrout * abs(prc)`
- **Note**: Uses rolling standard deviation with Polars for efficient calculation

---

## 164. STreversal.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `ret`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - File exists (from `AP_SignalMasterTable.py`)
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` structure)
  - Contains `ret` (monthly returns) - comes from `AP_monthlyCRSP.parquet`
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor calculates short-term reversal: `STreversal = ret` (filled with 0 if missing)
- Simple 1-month momentum/reversal predictor
- Uses current month's return as the reversal signal
- **Note**: Missing returns are filled with 0 (line 41)

---

## 165. SurpriseRD.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `xrd`, `revt`, `at`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - File exists (from `AP_CompustatAnnual.py`)
  - Contains `gvkey` (surrogate: CIK/permno) - from `AP_CompustatAnnual.py` structure
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `xrd` (R&D expenses) - mapped from XBRL tags (line 242-243): `['ResearchAndDevelopmentExpense', 'ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost', 'ResearchAndDevelopmentExpenseSoftwareExcludingAcquiredInProcessCost']`
  - Contains `revt` (total revenue) - mapped from XBRL tags (line 232): `['Revenues', 'RevenueFromContractWithCustomerExcludingAssessedTax']`
  - Contains `at` (total assets) - mapped from XBRL tags (line 84): `['Assets']`
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor calculates unexpected R&D increase: `SurpriseRD = 1` if multiple conditions are met:
  - `xrd / revt > 0` (positive R&D to revenue ratio)
  - `xrd / at > 0` (positive R&D to assets ratio)
  - `xrd / xrd_lag12 > 1.05` (R&D increased by more than 5% from 12 months ago)
  - `(xrd / at) / (xrd_lag12 / at_lag12) > 1.05` (R&D intensity increased by more than 5%)
- Uses 12-month lagged values for comparison
- **Note**: `gvkey` is loaded but not used in calculations (only kept for compatibility)
- **Note**: `gvkey` is a surrogate (CIK/permno) in AP version, but doesn't affect calculations

---

## Summary

### Overall Status:
**5 out of 5 predictors can be constructed** ✅

### Column Availability:
- ✅ All required columns are present in AP data sources
- ✅ No missing columns identified
- ✅ No column-related issues found

### Key Notes:
1. **For SP**: 
   - **VERIFICATION**: Test that `sale` and `mve_permco` are correctly populated
   - Simple ratio calculation: `SP = sale / mve_permco`
   - **Note**: `mve_permco` comes from `AP_monthlyCRSP.parquet` where it's calculated as `mve_c`

2. **For Spinoff**: 
   - **VERIFICATION**: Test that `SpinoffCo` is correctly populated
   - Uses firm age to identify newly spun-off companies (first 24 months)
   - **Note**: `AP_CRSPAcquisitions.py` identifies spinoff companies by parsing SEC 8-K filings
   - **Note**: Coverage may be lower than CRSP for historical data (pre-2000)

3. **For std_turn**: 
   - **VERIFICATION**: Test that `vol`, `shrout`, and `prc` are correctly populated
   - Calculates standard deviation of turnover over past 36 months
   - Sets to missing for size quintiles 4-5 (tiny spread)
   - **Note**: Uses Polars for efficient rolling window calculations

4. **For STreversal**: 
   - **VERIFICATION**: Test that `ret` is correctly populated
   - Simple 1-month momentum/reversal predictor
   - **Note**: Missing returns are filled with 0

5. **For SurpriseRD**: 
   - **VERIFICATION**: Test that `xrd`, `revt`, and `at` are correctly populated
   - Uses multiple conditions to identify unexpected R&D increases
   - Requires 12-month lagged values for comparison
   - **Note**: `gvkey` is loaded but not used in calculations (only kept for compatibility)

6. **Note on gvkey Surrogate**: 
   - `gvkey` is not directly available from EDGAR (Compustat-specific identifier)
   - AP version uses `cik` (SEC identifier) as surrogate, falls back to `permno` if unavailable
   - For `SurpriseRD.py`, `gvkey` is loaded but not used in calculations (only kept for compatibility)

7. **Note on File Names**: 
   - File name mismatches are ignored per user instructions (from Group 19 onwards)
   - Predictors will need to be modified to use AP file names (`AP_dailyCRSP.parquet`, `AP_monthlyCRSP.parquet`, etc.) or AP files can be renamed to match expected names

