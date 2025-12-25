# Group 39 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 39.

---

## 191. ZZ2_AbnormalAccruals_AbnormalAccrualsPercent.py

### Required Columns:
- **a_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `fyear`, `datadate`, `at`, `oancf`, `fopt`, `act`, `che`, `lct`, `dlc`, `ib`, `sale`, `ppegt`, `ni`, `sic`
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `exchcd`

### AP File Status:
- ✅ **AP_a_aCompustat.parquet**: 
  - File exists (from `AP_CompustatAnnual.py`)
  - Contains `gvkey` (surrogate: CIK/permno) - from `AP_CompustatAnnual.py` structure
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `fyear` (fiscal year) - renamed from `fiscal_year` (line 935)
  - Contains `datadate` (fiscal period end date) - renamed from `period_end` (line 934)
  - Contains `at` (total assets) - mapped from XBRL tags (line 84): `['Assets']`
  - Contains `oancf` (operating activities net cash flow) - mapped from XBRL tags (line 306): `['NetCashProvidedByUsedInOperatingActivities', ...]`
  - Contains `fopt` (foreign operations profit) - mapped from XBRL tags (line 291): `['IncomeLossFromContinuingOperationsAttributableToForeignOperations', ...]`
  - Contains `act` (current assets) - mapped from XBRL tags (line 87): `['AssetsCurrent']`
  - Contains `che` (cash) - mapped from XBRL tags (line 90): `['CashAndCashEquivalentsAtCarryingValue', 'CashAndDueFromBanks', 'Cash']`
  - Contains `lct` (current liabilities) - mapped from XBRL tags (line 140): `['LiabilitiesCurrent']`
  - Contains `dlc` (debt current) - mapped from XBRL tags (line 143-146): `['DebtCurrent', 'ShortTermBorrowings', ...]`
  - Contains `ib` (income before extraordinary items) - mapped from XBRL tags (line 269-271): `['NetIncomeLoss', 'ProfitLoss', ...]`
  - Contains `sale` (sales revenue) - mapped from XBRL tags (line 230): `['Revenues', 'SalesRevenueNet', ...]`
  - Contains `ppegt` (property, plant, equipment gross) - mapped from XBRL tags (line 107): `['PropertyPlantAndEquipmentGross']`
  - Contains `ni` (net income) - mapped from XBRL tags (line 272-274): `['NetIncomeLoss', 'ProfitLoss', ...]`
  - Contains `sic` (SIC code) - added from static mapping file (line 1223-1283)
  - All required columns are present

- ✅ **AP_SignalMasterTable.parquet**: 
  - File exists (from `AP_SignalMasterTable.py`)
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` structure)
  - Contains `exchcd` (exchange code) - from `AP_SignalMasterTable.py` structure
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Calculates abnormal accruals using cross-sectional regressions by year and industry (SIC2)
- **AbnormalAccruals**: Residuals from regression: `tempAccruals ~ tempInvTA + tempDelRev + tempPPE` by `fyear` and `sic2`
- **AbnormalAccrualsPercent**: Abnormal accruals as percentage of net income = `AbnormalAccruals * l.at / abs(ni)`
- Uses cash flow from operations: `tempCFO = oancf` if available, otherwise `fopt - (act - l1_act) + (che - l1_che) + (lct - l1_lct) - (dlc - l1_dlc)`
- Calculates accruals: `tempAccruals = (ib - tempCFO) / l1_at`
- Winsorizes variables at 0.1% and 99.9% levels before regression
- Requires minimum 6 observations per year-industry group
- Excludes NASDAQ observations before 1982 (data quality issues)
- Expands annual observations to monthly with forward fill for 12 months

---

## 192. ZZ2_AnnouncementReturn.py

### Required Columns:
- **CCMLinkingTable.parquet**: `gvkey`, `permno`, `timeLinkStart_d`, `timeLinkEnd_d`
- **m_QCompustat.parquet**: `gvkey`, `rdq`
- **dailyCRSP.parquet**: `permno`, `time_d`, `ret`
- **dailyFF.parquet**: `time_d`, `mktrf`, `rf`

### AP File Status:
- ⚠️ **CCMLinkingTable.parquet**: **FILE EXISTS** (WRDS version)
  - File exists: `CCMLinkingTable.parquet` (from `CCMLinkingTable.py`)
  - Contains `gvkey`, `permno`, `timeLinkStart_d`, `timeLinkEnd_d`
  - **Note**: This is NOT an AP script - it downloads from WRDS
  - **Issue**: Uses real Compustat `gvkey` values, while AP Compustat data uses surrogate `gvkey` (CIK/permno)
  - **Can be used IF**: Using WRDS `m_QCompustat.parquet` (real gvkey) - linking table will match
  - **Cannot be used IF**: Using AP Compustat data (surrogate gvkey) - linking table won't match

- ✅ **AP_m_QCompustat.parquet**: 
  - File exists (from `AP_CompustatQuarterly.py`)
  - Contains `gvkey` (surrogate: CIK/permno) - from `AP_CompustatQuarterly.py` structure
  - Contains `rdq` (report date/filing date) - from `AP_CompustatQuarterly.py` line 550
  - All required columns are present

- ✅ **AP_dailyCRSP.parquet**: 
  - File exists (from `AP_CRSPDaily.py`)
  - Contains `permno`, `time_d` (from `AP_CRSPDaily.py` structure)
  - Contains `ret` (daily return) - from `AP_CRSPDaily.py` structure
  - All required columns are present

- ✅ **AP_dailyFF.parquet**: 
  - File exists (from `AP_FamaFrenchDaily.py`)
  - Contains `time_d` (from `AP_FamaFrenchDaily.py` structure)
  - Contains `mktrf` (market return minus risk-free rate) - calculated as `mkt - rf_val` (line 541)
  - Contains `rf` (risk-free rate) - calculated from 3-month T-bill rate (line 289-303)
  - All required columns are present

### Can Be Constructed?
**PARTIALLY** - `gvkey` mismatch issue:
- ⚠️ `CCMLinkingTable.parquet` exists but has `gvkey` mismatch (WRDS version uses real gvkey)
- ✅ All other required columns are present

### Additional Work Needed?
**YES** - `gvkey` mismatch:

#### What Needs to Be Done:
1. **Create AP version of CCMLinkingTable.parquet**: 
   - R script uses `CCMLinkingTable.parquet` to link CRSP `permno` to Compustat `gvkey`
   - **Solution**: Create `AP_CCMLinkingTable.parquet` that links `permno` to surrogate `gvkey` (CIK/permno)
   - **Note**: This would allow AP Compustat data to match AP linking table
   - **Alternative**: Use WRDS `m_QCompustat.parquet` (real gvkey) with existing `CCMLinkingTable.parquet`

#### Implementation Notes:
- Calculates earnings announcement returns using 3-day window around announcement dates
- **AnnouncementReturn**: Stock return minus market return (`ret - (mktrf + rf)`) summed over 3-day window
- Uses quarterly earnings announcement dates (`rdq`) from Compustat quarterly data
- Creates 3-day window: 2 business days before announcement, announcement day, 1 business day after
- Forward fills announcement returns up to 6 months for months without announcements
- Requires linking table to match CRSP `permno` to Compustat `gvkey` for announcement dates

---

## 193. ZZ2_BetaFP.py

### Required Columns:
- **dailyCRSP.parquet**: `permno`, `time_d`, `ret`
- **dailyFF.parquet**: `time_d`, `rf`, `mktrf`

### AP File Status:
- ✅ **AP_dailyCRSP.parquet**: 
  - File exists (from `AP_CRSPDaily.py`)
  - Contains `permno`, `time_d` (from `AP_CRSPDaily.py` structure)
  - Contains `ret` (daily return) - from `AP_CRSPDaily.py` structure
  - All required columns are present

- ✅ **AP_dailyFF.parquet**: 
  - File exists (from `AP_FamaFrenchDaily.py`)
  - Contains `time_d` (from `AP_FamaFrenchDaily.py` structure)
  - Contains `rf` (risk-free rate) - calculated from 3-month T-bill rate (line 289-303)
  - Contains `mktrf` (market return minus risk-free rate) - calculated as `mkt - rf_val` (line 541)
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Calculates Frazzini-Pedersen beta using R-squared from 3-day overlapping returns
- **BetaFP**: `sqrt(R²) * (sd252_LogRet / sd252_LogMkt)` where R² is from regressing 3-day stock returns on 3-day market returns
- Uses 252-day rolling volatility (minimum 120 observations)
- Uses 1260-day rolling R-squared calculation (minimum 500 observations)
- Calculates R² using correlation approach: `R² = corr² = (cov / (std_x * std_y))²`
- Converts to monthly frequency by keeping last observation per month
- Uses log returns for volatility and correlation calculations

---

## 194. ZZ2_betaVIX.py

### Required Columns:
- **dailyCRSP.parquet**: `permno`, `time_d`, `ret`
- **dailyFF.parquet**: `time_d`, `rf`, `mktrf`
- **d_vix.parquet**: `time_d`, `dVIX`

### AP File Status:
- ✅ **AP_dailyCRSP.parquet**: 
  - File exists (from `AP_CRSPDaily.py`)
  - Contains `permno`, `time_d` (from `AP_CRSPDaily.py` structure)
  - Contains `ret` (daily return) - from `AP_CRSPDaily.py` structure
  - All required columns are present

- ✅ **AP_dailyFF.parquet**: 
  - File exists (from `AP_FamaFrenchDaily.py`)
  - Contains `time_d` (from `AP_FamaFrenchDaily.py` structure)
  - Contains `rf` (risk-free rate) - calculated from 3-month T-bill rate (line 289-303)
  - Contains `mktrf` (market return minus risk-free rate) - calculated as `mkt - rf_val` (line 541)
  - All required columns are present

- ✅ **AP_d_vix.parquet**: 
  - File exists (from `AP_VIX.py`)
  - Contains `time_d` (from `AP_VIX.py` structure)
  - Contains `dVIX` (daily change in VIX) - calculated as `vix.diff()` (line 178)
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Calculates systematic volatility (betaVIX) from rolling regression of stock returns on market and VIX changes
- **betaVIX**: Coefficient on daily change in VIX from 1-month rolling regression (20-day window, minimum 15 observations)
- Regression: `ret_excess ~ mktrf + dVIX` with intercept
- Uses 20-day rolling window with minimum 15 observations
- Converts to monthly frequency by keeping last non-null observation per month
- Measures sensitivity of stock returns to changes in market volatility (VIX)

---

## 195. ZZ2_IdioVolAHT.py

### Required Columns:
- **dailyCRSP.parquet**: `permno`, `time_d`, `ret`
- **dailyFF.parquet**: `time_d`, `rf`, `mktrf`

### AP File Status:
- ✅ **AP_dailyCRSP.parquet**: 
  - File exists (from `AP_CRSPDaily.py`)
  - Contains `permno`, `time_d` (from `AP_CRSPDaily.py` structure)
  - Contains `ret` (daily return) - from `AP_CRSPDaily.py` structure
  - All required columns are present

- ✅ **AP_dailyFF.parquet**: 
  - File exists (from `AP_FamaFrenchDaily.py`)
  - Contains `time_d` (from `AP_FamaFrenchDaily.py` structure)
  - Contains `rf` (risk-free rate) - calculated from 3-month T-bill rate (line 289-303)
  - Contains `mktrf` (market return minus risk-free rate) - calculated as `mkt - rf_val` (line 541)
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Calculates idiosyncratic volatility from 252-day rolling CAPM regression residuals
- **IdioVolAHT**: Root mean squared error (RMSE) from CAPM regression: `ret_excess ~ mktrf` with intercept
- Uses 252-day rolling window (1 trading year) with minimum 100 observations
- Filters out missing returns before creating rolling windows
- Converts to monthly frequency by keeping last non-null observation per month
- Measures stock-specific volatility after removing market risk

---

## Summary

### Overall Status:
**4 out of 5 predictors can be constructed** ✅

### Column Availability:
- ✅ **ZZ2_AbnormalAccruals_AbnormalAccrualsPercent.py**: All required columns are present
- ⚠️ **ZZ2_AnnouncementReturn.py**: Partially constructible - `gvkey` mismatch for `CCMLinkingTable.parquet`
- ✅ **ZZ2_BetaFP.py**: All required columns are present
- ✅ **ZZ2_betaVIX.py**: All required columns are present
- ✅ **ZZ2_IdioVolAHT.py**: All required columns are present

### Key Notes:
1. **For ZZ2_AbnormalAccruals_AbnormalAccrualsPercent**: 
   - **VERIFICATION**: Test that `fyear`, `fopt`, `oancf`, `act`, `che`, `lct`, `dlc`, `ib`, `sale`, `ppegt`, `ni`, `sic` are correctly populated
   - Uses cross-sectional regressions by year and industry (SIC2) to calculate abnormal accruals
   - Calculates accruals using cash flow from operations
   - Winsorizes variables at 0.1% and 99.9% levels before regression
   - Requires minimum 6 observations per year-industry group

2. **For ZZ2_AnnouncementReturn**: 
   - **VERIFICATION**: Test that `rdq` is correctly populated in `AP_m_QCompustat.parquet`
   - Requires linking table to match CRSP `permno` to Compustat `gvkey` for announcement dates
   - **Critical Issue**: `CCMLinkingTable.parquet` has `gvkey` mismatch (WRDS version uses real gvkey)
   - **Solution**: Create `AP_CCMLinkingTable.parquet` that links `permno` to surrogate `gvkey` (CIK/permno)
   - Uses 3-day window around earnings announcement dates
   - Forward fills announcement returns up to 6 months

3. **For ZZ2_BetaFP**: 
   - **VERIFICATION**: Test that `ret`, `rf`, `mktrf` are correctly populated
   - Calculates Frazzini-Pedersen beta using R-squared from 3-day overlapping returns
   - Uses 252-day rolling volatility and 1260-day rolling R-squared
   - Converts to monthly frequency by keeping last observation per month

4. **For ZZ2_betaVIX**: 
   - **VERIFICATION**: Test that `ret`, `rf`, `mktrf`, `dVIX` are correctly populated
   - Calculates systematic volatility from rolling regression of stock returns on market and VIX changes
   - Uses 20-day rolling window with minimum 15 observations
   - Converts to monthly frequency by keeping last non-null observation per month

5. **For ZZ2_IdioVolAHT**: 
   - **VERIFICATION**: Test that `ret`, `rf`, `mktrf` are correctly populated
   - Calculates idiosyncratic volatility from 252-day rolling CAPM regression residuals
   - Uses 252-day rolling window (1 trading year) with minimum 100 observations
   - Converts to monthly frequency by keeping last non-null observation per month

6. **Note on gvkey Surrogate**: 
   - `gvkey` is not directly available from EDGAR (Compustat-specific identifier)
   - AP version uses `cik` (SEC identifier) as surrogate, falls back to `permno` if unavailable
   - For `ZZ2_AnnouncementReturn.py`, `gvkey` is used for merging with `CCMLinkingTable.parquet`
   - **Issue**: WRDS `CCMLinkingTable.parquet` uses real Compustat `gvkey`, won't match AP surrogate `gvkey`

7. **Note on File Names**: 
   - File name mismatches are ignored per user instructions (from Group 19 onwards)
   - Predictors will need to be modified to use AP file names (`AP_dailyCRSP.parquet`, `AP_monthlyCRSP.parquet`, etc.) or AP files can be renamed to match expected names

8. **Note on VIX Data**: 
   - **AP_d_vix.parquet**: Generated by `AP_VIX.py` from FRED API (free)
   - Contains `time_d`, `vix`, `dVIX` (daily VIX level and changes)
   - Uses blended VXO/VIX series for historical coverage (VXO before 2021-09-23, VIX after)

