# Group 38 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 38.

---

## 186. ZZ1_OrgCap_OrgCapNoAdj.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `sicCRSP`, `shrcd`, `exchcd`
- **m_aCompustat.parquet**: `permno`, `time_avail_m`, `xsga`, `at`, `datadate`, `sic`
- **GNPdefl.parquet**: `time_avail_m`, `gnpdefl`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - File exists (from `AP_SignalMasterTable.py`)
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` structure)
  - Contains `sicCRSP` (SIC code from CRSP) - from `AP_SignalMasterTable.py` structure
  - Contains `shrcd` (share class code) - from `AP_SignalMasterTable.py` structure
  - Contains `exchcd` (exchange code) - from `AP_SignalMasterTable.py` structure
  - All required columns are present

- ✅ **AP_m_aCompustat.parquet**: 
  - File exists (from `AP_CompustatAnnual.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `xsga` (selling, general, administrative expenses) - mapped from XBRL tags (line 239): `['SellingGeneralAndAdministrativeExpense', 'GeneralAndAdministrativeExpense']`
  - Contains `at` (total assets) - mapped from XBRL tags (line 84): `['Assets']`
  - Contains `datadate` (fiscal period end date) - renamed from `period_end` (line 934)
  - Contains `sic` (SIC code) - added from static mapping file (line 1223-1283)
  - All required columns are present

- ✅ **AP_GNPdefl.parquet**: 
  - File exists (from `AP_GNPDeflator.py`)
  - Contains `time_avail_m` (from `AP_GNPDeflator.py` structure)
  - Contains `gnpdefl` (GNP deflator) - calculated from FRED GNPCTPI series (line 178)
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Calculates organizational capital using SG&A expenses with depreciation
- **OrgCapNoAdj**: Raw organizational capital = recursive function of `xsga` scaled by `at`
- **OrgCap**: Industry-adjusted organizational capital (winsorized and standardized by Fama-French 17 industries)
- Uses recursive function: `y[t] = 0.85 * y[t-12] + xsga[t]` with initialization `y[:12] = 4.0 * xsga[:12]`
- Deflates `xsga` by GNP deflator before calculation
- Filters to December fiscal year ends only
- Excludes financial firms (SIC 6000-6999, except 6720-6730 and 6798)
- Uses Fama-French 17 industry classification for industry adjustment

---

## 187. ZZ1_ResidualMomentum6m_ResidualMomentum.py

### Required Columns:
- **monthlyCRSP.parquet**: `permno`, `time_avail_m`, `ret`
- **monthlyFF.parquet**: `time_avail_m`, `rf`, `mktrf`, `smb`, `hml`

### AP File Status:
- ✅ **AP_monthlyCRSP.parquet**: 
  - File exists (from `AP_CRSPMonthly.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` structure)
  - Contains `ret` (monthly return) - from `AP_CRSPMonthly.py` structure
  - All required columns are present

- ✅ **AP_monthlyFF.parquet**: 
  - File exists (from `AP_FamaFrenchMonthly.py`)
  - Contains `time_avail_m` (from `AP_FamaFrenchMonthly.py` structure)
  - Contains `rf` (risk-free rate) - from `AP_FamaFrenchMonthly.py` structure
  - Contains `mktrf` (market return minus risk-free rate) - from `AP_FamaFrenchMonthly.py` structure
  - Contains `smb` (small minus big) - from `AP_FamaFrenchMonthly.py` structure
  - Contains `hml` (high minus low) - from `AP_FamaFrenchMonthly.py` structure
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Calculates momentum predictors based on Fama-French 3-factor residuals
- **ResidualMomentum6m**: 6-month residual momentum (placebo) = `mean6(temp) / sd6(temp)` where `temp = l1._residuals`
- **ResidualMomentum**: 11-month residual momentum (predictor) = `mean11(temp) / sd11(temp)`
- Uses rolling 36-observation FF3 regressions: `excess_return = alpha + beta1*mktrf + beta2*smb + beta3*hml + residual`
- Requires minimum 36 observations for regression, minimum 6 observations for 6-month momentum, minimum 11 observations for 11-month momentum
- Uses position-based indexing (`time_temp`) for rolling windows

---

## 188. ZZ1_RIO_MB_RIO_Disp_RIO_Turnover_RIO_Volatility.py

### Required Columns:
- **IBES_EPS_Unadj.parquet**: `tickerIBES`, `time_avail_m`, `stdev`
- **TR_13F.parquet**: `permno`, `time_avail_m`, `instown_perc`
- **SignalMasterTable.parquet**: `permno`, `tickerIBES`, `time_avail_m`, `exchcd`, `mve_permco`, `mve_c`
- **m_aCompustat.parquet**: `permno`, `time_avail_m`, `at`, `ceq`, `txditc`
- **monthlyCRSP.parquet**: `permno`, `time_avail_m`, `vol`, `shrout`, `ret`

### AP File Status:
- ✅ **AP_IBES_EPS_Unadj.parquet**: 
  - File exists (from `AP_IBESEPSUnadjusted.py`)
  - Contains `tickerIBES`, `time_avail_m` (from `AP_IBESEPSUnadjusted.py` structure)
  - Contains `stdev` (standard deviation of EPS forecasts) - mapped from "Earnings Per Share - Standard Deviation" (line 255)
  - All required columns are present

- ✅ **AP_TR_13F.parquet**: 
  - File exists (from `AP_InstitutionalHoldings13F.py`)
  - Contains `permno`, `time_avail_m` (from `AP_InstitutionalHoldings13F.py` structure)
  - Contains `instown_perc` (institutional ownership percentage) - calculated from 13F holdings data (line 384, 532)
  - All required columns are present

- ✅ **AP_SignalMasterTable.parquet**: 
  - File exists (from `AP_SignalMasterTable.py`)
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` structure)
  - Contains `tickerIBES` (IBES ticker) - populated from `AP_IBESCRSPLinkingTable.parquet` if available (line 99-116)
  - Contains `exchcd` (exchange code) - from `AP_SignalMasterTable.py` structure
  - Contains `mve_permco` (market value of equity at permco level) - included in column list (line 128), comes from `AP_monthlyCRSP.parquet`
  - Contains `mve_c` (market value of equity at company level) - included in column list (line 128), comes from `AP_monthlyCRSP.parquet`
  - All required columns are present

- ✅ **AP_m_aCompustat.parquet**: 
  - File exists (from `AP_CompustatAnnual.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `at` (total assets) - mapped from XBRL tags (line 84): `['Assets']`
  - Contains `ceq` (common equity) - mapped from XBRL tags (line 190-191): `['StockholdersEquity', ...]`
  - Contains `txditc` (deferred taxes and investment tax credit) - mapped from XBRL tags (line 348): `['DeferredTaxAssetsLiabilitiesNet', 'DeferredIncomeTaxLiabilities', ...]`
  - All required columns are present

- ✅ **AP_monthlyCRSP.parquet**: 
  - File exists (from `AP_CRSPMonthly.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` structure)
  - Contains `vol` (trading volume) - from `AP_CRSPMonthly.py` structure
  - Contains `shrout` (shares outstanding) - line 348
  - Contains `ret` (monthly return) - from `AP_CRSPMonthly.py` structure
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Calculates Residual Institutional Ownership (RIO) predictors combining institutional ownership with firm characteristics
- **RIO**: Residual institutional ownership = `log(temp/(1-temp)) + 23.66 - 2.89*log(mve_c) + 0.08*(log(mve_c))^2` where `temp = instown_perc/100`
- **RIO_MB**: RIO for high market-to-book firms (top quintile)
- **RIO_Disp**: RIO for high forecast dispersion firms (top 2 quintiles)
- **RIO_Turnover**: RIO for high turnover firms (top quintile)
- **RIO_Volatility**: RIO for high volatility firms (top quintile)
- Filters out bottom size quintile (below 20th percentile NYSE/AMEX market cap)
- Uses 6-month calendar-based lag for RIO (`RIOlag = l6.RIO`)
- Creates quintiles based on characteristics within each time period
- Uses rolling 12-month standard deviation for volatility (minimum 6 observations)

---

## 189. ZZ1_RIVolSpread.py

### Required Columns:
- **bali_hovak_imp_vol.csv**: `secid`, `date`, `cp_flag`, `mean_imp_vol`
- **RealizedVol.csv**: `permno`, `yyyymm`, `RealizedVol`
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `secid`, `sicCRSP`

### AP File Status:
- ❌ **bali_hovak_imp_vol.csv**: **FILE DOES NOT EXIST** (proprietary data)
  - File would be generated by `PrepScripts/bali_hovak.R` (requires WRDS OptionMetrics access)
  - Would contain `secid`, `date`, `cp_flag`, `mean_imp_vol` (OptionMetrics implied volatility data)
  - **Note**: This is proprietary OptionMetrics data, not available from free sources
  - **Note**: Requires WRDS OptionMetrics database access

- ✅ **RealizedVol.csv**: **FILE GENERATED BY PREDICTOR**
  - File is generated by `ZZ0_RealizedVol_IdioVol3F_ReturnSkew3F.py` (Group 36, predictor 176)
  - Contains `permno`, `yyyymm`, `RealizedVol` (realized volatility from daily returns)
  - **Note**: `ZZ0_RealizedVol_IdioVol3F_ReturnSkew3F.py` can be constructed with AP data (confirmed in Group 36 analysis)
  - All required columns will be present if `RealizedVol.csv` is generated

- ⚠️ **AP_SignalMasterTable.parquet**: **MISSING COLUMN**
  - File exists (from `AP_SignalMasterTable.py`)
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` structure)
  - Contains `sicCRSP` (SIC code from CRSP) - from `AP_SignalMasterTable.py` structure
  - **Missing**: `secid` (OptionMetrics security identifier) - initialized to `np.nan` in `AP_SignalMasterTable.py` (line 130)
  - **Note**: `secid` is required to link to OptionMetrics data

### Can Be Constructed?
**NO** - Missing required data:
- ❌ `bali_hovak_imp_vol.csv` does not exist (proprietary OptionMetrics data)
- ❌ `secid` missing from `AP_SignalMasterTable.parquet`

### Additional Work Needed?
**YES** - Multiple issues:

#### What Needs to Be Done:
1. **Populate `secid` in AP_SignalMasterTable.parquet**: 
   - `secid` is OptionMetrics security identifier, required to link to OptionMetrics data
   - **Solution**: Create mapping from `permno` to `secid` using OptionMetrics database or historical mapping files
   - **Note**: OptionMetrics uses `secid` as primary identifier, CRSP uses `permno` - mapping is complex and time-varying
   - **Alternative**: If OptionMetrics data is not available, predictor cannot be constructed

2. **Obtain bali_hovak_imp_vol.csv**: 
   - OptionMetrics data is proprietary and requires WRDS subscription
   - **Solution**: Run `PrepScripts/bali_hovak.R` on WRDS to generate `bali_hovak_imp_vol.csv`
   - **Note**: This requires WRDS OptionMetrics database access (not available from free sources)
   - **Alternative**: If OptionMetrics data is not available, predictor cannot be constructed

#### Implementation Notes:
- Calculates realized minus implied volatility spread: `RIVolSpread = RealizedVol - impvol`
- Uses average implied volatility from call and put options (`impvol = (impvolC + impvolP) / 2`)
- Annualizes realized volatility: `RealizedVol = RealizedVol * sqrt(252)`
- Excludes closed-end funds (SIC 6720-6730) and REITs (SIC 6798)
- Requires `secid` to link OptionMetrics data to CRSP data
- **Note**: OptionMetrics data is proprietary and not available from free sources

---

## 190. ZZ1_zerotrade_zerotradeAlt1_zerotradeAlt12.py

### Required Columns:
- **dailyCRSP.parquet**: `permno`, `time_d`, `vol`, `shrout`

### AP File Status:
- ✅ **AP_dailyCRSP.parquet**: 
  - File exists (from `AP_CRSPDaily.py`)
  - Contains `permno`, `time_d` (from `AP_CRSPDaily.py` structure)
  - Contains `vol` (trading volume) - from `AP_CRSPDaily.py` structure
  - Contains `shrout` (shares outstanding) - from `AP_CRSPDaily.py` structure
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Calculates liquidity measures based on zero trading days following Liu (2006)
- **zerotrade1M**: 1-month zero trade measure = `(countzero + (1/turn)/480000) * (21/ndays)` lagged by 1 month
- **zerotrade6M**: 6-month zero trade measure = `(countzero6 + (1/Turn6)/11000) * (21*6/ndays6)` lagged by 1 month
- **zerotrade12M**: 12-month zero trade measure = `(countzero12 + (1/Turn12)/11000) * (21*12/ndays12)` lagged by 1 month
- Uses turnover-adjusted number of zero daily trading volumes
- Aggregates daily data to monthly by summing `countzero` and `turn`, counting `ndays`
- Uses different deflators: 480,000 for 1-month measure, 11,000 for 6-month and 12-month measures
- Calculates turnover as `turn = vol / shrout` (daily turnover)

---

## Summary

### Overall Status:
**4 out of 5 predictors can be constructed** ✅

### Column Availability:
- ✅ **ZZ1_OrgCap_OrgCapNoAdj.py**: All required columns are present
- ✅ **ZZ1_ResidualMomentum6m_ResidualMomentum.py**: All required columns are present
- ✅ **ZZ1_RIO_MB_RIO_Disp_RIO_Turnover_RIO_Volatility.py**: All required columns are present
- ❌ **ZZ1_RIVolSpread.py**: Cannot be constructed - missing `bali_hovak_imp_vol.csv` (proprietary OptionMetrics data) and `secid`
- ✅ **ZZ1_zerotrade_zerotradeAlt1_zerotradeAlt12.py**: All required columns are present

### Key Notes:
1. **For ZZ1_OrgCap_OrgCapNoAdj**: 
   - **VERIFICATION**: Test that `xsga`, `at`, `datadate`, `sic`, `sicCRSP`, `gnpdefl` are correctly populated
   - Calculates organizational capital using recursive function of SG&A expenses
   - Deflates SG&A by GNP deflator before calculation
   - Filters to December fiscal year ends only
   - Excludes financial firms

2. **For ZZ1_ResidualMomentum6m_ResidualMomentum**: 
   - **VERIFICATION**: Test that `ret`, `rf`, `mktrf`, `smb`, `hml` are correctly populated
   - Uses rolling 36-observation FF3 regressions to extract residuals
   - Calculates momentum signals from lagged residuals using rolling mean/std ratios
   - Requires minimum 36 observations for regression, minimum 6/11 observations for momentum signals

3. **For ZZ1_RIO_MB_RIO_Disp_RIO_Turnover_RIO_Volatility**: 
   - **VERIFICATION**: Test that `tickerIBES`, `stdev`, `instown_perc`, `mve_permco`, `mve_c`, `at`, `ceq`, `txditc`, `vol`, `shrout`, `ret` are correctly populated
   - Calculates Residual Institutional Ownership (RIO) combining institutional ownership with firm characteristics
   - Creates RIO predictors for high market-to-book, forecast dispersion, turnover, and volatility firms
   - Filters out bottom size quintile
   - Uses 6-month calendar-based lag for RIO

4. **For ZZ1_RIVolSpread**: 
   - **VERIFICATION**: Test that `secid` is correctly populated in `AP_SignalMasterTable.parquet`
   - Requires proprietary OptionMetrics data (`bali_hovak_imp_vol.csv`)
   - **Critical Issues**:
     - `secid` missing from `AP_SignalMasterTable.parquet` (initialized to `np.nan`)
     - `bali_hovak_imp_vol.csv` does not exist (proprietary OptionMetrics data)
   - **Note**: `RealizedVol.csv` can be generated by `ZZ0_RealizedVol_IdioVol3F_ReturnSkew3F.py` (Group 36), which can be constructed with AP data
   - **Note**: OptionMetrics data is proprietary and requires WRDS subscription

5. **For ZZ1_zerotrade_zerotradeAlt1_zerotradeAlt12**: 
   - **VERIFICATION**: Test that `vol`, `shrout` are correctly populated
   - Calculates liquidity measures based on zero trading days
   - Aggregates daily data to monthly by summing zero trade counts and turnover
   - Uses different deflators for 1-month vs 6-month/12-month measures

6. **Note on File Names**: 
   - File name mismatches are ignored per user instructions (from Group 19 onwards)
   - Predictors will need to be modified to use AP file names (`AP_dailyCRSP.parquet`, `AP_monthlyCRSP.parquet`, etc.) or AP files can be renamed to match expected names

7. **Note on Proprietary Data**: 
   - **bali_hovak_imp_vol.csv**: Requires proprietary OptionMetrics database access (WRDS subscription)
   - **secid**: OptionMetrics security identifier, requires mapping from `permno` to `secid`

8. **Note on RealizedVol.csv**: 
   - `RealizedVol.csv` is generated by `ZZ0_RealizedVol_IdioVol3F_ReturnSkew3F.py` (Group 36)
   - This predictor can be constructed with AP data (confirmed in Group 36 analysis)
   - If `RealizedVol.csv` is generated, it will contain the required columns for `ZZ1_RIVolSpread.py`

