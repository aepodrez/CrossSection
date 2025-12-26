# Group 36 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 36.

---

## 176. ZZ0_RealizedVol_IdioVol3F_ReturnSkew3F.py

### Required Columns:
- **dailyCRSP.parquet**: `permno`, `time_d`, `ret`
- **dailyFF.parquet**: `time_d`, `rf`, `mktrf`, `smb`, `hml`

### AP File Status:
- ✅ **AP_dailyCRSP.parquet**: 
  - File exists (from `AP_CRSPDaily.py`)
  - Contains `permno`, `time_d` (from `AP_CRSPDaily.py` structure)
  - Contains `ret` (daily returns) - from `AP_CRSPDaily.py` structure
  - All required columns are present

- ✅ **AP_dailyFF.parquet**: 
  - File exists (from `AP_FamaFrenchDaily.py`)
  - Contains `time_d` (from `AP_FamaFrenchDaily.py` structure)
  - Contains `rf` (risk-free rate) - calculated from 3-month T-bill rate (line 289-303)
  - Contains `mktrf` (market return minus risk-free rate) - calculated as `mkt - rf_val` (line 541)
  - Contains `smb` (small minus big) - calculated from size-sorted portfolios (line 361, 387)
  - Contains `hml` (high minus low) - calculated from value-sorted portfolios (line 364, 388)
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor calculates three volatility/skewness measures:
  - **RealizedVol**: Standard deviation of daily excess returns (total volatility)
  - **IdioVol3F**: Standard deviation of FF3 residuals (idiosyncratic volatility)
  - **ReturnSkew3F**: Skewness of FF3 residuals (idiosyncratic skewness)
- Runs Fama-French 3-factor regression for each permno-month: `excess_return = alpha + beta1*mktrf + beta2*smb + beta3*hml + residual`
- Requires minimum 15 daily observations per month
- Uses `polars_ols` for efficient regression calculations

---

## 177. ZZ1_Activism1_Activism2.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `ticker`, `exchcd`
- **TR_13F.parquet**: `permno`, `time_avail_m`, `maxinstown_perc`
- **monthlyCRSP.parquet**: `permno`, `time_avail_m`, `shrcls`
- **GovIndex.parquet**: `ticker`, `time_avail_m`, `G`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - File exists (from `AP_SignalMasterTable.py`)
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` structure)
  - Contains `ticker` (stock ticker) - from `AP_SignalMasterTable.py` structure
  - Contains `exchcd` (exchange code) - from `AP_SignalMasterTable.py` structure
  - All required columns are present

- ❌ **AP_TR_13F.parquet**: **MISSING COLUMN**
  - File exists (from `AP_InstitutionalHoldings13F.py`)
  - Contains `permno`, `time_avail_m` (from `AP_InstitutionalHoldings13F.py` structure)
  - Contains `numinstown`, `dbreadth`, `instown_perc` (line 532)
  - **Missing**: `maxinstown_perc` (maximum institutional ownership percentage) - NOT generated in `AP_InstitutionalHoldings13F.py`
  - **Note**: WRDS version (`TR_13F.parquet`) includes `maxinstown_perc`, but AP version does not

- ✅ **AP_monthlyCRSP.parquet**: 
  - File exists (from `AP_CRSPMonthly.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` structure)
  - Contains `shrcls` (share class) - extracted from ticker suffix (line 357, 454)
  - All required columns are present

- ❌ **GovIndex.parquet**: **FILE DOES NOT EXIST**
  - Script exists: `GovernanceIndex.py` (generates `GovIndex.parquet`)
  - Would contain `ticker`, `time_avail_m`, `G` (governance index score)
  - **Note**: This is NOT an AP script - it downloads from WRDS or uses proprietary data
  - **Note**: Requires proprietary governance index dataset from Gompers-Ishii-Metrick (2003)

### Can Be Constructed?
**NO** - Missing required data:
- ❌ `maxinstown_perc` missing from `AP_TR_13F.parquet`
- ❌ `GovIndex.parquet` does not exist (proprietary governance index data)

### Additional Work Needed?
**YES** - Multiple issues:

#### What Needs to Be Done:
1. **Add `maxinstown_perc` to AP_TR_13F.parquet**: 
   - `maxinstown_perc` is the maximum institutional ownership percentage across all institutional owners for a given stock
   - Currently, `AP_InstitutionalHoldings13F.py` only calculates `instown_perc` (aggregate institutional ownership percentage)
   - **Solution**: Modify `AP_InstitutionalHoldings13F.py` to calculate `maxinstown_perc` by finding the maximum ownership percentage across all institutional managers for each stock-quarter
   - **Note**: This requires aggregating individual manager holdings before calculating the maximum

2. **Obtain or Create GovIndex.parquet**: 
   - `GovIndex.parquet` contains governance index scores (G-index) from Gompers-Ishii-Metrick (2003)
   - This is a proprietary dataset that requires WRDS access or manual construction
   - **Alternative**: Could potentially construct from SEC proxy filings, but would require significant text parsing and manual coding
   - **Note**: `GovernanceIndex.py` exists but downloads from WRDS (not an AP script)

#### Implementation Notes:
- **Activism1**: External governance among large blockheld firms (top quartile of blockholder firms)
- **Activism2**: Blockholdings among high external governance firms (external governance >= 19)
- Both predictors exclude dual-class shares (`shrcls != ""`)
- **Activism1** uses `maxinstown_perc` to identify large blockholders (>5% threshold)
- **Activism2** uses `maxinstown_perc` to measure blockholdings among high external governance firms
- External governance = 24 - G (higher values = better external governance)

---

## 178. ZZ1_AnalystValue_AOP_PredictedFE_IntrinsicValue.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `tickerIBES`, `time_avail_m`, `prc`
- **IBES_EPS_Unadj.parquet**: `tickerIBES`, `time_avail_m`, `fpi`, `statpers`, `fpedats`, `meanest`
- **monthlyCRSP.parquet**: `permno`, `time_avail_m`, `shrout`
- **m_aCompustat.parquet**: `permno`, `time_avail_m`, `ceq`, `ib`, `ibcom`, `ni`, `sale`, `datadate`, `dvc`, `at`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - File exists (from `AP_SignalMasterTable.py`)
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` structure)
  - Contains `tickerIBES` (IBES ticker) - populated from `AP_IBESCRSPLinkingTable.parquet` if available (line 99-116)
  - Contains `prc` (stock price) - comes from `AP_monthlyCRSP.parquet`
  - All required columns are present

- ✅ **AP_IBES_EPS_Unadj.parquet**: 
  - File exists (from `AP_IBESEPSUnadjusted.py`)
  - Contains `tickerIBES`, `time_avail_m` (from `AP_IBESEPSUnadjusted.py` structure)
  - Contains `fpi` (forecast period indicator) - from `AP_IBESEPSUnadjusted.py` structure
  - Contains `statpers` (statement period date) - mapped from "Date" (line 261)
  - Contains `fpedats` (forecast period end date) - mapped from "Period End Date" (line 262)
  - Contains `meanest` (mean EPS estimate) - mapped from "Earnings Per Share - Mean Estimate" (line 254)
  - All required columns are present

- ✅ **AP_monthlyCRSP.parquet**: 
  - File exists (from `AP_CRSPMonthly.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` structure)
  - Contains `shrout` (shares outstanding in millions) - line 348
  - All required columns are present

- ✅ **AP_m_aCompustat.parquet**: 
  - File exists (from `AP_CompustatAnnual.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `ceq` (common equity) - mapped from XBRL tags (line 190-191): `['StockholdersEquity', ...]`
  - Contains `ib` (income before extraordinary items) - mapped from XBRL tags (line 269-271): `['NetIncomeLoss', 'ProfitLoss', ...]`
  - Contains `ibcom` (income available to common stockholders) - mapped from XBRL tags (line 275-276): `['NetIncomeLossAvailableToCommonStockholdersBasic', ...]`
  - Contains `ni` (net income) - mapped from XBRL tags (line 272-274): `['NetIncomeLoss', 'ProfitLoss', ...]`
  - Contains `sale` (sales revenue) - mapped from XBRL tags (line 230): `['Revenues', 'SalesRevenueNet', ...]`
  - Contains `datadate` (fiscal period end date) - renamed from `period_end` (line 934)
  - Contains `dvc` (dividends common stock) - mapped from XBRL tags (line 336): `['DividendsCommonStock', 'PaymentsOfDividendsCommonStock', ...]`
  - Contains `at` (total assets) - mapped from XBRL tags (line 84): `['Assets']`
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor calculates multiple equity valuation measures:
  - **AnalystValue**: Multi-stage equity valuation using analyst forecasts (3-stage model)
  - **AOP**: Analyst optimism as difference between AnalystValue and IntrinsicValue
  - **PredictedFE**: Predicted forecast error from cross-sectional regression
  - **IntrinsicValue**: Intrinsic value using historical ROE (placebo, 2-stage model)
- Uses IBES forecasts for 1-year ahead (`fpi=1`), 2-year ahead (`fpi=2`), and long-term growth (`fpi=0`)
- Filters for May statement periods and June observations
- Requires complete forecast data (`feps1` and `feps2` must be non-null)
- Uses 12% discount rate (`r=0.12`) per Frankel and Lee (1998)
- Expands annual observations to 12 monthly observations (holds for one year)

---

## 179. ZZ1_EBM_BPEBM.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `che`, `dltt`, `dlc`, `dc`, `dvpa`, `tstkp`, `ceq`
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `mve_permco`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - File exists (from `AP_CompustatAnnual.py`)
  - Contains `gvkey` (surrogate: CIK/permno) - from `AP_CompustatAnnual.py` structure
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `che` (cash) - mapped from XBRL tags (line 90): `['CashAndCashEquivalentsAtCarryingValue', 'CashAndDueFromBanks', 'Cash']`
  - Contains `dltt` (long-term debt) - mapped from XBRL tags (line 149-151): `['LongTermDebtNoncurrent', 'LongTermDebt', ...]`
  - Contains `dlc` (debt current) - mapped from XBRL tags (line 143-146): `['DebtCurrent', 'ShortTermBorrowings', ...]`
  - Contains `dc` (deferred charges) - derived from `dcpstk`, `pstk`, `dcvt` (line 382, 948-963)
  - Contains `dvpa` (dividends per share) - mapped from XBRL tags (line 343): `['CommonStockDividendsPerShareDeclared', 'CommonStockDividendsPerShareCashPaid']`
  - Contains `tstkp` (treasury stock value) - mapped from XBRL tags (line 206): `['TreasuryStockValue', 'TreasuryStockCommonValue', 'TreasuryStockAtCost']`
  - Contains `ceq` (common equity) - mapped from XBRL tags (line 190-191): `['StockholdersEquity', ...]`
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
- **EBM**: Enterprise book-to-market ratio = `(ceq + temp) / (mve_permco + temp)` where `temp = che - dltt - dlc - dc - dvpa + tstkp`
- **BPEBM**: Book-to-price minus enterprise book-to-market = `BP - EBM` where `BP = (ceq + tstkp - dvpa) / mve_permco`
- Uses enterprise value approach (adjusts for cash, debt, preferred stock, treasury stock)
- **Note**: `dc` is derived from `dcpstk`, `pstk`, and `dcvt` (line 948-963 in `AP_CompustatAnnual.py`)
- **Note**: `gvkey` is loaded but not used in calculations (only kept for compatibility)

---

## 180. ZZ1_FR_FRbook.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `gvkey`, `time_avail_m`, `shrcd`, `mve_permco`
- **CompustatPensions.parquet**: `gvkey`, `year`, `pbnaa`, `pplao`, `pplau`, `pbnvv`, `pbpro`, `pbpru`
- **m_aCompustat.parquet**: `gvkey`, `time_avail_m`, `at`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - File exists (from `AP_SignalMasterTable.py`)
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` structure)
  - Contains `gvkey` (surrogate: CIK/permno) - line 119, filled from `permno` if missing
  - Contains `shrcd` (share class code) - from `AP_SignalMasterTable.py` structure
  - Contains `mve_permco` (market value of equity at permco level) - included in column list (line 128), comes from `AP_monthlyCRSP.parquet`
  - All required columns are present

- ❌ **CompustatPensions.parquet**: **FILE DOES NOT EXIST YET**
  - Script exists: `CompustatPensions.py` (generates `CompustatPensions.parquet`)
  - Would contain `gvkey`, `year`, `pbnaa`, `pplao`, `pplau`, `pbnvv`, `pbpro`, `pbpru` (pension plan data)
  - **Note**: This is NOT an AP script - it downloads from WRDS Compustat pension fund database
  - **Note**: Requires WRDS Compustat pension fund database access (proprietary)

- ✅ **AP_m_aCompustat.parquet**: 
  - File exists (from `AP_CompustatAnnual.py`)
  - Contains `gvkey` (surrogate: CIK/permno) - from `AP_CompustatAnnual.py` structure
  - Contains `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `at` (total assets) - mapped from XBRL tags (line 84): `['Assets']`
  - All required columns are present

### Can Be Constructed?
**NO** - Missing required data:
- ❌ `CompustatPensions.parquet` does not exist (proprietary Compustat pension fund data)

### Additional Work Needed?
**YES** - Missing file:

#### What Needs to Be Done:
1. **Create CompustatPensions.parquet**: 
   - Run `CompustatPensions.py` to generate `CompustatPensions.parquet`
   - Requires WRDS Compustat pension fund database access (proprietary)
   - **Alternative**: Could potentially extract pension data from SEC 10-K filings (pension footnotes), but would require significant text parsing and manual extraction
   - **Note**: Pension data includes:
     - `pbnaa`: Plan assets at net asset value (1980-1986)
     - `pplao`: Plan assets at fair value - overfunded plans (1987-1997)
     - `pplau`: Plan assets at fair value - underfunded plans (1987-1997)
     - `pplao`: Plan assets at fair value (1998+)
     - `pbnvv`: Projected benefit obligation at net present value (1980-1986)
     - `pbpro`: Projected benefit obligation - overfunded plans (1987-1997)
     - `pbpru`: Projected benefit obligation - underfunded plans (1987-1997)
     - `pbpro`: Projected benefit obligation (1998+)

#### Implementation Notes:
- **FR**: Funding ratio scaled by market value = `(FVPA - PBO) / mve_permco`
- **FRbook**: Funding ratio scaled by book assets = `(FVPA - PBO) / at`
- Uses time-varying pension data fields based on year (different fields for 1980-1986, 1987-1997, 1998+)
- Fair Value of Plan Assets (FVPA) and Projected Benefit Obligation (PBO) are calculated from different fields depending on year
- Excludes non-standard share classes (`shrcd > 11`)
- **Note**: `gvkey` is used for merging, surrogate approach is acceptable IF `CompustatPensions.parquet` uses the same surrogate

---

## Summary

### Overall Status:
**3 out of 5 predictors can be constructed** ✅

### Column Availability:
- ✅ **ZZ0_RealizedVol_IdioVol3F_ReturnSkew3F.py**: All required columns are present
- ❌ **ZZ1_Activism1_Activism2.py**: Missing `maxinstown_perc` from `AP_TR_13F.parquet`; missing `GovIndex.parquet` (proprietary)
- ✅ **ZZ1_AnalystValue_AOP_PredictedFE_IntrinsicValue.py**: All required columns are present
- ✅ **ZZ1_EBM_BPEBM.py**: All required columns are present
- ❌ **ZZ1_FR_FRbook.py**: Missing `CompustatPensions.parquet` (proprietary Compustat pension fund data)

### Key Notes:
1. **For ZZ0_RealizedVol_IdioVol3F_ReturnSkew3F**: 
   - **VERIFICATION**: Test that `ret`, `rf`, `mktrf`, `smb`, `hml` are correctly populated
   - Calculates three volatility/skewness measures from daily returns
   - Uses Fama-French 3-factor regression to extract idiosyncratic returns
   - Requires minimum 15 daily observations per month

2. **For ZZ1_Activism1_Activism2**: 
   - **VERIFICATION**: Test that `maxinstown_perc` is correctly calculated in `AP_TR_13F.parquet`
   - Requires `maxinstown_perc` (maximum institutional ownership percentage) - currently missing
   - Requires `GovIndex.parquet` (governance index scores) - proprietary dataset
   - **Note**: `AP_InstitutionalHoldings13F.py` needs to be modified to calculate `maxinstown_perc` from individual manager holdings

3. **For ZZ1_AnalystValue_AOP_PredictedFE_IntrinsicValue**: 
   - **VERIFICATION**: Test that `tickerIBES`, `feps1`, `feps2`, `LTG`, `ceq`, `ibcom`, `dvc`, `at` are correctly populated
   - Complex multi-stage equity valuation using analyst forecasts
   - Requires complete IBES forecast data (1-year, 2-year, long-term growth)
   - Filters for May statement periods and June observations
   - Expands annual observations to 12 monthly observations

4. **For ZZ1_EBM_BPEBM**: 
   - **VERIFICATION**: Test that `che`, `dltt`, `dlc`, `dc`, `dvpa`, `tstkp`, `ceq`, and `mve_permco` are correctly populated
   - Calculates enterprise book-to-market ratio and difference from book-to-price
   - Uses enterprise value approach (adjusts for cash, debt, preferred stock, treasury stock)
   - **Note**: `dc` is derived from `dcpstk`, `pstk`, and `dcvt`

5. **For ZZ1_FR_FRbook**: 
   - **VERIFICATION**: Test that `gvkey` (surrogate) matches between `AP_SignalMasterTable.parquet` and `CompustatPensions.parquet` (if created)
   - Requires `CompustatPensions.parquet` (pension plan asset/obligation data) - proprietary dataset
   - Uses time-varying pension data fields based on year
   - **Note**: `CompustatPensions.parquet` would need to use surrogate `gvkey` (CIK/permno) to match AP Compustat data

6. **Note on gvkey Surrogate**: 
   - `gvkey` is not directly available from EDGAR (Compustat-specific identifier)
   - AP version uses `cik` (SEC identifier) as surrogate, falls back to `permno` if unavailable
   - For `ZZ1_FR_FRbook.py`, `gvkey` is used for merging, surrogate approach is acceptable IF `CompustatPensions.parquet` uses the same surrogate

7. **Note on File Names**: 
   - File name mismatches are ignored per user instructions (from Group 19 onwards)
   - Predictors will need to be modified to use AP file names (`AP_dailyCRSP.parquet`, `AP_monthlyCRSP.parquet`, etc.) or AP files can be renamed to match expected names

8. **Note on Proprietary Data**: 
   - **GovIndex.parquet**: Requires proprietary governance index dataset from Gompers-Ishii-Metrick (2003)
   - **CompustatPensions.parquet**: Requires WRDS Compustat pension fund database access (proprietary)
   - Both datasets could potentially be constructed from SEC filings, but would require significant text parsing and manual extraction

