# Group 24 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 24.

---

## 116. MomVol.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `ret`
- **monthlyCRSP.parquet**: `permno`, `time_avail_m`, `vol`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno` (from `AP_SignalMasterTable.py` line 124)
  - Contains `time_avail_m` (from `AP_SignalMasterTable.py` line 126)
  - Contains `ret` (return) - from `AP_SignalMasterTable.py` line 133
- ✅ **AP_monthlyCRSP.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` structure)
  - Contains `vol` (volume) - from `AP_CRSPMonthly.py` line 321-322

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates momentum decile rank for high volume (tercile 3) stocks only
- Calculates 6-month momentum using lags 1-5
- Calculates 6-month calendar-based rolling mean volume
- Creates momentum deciles and volume terciles within each month
- Sets `MomVol` to momentum decile only for high volume stocks (tercile 3)
- Filters out observations with obs_num < 24

---

## 117. MRreversal.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `ret`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno` (from `AP_SignalMasterTable.py` line 124)
  - Contains `time_avail_m` (from `AP_SignalMasterTable.py` line 126)
  - Contains `ret` (return) - from `AP_SignalMasterTable.py` line 133

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates medium-run reversal: stock return between months t-18 and t-13
- Compounds monthly returns over months t-18 to t-13 (6 months)
- Uses calendar-based lag approach (not position-based)
- Filters out observations where all lag values were originally missing

---

## 118. MS.py

### Required Columns:
- **m_aCompustat.parquet**: `permno`, `gvkey`, `time_avail_m`, `datadate`, `at`, `ceq`, `ni`, `oancf`, `fopt`, `wcapch`, `ib`, `dp`, `xrd`, `capx`, `xad`, `revt`
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `mve_permco`, `sicCRSP`
- **m_QCompustat.parquet**: `gvkey`, `time_avail_m`, `niq`, `atq`, `saleq`, `oancfy`, `capxy`, `xrdq`, `fyearq`, `fqtr`, `datafqtr`, `datadateq`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `datadate` (period end date) - renamed from `period_end` in `AP_CompustatAnnual.py` line 854
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 84
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184
  - Contains `ni` (net income) - mapped in XBRL_TAG_MAP line 262-264
  - Contains `oancf` (operating cash flow) - mapped in XBRL_TAG_MAP line 280-282
  - Contains `fopt` (foreign operations income) - mapped in XBRL_TAG_MAP line 291
  - Contains `wcapch` (working capital change) - derived in `AP_CompustatAnnual.py` line 1014
  - Contains `ib` (income before extraordinary items) - mapped in XBRL_TAG_MAP line 262-264
  - Contains `dp` (depreciation) - mapped in XBRL_TAG_MAP line 245-247
  - Contains `xrd` (R&D expenses) - mapped in XBRL_TAG_MAP line 250-252
  - Contains `capx` (capital expenditures) - mapped in XBRL_TAG_MAP line 321-323
  - Contains `xad` (advertising expense) - mapped in XBRL_TAG_MAP line 233-234
  - Contains `revt` (total revenue) - mapped in XBRL_TAG_MAP line 232
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `mve_permco` (market value of equity per company) - from `AP_SignalMasterTable.py` line 128
  - Contains `sicCRSP` (SIC code from CRSP) - from `AP_SignalMasterTable.py` line 134
- ✅ **AP_m_QCompustat.parquet**: 
  - Contains `gvkey` (surrogate: CIK/permno) - from `AP_CompustatQuarterly.py` structure
  - Contains `time_avail_m` (from `AP_CompustatQuarterly.py` structure)
  - Contains `niq` (net income quarterly) - mapped in XBRL_TAG_MAP line 200-201
  - Contains `atq` (total assets quarterly) - mapped in XBRL_TAG_MAP line 70
  - Contains `saleq` (sales quarterly) - mapped in XBRL_TAG_MAP line 169
  - Contains `oancfy` (operating cash flow YTD) - mapped in XBRL_TAG_MAP line 216-217
  - Contains `capxy` (capital expenditures YTD) - mapped in XBRL_TAG_MAP line 228
  - Contains `xrdq` (R&D expenses quarterly) - mapped in XBRL_TAG_MAP line 178
  - Contains `fyearq` (fiscal year of quarter) - from `AP_CompustatQuarterly.py` line 551
  - Contains `fqtr` (fiscal quarter 1-4) - from `AP_CompustatQuarterly.py` line 552
  - Contains `datafqtr` (data fiscal quarter) - from `AP_CompustatQuarterly.py` line 554
  - Contains `datadateq` (fiscal period end date) - renamed from `datadate` in `AP_CompustatQuarterly.py` line 909-910

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates Mohanram G-score: composite accounting score using 8 financial strength indicators vs industry medians
- Limits sample to firms in lowest BM quintile (growth firms)
- Requires at least 3 firms in sic2D-time_avail_m combination
- Calculates 8 binary indicators (m1-m8) comparing firm metrics to industry medians
- Sums indicators to get tempMS (0-8 scale)
- Applies timing logic and forward-fills within permno groups
- Applies upper and lower bounds (1-6 scale)

---

## 119. NetDebtFinance.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `dlcch`, `dltis`, `dltr`, `at`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `dlcch` (change in current debt) - mapped in XBRL_TAG_MAP line 154
  - Contains `dltis` (long-term debt issuance) - mapped in XBRL_TAG_MAP line 155
  - Contains `dltr` (long-term debt reduction) - mapped in XBRL_TAG_MAP line 156
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 84

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates net debt financing activity scaled by average total assets
- Formula: `NetDebtFinance = (dltis - dltr + dlcch) / (0.5 * (at + l12_at))`
- Uses 12-month lag of total assets
- Sets to missing if `abs(NetDebtFinance) > 1` (outlier filter)
- **Note**: `gvkey` is loaded but not used in calculations (only kept for compatibility)

---

## 120. NetDebtPrice.py

### Required Columns:
- **m_aCompustat.parquet**: `permno`, `time_avail_m`, `at`, `dltt`, `dlc`, `pstk`, `dvpa`, `tstkp`, `che`, `sic`, `ib`, `csho`, `ceq`, `prcc_f`
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `mve_permco`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 84
  - Contains `dltt` (long-term debt) - mapped in XBRL_TAG_MAP line 142-144
  - Contains `dlc` (debt current) - mapped in XBRL_TAG_MAP line 136-139
  - Contains `pstk` (preferred stock) - mapped in XBRL_TAG_MAP line 189
  - Contains `dvpa` (preferred dividends) - mapped in XBRL_TAG_MAP line 343
  - Contains `tstkp` (treasury stock) - mapped in XBRL_TAG_MAP line 206
  - Contains `che` (cash) - mapped in XBRL_TAG_MAP line 88
  - ✅ **`sic`**: **ADDED** - SIC code has been added to `AP_m_aCompustat.parquet`
  - Contains `ib` (income before extraordinary items) - mapped in XBRL_TAG_MAP line 262-264
  - Contains `csho` (shares outstanding) - mapped in XBRL_TAG_MAP line 215-216
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184
  - ⚠️ **`prcc_f`**: **NOT EXTRACTED** - Fiscal year-end stock price is mapped in XBRL_TAG_MAP line 367 but not extracted from XBRL statements (market data rarely in 10-K filings)
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `mve_permco` (market value of equity per company) - from `AP_SignalMasterTable.py` line 128

### Can Be Constructed?
**PARTIALLY** - Missing `prcc_f` (fiscal year-end stock price).

### Additional Work Needed?
**YES** - Need to verify `prcc_f` availability or supplement with alternative data source.

#### What Needs to Be Done:
1. **Verify prcc_f extraction**: Check if `prcc_f` is actually extracted from XBRL data in `AP_CompustatAnnual.py`
   - XBRL comment notes: "These are rarely in 10-K XBRL - use yfinance or real-time data vendor" (line 366)
   - May need to supplement with yfinance data for fiscal year-end prices
   - Or use `prcc_c` (current price) as fallback if available

2. **Alternative Data Source**: If `prcc_f` is not available from XBRL:
   - Use yfinance to get stock price at fiscal year-end date (`datadate`)
   - Merge price data into `AP_m_aCompustat.parquet` based on `permno` and `datadate`
   - Or modify `AP_CompustatAnnual.py` to fetch fiscal year-end prices from yfinance

#### Implementation Notes:
- Predictor calculates net debt to price: net debt (debt + preferred stock - cash) divided by market value
- Formula: `NetDebtPrice = ((dltt + dlc + pstk + dvpa - tstkp) - che) / mve_permco`
- Excludes financial firms (SIC 6000-6999)
- Excludes bottom 2 BM quintiles (keeps only quintiles 3, 4, 5)
- Sets to missing if key variables (`at`, `ib`, `csho`, `ceq`, `prcc_f`) are missing
- **Note**: `prcc_f` is used for filtering (checking if missing), but the actual calculation uses `mve_permco` from SignalMasterTable, so `prcc_f` may not be strictly required for the calculation itself, but is required for the filter logic

---

## Summary

### Overall Status:
**4 OUT OF 5 PREDICTORS CAN BE CONSTRUCTED** - One predictor (`NetDebtPrice.py`) requires `prcc_f` which may not be extracted from XBRL.

### Column Availability:
- ✅ **SignalMasterTable columns**: `permno`, `time_avail_m`, `ret`, `mve_permco`, `sicCRSP` - All available
- ✅ **Monthly CRSP columns**: `permno`, `time_avail_m`, `vol` - All available
- ✅ **Compustat annual columns**: `gvkey` (surrogate), `permno`, `time_avail_m`, `datadate`, `at`, `ceq`, `ni`, `oancf`, `fopt`, `wcapch`, `ib`, `dp`, `xrd`, `capx`, `xad`, `revt`, `dlcch`, `dltis`, `dltr`, `dltt`, `dlc`, `pstk`, `dvpa`, `tstkp`, `che`, `sic` (added), `csho` - All available
- ✅ **Compustat quarterly columns**: `gvkey` (surrogate), `time_avail_m`, `niq`, `atq`, `saleq`, `oancfy`, `capxy`, `xrdq`, `fyearq`, `fqtr`, `datafqtr`, `datadateq` - All available
- ⚠️ **`prcc_f`**: **NOT EXTRACTED** - Fiscal year-end stock price is mapped but not extracted from XBRL statements

### Additional Notes:
1. **For MomVol**: 
   - **VERIFICATION**: Test that `ret` and `vol` are correctly populated
   - Verify sufficient historical coverage for 6-month momentum and rolling mean volume calculations

2. **For MRreversal**: 
   - **VERIFICATION**: Test that `ret` is correctly populated
   - Verify sufficient historical coverage for 18-month lag calculation

3. **For MS**: 
   - **VERIFICATION**: Test that all annual and quarterly Compustat columns are correctly populated
   - Verify sufficient historical coverage for 48-month rolling volatility calculations
   - Note: Predictor requires complex calculations with multiple rolling windows and industry medians

4. **For NetDebtFinance**: 
   - **VERIFICATION**: Test that `dlcch`, `dltis`, `dltr`, `at` are correctly populated
   - Verify sufficient historical coverage for 12-month lag calculation

5. **For NetDebtPrice**: 
   - **CRITICAL**: Verify `prcc_f` availability or supplement with yfinance data
   - **VERIFICATION**: Test that all other columns are correctly populated
   - Note: `prcc_f` is used in filter logic (checking if missing), but calculation uses `mve_permco` from SignalMasterTable
   - **Recommended**: Enhance `AP_CompustatAnnual.py` to fetch fiscal year-end prices from yfinance when not available in XBRL

6. **Note on gvkey Surrogate**: 
   - `gvkey` is not directly available from EDGAR (Compustat-specific identifier)
   - AP version uses `cik` (SEC identifier) as surrogate, falls back to `permno` if unavailable
   - For `NetDebtFinance.py`, `gvkey` is loaded but not used in calculations (only kept for compatibility)
   - For `MS.py`, `gvkey` is used for merging with quarterly Compustat data, surrogate approach is acceptable

7. **Note on prcc_f**: 
   - Fiscal year-end stock price (`prcc_f`) is mapped in XBRL_TAG_MAP but rarely available in 10-K XBRL filings
   - Market data is typically not included in financial statement XBRL files
   - **Recommended**: Supplement with yfinance data using `datadate` and `permno` to get fiscal year-end prices
   - For `NetDebtPrice.py`, `prcc_f` is used in filter logic but calculation uses `mve_permco`, so may not be strictly required if filter logic is adjusted

