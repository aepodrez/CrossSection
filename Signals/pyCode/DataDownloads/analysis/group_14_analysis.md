# Group 14 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 14.

---

## 66. EarnSupBig.py

### Required Columns:
- **AP_SignalMasterTable.parquet**: `permno`, `gvkey`, `time_avail_m`, `mve_c`, `sicCRSP`
- **AP_m_QCompustat.parquet**: `gvkey`, `time_avail_m`, `epspxq`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` line 119)
  - Contains `mve_c` (market value of equity) - from `AP_SignalMasterTable.py` line 127
  - Contains `sicCRSP` (SIC code from CRSP) - from `AP_SignalMasterTable.py` line 134
- ✅ **AP_m_QCompustat.parquet**: 
  - Contains `gvkey` (surrogate: CIK/permno) - from `AP_CompustatQuarterly.py` structure
  - Contains `time_avail_m` (from `AP_CompustatQuarterly.py` structure)
  - Contains `epspxq` (earnings per share quarterly) - mapped in XBRL_TAG_MAP line 205

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 67. EntMult.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `dltt`, `dlc`, `dc`, `che`, `oibdp`, `ceq`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `mve_permco`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `dltt` (long-term debt) - mapped in XBRL_TAG_MAP line 140-142
  - Contains `dlc` (debt current) - mapped in XBRL_TAG_MAP line 136-139
  - ⚠️ **`dc`**: **NOT DIRECTLY MAPPED** - Listed as derived field but not implemented, zero-filled if missing (from `AP_CompustatAnnual.py` line 1007)
  - Contains `che` (cash) - mapped in XBRL_TAG_MAP line 88
  - Contains `oibdp` (operating income before depreciation) - mapped in XBRL_TAG_MAP line 256
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `mve_permco` (market value per company) - from `AP_SignalMasterTable.py` line 128

### Can Be Constructed?
**YES** - All required columns are present. `dc` is zero-filled, which may affect accuracy but won't prevent construction.

### Additional Work Needed?
**OPTIONAL** - Consider implementing derivation logic for `dc` (deferred charges) from `dcpstk`, `pstk`, and `dcvt` as done in original `CompustatAnnual.py` for better accuracy.

---

## 68. EP.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `ib`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `mve_permco`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `ib` (income before extraordinary items) - mapped in XBRL_TAG_MAP line 262-264
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `mve_permco` (market value per company) - from `AP_SignalMasterTable.py` line 128

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 69. EquityDuration.py

### Required Columns:
- **AP_a_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `fyear`, `datadate`, `ceq`, `ib`, `sale`, `prcc_f`, `csho`

### AP File Status:
- ✅ **AP_a_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `fyear` (fiscal year) - extracted from `period_end.year` and renamed from `fiscal_year` (from `AP_CompustatAnnual.py` line 758, 928)
  - Contains `datadate` (data date) - renamed from `period_end` (from `AP_CompustatAnnual.py` line 927)
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184
  - Contains `ib` (income before extraordinary items) - mapped in XBRL_TAG_MAP line 262-264
  - Contains `sale` (sales) - mapped in XBRL_TAG_MAP line 238-240
  - ❌ **`prcc_f`**: **NOT EXTRACTED** - Market data is rarely in 10-K XBRL filings (from `AP_CompustatAnnual.py` line 359-360). `prcc_f` is mapped in `OTHER_FIELDS` but is not extracted from balance sheet, income statement, or cash flow statements. Needs supplementation with yfinance or other data source.
  - Contains `csho` (common shares outstanding) - mapped in XBRL_TAG_MAP line 205

### Can Be Constructed?
**NO** - `prcc_f` (fiscal year-end stock price) is not extracted from XBRL data.

### Additional Work Needed?
**YES** - Need to verify `prcc_f` availability or supplement with alternative data source.

#### What Needs to Be Done:
1. **Verify prcc_f extraction**: Check if `prcc_f` is actually extracted from XBRL data in `AP_CompustatAnnual.py`
   - XBRL comment notes: "These are rarely in 10-K XBRL - use yfinance or real-time data vendor" (line 359)
   - May need to supplement with yfinance data for fiscal year-end prices
   - Or use `prcc_c` (current price) as fallback if available

2. **Alternative Data Source**: If `prcc_f` is not available from XBRL:
   - Use yfinance to get stock price at fiscal year-end date (`datadate`)
   - Merge price data into `AP_a_aCompustat.parquet` based on `permno` and `datadate`
   - Or modify `AP_CompustatAnnual.py` to fetch fiscal year-end prices from yfinance

#### Implementation Notes:
- The predictor calculates equity duration using cash flow projections and discount rates
- Requires fiscal year-end stock price (`prcc_f`) to calculate market equity (`tempME = prcc_f * csho`)
- If `prcc_f` is missing, predictor will fail or produce incorrect results
- **Recommended**: Enhance `AP_CompustatAnnual.py` to fetch fiscal year-end prices from yfinance when not available in XBRL

---

## 70. ExchSwitch.py

### Required Columns:
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `exchcd`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `exchcd` (exchange code) - from `AP_SignalMasterTable.py` line 130

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## Summary

| Predictor | All Columns Available? | Can Be Constructed? | Additional Work Needed? |
|-----------|----------------------|---------------------|----------------------|
| EarnSupBig | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |
| EntMult | ✅ Yes* | ✅ Yes | Optional: Implement dc derivation |
| EP | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |
| EquityDuration | ❌ No | ❌ No | **Extract/supplement prcc_f** |
| ExchSwitch | ✅ Yes | ✅ Yes | None |

### Critical Missing Components:
1. **`prcc_f` (fiscal year-end stock price) in AP_a_aCompustat.parquet**: Required for `EquityDuration.py`. **NOT EXTRACTED** from XBRL data (market data is rarely in 10-K filings). Needs supplementation with yfinance data.

### Recommendations:
1. **For EarnSupBig**: 
   - **VERIFICATION**: Test that `epspxq` is correctly populated in `AP_m_QCompustat.parquet`
   - Verify sufficient historical coverage for 24-month rolling windows (predictor uses lags up to 24 months)
   - Verify `mve_c` and `sicCRSP` are correctly populated in `AP_SignalMasterTable.parquet`

2. **For EntMult**: 
   - **VERIFICATION**: Test that all required columns (`dltt`, `dlc`, `che`, `oibdp`, `ceq`) are correctly populated
   - **OPTIONAL**: Consider implementing derivation logic for `dc` (deferred charges) from `dcpstk`, `pstk`, and `dcvt` for better accuracy
   - Note: `dc` is currently zero-filled, which may affect enterprise value calculation slightly

3. **For EP**: 
   - **VERIFICATION**: Test that `ib` is correctly populated in `AP_m_aCompustat.parquet`
   - Verify `mve_permco` is correctly populated in `AP_SignalMasterTable.parquet`
   - Verify sufficient historical coverage for 6-month lagged market value calculation

4. **For EquityDuration**: 
   - **CRITICAL**: `prcc_f` is **NOT EXTRACTED** from XBRL data - needs supplementation
   - **Required Actions**:
     - Enhance `AP_CompustatAnnual.py` to fetch fiscal year-end prices from yfinance
     - Use `datadate` (fiscal year-end date) to get correct price at fiscal year-end
     - Merge price data based on `permno` and `datadate`
     - Add extraction logic after XBRL extraction to fetch prices from yfinance for missing `prcc_f` values
   - **VERIFICATION**: Test that all other required columns (`ceq`, `ib`, `sale`, `csho`, `fyear`, `datadate`) are correctly populated
   - Verify sufficient historical coverage for multi-year projections (predictor projects 10 years forward)

5. **For ExchSwitch**: 
   - **VERIFICATION**: Test that `exchcd` is correctly populated in `AP_SignalMasterTable.parquet`
   - Verify exchange codes are correctly mapped (NYSE=1, AMEX=2, NASDAQ=3)
   - Verify sufficient historical coverage for 12-month lag operations

6. **Note on gvkey Surrogate**: 
   - Most predictors use `gvkey` which is not directly available from EDGAR
   - AP version uses `cik` (SEC identifier) as surrogate or falls back to `permno`
   - This should work for most predictors, but verify linking logic if issues arise

7. **Note on dc (Deferred Charges)**: 
   - `dc` is not directly mapped in XBRL but is zero-filled in `AP_CompustatAnnual.py`
   - For `EntMult.py`, this may slightly affect enterprise value calculation
   - Consider implementing derivation logic from `dcpstk`, `pstk`, and `dcvt` if accuracy is critical

8. **Note on prcc_f (Fiscal Year-End Stock Price)**: 
   - Market data is rarely in 10-K XBRL filings
   - `AP_CompustatAnnual.py` maps `prcc_f` in `OTHER_FIELDS` but does NOT extract it from XBRL statements
   - Extraction only happens for balance sheet, income statement, and cash flow statement fields
   - For `EquityDuration.py`, this is critical - **MUST** supplement with yfinance
   - **Implementation**: Add post-processing step in `AP_CompustatAnnual.py` to fetch fiscal year-end prices from yfinance using `datadate` and `permno`
   - Alternative: Use `prcc_c` (current price) if available, but fiscal year-end price is preferred for accuracy

