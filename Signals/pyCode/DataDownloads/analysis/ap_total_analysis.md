# Group 1 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 1.

---

## 1. ZZ2_PriceDelaySlope_PriceDelayRsq_PriceDelayTstat.py

### Required Columns:
- **AP_dailyCRSP.parquet**: `permno`, `time_d`, `ret`
- **AP_dailyFF.parquet**: `time_d`, `mktrf`, `rf`

### AP File Status:
- ✅ **AP_dailyCRSP.parquet**: Contains `permno`, `time_d`, `ret` (from `AP_CRSPDaily.py` lines 282-283)
- ✅ **AP_dailyFF.parquet**: Contains `time_d`, `mktrf`, `rf` (from `AP_FamaFrenchDaily.py` lines 538-546)

### Can Be Constructed?
**YES** - All required columns are present in the AP files.

### Additional Work Needed?
None. The AP versions have all required columns.

---

## 2. Accruals.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `txp`, `act`, `che`, `lct`, `dlc`, `at`, `dp`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `txp` (taxes payable) - mapped in XBRL_TAG_MAP line 156-157
  - Contains `act` (current assets) - mapped in XBRL_TAG_MAP line 90
  - Contains `che` (cash) - mapped in XBRL_TAG_MAP line 88
  - Contains `lct` (current liabilities) - mapped in XBRL_TAG_MAP line 133
  - Contains `dlc` (debt current) - mapped in XBRL_TAG_MAP line 136-139
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 86
  - Contains `dp` (depreciation) - mapped in XBRL_TAG_MAP line 245-247
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` lines 64-66)

### Can Be Constructed?
**YES** - All required columns are present, though `gvkey` is a surrogate (CIK/permno).

### Additional Work Needed?
None. The predictor doesn't actually use `gvkey` in calculations (it's only kept for compatibility), so the surrogate is acceptable.

---

## 3. AccrualsBM.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `ceq`, `act`, `che`, `lct`, `dlc`, `txp`, `at`
- **AP_SignalMasterTable.parquet**: `mve_permco`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains all required columns (same as Accruals.py above)
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184
- ✅ **AP_SignalMasterTable.parquet**: Contains `mve_permco` (from `AP_SignalMasterTable.py` line 105)
- ⚠️ **`gvkey`**: Same surrogate issue as Accruals.py (not used in calculations)

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 4. AdExp.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `permno`, `time_avail_m`, `xad`
- **AP_SignalMasterTable.parquet**: `mve_permco`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m`
  - Contains `xad` (advertising expense) - mapped in XBRL_TAG_MAP line 233-234
- ✅ **AP_SignalMasterTable.parquet**: Contains `mve_permco` (from `AP_SignalMasterTable.py` line 105)

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 5. AgeIPO.py

### Required Columns:
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`
- **IPODates.parquet**: `permno`, `IPOdate`, `FoundingYear`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 100-103)
- ❌ **IPODates.parquet**: **NO AP VERSION EXISTS**

### Can Be Constructed?
**PARTIALLY** - Missing AP version of IPODates.

### Additional Work Needed?
**YES** - Need to create `AP_IPODates.parquet` or `AP_IPODates.csv`.

#### What Needs to Be Done:
1. **Create AP_IPODates.py script** in `DataDownloads/` directory
2. **Data Source**: The original `IPODates.py` downloads from Ritter's website (https://site.warrington.ufl.edu/ritter/files/IPO-age.xlsx)
3. **Required Columns**: `permno`, `IPOdate`, `FoundingYear`
4. **Linking Challenge**: The original script uses CRSP permno. For AP version, you'll need to:
   - Download the same Excel file from Ritter's website
   - Map tickers/CUSIPs to AP permnos using the `AP_ticker_to_permno.csv` mapping
   - Or use the same permno if the mapping is consistent

#### Implementation Notes:
- The original `IPODates.py` (lines 27-91) downloads from Ritter's website
- It extracts `permno`, `FoundingYear`, and `IPOdate` columns
- For AP version, you may need to add ticker-to-permno mapping logic similar to other AP scripts
- The file should be saved as `AP_IPODates.parquet` in `../pyData/Intermediate/`

---

## Summary

| Predictor | All Columns Available? | Can Be Constructed? | Additional Work Needed? |
|-----------|----------------------|---------------------|----------------------|
| ZZ2_PriceDelaySlope_PriceDelayRsq_PriceDelayTstat | ✅ Yes | ✅ Yes | None |
| Accruals | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |
| AccrualsBM | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |
| AdExp | ✅ Yes | ✅ Yes | None |
| AgeIPO | ❌ No | ❌ No | **Create AP_IPODates.py** |

### Critical Missing Component:
- **AP_IPODates.parquet**: This file is required for `AgeIPO.py` but does not exist. An AP version needs to be created.

### Recommendations:
1. **Immediate**: Create `AP_IPODates.py` script to generate `AP_IPODates.parquet`
2. **Verification**: Test that all AP files actually contain the columns listed above by loading sample data
3. **Documentation**: Note that `gvkey` in AP files is a surrogate (CIK/permno) rather than true Compustat gvkey

# Group 2 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 2.

---

## 6. AM.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `permno`, `time_avail_m`, `at`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `mve_permco`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 86
- ✅ **AP_SignalMasterTable.parquet**: Contains `mve_permco` (from `AP_SignalMasterTable.py` line 105)

### Can Be Constructed?
**YES** - All required columns are present in the AP files.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 7. AnalystRevision.py

### Required Columns:
- **AP_IBES_EPS_Unadj.parquet**: `tickerIBES`, `time_avail_m`, `meanest`, `fpi`
- **AP_SignalMasterTable.parquet**: `permno`, `tickerIBES`, `time_avail_m`

### AP File Status:
- ✅ **AP_IBES_EPS_Unadj.parquet**: 
  - Contains `tickerIBES` (from `AP_IBESEPSUnadjusted.py` line 253)
  - Contains `time_avail_m` (from `AP_IBESEPSUnadjusted.py` line 277-278)
  - Contains `meanest` (from `AP_IBESEPSUnadjusted.py` line 254)
  - Contains `fpi` (forecast period indicator) - present in output columns (line 295)
- ⚠️ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 100-103)
  - Contains `tickerIBES` column BUT it's set to empty string `""` (from `AP_SignalMasterTable.py` line 93)

### Can Be Constructed?
**PARTIALLY** - The `tickerIBES` column exists in AP_SignalMasterTable but is empty. The predictor needs this column populated to merge with IBES data.

### Additional Work Needed?
**YES** - Need to populate `tickerIBES` in `AP_SignalMasterTable.parquet`.

#### What Needs to Be Done:
1. **Modify AP_SignalMasterTable.py**: Currently sets `tickerIBES = ""` on line 93
2. **Merge from AP_IBESCRSPLinkingTable**: The linking table already exists (`AP_IBESCRSPLink.py` creates `AP_IBESCRSPLinkingTable.parquet`)

#### Implementation Notes:
- ✅ **AP_IBESCRSPLink.py exists** and creates `AP_IBESCRSPLinkingTable.parquet` with columns: `tickerIBES`, `permno`, `time_avail_m`, `score`
- **Modify AP_SignalMasterTable.py** line 93 to:
  1. Load `AP_IBESCRSPLinkingTable.parquet` if it exists
  2. Merge: `df = df.merge(ibes_link[['permno', 'time_avail_m', 'tickerIBES']], on=['permno', 'time_avail_m'], how='left')`
  3. Fill missing: `df['tickerIBES'] = df['tickerIBES'].fillna('')`
- **Execution Order**: Ensure `AP_IBESCRSPLink.py` runs before `AP_SignalMasterTable.py` in the pipeline

---

## 8. AP_BetaTailRisk.py

### Required Columns:
- **AP_dailyCRSP.parquet**: `permno`, `time_d`, `ret`
- **AP_monthlyCRSP.parquet**: `permno`, `time_avail_m`, `ret`, `shrcd`

### AP File Status:
- ✅ **AP_dailyCRSP.parquet**: Contains `permno`, `time_d`, `ret` (from `AP_CRSPDaily.py` lines 282-283)
- ✅ **AP_monthlyCRSP.parquet**: 
  - Contains `permno`, `time_avail_m`, `ret` (from `AP_CRSPMonthly.py` lines 476-480)
  - Contains `shrcd` (share code) - from `AP_CRSPMonthly.py` line 353

### Can Be Constructed?
**YES** - All required columns are present. This predictor is specifically designed for AP data.

### Additional Work Needed?
None. This predictor already uses AP files by design.

---

## 9. AssetGrowth.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `at`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 86
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` lines 64-66)

### Can Be Constructed?
**YES** - All required columns are present, though `gvkey` is a surrogate (CIK/permno).

### Additional Work Needed?
None. The predictor doesn't actually use `gvkey` in calculations (it's only kept for compatibility), so the surrogate is acceptable.

---

## 10. Beta.py

### Required Columns:
- **AP_monthlyCRSP.parquet**: `permno`, `time_avail_m`, `ret`
- **AP_monthlyFF.parquet**: `time_avail_m`, `rf`
- **AP_monthlyMarket.parquet**: `time_avail_m`, `ewretd`

### AP File Status:
- ✅ **AP_monthlyCRSP.parquet**: Contains `permno`, `time_avail_m`, `ret` (from `AP_CRSPMonthly.py` lines 476-480)
- ✅ **AP_monthlyFF.parquet**: 
  - Contains `time_avail_m` (from `AP_FamaFrenchMonthly.py` line 490)
  - Contains `rf` (risk-free rate) - from `AP_FamaFrenchMonthly.py` line 494
- ✅ **AP_monthlyMarket.parquet**: 
  - Contains `time_avail_m` (from `AP_MarketReturns.py` line 233)
  - Contains `ewretd` (equal-weighted return) - from `AP_MarketReturns.py` line 233

### Can Be Constructed?
**YES** - All required columns are present in the AP files.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## Summary

| Predictor | All Columns Available? | Can Be Constructed? | Additional Work Needed? |
|-----------|----------------------|---------------------|----------------------|
| AM | ✅ Yes | ✅ Yes | None |
| AnalystRevision | ⚠️ Partial | ❌ No | **Populate tickerIBES in AP_SignalMasterTable** |
| AP_BetaTailRisk | ✅ Yes | ✅ Yes | None (designed for AP) |
| AssetGrowth | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |
| Beta | ✅ Yes | ✅ Yes | None |

### Critical Missing Component:
- **tickerIBES in AP_SignalMasterTable**: The column exists but is empty. Needs to be populated from IBES linking table.

### Recommendations:
1. **Immediate**: 
   - Check if `AP_IBESCRSPLink.py` exists and creates `AP_IBESCRSPLink.parquet`
   - Modify `AP_SignalMasterTable.py` to merge IBES linking data instead of setting `tickerIBES = ""`
   - The merge should populate `tickerIBES` from the linking table

2. **Verification**: 
   - Test that all AP files actually contain the columns listed above by loading sample data
   - Verify that `AP_IBES_EPS_Unadj.parquet` has sufficient coverage for your universe

3. **Documentation**: 
   - Note that `gvkey` in AP files is a surrogate (CIK/permno) rather than true Compustat gvkey
   - Document that `tickerIBES` requires IBES linking table to be populated

### Implementation Details for AnalystRevision:
The `AP_SignalMasterTable.py` currently sets `tickerIBES = ""` on line 93. To fix this:

**Solution**: Modify `AP_SignalMasterTable.py` to merge from `AP_IBESCRSPLinkingTable.parquet`:

```python
# After line 90 (after merging comp data)
# Load IBES linking table if it exists
ibes_link_path = Path("../pyData/Intermediate/AP_IBESCRSPLinkingTable.parquet")
if ibes_link_path.exists():
    ibes_link = pd.read_parquet(ibes_link_path)
    df = df.merge(ibes_link[['permno', 'time_avail_m', 'tickerIBES']], 
                  on=['permno', 'time_avail_m'], how='left')
    df['tickerIBES'] = df['tickerIBES'].fillna('')
else:
    df['tickerIBES'] = ''  # Fallback if linking table doesn't exist
```

**Execution Order**: Ensure `AP_IBESCRSPLink.py` runs before `AP_SignalMasterTable.py` in your pipeline.


# Group 3 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 3.

---

## 11. BetaLiquidityPS.py

### Required Columns:
- **AP_monthlyCRSP.parquet**: `permno`, `time_avail_m`, `ret`
- **AP_monthlyFF.parquet**: `time_avail_m`, `rf`, `mktrf`, `hml`, `smb`
- **AP_monthlyLiquidity.parquet**: `time_avail_m`, `ps_innov`

### AP File Status:
- ✅ **AP_monthlyCRSP.parquet**: Contains `permno`, `time_avail_m`, `ret` (from `AP_CRSPMonthly.py` lines 476-480)
- ✅ **AP_monthlyFF.parquet**: 
  - Contains `time_avail_m` (from `AP_FamaFrenchMonthly.py` line 490)
  - Contains `rf`, `mktrf`, `hml`, `smb` (from `AP_FamaFrenchMonthly.py` lines 491-494)
- ❌ **AP_monthlyLiquidity.parquet**: **NO AP VERSION EXISTS**

### Can Be Constructed?
**NO** - Missing AP version of monthlyLiquidity.

### Additional Work Needed?
**YES** - Need to create `AP_monthlyLiquidity.parquet` or `AP_monthlyLiquidity.csv`.

#### What Needs to Be Done:
1. **Create AP_LiquidityFactor.py script** in `DataDownloads/` directory
2. **Data Source**: The original `LiquidityFactor.py` downloads from WRDS `ff.liq_ps` table
3. **Required Columns**: `time_avail_m`, `ps_innov` (Pastor-Stambaugh liquidity innovation)
4. **Alternative Sources**: 
   - Ken French's website (http://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html) may have liquidity factors
   - Or compute from CRSP data using Pastor-Stambaugh methodology (complex)

#### Implementation Notes:
- The original `LiquidityFactor.py` downloads from WRDS `ff.liq_ps` table
- Pastor-Stambaugh liquidity factor requires sophisticated calculation from daily returns
- For AP version, you may need to:
  - Download from Ken French's website if available
  - Or implement Pastor-Stambaugh calculation (requires daily CRSP data and complex methodology)
  - The file should be saved as `AP_monthlyLiquidity.parquet` in `../pyData/Intermediate/`

---

## 12. BetaTailRisk.py

### Required Columns:
- **AP_dailyCRSP.parquet**: `permno`, `time_d`, `ret`
- **AP_monthlyCRSP.parquet**: `permno`, `time_avail_m`, `ret`, `shrcd`

### AP File Status:
- ✅ **AP_dailyCRSP.parquet**: Contains `permno`, `time_d`, `ret` (from `AP_CRSPDaily.py` lines 282-283)
- ✅ **AP_monthlyCRSP.parquet**: 
  - Contains `permno`, `time_avail_m`, `ret` (from `AP_CRSPMonthly.py` lines 476-480)
  - Contains `shrcd` (share code) - from `AP_CRSPMonthly.py` line 353

### Can Be Constructed?
**YES** - All required columns are present. Note: This is similar to `AP_BetaTailRisk.py` which already exists and uses AP files.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 13. BidAskSpread.py

### Required Columns:
- **corwin_schultz_spread.csv**: Pre-computed bid-ask spreads from `../pyData/Prep/corwin_schultz_spread.csv`
  - Columns: `PERMNO` (or `permno`), `month` (YYYYMM format), `hlspread`

### AP File Status:
- ❌ **corwin_schultz_spread.csv**: **NO AP VERSION EXISTS** in Prep folder
- ⚠️ **Note**: This is a pre-computed file from SAS code (`Corwin_Schultz_Edit.sas`)

### Can Be Constructed?
**NO** - Missing AP version of Corwin-Schultz spread data.

### Additional Work Needed?
**YES** - Need to create `AP_corwin_schultz_spread.csv` or implement Corwin-Schultz calculation.

#### What Needs to Be Done:
1. **Create AP_CorwinSchultz.py script** in `DataDownloads/` directory
2. **Data Source**: Requires daily high/low prices from CRSP
3. **Required Columns**: `permno`, `month` (YYYYMM), `hlspread`
4. **Methodology**: Corwin-Schultz (2012) bid-ask spread estimator using high-low prices

#### Implementation Notes:
- The original uses SAS code (`Corwin_Schultz_Edit.sas`) to compute spreads
- Corwin-Schultz estimator requires:
  - Daily high prices
  - Daily low prices
  - Two-day rolling windows
- For AP version:
  - **yfinance provides High/Low**: The `hist` DataFrame from yfinance includes 'High' and 'Low' columns
  - **Currently NOT saved**: `AP_CRSPDaily.py` doesn't save High/Low (only saves Close, Volume, etc.)
  - **Solution**: Enhance `AP_CRSPDaily.py` to include High/Low columns, then implement Corwin-Schultz calculation
  - The file should be saved as `AP_corwin_schultz_spread.csv` in `../pyData/Prep/`

#### Required Changes:
1. **Enhance AP_CRSPDaily.py**: Add `high` and `low` columns to saved data (yfinance already provides these)
2. **Create AP_CorwinSchultz.py**: Implement Corwin-Schultz calculation using High/Low from `AP_dailyCRSP.parquet`

---

## 14. BM.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `permno`, `time_avail_m`, `datadate`, `ceqt`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `mve_permco`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `datadate` (renamed from `period_end` in `AP_CompustatAnnual.py` line 854)
  - Contains `ceqt` (derived from `ceq - pstk - mib` in `AP_CompustatAnnual.py` lines 895-901)
- ✅ **AP_SignalMasterTable.parquet**: Contains `mve_permco` (from `AP_SignalMasterTable.py` line 105)

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 15. BMdec.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `permno`, `time_avail_m`, `txditc`, `seq`, `ceq`, `at`, `lt`, `pstk`, `pstkrv`, `pstkl`
- **AP_monthlyCRSP.parquet**: `permno`, `time_avail_m`, `prc`, `shrout`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m`
  - Contains `txditc` (deferred tax) - mapped in XBRL_TAG_MAP line 341-343
  - Contains `seq` (stockholders equity) - mapped in XBRL_TAG_MAP line 185-186
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 86
  - Contains `lt` (total liabilities) - mapped in XBRL_TAG_MAP line 130
  - Contains `pstk` (preferred stock) - mapped in XBRL_TAG_MAP line 189
  - ⚠️ **`pstkrv`** (preferred stock redemption value) - marked as "not mappable" in XBRL_TAG_MAP line 192
  - Contains `pstkl` (preferred stock liquidation) - mapped in XBRL_TAG_MAP line 191
- ✅ **AP_monthlyCRSP.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` lines 476-480)
  - Contains `prc` (price) - from `AP_CRSPMonthly.py` line 349
  - Contains `shrout` (shares outstanding) - from `AP_CRSPMonthly.py` line 348

### Can Be Constructed?
**PARTIALLY** - Missing `pstkrv` column, but the predictor has fallback logic.

### Additional Work Needed?
**MINIMAL** - The predictor has fallback logic that handles missing `pstkrv`.

#### Implementation Notes:
- The predictor uses `pstkrv` as a fallback for `pstk` (line 90-91)
- If `pstkrv` is missing, it falls back to `pstkl` (line 91)
- Since `pstkrv` is marked as "not mappable" in XBRL, it will always be missing in AP data
- This is acceptable because the predictor has proper fallback logic:
  ```python
  df["tempPS"] = df["pstk"].copy()
  df["tempPS"] = df["tempPS"].fillna(df["pstkrv"])  # Will be NaN
  df["tempPS"] = df["tempPS"].fillna(df["pstkl"])    # Will use this
  ```

#### Can Be Constructed?
**YES** - Despite missing `pstkrv`, the fallback logic makes this workable.

---

## Summary

| Predictor | All Columns Available? | Can Be Constructed? | Additional Work Needed? |
|-----------|----------------------|---------------------|----------------------|
| BetaLiquidityPS | ❌ No | ❌ No | **Create AP_monthlyLiquidity.py** |
| BetaTailRisk | ✅ Yes | ✅ Yes | None |
| BidAskSpread | ❌ No | ❌ No | **Create AP_CorwinSchultz.py** |
| BM | ✅ Yes | ✅ Yes | None |
| BMdec | ⚠️ Partial* | ✅ Yes | None (*pstkrv missing but has fallback) |

### Critical Missing Components:
1. **AP_monthlyLiquidity.parquet**: Required for `BetaLiquidityPS.py`
   - Needs Pastor-Stambaugh liquidity factor (`ps_innov`)
   - May be available from Ken French's website
   - Or requires complex calculation from daily returns

2. **AP_corwin_schultz_spread.csv**: Required for `BidAskSpread.py`
   - Needs Corwin-Schultz bid-ask spread calculation
   - Requires daily high/low prices
   - Check if `AP_dailyCRSP.parquet` has high/low columns

### Recommendations:
1. **Immediate**: 
   - Check if `AP_dailyCRSP.parquet` has high/low price columns for Corwin-Schultz
   - Check Ken French's website for downloadable liquidity factors

2. **For BetaLiquidityPS**:
   - Option 1: Download from Ken French's website (if available)
   - Option 2: Implement Pastor-Stambaugh calculation (complex, requires daily data)

3. **For BidAskSpread**:
   - Verify `AP_dailyCRSP.parquet` has high/low prices
   - If yes: Implement Corwin-Schultz calculation in Python
   - If no: Enhance `AP_CRSPDaily.py` to include high/low prices from yfinance

4. **Verification**: 
   - Test that all AP files actually contain the columns listed above by loading sample data
   - Verify `ceqt` and `datadate` are correctly populated in `AP_m_aCompustat.parquet`

# Group 4 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 4.

---

## 16. BookLeverage.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `permno`, `time_avail_m`, `at`, `lt`, `txditc`, `pstk`, `pstkrv`, `pstkl`, `seq`, `ceq`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 86
  - Contains `lt` (total liabilities) - mapped in XBRL_TAG_MAP line 130
  - Contains `txditc` (deferred tax) - mapped in XBRL_TAG_MAP line 341-343
  - Contains `pstk` (preferred stock) - mapped in XBRL_TAG_MAP line 189
  - ⚠️ **`pstkrv`** (preferred stock redemption value) - marked as "not mappable" in XBRL_TAG_MAP line 192
  - Contains `pstkl` (preferred stock liquidation) - mapped in XBRL_TAG_MAP line 191
  - Contains `seq` (stockholders equity) - mapped in XBRL_TAG_MAP line 185-186
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184

### Can Be Constructed?
**YES** - All required columns are present, though `pstkrv` will be missing (has fallback logic).

### Additional Work Needed?
None. The predictor has fallback logic that handles missing `pstkrv`:
```python
df["tempPS"] = df["pstk"].copy()
df["tempPS"] = df["tempPS"].fillna(df["pstkrv"])  # Will be NaN
df["tempPS"] = df["tempPS"].fillna(df["pstkl"])    # Will use this
```

---

## 17. BrandInvest.py

### Required Columns:
- **AP_a_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `fyear`, `datadate`, `xad`, `xad0`, `at`, `sic`

### AP File Status:
- ✅ **AP_a_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` lines 64-66)
  - Contains `fyear` (fiscal year) - renamed from `fiscal_year` in `AP_CompustatAnnual.py` line 855
  - Contains `datadate` (period end date) - renamed from `period_end` in `AP_CompustatAnnual.py` line 854
  - Contains `xad` (advertising expense) - mapped in XBRL_TAG_MAP line 233-234
  - Contains `xad0` (zero-filled advertising) - created in `AP_CompustatAnnual.py` line 931
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 86
  - ✅ **`sic`**: **ADDED** - SIC code has been added to `AP_a_aCompustat.parquet`

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate, sic has been added).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 18. Cash.py

### Required Columns:
- **AP_m_QCompustat.parquet**: `gvkey`, `rdq`, `cheq`, `atq`
- **AP_SignalMasterTable.parquet**: `permno`, `gvkey`, `time_avail_m`

### AP File Status:
- ✅ **AP_m_QCompustat.parquet**: 
  - Contains `gvkey` (surrogate: CIK/permno) - from `AP_CompustatQuarterly.py` structure
  - Contains `rdq` (report date/filing date) - from `AP_CompustatQuarterly.py` line 550
  - Contains `cheq` (cash quarterly) - mapped in XBRL_TAG_MAP (quarterly version)
  - Contains `atq` (total assets quarterly) - mapped in XBRL_TAG_MAP (quarterly version)
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 100-103)
  - Contains `gvkey` (surrogate: CIK/permno) - from `AP_SignalMasterTable.py` line 96

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 19. CashProd.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `permno`, `time_avail_m`, `at`, `che`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `mve_permco`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 86
  - Contains `che` (cash) - mapped in XBRL_TAG_MAP line 88
- ✅ **AP_SignalMasterTable.parquet**: Contains `mve_permco` (from `AP_SignalMasterTable.py` line 105)

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 20. CBOperProf.py

### Required Columns:
- **AP_SignalMasterTable.parquet**: `permno`, `gvkey`, `time_avail_m`, `exchcd`, `sicCRSP`, `shrcd`, `mve_permco`, `mve_c`
- **AP_m_aCompustat.parquet**: `permno`, `time_avail_m`, `revt`, `cogs`, `xsga`, `xrd`, `rect`, `invt`, `xpp`, `drc`, `drlt`, `ap`, `xacc`, `at`, `ceq`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 100-103)
  - Contains `gvkey` (surrogate: CIK/permno) - from `AP_SignalMasterTable.py` line 96
  - Contains `exchcd` (exchange code) - from `AP_SignalMasterTable.py` line 107
  - Contains `sicCRSP` (SIC code) - from `AP_SignalMasterTable.py` line 111
  - Contains `shrcd` (share code) - from `AP_SignalMasterTable.py` line 108
  - Contains `mve_permco` (market value) - from `AP_SignalMasterTable.py` line 105
  - Contains `mve_c` (market value) - from `AP_SignalMasterTable.py` line 104
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m`
  - Contains `revt` (revenue) - mapped in XBRL_TAG_MAP line 225
  - Contains `cogs` (cost of goods sold) - mapped in XBRL_TAG_MAP line 228-229
  - Contains `xsga` (SG&A) - mapped in XBRL_TAG_MAP line 232
  - Contains `xrd` (R&D) - mapped in XBRL_TAG_MAP line 235-236
  - Contains `rect` (receivables) - mapped in XBRL_TAG_MAP line 92
  - Contains `invt` (inventory) - mapped in XBRL_TAG_MAP line 94
  - Contains `xpp` (prepaid expenses) - mapped in XBRL_TAG_MAP line 239-240
  - Contains `drc` (deferred revenue current) - mapped in XBRL_TAG_MAP line 166
  - Contains `drlt` (deferred revenue noncurrent) - mapped in XBRL_TAG_MAP line 167
  - Contains `ap` (accounts payable) - mapped in XBRL_TAG_MAP line 152-153
  - Contains `xacc` (accrued expenses) - mapped in XBRL_TAG_MAP line 241-242
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 86
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## Summary

| Predictor | All Columns Available? | Can Be Constructed? | Additional Work Needed? |
|-----------|----------------------|---------------------|----------------------|
| BookLeverage | ⚠️ Partial* | ✅ Yes | None (*pstkrv missing but has fallback) |
| BrandInvest | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate, sic added) |
| Cash | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |
| CashProd | ✅ Yes | ✅ Yes | None |
| CBOperProf | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |

### Critical Missing Component:
None - All required columns are now available.

### Recommendations:
1. **Verification**: 
   - Test that all AP files actually contain the columns listed above by loading sample data
   - Verify `xad0` is correctly populated in `AP_a_aCompustat.parquet`
   - Verify `rdq` is correctly populated in `AP_m_QCompustat.parquet`
   - Verify `sic` is correctly populated in `AP_a_aCompustat.parquet` (recently added)

2. **Note**: SIC codes in AP data may be approximate (from heuristic mapping or ticker mapping file) rather than official CRSP SIC codes, but should be sufficient for industry filtering purposes.

# Group 5 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 5.

---

## 21. CF.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `ib`, `dp`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `mve_permco`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` lines 64-66)
  - Contains `ib` (net income) - mapped in XBRL_TAG_MAP line 262-264
  - Contains `dp` (depreciation) - mapped in XBRL_TAG_MAP line 245-247
- ✅ **AP_SignalMasterTable.parquet**: Contains `mve_permco` (from `AP_SignalMasterTable.py` line 105)

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 22. cfp.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `act`, `che`, `lct`, `dlc`, `txp`, `dp`, `ib`, `oancf`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `mve_permco`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `act` (current assets) - mapped in XBRL_TAG_MAP line 90
  - Contains `che` (cash) - mapped in XBRL_TAG_MAP line 88
  - Contains `lct` (current liabilities) - mapped in XBRL_TAG_MAP line 133
  - Contains `dlc` (debt current) - mapped in XBRL_TAG_MAP line 136-139
  - Contains `txp` (taxes payable) - mapped in XBRL_TAG_MAP line 156-157
  - Contains `dp` (depreciation) - mapped in XBRL_TAG_MAP line 245-247
  - Contains `ib` (net income) - mapped in XBRL_TAG_MAP line 262-264
  - Contains `oancf` (operating cash flow) - mapped in XBRL_TAG_MAP line 299-301
- ✅ **AP_SignalMasterTable.parquet**: Contains `mve_permco` (from `AP_SignalMasterTable.py` line 105)

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 23. ChangeInRecommendation.py

### Required Columns:
- **AP_IBES_Recommendations.parquet**: `tickerIBES`, `amaskcd`, `anndats`, `time_avail_m`, `ireccd`
- **AP_SignalMasterTable.parquet**: `permno`, `tickerIBES`, `time_avail_m`

### AP File Status:
- ❌ **AP_IBES_Recommendations.parquet**: **FILE DOES NOT EXIST YET**
  - Script exists: `AP_IBESRecommendations.py` (generates `AP_IBES_Recommendations.parquet`)
  - Would contain `tickerIBES` (from `AP_IBESRecommendations.py` line 282)
  - Would contain `amaskcd` (analyst mask code) - from `AP_IBESRecommendations.py` line 254-280
  - Would contain `anndats` (announcement date) - from `AP_IBESRecommendations.py` line 252
  - Would contain `time_avail_m` (monthly availability) - from `AP_IBESRecommendations.py` line 306-307
  - Would contain `ireccd` (recommendation code) - from `AP_IBESRecommendations.py` line 248
- ⚠️ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 100-103)
  - Contains `tickerIBES` column - should be populated from `AP_IBESCRSPLinkingTable.parquet` (after fix in Group 2)

### Can Be Constructed?
**NO** - Missing `AP_IBES_Recommendations.parquet` file.

### Additional Work Needed?
**YES** - Need to run `AP_IBESRecommendations.py` to generate `AP_IBES_Recommendations.parquet`.

#### What Needs to Be Done:
1. **Run AP_IBESRecommendations.py**: Execute the script to download IBES recommendations from Eikon/LSEG API
2. **Requirements**:
   - Eikon/LSEG API subscription (proprietary data source)
   - `eikon` Python package installed
   - Eikon API credentials configured
3. **Output**: Generates `AP_IBES_Recommendations.parquet` with required columns

#### Implementation Notes:
- The script `AP_IBESRecommendations.py` exists and is ready to run
- Requires Eikon/LSEG API access (paid subscription)
- If Eikon is not available, this predictor cannot be constructed with AP data
- Alternative: Could potentially use other free sources (e.g., Yahoo Finance analyst recommendations), but format would need adaptation

---

## 24. ChAssetTurnover.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `rect`, `invt`, `aco`, `ppent`, `intan`, `ap`, `lco`, `lo`, `sale`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `rect` (receivables) - mapped in XBRL_TAG_MAP line 92
  - Contains `invt` (inventory) - mapped in XBRL_TAG_MAP line 91
  - ⚠️ **`aco`** (other current assets) - mapped in XBRL_TAG_MAP line 95, but marked as "highly unreliable" and prefers derivation
  - Contains `ppent` (PP&E net) - mapped in XBRL_TAG_MAP line 98
  - Contains `intan` (intangibles) - mapped in XBRL_TAG_MAP line 107
  - Contains `ap` (accounts payable) - mapped in XBRL_TAG_MAP line 152-153
  - Contains `lco` (other current liabilities) - mapped in XBRL_TAG_MAP line 160-161
  - Contains `lo` (other noncurrent liabilities) - mapped in XBRL_TAG_MAP line 163
  - Contains `sale` (sales revenue) - mapped in XBRL_TAG_MAP line 223

### Can Be Constructed?
**YES** - All required columns are present, though `aco` may be derived rather than directly mapped.

### Additional Work Needed?
None. The predictor can be constructed with the available columns. Note that `aco` is marked as "highly unreliable" in XBRL mapping and may be derived from `act - (che + rect + invt)` if direct mapping fails.

---

## 25. ChEQ.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `ceq`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## Summary

| Predictor | All Columns Available? | Can Be Constructed? | Additional Work Needed? |
|-----------|----------------------|---------------------|----------------------|
| CF | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |
| cfp | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |
| ChangeInRecommendation | ❌ No | ❌ No | **Run AP_IBESRecommendations.py** |
| ChAssetTurnover | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate, aco may be derived) |
| ChEQ | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |

### Critical Missing Component:
- **`AP_IBES_Recommendations.parquet`**: Required for `ChangeInRecommendation.py`. File does not exist yet - needs to be generated by running `AP_IBESRecommendations.py`.

### Recommendations:
1. **Verification**: 
   - Test that all AP files actually contain the columns listed above by loading sample data
   - Verify `oancf` (operating cash flow) is correctly populated in `AP_m_aCompustat.parquet`
   - Verify `aco` (other current assets) is populated (may be derived rather than directly mapped)
   - Verify `AP_IBES_Recommendations.parquet` exists and has required columns

2. **For ChangeInRecommendation**: 
   - **CRITICAL**: Run `AP_IBESRecommendations.py` to generate `AP_IBES_Recommendations.parquet`
   - Requires Eikon/LSEG API subscription (proprietary data source)
   - Ensure `tickerIBES` is populated in `AP_SignalMasterTable.parquet` (fix from Group 2)
   - If Eikon is not available, this predictor cannot be constructed with AP data

3. **Note on aco (Other Current Assets)**: 
   - Marked as "highly unreliable" in XBRL mapping
   - May be derived as: `aco = act - (che + rect + invt + other known current assets)`
   - This derivation is acceptable for predictor construction

4. **Note on gvkey**: 
   - All predictors use surrogate `gvkey` (CIK/permno) instead of true Compustat gvkey
   - This is acceptable as `gvkey` is typically only used for grouping/identification, not calculations

# Group 7 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 7.

---

## 31. ChNWC.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `permno`, `time_avail_m`, `act`, `che`, `lct`, `dlc`, `at`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `act` (current assets) - mapped in XBRL_TAG_MAP line 90
  - Contains `che` (cash) - mapped in XBRL_TAG_MAP line 88
  - Contains `lct` (current liabilities) - mapped in XBRL_TAG_MAP line 133
  - Contains `dlc` (debt current) - mapped in XBRL_TAG_MAP line 136-139
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 86

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 32. ChTax.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `permno`, `gvkey`, `time_avail_m`, `at`
- **AP_m_QCompustat.parquet**: `gvkey`, `time_avail_m`, `txtq`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 86
- ✅ **AP_m_QCompustat.parquet**: 
  - Contains `gvkey` (surrogate: CIK/permno) - from `AP_CompustatQuarterly.py` structure
  - Contains `time_avail_m` (from `AP_CompustatQuarterly.py` structure)
  - Contains `txtq` (total taxes quarterly) - mapped in XBRL_TAG_MAP line 196

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 33. CitationsRD.py

### Required Columns:
- **AP_SignalMasterTable.parquet**: `permno`, `gvkey`, `time_avail_m`, `mve_c`, `sicCRSP`, `exchcd`
- **AP_m_aCompustat.parquet**: `permno`, `time_avail_m`, `xrd`, `sich`, `datadate`, `ceq`
- **PatentDataProcessed.parquet**: `gvkey`, `year`, `ncitscale`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` line 119)
  - Contains `mve_c` (market value of equity) - from `AP_SignalMasterTable.py` line 127
  - Contains `sicCRSP` (SIC code from CRSP) - from `AP_SignalMasterTable.py` line 134
  - Contains `exchcd` (exchange code) - from `AP_SignalMasterTable.py` line 130
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `xrd` (R&D expenses) - mapped in XBRL_TAG_MAP line 250-252
  - ⚠️ **`sich`**: **NOT EXTRACTED** - Historical SIC code is not extracted from XBRL in `AP_CompustatAnnual.py`. However, `sich` is loaded but **NOT USED** in the predictor logic (only `sicCRSP` is used for filtering).
  - ✅ **`datadate`**: Available - from `AP_CompustatAnnual.py` line 1017 (set from `period_end`)
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184
- ❌ **PatentDataProcessed.parquet**: **FILE DOES NOT EXIST YET**
  - Script exists: `PatentCitations.py` (generates `PatentDataProcessed.parquet`)
  - Would contain `gvkey`, `year`, `ncitscale` (scaled patent citations)
  - Requires patent citation data from USPTO/patent databases

### Can Be Constructed?
**NO** - Missing `PatentDataProcessed.parquet`. Note: `sich` is loaded but not used in calculations.

### Additional Work Needed?
**YES** - Missing patent data:

#### What Needs to Be Done:
1. **Create PatentDataProcessed.parquet**: 
   - Run `PatentCitations.py` to generate patent citation data
   - Requires patent citation database access (USPTO or proprietary sources)
   - Note: This is NOT an AP script - it uses WRDS/patent databases
   - Must contain `gvkey`, `year`, `ncitscale` columns

2. **Note on `sich`**: 
   - `sich` is loaded in the predictor but **NOT USED** in the actual signal construction
   - Only `sicCRSP` from `SignalMasterTable` is used for filtering financial firms (line 140)
   - Can be set to empty/NaN or omitted from the data load without affecting results

#### Implementation Notes:
- **For PatentDataProcessed**: This predictor requires patent citation data which is typically sourced from proprietary databases (USPTO, NBER Patent Database, etc.). The `PatentCitations.py` script processes patent data to create citation counts (`ncitscale`). Without this data, the predictor cannot be constructed.
- **For sich**: While `sich` is listed as a required column, it is not actually used in the predictor logic. The predictor only uses `sicCRSP` from `SignalMasterTable` for filtering financial firms. However, to match the original code exactly, `sich` should still be included in the data load (can be NaN/empty).
- **Alternative**: If patent data is not available, this predictor cannot be constructed with AP data sources.

---

## 34. CompEquIss.py

### Required Columns:
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `ret`, `mve_c`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `ret` (returns) - from `AP_SignalMasterTable.py` line 133
  - Contains `mve_c` (market value of equity) - from `AP_SignalMasterTable.py` line 127

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 35. CompositeDebtIssuance.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `dltt`, `dlc`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `dltt` (long-term debt) - mapped in XBRL_TAG_MAP line 142-144
  - Contains `dlc` (debt current) - mapped in XBRL_TAG_MAP line 136-139

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## Summary

| Predictor | All Columns Available? | Can Be Constructed? | Additional Work Needed? |
|-----------|----------------------|---------------------|----------------------|
| ChNWC | ✅ Yes | ✅ Yes | None |
| ChTax | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |
| CitationsRD | ❌ No | ❌ No | **Create PatentDataProcessed.parquet, add sich column** |
| CompEquIss | ✅ Yes | ✅ Yes | None |
| CompositeDebtIssuance | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |

### Critical Missing Component:
1. **`PatentDataProcessed.parquet`**: Required for `CitationsRD.py`. File does not exist yet - needs to be generated by running `PatentCitations.py` (requires patent citation database access).

### Recommendations:
1. **For CitationsRD**: 
   - **CRITICAL**: Run `PatentCitations.py` to generate `PatentDataProcessed.parquet`
   - Requires patent citation database access (USPTO, NBER Patent Database, or proprietary sources)
   - **Note on `sich`**: While `sich` is loaded, it is not used in calculations. Can be set to NaN/empty or omitted from data load. If needed for exact code matching, can merge from `AP_monthlyCRSP.parquet` using `sicCRSP` column.
   - If patent data is not available, this predictor cannot be constructed with AP data sources

2. **Verification**: 
   - Test that all AP files actually contain the columns listed above by loading sample data
   - Verify `txtq` (total taxes quarterly) is correctly populated in `AP_m_QCompustat.parquet`
   - Verify `dltt` and `dlc` (debt columns) are correctly populated in `AP_m_aCompustat.parquet`

3. **Note on Patent Data**: 
   - Patent citation data is typically sourced from proprietary databases
   - The `PatentCitations.py` script processes patent data to create citation counts
   - Without access to patent databases, this predictor cannot be constructed
   - Consider alternative data sources or skip this predictor if patent data is unavailable

4. **Note on sich**: 
   - `sich` is loaded but not used in the predictor logic
   - Only `sicCRSP` from `SignalMasterTable` is used for filtering financial firms
   - Can be safely omitted or set to NaN/empty without affecting results

# Group 8 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 8.

---

## 36. ConsRecomm.py

### Required Columns:
- **AP_IBES_Recommendations.parquet**: `tickerIBES`, `amaskcd`, `anndats`, `time_avail_m`, `ireccd`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `tickerIBES`

### AP File Status:
- ❌ **AP_IBES_Recommendations.parquet**: **FILE DOES NOT EXIST YET**
  - Script exists: `AP_IBESRecommendations.py` (generates `AP_IBES_Recommendations.parquet`)
  - Would contain `tickerIBES` (from `AP_IBESRecommendations.py` line 282)
  - Would contain `amaskcd` (analyst mask code) - from `AP_IBESRecommendations.py` line 254-280
  - Would contain `anndats` (announcement date) - from `AP_IBESRecommendations.py` line 252
  - Would contain `time_avail_m` (monthly availability) - from `AP_IBESRecommendations.py` line 306-307
  - Would contain `ireccd` (recommendation code) - from `AP_IBESRecommendations.py` line 248
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `tickerIBES` column - should be populated from `AP_IBESCRSPLinkingTable.parquet` (after fix in Group 2)

### Can Be Constructed?
**NO** - Missing `AP_IBES_Recommendations.parquet` file.

### Additional Work Needed?
**YES** - Need to run `AP_IBESRecommendations.py` to generate `AP_IBES_Recommendations.parquet`.

#### What Needs to Be Done:
1. **Run AP_IBESRecommendations.py**: Execute the script to download IBES recommendations from Eikon/LSEG API
2. **Requirements**:
   - Eikon/LSEG API subscription (proprietary data source)
   - `eikon` Python package installed
   - Eikon API credentials configured
3. **Output**: Generates `AP_IBES_Recommendations.parquet` with required columns

#### Implementation Notes:
- The script `AP_IBESRecommendations.py` exists and is ready to run
- Requires Eikon/LSEG API access (paid subscription)
- If Eikon is not available, this predictor cannot be constructed with AP data
- Alternative: Could potentially use other free sources (e.g., Yahoo Finance analyst recommendations), but format would need adaptation

---

## 37. ConvDebt.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `dc`, `cshrc`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - ⚠️ **`dc`**: **NOT EXPLICITLY MAPPED** - Deferred charges is not mapped in XBRL_TAG_MAP, but is included in `zero_fill_vars` list (line 1007), meaning it will be filled with 0 if missing
  - ✅ **`cshrc`**: Available - mapped in XBRL_TAG_MAP line 206-207 (stock repurchased during period shares)

### Can Be Constructed?
**PARTIALLY** - `dc` is not explicitly mapped but will be zero-filled if missing.

### Additional Work Needed?
**YES** - Need to add `dc` (deferred charges) mapping to XBRL_TAG_MAP.

#### What Needs to Be Done:
1. **Add `dc` to XBRL_TAG_MAP**: Map deferred charges from XBRL tags
   - Common XBRL tags: `DeferredCharges`, `DeferredChargesNoncurrent`, `OtherAssetsNoncurrent`
   - Or derive from other balance sheet items if not directly available
2. **Current Behavior**: `dc` is in `zero_fill_vars` list, so missing values are filled with 0
   - This means the predictor will work but may miss some convertible debt indicators
   - If `dc` is always 0, then `ConvDebt` will only be 1 when `cshrc > 0`

#### Implementation Notes:
- The predictor checks: `ConvDebt = 1 if (dc != 0) OR (cshrc != 0)`
- If `dc` is always 0 (due to zero-filling), the predictor will only flag convertible debt when `cshrc > 0`
- This may miss some convertible debt cases that are captured by `dc` in the original data
- **Recommended**: Add `dc` mapping to XBRL_TAG_MAP for more accurate results

---

## 38. CoskewACX.py

### Required Columns:
- **AP_dailyCRSP.parquet**: `permno`, `time_d`, `ret`
- **AP_dailyFF.parquet**: `time_d`, `mktrf`, `rf`

### AP File Status:
- ✅ **AP_dailyCRSP.parquet**: 
  - Contains `permno`, `time_d` (from `AP_CRSPDaily.py` lines 282-283)
  - Contains `ret` (daily returns) - from `AP_CRSPDaily.py` line 138
- ✅ **AP_dailyFF.parquet**: 
  - Contains `time_d`, `mktrf`, `rf` (from `AP_FamaFrenchDaily.py` lines 538-546)
  - Generated by `AP_FamaFrenchDaily.py`

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 39. Coskewness.py

### Required Columns:
- **AP_monthlyCRSP.parquet**: `permno`, `time_avail_m`, `ret`
- **AP_monthlyFF.parquet**: `time_avail_m`, `mktrf`, `rf`

### AP File Status:
- ✅ **AP_monthlyCRSP.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` structure)
  - Contains `ret` (monthly returns) - from `AP_CRSPMonthly.py` line 345
- ✅ **AP_monthlyFF.parquet**: 
  - Contains `time_avail_m`, `mktrf`, `rf` (from `AP_FamaFrenchMonthly.py` lines 488-496)
  - Generated by `AP_FamaFrenchMonthly.py`

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 40. CPVolSpread.py

### Required Columns:
- **bali_hovak_imp_vol.csv**: OptionMetrics implied volatility data with `secid`, `time_avail_m`, `cp_flag`, `mean_imp_vol`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `secid`, `sicCRSP`

### AP File Status:
- ❌ **bali_hovak_imp_vol.csv**: **FILE DOES NOT EXIST**
  - This file is typically generated from OptionMetrics data (proprietary database)
  - Contains implied volatility data for call and put options
  - Required columns: `secid`, `date`, `cp_flag` (C/P), `mean_imp_vol`, `mean_day`, `nobs`, `ticker`
- ⚠️ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `sicCRSP` (from `AP_SignalMasterTable.py` line 134)
  - ⚠️ **`secid`**: Set to `np.nan` (from `AP_SignalMasterTable.py` line 118) - **NOT POPULATED**

### Can Be Constructed?
**NO** - Missing `bali_hovak_imp_vol.csv` and `secid` is not populated in `AP_SignalMasterTable.parquet`.

### Additional Work Needed?
**YES** - Multiple issues:

#### What Needs to Be Done:
1. **Create bali_hovak_imp_vol.csv**: 
   - This requires OptionMetrics data (proprietary database)
   - OptionMetrics provides implied volatility data for options
   - Need to process OptionMetrics data to create the required format
   - Script would need to:
     - Download OptionMetrics implied volatility data
     - Filter to ATM (at-the-money) options
     - Aggregate by `secid`, `date`, and `cp_flag` (call/put)
     - Calculate `mean_imp_vol` for each group

2. **Populate `secid` in AP_SignalMasterTable.parquet**:
   - `secid` is OptionMetrics security identifier
   - Need to create linking table between CRSP `permno` and OptionMetrics `secid`
   - This typically requires OptionMetrics-CRSP linking table
   - Without `secid`, cannot merge options data with stock data

#### Implementation Notes:
- **For bali_hovak_imp_vol.csv**: OptionMetrics is a proprietary database that requires subscription
- The predictor calculates: `CPVolSpread = mean_imp_volC - mean_imp_volP` (call minus put volatility spread)
- Without OptionMetrics access, this predictor cannot be constructed
- **Alternative**: Could potentially use free options data sources (e.g., CBOE, Yahoo Finance options), but would need significant adaptation of the data processing logic

- **For secid**: OptionMetrics uses `secid` as its primary identifier, which maps to CRSP `permno` via a linking table
- Without OptionMetrics-CRSP linking table, cannot populate `secid` in `AP_SignalMasterTable.parquet`
- Even if `bali_hovak_imp_vol.csv` exists, cannot merge without `secid`

---

## Summary

| Predictor | All Columns Available? | Can Be Constructed? | Additional Work Needed? |
|-----------|----------------------|---------------------|----------------------|
| ConsRecomm | ❌ No | ❌ No | **Run AP_IBESRecommendations.py** |
| ConvDebt | ⚠️ Partial* | ⚠️ Partial | **Add dc mapping to XBRL_TAG_MAP** (*dc zero-filled) |
| CoskewACX | ✅ Yes | ✅ Yes | None |
| Coskewness | ✅ Yes | ✅ Yes | None |
| CPVolSpread | ❌ No | ❌ No | **Create bali_hovak_imp_vol.csv, populate secid** |

### Critical Missing Components:
1. **`AP_IBES_Recommendations.parquet`**: Required for `ConsRecomm.py`. File does not exist yet - needs to be generated by running `AP_IBESRecommendations.py` (requires Eikon/LSEG API subscription).
2. **`bali_hovak_imp_vol.csv`**: Required for `CPVolSpread.py`. File does not exist - needs to be generated from OptionMetrics data (proprietary database).
3. **`secid` column in AP_SignalMasterTable.parquet**: Required for `CPVolSpread.py`. Currently set to `np.nan` - needs OptionMetrics-CRSP linking table to populate.

### Recommendations:
1. **For ConsRecomm**: 
   - **CRITICAL**: Run `AP_IBESRecommendations.py` to generate `AP_IBES_Recommendations.parquet`
   - Requires Eikon/LSEG API subscription (proprietary data source)
   - If Eikon is not available, this predictor cannot be constructed with AP data

2. **For ConvDebt**: 
   - **RECOMMENDED**: Add `dc` (deferred charges) mapping to XBRL_TAG_MAP in `AP_CompustatAnnual.py`
   - Common XBRL tags: `DeferredCharges`, `DeferredChargesNoncurrent`, `OtherAssetsNoncurrent`
   - Currently, `dc` is zero-filled if missing, which may cause the predictor to miss some convertible debt cases
   - The predictor will still work but may be less accurate

3. **For CPVolSpread**: 
   - **CRITICAL**: Requires OptionMetrics data access (proprietary database)
   - Need to create `bali_hovak_imp_vol.csv` from OptionMetrics implied volatility data
   - Need to populate `secid` in `AP_SignalMasterTable.parquet` using OptionMetrics-CRSP linking table
   - Without OptionMetrics access, this predictor cannot be constructed
   - **Alternative**: Could potentially use free options data sources (e.g., CBOE, Yahoo Finance options), but would require significant code adaptation

4. **Verification**: 
   - Test that all AP files actually contain the columns listed above by loading sample data
   - Verify `cshrc` is correctly populated in `AP_m_aCompustat.parquet`
   - Verify `ret` is correctly populated in `AP_dailyCRSP.parquet` and `AP_monthlyCRSP.parquet`
   - Verify `mktrf` and `rf` are correctly populated in `AP_dailyFF.parquet` and `AP_monthlyFF.parquet`

5. **Note on Proprietary Data Sources**: 
   - IBES recommendations (Eikon/LSEG) and OptionMetrics are proprietary databases requiring paid subscriptions
   - Without access to these databases, `ConsRecomm` and `CPVolSpread` cannot be constructed with AP data
   - Consider alternative free data sources or skip these predictors if proprietary data is unavailable

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

# Group 10 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 10.

---

## 46. DelCOA.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `at`, `act`, `che`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 77
  - Contains `act` (current assets) - mapped in XBRL_TAG_MAP line 80
  - Contains `che` (cash) - mapped in XBRL_TAG_MAP line 83

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 47. DelCOL.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `at`, `lct`, `dlc`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 77
  - Contains `lct` (current liabilities) - mapped in XBRL_TAG_MAP line 133
  - Contains `dlc` (debt current) - mapped in XBRL_TAG_MAP line 136-139

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 48. DelDRC.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `drc`, `at`, `ceq`, `sale`, `sic`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `drc` (deferred revenue current) - mapped in XBRL_TAG_MAP line 166
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 77
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184
  - Contains `sale` (sales revenue) - mapped in XBRL_TAG_MAP line 223
  - ✅ **`sic`**: **ADDED** - SIC code has been added to `AP_a_aCompustat.parquet` (from Group 4 fix)

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate, sic has been added).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 49. DelEqu.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `at`, `ceq`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 77
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 50. DelFINL.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `at`, `pstk`, `dltt`, `dlc`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 77
  - Contains `pstk` (preferred stock) - mapped in XBRL_TAG_MAP line 189
  - Contains `dltt` (long-term debt) - mapped in XBRL_TAG_MAP line 142-144
  - Contains `dlc` (debt current) - mapped in XBRL_TAG_MAP line 136-139

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## Summary

| Predictor | All Columns Available? | Can Be Constructed? | Additional Work Needed? |
|-----------|----------------------|---------------------|----------------------|
| DelCOA | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |
| DelCOL | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |
| DelDRC | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate, sic added) |
| DelEqu | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |
| DelFINL | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |

### Critical Missing Components:
None - All required columns are available.

### Recommendations:
1. **Verification**: 
   - Test that all AP files actually contain the columns listed above by loading sample data
   - Verify `drc` (deferred revenue current) is correctly populated in `AP_m_aCompustat.parquet`
   - Verify `sale` (sales revenue) is correctly populated in `AP_m_aCompustat.parquet`
   - Verify `sic` is correctly populated in `AP_m_aCompustat.parquet` (recently added)
   - Verify `pstk` (preferred stock) is correctly populated in `AP_m_aCompustat.parquet`

2. **Note on gvkey**: 
   - All predictors use surrogate `gvkey` (CIK/permno) instead of true Compustat gvkey
   - This is acceptable as `gvkey` is typically only used for grouping/identification, not calculations

3. **Note on sic**: 
   - `sic` was added to `AP_a_aCompustat.parquet` in Group 4
   - This is required for `DelDRC.py` to filter out financial firms (SIC 6000-6999)
   - Verify that `sic` is correctly propagated to `AP_m_aCompustat.parquet` (monthly version)

4. **Data Quality Checks**: 
   - All predictors calculate year-over-year changes using 12-month lags
   - Ensure that the monthly expansion of annual data preserves the correct temporal relationships
   - Verify that lag operations work correctly with the monthly data structure

# Group 11 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 11.

---

## 51. DelLTI.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `at`, `ivao`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 77
  - Contains `ivao` (investments and other noncurrent assets) - mapped in XBRL_TAG_MAP line 116-119

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 52. DelNetFin.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `at`, `pstk`, `dltt`, `dlc`, `ivst`, `ivao`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 77
  - Contains `pstk` (preferred stock) - mapped in XBRL_TAG_MAP line 189
  - Contains `dltt` (long-term debt) - mapped in XBRL_TAG_MAP line 142-144
  - Contains `dlc` (debt current) - mapped in XBRL_TAG_MAP line 136-139
  - Contains `ivst` (short-term investments) - mapped in XBRL_TAG_MAP line 115
  - Contains `ivao` (investments and other noncurrent assets) - mapped in XBRL_TAG_MAP line 116-119

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 53. DivInit.py

### Required Columns:
- **AP_CRSPdistributions.parquet**: `permno`, `exdt`, `cd2`, `divamt`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `exchcd`, `shrcd`

### AP File Status:
- ✅ **AP_CRSPdistributions.parquet**: 
  - File exists (from `AP_CRSPDistributions.py`)
  - Contains `permno` (from `AP_CRSPDistributions.py` line 223)
  - Contains `exdt` (ex-dividend date) - from `AP_CRSPDistributions.py` line 225
  - Contains `cd2` (distribution code digit 2) - from `AP_CRSPDistributions.py` line 264
  - Contains `divamt` (dividend amount) - from `AP_CRSPDistributions.py` line 226
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `exchcd` (exchange code) - from `AP_SignalMasterTable.py` line 130
  - Contains `shrcd` (share code) - from `AP_SignalMasterTable.py` line 131

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 54. DivOmit.py

### Required Columns:
- **AP_CRSPdistributions.parquet**: `permno`, `exdt`, `divamt`, `cd2`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `exchcd`, `shrcd`

### AP File Status:
- ✅ **AP_CRSPdistributions.parquet**: 
  - File exists (from `AP_CRSPDistributions.py`)
  - Contains `permno` (from `AP_CRSPDistributions.py` line 223)
  - Contains `exdt` (ex-dividend date) - from `AP_CRSPDistributions.py` line 225
  - Contains `divamt` (dividend amount) - from `AP_CRSPDistributions.py` line 226
  - Contains `cd2` (distribution code digit 2) - from `AP_CRSPDistributions.py` line 264
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `exchcd` (exchange code) - from `AP_SignalMasterTable.py` line 130
  - Contains `shrcd` (share code) - from `AP_SignalMasterTable.py` line 131

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 55. DivSeason.py

### Required Columns:
- **AP_CRSPdistributions.parquet**: `permno`, `cd1`, `cd2`, `cd3`, `divamt`, `exdt`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`

### AP File Status:
- ✅ **AP_CRSPdistributions.parquet**: 
  - File exists (from `AP_CRSPDistributions.py`)
  - Contains `permno` (from `AP_CRSPDistributions.py` line 223)
  - Contains `cd1` (distribution code digit 1) - from `AP_CRSPDistributions.py` line 263
  - Contains `cd2` (distribution code digit 2) - from `AP_CRSPDistributions.py` line 264
  - Contains `cd3` (distribution code digit 3) - from `AP_CRSPDistributions.py` line 265
  - Contains `divamt` (dividend amount) - from `AP_CRSPDistributions.py` line 226
  - Contains `exdt` (ex-dividend date) - from `AP_CRSPDistributions.py` line 225
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## Summary

| Predictor | All Columns Available? | Can Be Constructed? | Additional Work Needed? |
|-----------|----------------------|---------------------|----------------------|
| DelLTI | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |
| DelNetFin | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |
| DivInit | ✅ Yes | ✅ Yes | None |
| DivOmit | ✅ Yes | ✅ Yes | None |
| DivSeason | ✅ Yes | ✅ Yes | None |

### Critical Missing Components:
None - All required columns are available.

### Recommendations:
1. **Verification**: 
   - Test that all AP files actually contain the columns listed above by loading sample data
   - Verify `ivao` (investments and other noncurrent assets) is correctly populated in `AP_m_aCompustat.parquet`
   - Verify `ivst` (short-term investments) is correctly populated in `AP_m_aCompustat.parquet`
   - Verify `AP_CRSPdistributions.parquet` has complete dividend history (may be limited by yfinance data availability)
   - Verify distribution codes (`cd1`, `cd2`, `cd3`) are correctly assigned in `AP_CRSPdistributions.parquet`

2. **Note on gvkey**: 
   - `DelLTI` and `DelNetFin` use surrogate `gvkey` (CIK/permno) instead of true Compustat gvkey
   - This is acceptable as `gvkey` is typically only used for grouping/identification, not calculations

3. **Note on AP_CRSPdistributions.parquet**: 
   - Generated by `AP_CRSPDistributions.py` using yfinance
   - Historical dividend data typically available from ~2000 onwards (yfinance limitation)
   - Distribution codes are approximated based on dividend amounts and split ratios
   - May not include all special distributions (spinoffs, rights) comprehensively
   - **Coverage**: Good for regular dividends and splits, but may miss some special distributions

4. **Data Quality Checks**: 
   - All dividend predictors (`DivInit`, `DivOmit`, `DivSeason`) rely on accurate dividend history
   - Verify that `AP_CRSPdistributions.parquet` has sufficient historical coverage for rolling window calculations
   - `DivInit` uses 24-month rolling windows, `DivOmit` uses 3/6/12/18/24-month windows, `DivSeason` uses 12-month windows
   - Ensure dividend amounts (`divamt`) are correctly extracted from yfinance
   - Verify distribution code digits (`cd1`, `cd2`, `cd3`) correctly classify dividend types

5. **Historical Data Limitations**: 
   - yfinance dividend data may have limited historical coverage compared to CRSP
   - Some older dividend records may be missing
   - This could affect predictors that require long historical windows (e.g., `DivOmit` uses 24-month windows)
   - Consider data availability when interpreting results for early periods

# Group 12 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 12.

---

## 56. DivYieldST.py

### Required Columns:
- **AP_CRSPdistributions.parquet**: `permno`, `cd1`, `cd2`, `cd3`, `divamt`, `exdt`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `prc`
- **AP_monthlyCRSP.parquet**: `permno`, `time_avail_m`, `ret`, `retx`

### AP File Status:
- ✅ **AP_CRSPdistributions.parquet**: 
  - File exists (from `AP_CRSPDistributions.py`)
  - Contains `permno` (from `AP_CRSPDistributions.py` line 223)
  - Contains `cd1`, `cd2`, `cd3` (distribution code digits) - from `AP_CRSPDistributions.py` lines 263-265
  - Contains `divamt` (dividend amount) - from `AP_CRSPDistributions.py` line 226
  - Contains `exdt` (ex-dividend date) - from `AP_CRSPDistributions.py` line 225
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `prc` (price) - from `AP_SignalMasterTable.py` line 132
- ✅ **AP_monthlyCRSP.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` structure)
  - Contains `ret` (returns) - from `AP_CRSPMonthly.py` line 345
  - Contains `retx` (returns excluding dividends) - from `AP_CRSPMonthly.py` line 346

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 57. dNoa.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `permno`, `time_avail_m`, `at`, `che`, `dltt`, `dlc`, `mib`, `pstk`, `ceq`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 77
  - Contains `che` (cash) - mapped in XBRL_TAG_MAP line 83
  - Contains `dltt` (long-term debt) - mapped in XBRL_TAG_MAP line 142-144
  - Contains `dlc` (debt current) - mapped in XBRL_TAG_MAP line 136-139
  - Contains `mib` (minority interest) - mapped in XBRL_TAG_MAP line 175
  - Contains `pstk` (preferred stock) - mapped in XBRL_TAG_MAP line 189
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 58. DolVol.py

### Required Columns:
- **AP_monthlyCRSP.parquet**: `permno`, `time_avail_m`, `vol`, `prc`

### AP File Status:
- ✅ **AP_monthlyCRSP.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` structure)
  - Contains `vol` (volume) - from `AP_CRSPMonthly.py` line 347
  - Contains `prc` (price) - from `AP_CRSPMonthly.py` line 349

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 59. DownRecomm.py

### Required Columns:
- **AP_IBES_Recommendations.parquet**: `tickerIBES`, `amaskcd`, `anndats`, `time_avail_m`, `ireccd`
- **AP_SignalMasterTable.parquet**: `permno`, `tickerIBES`, `time_avail_m`

### AP File Status:
- ❌ **AP_IBES_Recommendations.parquet**: **FILE DOES NOT EXIST YET**
  - Script exists: `AP_IBESRecommendations.py` (generates `AP_IBES_Recommendations.parquet`)
  - Would contain `tickerIBES` (from `AP_IBESRecommendations.py` line 282)
  - Would contain `amaskcd` (analyst mask code) - from `AP_IBESRecommendations.py` line 254-280
  - Would contain `anndats` (announcement date) - from `AP_IBESRecommendations.py` line 252
  - Would contain `time_avail_m` (monthly availability) - from `AP_IBESRecommendations.py` line 306-307
  - Would contain `ireccd` (recommendation code) - from `AP_IBESRecommendations.py` line 248
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `tickerIBES` column - should be populated from `AP_IBESCRSPLinkingTable.parquet` (after fix in Group 2)

### Can Be Constructed?
**NO** - Missing `AP_IBES_Recommendations.parquet` file.

### Additional Work Needed?
**YES** - Need to run `AP_IBESRecommendations.py` to generate `AP_IBES_Recommendations.parquet`.

#### What Needs to Be Done:
1. **Run AP_IBESRecommendations.py**: Execute the script to download IBES recommendations from Eikon/LSEG API
2. **Requirements**:
   - Eikon/LSEG API subscription (proprietary data source)
   - `eikon` Python package installed
   - Eikon API credentials configured
3. **Output**: Generates `AP_IBES_Recommendations.parquet` with required columns

#### Implementation Notes:
- The script `AP_IBESRecommendations.py` exists and is ready to run
- Requires Eikon/LSEG API access (paid subscription)
- If Eikon is not available, this predictor cannot be constructed with AP data
- Alternative: Could potentially use other free sources (e.g., Yahoo Finance analyst recommendations), but format would need adaptation

---

## 60. dVolCall.py

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
**YES** - Same issues as `dCPVolSpread.py` (Group 8):

#### What Needs to Be Done:
1. **Create OptionMetricsVolSurf.csv**: 
   - This requires OptionMetrics data (proprietary database)
   - OptionMetrics provides implied volatility surface data for options
   - Need to process OptionMetrics data to create volatility surface with required dimensions
   - Script would need to:
     - Download OptionMetrics implied volatility data
     - Filter to specific `days` (30) and `delta` (50) for ATM options
     - Aggregate by `secid`, `time_avail_m`, `days`, `delta`, and `cp_flag` (call/put)

2. **Populate `secid` in AP_SignalMasterTable.parquet**:
   - `secid` is OptionMetrics security identifier
   - Need to create linking table between CRSP `permno` and OptionMetrics `secid`
   - This typically requires OptionMetrics-CRSP linking table
   - Without `secid`, cannot merge options data with stock data

#### Implementation Notes:
- **For OptionMetricsVolSurf**: OptionMetrics is a proprietary database that requires subscription
- The predictor calculates change in call implied volatility: `dVolCall = impl_vol - l1_impl_vol`
- Without OptionMetrics access, this predictor cannot be constructed
- **Alternative**: Could potentially use free options data sources (e.g., CBOE, Yahoo Finance options), but would need significant adaptation of the data processing logic

- **For secid**: OptionMetrics uses `secid` as its primary identifier, which maps to CRSP `permno` via a linking table
- Without OptionMetrics-CRSP linking table, cannot populate `secid` in `AP_SignalMasterTable.parquet`
- Even if `OptionMetricsVolSurf.csv` exists, cannot merge without `secid`

---

## Summary

| Predictor | All Columns Available? | Can Be Constructed? | Additional Work Needed? |
|-----------|----------------------|---------------------|----------------------|
| DivYieldST | ✅ Yes | ✅ Yes | None |
| dNoa | ✅ Yes | ✅ Yes | None |
| DolVol | ✅ Yes | ✅ Yes | None |
| DownRecomm | ❌ No | ❌ No | **Run AP_IBESRecommendations.py** |
| dVolCall | ❌ No | ❌ No | **Create OptionMetricsVolSurf.csv, populate secid** |

### Critical Missing Components:
1. **`AP_IBES_Recommendations.parquet`**: Required for `DownRecomm.py`. File does not exist yet - needs to be generated by running `AP_IBESRecommendations.py` (requires Eikon/LSEG API subscription).
2. **`OptionMetricsVolSurf.csv`**: Required for `dVolCall.py`. File does not exist - needs to be generated from OptionMetrics data (proprietary database).
3. **`secid` column in AP_SignalMasterTable.parquet**: Required for `dVolCall.py`. Currently set to `np.nan` - needs OptionMetrics-CRSP linking table to populate.

### Recommendations:
1. **For DivYieldST**: 
   - **VERIFICATION**: Test that all AP files actually contain the columns listed above by loading sample data
   - Verify `retx` (returns excluding dividends) is correctly calculated in `AP_monthlyCRSP.parquet`
   - Verify `prc` (price) is correctly populated in `AP_SignalMasterTable.parquet`
   - Verify distribution codes (`cd1`, `cd2`, `cd3`) are correctly assigned in `AP_CRSPdistributions.parquet`

2. **For dNoa**: 
   - **VERIFICATION**: Test that all AP files actually contain the columns listed above by loading sample data
   - Verify `mib` (minority interest) is correctly populated in `AP_m_aCompustat.parquet`
   - Verify all balance sheet components (`at`, `che`, `dltt`, `dlc`, `pstk`, `ceq`) are correctly populated

3. **For DolVol**: 
   - **VERIFICATION**: Test that `vol` (volume) and `prc` (price) are correctly populated in `AP_monthlyCRSP.parquet`
   - Verify volume data is available for the required historical period

4. **For DownRecomm**: 
   - **CRITICAL**: Run `AP_IBESRecommendations.py` to generate `AP_IBES_Recommendations.parquet`
   - Requires Eikon/LSEG API subscription (proprietary data source)
   - If Eikon is not available, this predictor cannot be constructed with AP data

5. **For dVolCall**: 
   - **CRITICAL**: Requires OptionMetrics data access (proprietary database)
   - Need to create `OptionMetricsVolSurf.csv` from OptionMetrics implied volatility data
   - Need to populate `secid` in `AP_SignalMasterTable.parquet` using OptionMetrics-CRSP linking table
   - Without OptionMetrics access, this predictor cannot be constructed
   - **Alternative**: Could potentially use free options data sources, but would require significant code adaptation

6. **Note on Proprietary Data Sources**: 
   - IBES recommendations (Eikon/LSEG) and OptionMetrics are proprietary databases requiring paid subscriptions
   - Without access to these databases, `DownRecomm` and `dVolCall` cannot be constructed with AP data
   - Consider alternative free data sources or skip these predictors if proprietary data is unavailable

7. **Note on Historical Data**: 
   - All predictors that can be constructed use basic balance sheet or market data that should be available
   - Verify sufficient historical coverage for predictors that use rolling windows or lags
   - `DivYieldST` uses 12-month rolling windows, `dNoa` uses 12-month lags, `DolVol` uses 2-month lags

# Group 13 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 13.

---

## 61. dVolPut.py

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
**YES** - Same issues as `dVolCall.py` (Group 12):

#### What Needs to Be Done:
1. **Create OptionMetricsVolSurf.csv**: 
   - This requires OptionMetrics data (proprietary database)
   - OptionMetrics provides implied volatility surface data for options
   - Need to process OptionMetrics data to create volatility surface with required dimensions
   - Script would need to:
     - Download OptionMetrics implied volatility data
     - Filter to specific `days` (30) and `delta` (50) for ATM options
     - Filter to put options (`cp_flag == "P"`)
     - Aggregate by `secid`, `time_avail_m`, `days`, `delta`, and `cp_flag`

2. **Populate `secid` in AP_SignalMasterTable.parquet**:
   - `secid` is OptionMetrics security identifier
   - Need to create linking table between CRSP `permno` and OptionMetrics `secid`
   - This typically requires OptionMetrics-CRSP linking table
   - Without `secid`, cannot merge options data with stock data

#### Implementation Notes:
- **For OptionMetricsVolSurf**: OptionMetrics is a proprietary database that requires subscription
- The predictor calculates change in put implied volatility: `dVolPut = impl_vol - l1_impl_vol`
- Without OptionMetrics access, this predictor cannot be constructed
- **Alternative**: Could potentially use free options data sources (e.g., CBOE, Yahoo Finance options), but would need significant adaptation of the data processing logic

- **For secid**: OptionMetrics uses `secid` as its primary identifier, which maps to CRSP `permno` via a linking table
- Without OptionMetrics-CRSP linking table, cannot populate `secid` in `AP_SignalMasterTable.parquet`
- Even if `OptionMetricsVolSurf.csv` exists, cannot merge without `secid`

---

## 62. EarningsConsistency.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `permno`, `time_avail_m`, `epspx`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `epspx` (earnings per share diluted) - mapped in XBRL_TAG_MAP line 293

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 63. EarningsForecastDisparity.py

### Required Columns:
- **AP_IBES_EPS_Unadj.parquet**: `tickerIBES`, `fpi`, `fpedats`, `statpers`, `meanest`
- **AP_IBES_UnadjustedActuals.parquet**: `tickerIBES`, `time_avail_m`, `fy0a`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `tickerIBES`

### AP File Status:
- ✅ **AP_IBES_EPS_Unadj.parquet**: 
  - File exists (from `AP_IBESEPSUnadjusted.py`)
  - Contains `tickerIBES` (from `AP_IBESEPSUnadjusted.py` line 253)
  - Contains `fpi` (forecast period indicator) - from `AP_IBESEPSUnadjusted.py` line 295
  - Contains `fpedats` (forecast period end date) - from `AP_IBESEPSUnadjusted.py` line 300
  - Contains `statpers` (statistical period) - from `AP_IBESEPSUnadjusted.py` line 294
  - Contains `meanest` (mean estimate) - from `AP_IBESEPSUnadjusted.py` line 298
- ⚠️ **AP_IBES_UnadjustedActuals.parquet**: 
  - Script exists: `AP_IBESUnadjustedActuals.py` (generates `AP_IBES_UnadjustedActuals.parquet`)
  - File may not exist yet - needs to be generated
  - Would contain `tickerIBES` (from `AP_IBESUnadjustedActuals.py` structure)
  - Would contain `time_avail_m` (from `AP_IBESUnadjustedActuals.py` line 301)
  - ⚠️ **`fy0a`**: **NOT EXTRACTED** - AP version only extracts `int0a` (actual EPS unadjusted), but original WRDS version has both `int0a` and `fy0a` columns. Predictor expects `fy0a`.
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `tickerIBES` column - should be populated from `AP_IBESCRSPLinkingTable.parquet` (after fix in Group 2)

### Can Be Constructed?
**PARTIALLY** - Missing `AP_IBES_UnadjustedActuals.parquet` and column name mismatch (`int0a` vs `fy0a`).

### Additional Work Needed?
**YES** - Multiple issues:

#### What Needs to Be Done:
1. **Run AP_IBESUnadjustedActuals.py**: Execute the script to generate `AP_IBES_UnadjustedActuals.parquet`
   - Requires Eikon/LSEG API subscription (proprietary data source)
   - `eikon` Python package installed
   - Eikon API credentials configured

2. **Column Name Mapping**: 
   - AP version only extracts `int0a` (actual EPS unadjusted) from Eikon
   - Original WRDS version has both `int0a` and `fy0a` columns (both appear to be actual EPS, possibly different fiscal periods)
   - Predictor expects `fy0a`
   - **Solution Options**:
     a. Check Eikon API for `fy0a` equivalent field and extract it
     b. Use `int0a` as substitute for `fy0a` if they're equivalent
     c. Modify `EarningsForecastDisparity.py` to use `int0a` instead of `fy0a`
   - **Recommended**: Check Eikon API documentation for `fy0a` equivalent, or verify if `int0a` can be used as substitute

#### Implementation Notes:
- **For AP_IBES_UnadjustedActuals**: The script exists and is ready to run
- Requires Eikon/LSEG API access (paid subscription)
- If Eikon is not available, this predictor cannot be constructed with AP data
- **Column Name Issue**: `int0a` and `fy0a` appear to be the same thing (actual EPS unadjusted), but the predictor expects `fy0a`
- Check original `IBES_UnadjustedActuals.py` to see if `fy0a` is derived from `int0a` or if they're the same

---

## 64. EarningsStreak.py

### Required Columns:
- **AP_IBES_EPS_Adj.parquet**: `tickerIBES`, `anndats_act`, `time_avail_m`, `fpi`, `actual`, `meanest`, `price`, `statpers`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `tickerIBES`

### AP File Status:
- ❌ **AP_IBES_EPS_Adj.parquet**: **FILE DOES NOT EXIST YET**
  - Script exists: `AP_IBESEPSAdjusted.py` (generates `AP_IBES_EPS_Adj.parquet`)
  - Would contain `tickerIBES` (from `AP_IBESEPSAdjusted.py` line 220)
  - Would contain `anndats_act` (actual announcement date) - from `AP_IBESEPSAdjusted.py` line 226
  - Would contain `time_avail_m` (from `AP_IBESEPSAdjusted.py` line 240)
  - ⚠️ **`fpi`**: Set to `1` (from `AP_IBESEPSAdjusted.py` line 243), but predictor filters for `fpi == "6"` (6-month ahead forecast). Script uses `Period="FQ1"` (next fiscal quarter) which corresponds to `fpi = 1`, not `fpi = 6`.
  - Would contain `actual` (actual earnings) - from `AP_IBESEPSAdjusted.py` line 225
  - Would contain `meanest` (mean estimate) - from `AP_IBESEPSAdjusted.py` line 221
  - Would contain `price` (price) - from `AP_IBESEPSAdjusted.py` line 229
  - Would contain `statpers` (statistical period) - from `AP_IBESEPSAdjusted.py` line 228
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `tickerIBES` column - should be populated from `AP_IBESCRSPLinkingTable.parquet` (after fix in Group 2)

### Can Be Constructed?
**NO** - Missing `AP_IBES_EPS_Adj.parquet` file.

### Additional Work Needed?
**YES** - Need to run `AP_IBESEPSAdjusted.py` to generate `AP_IBES_EPS_Adj.parquet`.

#### What Needs to Be Done:
1. **Run AP_IBESEPSAdjusted.py**: Execute the script to download IBES adjusted EPS data from Refinitiv Platform
2. **Requirements**:
   - Refinitiv Platform API access (proprietary data source)
   - `refinitiv-dataplatform` Python package installed
   - Refinitiv API credentials configured
3. **Output**: Generates `AP_IBES_EPS_Adj.parquet` with required columns
4. **Note on fpi**: The script sets `fpi = 1` (line 243), but the predictor filters for `fpi == "6"` (6-month forecast). May need to modify script to download multiple forecast periods or modify predictor to use `fpi == 1`.

#### Implementation Notes:
- The script `AP_IBESEPSAdjusted.py` exists and is ready to run
- Requires Refinitiv Platform API access (paid subscription)
- If Refinitiv is not available, this predictor cannot be constructed with AP data
- **Forecast Period Issue**: Script sets `fpi = 1` (1-year ahead), but predictor filters for `fpi == "6"` (6-month ahead). Need to verify if script can download multiple forecast periods or if predictor needs modification.

---

## 65. EarningsSurprise.py

### Required Columns:
- **AP_SignalMasterTable.parquet**: `permno`, `gvkey`, `time_avail_m`
- **AP_m_QCompustat.parquet**: `gvkey`, `time_avail_m`, `epspxq`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` line 119)
- ✅ **AP_m_QCompustat.parquet**: 
  - Contains `gvkey` (surrogate: CIK/permno) - from `AP_CompustatQuarterly.py` structure
  - Contains `time_avail_m` (from `AP_CompustatQuarterly.py` structure)
  - Contains `epspxq` (earnings per share quarterly) - mapped in XBRL_TAG_MAP line 205

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## Summary

| Predictor | All Columns Available? | Can Be Constructed? | Additional Work Needed? |
|-----------|----------------------|---------------------|----------------------|
| dVolPut | ❌ No | ❌ No | **Create OptionMetricsVolSurf.csv, populate secid** |
| EarningsConsistency | ✅ Yes | ✅ Yes | None |
| EarningsForecastDisparity | ⚠️ Partial | ⚠️ Partial | **Run AP_IBESUnadjustedActuals.py, fix column name (int0a→fy0a)** |
| EarningsStreak | ❌ No | ❌ No | **Run AP_IBESEPSAdjusted.py, verify fpi** |
| EarningsSurprise | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |

### Critical Missing Components:
1. **`OptionMetricsVolSurf.csv`**: Required for `dVolPut.py`. File does not exist - needs to be generated from OptionMetrics data (proprietary database).
2. **`secid` column in AP_SignalMasterTable.parquet**: Required for `dVolPut.py`. Currently set to `np.nan` - needs OptionMetrics-CRSP linking table.
3. **`AP_IBES_UnadjustedActuals.parquet`**: Required for `EarningsForecastDisparity.py`. File does not exist yet - needs to be generated by running `AP_IBESUnadjustedActuals.py`.
4. **`AP_IBES_EPS_Adj.parquet`**: Required for `EarningsStreak.py`. File does not exist yet - needs to be generated by running `AP_IBESEPSAdjusted.py`.

### Recommendations:
1. **For dVolPut**: 
   - **CRITICAL**: Requires OptionMetrics data access (proprietary database)
   - Need to create `OptionMetricsVolSurf.csv` from OptionMetrics implied volatility data
   - Need to populate `secid` in `AP_SignalMasterTable.parquet` using OptionMetrics-CRSP linking table
   - Without OptionMetrics access, this predictor cannot be constructed
   - **Alternative**: Could use free options data sources, but would require significant code adaptation

2. **For EarningsConsistency**: 
   - **VERIFICATION**: Test that `epspx` is correctly populated in `AP_m_aCompustat.parquet`
   - Verify sufficient historical coverage for 48-month rolling windows

3. **For EarningsForecastDisparity**: 
   - **CRITICAL**: Run `AP_IBESUnadjustedActuals.py` to generate `AP_IBES_UnadjustedActuals.parquet`
   - Requires Eikon/LSEG API subscription (proprietary data source)
   - **CRITICAL**: Fix column name mismatch - add `fy0a` column (or rename `int0a` to `fy0a`) in `AP_IBESUnadjustedActuals.py`
   - Verify `AP_IBES_EPS_Unadj.parquet` has both `fpi == "0"` (long-term growth) and `fpi == "1"` (1-year ahead) forecasts

4. **For EarningsStreak**: 
   - **CRITICAL**: Run `AP_IBESEPSAdjusted.py` to generate `AP_IBES_EPS_Adj.parquet`
   - Requires Refinitiv Platform API access (proprietary data source)
   - **IMPORTANT**: Forecast period mismatch:
     - Script sets `fpi = 1` and uses `Period="FQ1"` (next fiscal quarter) in Refinitiv API
     - Predictor filters for `fpi == "6"` (6-month ahead forecast)
     - **Solution Options**:
       a. Modify script to download multiple forecast periods (FQ0, FQ1, FQ2, etc.) and map to appropriate `fpi` values
       b. Modify predictor to use `fpi == 1` instead of `fpi == "6"`
       c. Check Refinitiv API for 6-month ahead forecast period equivalent
     - **Recommended**: Check Refinitiv API documentation for 6-month ahead forecast period, or modify predictor to use available `fpi` values

5. **For EarningsSurprise**: 
   - **VERIFICATION**: Test that `epspxq` is correctly populated in `AP_m_QCompustat.parquet`
   - Verify sufficient historical coverage for 24-month rolling windows (predictor uses lags up to 24 months)

6. **Note on Proprietary Data Sources**: 
   - OptionMetrics, Eikon/LSEG, and Refinitiv Platform are proprietary databases requiring paid subscriptions
   - Without access to these databases, `dVolPut`, `EarningsForecastDisparity`, and `EarningsStreak` cannot be constructed
   - Consider alternative free data sources or skip these predictors if proprietary data is unavailable

7. **Note on Column Name Mismatches**: 
   - `AP_IBES_UnadjustedActuals.parquet` only extracts `int0a` (actual EPS unadjusted)
   - Original WRDS version has both `int0a` and `fy0a` columns
   - Predictor expects `fy0a` - need to verify if `int0a` can be used as substitute or if `fy0a` needs to be extracted from Eikon
   - Check Eikon API documentation for `fy0a` equivalent field (may be fiscal year 0 actuals vs interim actuals)

8. **Note on Forecast Period Indicators**: 
   - `AP_IBESEPSAdjusted.py` sets `fpi = 1` and uses `Period="FQ1"` (next fiscal quarter) in Refinitiv API
   - `EarningsStreak.py` filters for `fpi == "6"` (6-month ahead forecast)
   - These may not be equivalent - need to verify Refinitiv API forecast period mapping or modify predictor/script accordingly

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


# Group 15 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 15.

---

## 71. ExclExp.py

### Required Columns:
- **AP_SignalMasterTable.parquet**: `permno`, `gvkey`, `time_avail_m`, `tickerIBES`
- **AP_m_QCompustat.parquet**: `gvkey`, `time_avail_m`, `epspiq`
- **AP_IBES_UnadjustedActuals.parquet**: `tickerIBES`, `time_avail_m`, `int0a`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` line 119)
  - Contains `tickerIBES` column - should be populated from `AP_IBESCRSPLinkingTable.parquet` (after fix in Group 2)
- ✅ **AP_m_QCompustat.parquet**: 
  - Contains `gvkey` (surrogate: CIK/permno) - from `AP_CompustatQuarterly.py` structure
  - Contains `time_avail_m` (from `AP_CompustatQuarterly.py` structure)
  - Contains `epspiq` (earnings per share basic quarterly) - mapped in XBRL_TAG_MAP line 206
- ⚠️ **AP_IBES_UnadjustedActuals.parquet**: 
  - Script exists: `AP_IBESUnadjustedActuals.py` (generates `AP_IBES_UnadjustedActuals.parquet`)
  - File may not exist yet - needs to be generated
  - Would contain `tickerIBES` (from `AP_IBESUnadjustedActuals.py` structure)
  - Would contain `time_avail_m` (from `AP_IBESUnadjustedActuals.py` line 301)
  - Would contain `int0a` (actual EPS unadjusted) - from `AP_IBESUnadjustedActuals.py` line 229, 254

### Can Be Constructed?
**PARTIALLY** - Missing `AP_IBES_UnadjustedActuals.parquet` file.

### Additional Work Needed?
**YES** - Need to run `AP_IBESUnadjustedActuals.py` to generate `AP_IBES_UnadjustedActuals.parquet`.

#### What Needs to Be Done:
1. **Run AP_IBESUnadjustedActuals.py**: Execute the script to download IBES unadjusted actual earnings from Eikon/LSEG API
2. **Requirements**:
   - Eikon/LSEG API access (proprietary data source)
   - `eikon` Python package installed
   - Eikon API credentials configured
3. **Output**: Generates `AP_IBES_UnadjustedActuals.parquet` with required columns

#### Implementation Notes:
- The script `AP_IBESUnadjustedActuals.py` exists and is ready to run
- Requires Eikon/LSEG API access (paid subscription)
- If Eikon is not available, this predictor cannot be constructed with AP data
- The predictor calculates excluded expenses as the difference between IBES unadjusted earnings (`int0a`) and Compustat quarterly EPS (`epspiq`)

---

## 72. FEPS.py

### Required Columns:
- **AP_IBES_EPS_Unadj.parquet**: `tickerIBES`, `time_avail_m`, `meanest`, `fpi`
- **AP_SignalMasterTable.parquet**: `permno`, `tickerIBES`, `time_avail_m`

### AP File Status:
- ✅ **AP_IBES_EPS_Unadj.parquet**: 
  - File exists (from `AP_IBESEPSUnadjusted.py`)
  - Contains `tickerIBES` (from `AP_IBESEPSUnadjusted.py` line 253)
  - Contains `time_avail_m` (from `AP_IBESEPSUnadjusted.py` line 277-278)
  - Contains `meanest` (mean estimate) - from `AP_IBESEPSUnadjusted.py` line 298
  - Contains `fpi` (forecast period indicator) - from `AP_IBESEPSUnadjusted.py` line 295
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `tickerIBES` column - should be populated from `AP_IBESCRSPLinkingTable.parquet` (after fix in Group 2)

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 73. fgr5yrLag.py

### Required Columns:
- **AP_IBES_EPS_Unadj.parquet**: `tickerIBES`, `time_avail_m`, `meanest`, `fpi`
- **AP_m_aCompustat.parquet**: `permno`, `time_avail_m`, `ceq`, `ib`, `txdi`, `dv`, `sale`, `ni`, `dp`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `tickerIBES`

### AP File Status:
- ✅ **AP_IBES_EPS_Unadj.parquet**: 
  - File exists (from `AP_IBESEPSUnadjusted.py`)
  - Contains `tickerIBES` (from `AP_IBESEPSUnadjusted.py` line 253)
  - Contains `time_avail_m` (from `AP_IBESEPSUnadjusted.py` line 277-278)
  - Contains `meanest` (mean estimate) - from `AP_IBESEPSUnadjusted.py` line 298
  - Contains `fpi` (forecast period indicator) - from `AP_IBESEPSUnadjusted.py` line 295
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184
  - Contains `ib` (income before extraordinary items) - mapped in XBRL_TAG_MAP line 262-264
  - Contains `txdi` (deferred income tax expense) - mapped in XBRL_TAG_MAP line 349-350, included in IS_FIELDS line 404
  - Contains `dv` (dividends) - mapped in XBRL_TAG_MAP line 333-334, included in CF_FIELDS line 411
  - Contains `sale` (sales) - mapped in XBRL_TAG_MAP line 238-240
  - Contains `ni` (net income) - mapped in XBRL_TAG_MAP line 265-267, included in IS_FIELDS line 403
  - Contains `dp` (depreciation) - mapped in XBRL_TAG_MAP line 245-247
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `tickerIBES` column - should be populated from `AP_IBESCRSPLinkingTable.parquet` (after fix in Group 2)

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 74. FirmAge.py

### Required Columns:
- **AP_SignalMasterTable.parquet**: `gvkey`, `permno`, `time_avail_m`, `exchcd`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` line 119)
  - Contains `exchcd` (exchange code) - from `AP_SignalMasterTable.py` line 130

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate, but predictor only uses it for filtering, not calculation).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 75. FirmAgeMom.py

### Required Columns:
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `ret`, `prc`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `ret` (return) - from `AP_SignalMasterTable.py` line 133
  - Contains `prc` (price) - from `AP_SignalMasterTable.py` line 132

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## Summary

| Predictor | All Columns Available? | Can Be Constructed? | Additional Work Needed? |
|-----------|----------------------|---------------------|----------------------|
| ExclExp | ⚠️ Partial | ⚠️ Partial | **Run AP_IBESUnadjustedActuals.py** |
| FEPS | ✅ Yes | ✅ Yes | None |
| fgr5yrLag | ✅ Yes | ✅ Yes | None |
| FirmAge | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |
| FirmAgeMom | ✅ Yes | ✅ Yes | None |

### Critical Missing Components:
1. **`AP_IBES_UnadjustedActuals.parquet`**: Required for `ExclExp.py`. File does not exist yet - needs to be generated by running `AP_IBESUnadjustedActuals.py`.

### Recommendations:
1. **For ExclExp**: 
   - **CRITICAL**: Run `AP_IBESUnadjustedActuals.py` to generate `AP_IBES_UnadjustedActuals.parquet`
   - Requires Eikon/LSEG API subscription (proprietary data source)
   - Verify `epspiq` is correctly populated in `AP_m_QCompustat.parquet`
   - Verify `tickerIBES` is correctly populated in `AP_SignalMasterTable.parquet` for merging

2. **For FEPS**: 
   - **VERIFICATION**: Test that `meanest` is correctly populated in `AP_IBES_EPS_Unadj.parquet`
   - Verify `tickerIBES` is correctly populated in `AP_SignalMasterTable.parquet`
   - Verify `fpi == "1"` filter works correctly (predictor filters for 1-year ahead forecasts)

3. **For fgr5yrLag**: 
   - **VERIFICATION**: Test that all required columns (`ceq`, `ib`, `txdi`, `dv`, `sale`, `ni`, `dp`) are correctly populated in `AP_m_aCompustat.parquet`
   - Verify `fpi == "0"` filter works correctly (predictor filters for long-term growth forecasts)
   - Verify `tickerIBES` is correctly populated in `AP_SignalMasterTable.parquet`
   - Note: Predictor restricts to June observations and expands to 12 monthly observations

4. **For FirmAge**: 
   - **VERIFICATION**: Test that `exchcd` is correctly populated in `AP_SignalMasterTable.parquet`
   - Verify sufficient historical coverage (predictor calculates months since first appearance)
   - Note: Predictor excludes firms that started trading when CRSP began (July 1926)

5. **For FirmAgeMom**: 
   - **VERIFICATION**: Test that `ret` and `prc` are correctly populated in `AP_SignalMasterTable.parquet`
   - Verify sufficient historical coverage for 6-month momentum calculation (requires at least 12 months of history)
   - Note: Predictor filters for stocks with price >= $5 and restricts to youngest quintile (bottom 20%) by age

6. **Note on gvkey Surrogate**: 
   - `ExclExp.py` and `FirmAge.py` use `gvkey` which is not directly available from EDGAR
   - AP version uses `cik` (SEC identifier) as surrogate or falls back to `permno`
   - This should work for most predictors, but verify linking logic if issues arise

7. **Note on IBES Data**: 
   - `ExclExp.py` and `FEPS.py` require IBES data
   - `AP_IBES_EPS_Unadj.parquet` exists and can be used for `FEPS.py`
   - `AP_IBES_UnadjustedActuals.parquet` needs to be generated for `ExclExp.py`
   - Both require `tickerIBES` to be populated in `AP_SignalMasterTable.parquet` (from `AP_IBESCRSPLinkingTable.parquet`)

8. **Note on Forecast Period Indicators**: 
   - `FEPS.py` filters for `fpi == "1"` (1-year ahead forecast)
   - `fgr5yrLag.py` filters for `fpi == "0"` (long-term growth forecast)
   - Verify `AP_IBES_EPS_Unadj.parquet` contains both forecast periods

# Group 16 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 16.

---

## 76. ForecastDispersion.py

### Required Columns:
- **AP_IBES_EPS_Unadj.parquet**: `tickerIBES`, `time_avail_m`, `stdev`, `meanest`, `fpi`, `fpedats`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `tickerIBES`

### AP File Status:
- ✅ **AP_IBES_EPS_Unadj.parquet**: 
  - File exists (from `AP_IBESEPSUnadjusted.py`)
  - Contains `tickerIBES` (from `AP_IBESEPSUnadjusted.py` line 253)
  - Contains `time_avail_m` (from `AP_IBESEPSUnadjusted.py` line 277-278)
  - Contains `stdev` (standard deviation of estimates) - from `AP_IBESEPSUnadjusted.py` line 259, 299
  - Contains `meanest` (mean estimate) - from `AP_IBESEPSUnadjusted.py` line 298
  - Contains `fpi` (forecast period indicator) - from `AP_IBESEPSUnadjusted.py` line 295
  - Contains `fpedats` (forecast period end date) - from `AP_IBESEPSUnadjusted.py` line 262, 300
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `tickerIBES` column - should be populated from `AP_IBESCRSPLinkingTable.parquet` (after fix in Group 2)

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 77. Frontier.py

### Required Columns:
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `mve_permco`, `sicCRSP`
- **AP_m_aCompustat.parquet**: `permno`, `time_avail_m`, `at`, `ceq`, `dltt`, `capx`, `sale`, `xrd`, `xad`, `ppent`, `ebitda`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `mve_permco` (market value per company) - from `AP_SignalMasterTable.py` line 128
  - Contains `sicCRSP` (SIC code from CRSP) - from `AP_SignalMasterTable.py` line 134
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 86
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184
  - Contains `dltt` (long-term debt) - mapped in XBRL_TAG_MAP line 142-144
  - Contains `capx` (capital expenditures) - mapped in XBRL_TAG_MAP line 314-316, included in CF_FIELDS line 410
  - Contains `sale` (sales) - mapped in XBRL_TAG_MAP line 230
  - Contains `xrd` (R&D expenses) - mapped in XBRL_TAG_MAP line 235-236
  - Contains `xad` (advertising expenses) - mapped in XBRL_TAG_MAP line 233-234
  - Contains `ppent` (property, plant, equipment net) - mapped in XBRL_TAG_MAP line 98
  - Contains `ebitda` (earnings before interest, taxes, depreciation, amortization) - derived in `AP_CompustatAnnual.py` line 999-1005

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 78. Governance.py

### Required Columns:
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `ticker`, `exchcd`
- **GovIndex.parquet**: `ticker`, `time_avail_m`, `G`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `ticker` (ticker symbol) - from `AP_SignalMasterTable.py` line 129
  - Contains `exchcd` (exchange code) - from `AP_SignalMasterTable.py` line 130
- ❌ **GovIndex.parquet**: **FILE DOES NOT EXIST**
  - No script found to generate this file
  - Would contain governance index scores (`G`) from Gompers-Ishii-Metrick (2003) dataset
  - This is a proprietary dataset that requires manual compilation or purchase

### Can Be Constructed?
**NO** - Missing `GovIndex.parquet` file.

### Additional Work Needed?
**YES** - Need to create or obtain `GovIndex.parquet`.

#### What Needs to Be Done:
1. **Obtain Governance Index Data**: 
   - The governance index (`G`) comes from Gompers, Ishii, and Metrick (2003) "Corporate Governance and Equity Prices"
   - This is a proprietary dataset that measures corporate governance quality
   - The index ranges from 5 (best governance) to 14 (worst governance)
   - Requires manual compilation from SEC proxy statements or purchase from data vendors

2. **Create GovIndex.parquet**: 
   - File should contain columns: `ticker`, `time_avail_m`, `G`
   - `ticker`: Stock ticker symbol
   - `time_avail_m`: Monthly availability date
   - `G`: Governance index score (integer, typically 5-14)
   - Data should be merged with `AP_SignalMasterTable.parquet` on `ticker` and `time_avail_m`

#### Implementation Notes:
- The governance index is based on 24 governance provisions from corporate charters and bylaws
- Original data covers 1990-1999 period, but may have been extended by other researchers
- Without access to this proprietary dataset, this predictor cannot be constructed
- **Alternative**: Could potentially construct a simplified governance index from publicly available proxy statement data, but would not match the original methodology

---

## 79. GP.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `revt`, `cogs`, `at`, `sic`, `datadate`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `revt` (total revenue) - mapped in XBRL_TAG_MAP line 232
  - Contains `cogs` (cost of goods sold) - mapped in XBRL_TAG_MAP line 235-236
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 86
  - ⚠️ **`sic`**: **NOT DIRECTLY EXTRACTED** from XBRL in `AP_CompustatAnnual.py`. However, `sicCRSP` is available in `AP_monthlyCRSP.parquet` and can be merged.
  - Contains `datadate` (data date) - renamed from `period_end` (from `AP_CompustatAnnual.py` line 927)

### Can Be Constructed?
**PARTIALLY** - Missing `sic` column in `AP_m_aCompustat.parquet`.

### Additional Work Needed?
**YES** - Need to merge `sic` from `AP_monthlyCRSP.parquet` or add `sic` extraction to `AP_CompustatAnnual.py`.

#### What Needs to Be Done:
1. **Option 1: Merge sic from AP_monthlyCRSP.parquet**:
   - `AP_monthlyCRSP.parquet` contains `sicCRSP` (from `AP_CRSPMonthly.py`)
   - Merge `sicCRSP` from `AP_monthlyCRSP.parquet` into `AP_m_aCompustat.parquet` based on `permno` and `time_avail_m`
   - Rename `sicCRSP` to `sic` for compatibility with predictor

2. **Option 2: Extract sic from XBRL**:
   - Add SIC code extraction to `AP_CompustatAnnual.py` from XBRL filings
   - SIC codes may be available in XBRL cover page or company facts
   - However, XBRL may not always have SIC codes, so merging from CRSP is more reliable

#### Implementation Notes:
- The predictor filters for non-financial firms: `sic < 6000 or sic >= 7000`
- `sicCRSP` from `AP_monthlyCRSP.parquet` should work as a substitute for `sic`
- **Recommended**: Merge `sicCRSP` from `AP_monthlyCRSP.parquet` into `AP_m_aCompustat.parquet` during monthly expansion process

---

## 80. GrAdExp.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `permno`, `time_avail_m`, `at`, `xad`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `mve_c`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 86
  - Contains `xad` (advertising expenses) - mapped in XBRL_TAG_MAP line 233-234
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `mve_c` (market value of equity) - from `AP_SignalMasterTable.py` line 127

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## Summary

| Predictor | All Columns Available? | Can Be Constructed? | Additional Work Needed? |
|-----------|----------------------|---------------------|----------------------|
| ForecastDispersion | ✅ Yes | ✅ Yes | None |
| Frontier | ✅ Yes | ✅ Yes | None |
| Governance | ❌ No | ❌ No | **Create/obtain GovIndex.parquet** |
| GP | ⚠️ Partial | ⚠️ Partial | **Merge sic from AP_monthlyCRSP** |
| GrAdExp | ✅ Yes | ✅ Yes | None |

### Critical Missing Components:
1. **`GovIndex.parquet`**: Required for `Governance.py`. File does not exist - requires proprietary governance index dataset from Gompers-Ishii-Metrick (2003).
2. **`sic` column in AP_m_aCompustat.parquet**: Required for `GP.py`. Not directly extracted from XBRL, but `sicCRSP` is available in `AP_monthlyCRSP.parquet` and can be merged.

### Recommendations:
1. **For ForecastDispersion**: 
   - **VERIFICATION**: Test that `stdev` and `meanest` are correctly populated in `AP_IBES_EPS_Unadj.parquet`
   - Verify `tickerIBES` is correctly populated in `AP_SignalMasterTable.parquet`
   - Verify `fpi == "1"` filter works correctly (predictor filters for 1-year ahead forecasts)
   - Verify `fpedats` is not null (predictor filters for records with valid forecast period end dates)

2. **For Frontier**: 
   - **VERIFICATION**: Test that all required columns (`at`, `ceq`, `dltt`, `capx`, `sale`, `xrd`, `xad`, `ppent`, `ebitda`) are correctly populated in `AP_m_aCompustat.parquet`
   - Verify `mve_permco` and `sicCRSP` are correctly populated in `AP_SignalMasterTable.parquet`
   - Note: Predictor uses 60-month rolling window regressions, requires sufficient historical coverage
   - Note: Predictor is computationally intensive - uses sklearn LinearRegression with industry dummies

3. **For Governance**: 
   - **CRITICAL**: Obtain or create `GovIndex.parquet` with governance index scores
   - Requires proprietary dataset from Gompers-Ishii-Metrick (2003)
   - File should contain: `ticker`, `time_avail_m`, `G` (governance index score, typically 5-14)
   - Without this dataset, predictor cannot be constructed
   - **Alternative**: Could construct simplified governance index from proxy statements, but would not match original methodology

4. **For GP**: 
   - **CRITICAL**: Merge `sic` (or `sicCRSP`) into `AP_m_aCompustat.parquet`
   - **Recommended**: Merge `sicCRSP` from `AP_monthlyCRSP.parquet` during monthly expansion process
   - Verify `revt`, `cogs`, and `at` are correctly populated
   - Note: Predictor filters for non-financial firms (`sic < 6000 or sic >= 7000`)

5. **For GrAdExp**: 
   - **VERIFICATION**: Test that `xad` and `at` are correctly populated in `AP_m_aCompustat.parquet`
   - Verify `mve_c` is correctly populated in `AP_SignalMasterTable.parquet`
   - Verify sufficient historical coverage for 12-month lag calculation
   - Note: Predictor filters for `xad >= 0.1` and excludes smallest size decile

6. **Note on gvkey Surrogate**: 
   - `GP.py` uses `gvkey` which is not directly available from EDGAR
   - AP version uses `cik` (SEC identifier) as surrogate or falls back to `permno`
   - This should work for most predictors, but verify linking logic if issues arise

7. **Note on sic vs sicCRSP**: 
   - `GP.py` expects `sic` from Compustat
   - AP version has `sicCRSP` from CRSP data
   - These should be equivalent for most firms, but may differ for some companies
   - **Recommended**: Use `sicCRSP` as substitute for `sic` in `GP.py`

8. **Note on ebitda**: 
   - `Frontier.py` requires `ebitda` which is derived in `AP_CompustatAnnual.py` (line 999-1005)
   - Derived as: `ebitda = ebit + dp + am` (with fallbacks)
   - Verify derivation logic produces correct values

9. **Note on Governance Index**: 
   - The governance index (`G`) is a proprietary measure from Gompers, Ishii, and Metrick (2003)
   - Based on 24 governance provisions from corporate charters and bylaws
   - Original dataset covers 1990-1999, but may have been extended
   - Without access to this dataset, `Governance.py` cannot be constructed
   - Consider alternative approaches if governance data is needed

# Group 17 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 17.

---

## 81. GrLTNOA.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `rect`, `invt`, `ppent`, `aco`, `intan`, `ao`, `ap`, `lco`, `lo`, `at`, `dp`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `rect` (accounts receivable) - mapped in XBRL_TAG_MAP line 93
  - Contains `invt` (inventory) - mapped in XBRL_TAG_MAP line 98
  - Contains `ppent` (property, plant, equipment net) - mapped in XBRL_TAG_MAP line 98
  - Contains `aco` (other current assets) - derived in `AP_CompustatAnnual.py` line 972-982
  - Contains `intan` (intangible assets) - mapped in XBRL_TAG_MAP line 114
  - Contains `ao` (other noncurrent assets) - derived in `AP_CompustatAnnual.py` line 984-990
  - Contains `ap` (accounts payable) - mapped in XBRL_TAG_MAP line 152-153
  - Contains `lco` (other current liabilities) - mapped in XBRL_TAG_MAP line 167-168
  - Contains `lo` (other noncurrent liabilities) - mapped in XBRL_TAG_MAP line 170
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 86
  - Contains `dp` (depreciation) - mapped in XBRL_TAG_MAP line 245-247

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate, but predictor only uses it for filtering, not calculation).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 82. GrSaleToGrInv.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `sale`, `invt`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `sale` (sales) - mapped in XBRL_TAG_MAP line 230
  - Contains `invt` (inventory) - mapped in XBRL_TAG_MAP line 98

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 83. GrSaleToGrOverhead.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `sale`, `xsga`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `sale` (sales) - mapped in XBRL_TAG_MAP line 230
  - Contains `xsga` (selling, general, administrative expenses) - mapped in XBRL_TAG_MAP line 239

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 84. Herf.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `permno`, `time_avail_m`, `sale`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `sicCRSP`, `shrcd`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `sale` (sales) - mapped in XBRL_TAG_MAP line 230
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `sicCRSP` (SIC code from CRSP) - from `AP_SignalMasterTable.py` line 134
  - Contains `shrcd` (share code) - from `AP_SignalMasterTable.py` line 131

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## 85. HerfAsset.py

### Required Columns:
- **AP_m_aCompustat.parquet**: `permno`, `time_avail_m`, `at`
- **AP_SignalMasterTable.parquet**: `permno`, `time_avail_m`, `sicCRSP`, `shrcd`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 86
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `sicCRSP` (SIC code from CRSP) - from `AP_SignalMasterTable.py` line 134
  - Contains `shrcd` (share code) - from `AP_SignalMasterTable.py` line 131

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

---

## Summary

| Predictor | All Columns Available? | Can Be Constructed? | Additional Work Needed? |
|-----------|----------------------|---------------------|----------------------|
| GrLTNOA | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |
| GrSaleToGrInv | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |
| GrSaleToGrOverhead | ✅ Yes* | ✅ Yes | None (*gvkey is surrogate) |
| Herf | ✅ Yes | ✅ Yes | None |
| HerfAsset | ✅ Yes | ✅ Yes | None |

### Critical Missing Components:
None. All predictors can be constructed with available AP data.

### Recommendations:
1. **For GrLTNOA**: 
   - **VERIFICATION**: Test that all required columns (`rect`, `invt`, `ppent`, `aco`, `intan`, `ao`, `ap`, `lco`, `lo`, `at`, `dp`) are correctly populated in `AP_m_aCompustat.parquet`
   - Verify `aco` and `ao` derivation logic produces correct values
   - Verify sufficient historical coverage for 12-month lag calculations
   - Note: Predictor calculates growth in long-term net operating assets with working capital adjustment

2. **For GrSaleToGrInv**: 
   - **VERIFICATION**: Test that `sale` and `invt` are correctly populated in `AP_m_aCompustat.parquet`
   - Verify sufficient historical coverage for 12-month and 24-month lag calculations
   - Note: Predictor uses primary formula with 12/24-month average baselines, falls back to 12-month growth if primary unavailable

3. **For GrSaleToGrOverhead**: 
   - **VERIFICATION**: Test that `sale` and `xsga` are correctly populated in `AP_m_aCompustat.parquet`
   - Verify sufficient historical coverage for 12-month and 24-month lag calculations
   - Note: Predictor uses primary formula with 12/24-month average baselines, falls back to 12-month growth if primary unavailable

4. **For Herf**: 
   - **VERIFICATION**: Test that `sale` is correctly populated in `AP_m_aCompustat.parquet`
   - Verify `sicCRSP` and `shrcd` are correctly populated in `AP_SignalMasterTable.parquet`
   - Verify sufficient historical coverage for 36-month rolling average (3-year moving average)
   - Note: Predictor calculates industry concentration (Herfindahl index) based on sales, excludes regulated industries and non-common stock

5. **For HerfAsset**: 
   - **VERIFICATION**: Test that `at` is correctly populated in `AP_m_aCompustat.parquet`
   - Verify `sicCRSP` and `shrcd` are correctly populated in `AP_SignalMasterTable.parquet`
   - Verify sufficient historical coverage for 36-month rolling average (3-year moving average)
   - Note: Predictor calculates industry concentration (Herfindahl index) based on assets, excludes regulated industries and non-common stock

6. **Note on gvkey Surrogate**: 
   - `GrLTNOA.py`, `GrSaleToGrInv.py`, and `GrSaleToGrOverhead.py` use `gvkey` which is not directly available from EDGAR
   - AP version uses `cik` (SEC identifier) as surrogate or falls back to `permno`
   - These predictors only use `gvkey` for filtering/grouping, not for calculations, so surrogate should work fine

7. **Note on Derived Fields**: 
   - `aco` (other current assets) is derived as: `aco = act - (che + rect + invt)`
   - `ao` (other noncurrent assets) is derived as: `ao = at - act - ppent - ivao - intan - gdwl - fatb - fatl`
   - These derivations may not perfectly match Compustat values, but should be sufficient for predictor construction
   - Verify derivation logic produces reasonable values

8. **Note on Historical Coverage**: 
   - `GrLTNOA.py` requires 12-month lags
   - `GrSaleToGrInv.py` and `GrSaleToGrOverhead.py` require 12-month and 24-month lags
   - `Herf.py` and `HerfAsset.py` require 36-month rolling averages (minimum 12 months)
   - Ensure sufficient historical data coverage for these calculations

9. **Note on Industry Classification**: 
   - `Herf.py` and `HerfAsset.py` use 4-digit SIC codes (`sic3D`) from `sicCRSP`
   - Both predictors exclude regulated industries (utilities, transportation, telecommunications) before deregulation dates
   - Both predictors exclude non-common stock (`shrcd > 11`)
   - Verify `sicCRSP` is correctly populated for industry grouping

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

# Group 20 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 20.

---

## 96. InvGrowth.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `invt`, `sic`, `ppent`, `at`
- **GNPdefl.parquet**: `time_avail_m`, `gnpdefl`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `invt` (inventory) - mapped in XBRL_TAG_MAP line 98
  - ✅ **`sic`**: **ADDED** - SIC code is added via `add_sic_codes()` function (from `AP_CompustatAnnual.py` line 1265)
  - Contains `ppent` (property, plant, equipment net) - mapped in XBRL_TAG_MAP line 105
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 84
- ✅ **AP_GNPdefl.parquet**: 
  - File exists (from `AP_GNPDeflator.py`)
  - Contains `time_avail_m` (from `AP_GNPDeflator.py` line 181)
  - Contains `gnpdefl` (GNP deflator) - from `AP_GNPDeflator.py` line 178

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate, sic has been added).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates year-over-year inventory growth using GNP deflator for inflation adjustment
- Excludes utilities (SIC 4xxx) and financial firms (SIC 6xxx)
- Excludes firms with non-positive total assets or property, plant & equipment
- Uses 12-month calendar-based lag for inventory values
- **Note**: `gvkey` is loaded but not used in calculations (only kept for compatibility)

---

## 97. IO_ShortInterest.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `gvkey`, `time_avail_m`
- **TR_13F.parquet**: `permno`, `time_avail_m`, `instown_perc`
- **monthlyCRSP.parquet**: `permno`, `time_avail_m`, `shrout`
- **monthlyShortInterest.parquet**: `gvkey`, `time_avail_m`, `shortint`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` line 119)
- ✅ **AP_TR_13F.parquet**: 
  - File exists (from `AP_InstitutionalHoldings13F.py`)
  - Contains `permno` (from `AP_InstitutionalHoldings13F.py` line 532)
  - Contains `time_avail_m` (from `AP_InstitutionalHoldings13F.py` line 532)
  - Contains `instown_perc` (institutional ownership percentage) - from `AP_InstitutionalHoldings13F.py` line 384, 532
- ✅ **AP_monthlyCRSP.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` structure)
  - Contains `shrout` (shares outstanding) - from `AP_CRSPMonthly.py` line 303
- ✅ **AP_monthlyShortInterest.parquet**: 
  - File exists (from `AP_CompustatShortInterest.py`)
  - Contains `gvkey` (surrogate: CIK/permno) - from `AP_CompustatShortInterest.py` line 442-448
  - Contains `time_avail_m` (from `AP_CompustatShortInterest.py` line 465)
  - Contains `shortint` (short interest) - from `AP_CompustatShortInterest.py` line 465

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor excludes stocks below 99th percentile of short interest, calculates institutional ownership for remaining stocks
- Calculates short ratio: `tempshortratio = shortint / shrout`
- Calculates 99th percentile of short ratio by month
- Sets `IO_ShortInterest` to `instown_perc` if `tempshortratio >= temps99`, otherwise NaN
- **Note**: `gvkey` is used for merging with short interest data, surrogate approach is acceptable

---

## 98. Leverage.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `lt`
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `mve_permco`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `lt` (total liabilities) - mapped in XBRL_TAG_MAP line 137
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `mve_permco` (market value of equity per company) - from `AP_SignalMasterTable.py` line 128

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates market leverage as total liabilities divided by market value of equity
- Formula: `Leverage = lt / mve_permco`
- **Note**: `gvkey` is loaded but not used in calculations (only kept for compatibility)

---

## 99. LRreversal.py

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
- Predictor calculates long-run reversal: stock return between months t-36 and t-13
- Compounds monthly returns over months t-36 to t-13 (24 months)
- Uses position-based lag (not calendar-based) via `groupby().shift()`
- Missing lagged values result in missing `LRreversal`

---

## 100. MaxRet.py

### Required Columns:
- **dailyCRSP.parquet**: `permno`, `time_d`, `ret`

### AP File Status:
- ✅ **AP_dailyCRSP.parquet**: 
  - Contains `permno` (from `AP_CRSPDaily.py` line 250)
  - Contains `time_d` (date) - from `AP_CRSPDaily.py` line 251
  - Contains `ret` (return) - from `AP_CRSPDaily.py` line 253

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates maximum of daily returns over the previous month
- Groups by `permno` and `time_avail_m` (monthly), takes maximum of `ret`
- Converts daily date to monthly using `to_period('M').to_timestamp()`

---

## Summary

### Overall Status:
**ALL 5 PREDICTORS CAN BE CONSTRUCTED** - All required columns are available in AP data sources.

### Column Availability:
- ✅ **SignalMasterTable columns**: `permno`, `time_avail_m`, `ret`, `gvkey` (surrogate), `mve_permco` - All available
- ✅ **Compustat columns**: `gvkey` (surrogate), `permno`, `time_avail_m`, `invt`, `sic` (added), `ppent`, `at`, `lt` - All available
- ✅ **GNP deflator**: `time_avail_m`, `gnpdefl` - Available in `AP_GNPdefl.parquet`
- ✅ **13F data**: `permno`, `time_avail_m`, `instown_perc` - Available in `AP_TR_13F.parquet`
- ✅ **Monthly CRSP**: `permno`, `time_avail_m`, `shrout` - Available in `AP_monthlyCRSP.parquet`
- ✅ **Short interest**: `gvkey` (surrogate), `time_avail_m`, `shortint` - Available in `AP_monthlyShortInterest.parquet`
- ✅ **Daily CRSP**: `permno`, `time_d`, `ret` - Available in `AP_dailyCRSP.parquet`

### Additional Notes:
1. **For InvGrowth**: 
   - **VERIFICATION**: Test that `sic` is correctly populated in `AP_m_aCompustat.parquet`
   - Verify that `GNPdefl.parquet` is available (or use `AP_GNPdefl.parquet`)
   - Verify sufficient historical coverage for 12-month lag calculation
   - Note: Predictor excludes utilities (SIC 4xxx) and financial firms (SIC 6xxx)

2. **For IO_ShortInterest**: 
   - **VERIFICATION**: Test that `instown_perc` is correctly populated in `AP_TR_13F.parquet`
   - Verify that `shortint` and `shrout` are correctly populated
   - Verify sufficient historical coverage (13F data typically available from ~2000 onwards, FINRA short interest from Dec 2017 onwards)
   - Note: Predictor calculates 99th percentile of short ratio by month

3. **For Leverage**: 
   - **VERIFICATION**: Test that `lt` and `mve_permco` are correctly populated
   - Verify sufficient historical coverage

4. **For LRreversal**: 
   - **VERIFICATION**: Test that `ret` is correctly populated in `AP_SignalMasterTable.parquet`
   - Verify sufficient historical coverage for 36-month lag calculation (requires at least 36 months of data)

5. **For MaxRet**: 
   - **VERIFICATION**: Test that `ret` is correctly populated in `AP_dailyCRSP.parquet`
   - Verify sufficient historical coverage for monthly maximum calculation

6. **Note on gvkey Surrogate**: 
   - `gvkey` is not directly available from EDGAR (Compustat-specific identifier)
   - AP version uses `cik` (SEC identifier) as surrogate, falls back to `permno` if unavailable
   - For `InvGrowth.py` and `Leverage.py`, `gvkey` is loaded but not used in calculations (only kept for compatibility)
   - For `IO_ShortInterest.py`, `gvkey` is used for merging with short interest data, surrogate approach is acceptable

7. **Note on SIC Codes**: 
   - `sic` is added to `AP_m_aCompustat.parquet` via `add_sic_codes()` function
   - SIC codes come from static mapping file (`ticker_sic_mapping.xlsx`) or heuristic mapping
   - May be approximate rather than official CRSP SIC codes, but should be sufficient for industry filtering

# Group 21 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 21.

---

## 101. MeanRankRevGrowth.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `revt`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `revt` (total revenue) - mapped in XBRL_TAG_MAP line 232

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates weighted average of revenue growth ranks over past 5 years
- Calculates 12-month revenue growth: `log(revt) - log(revt_lag12)`
- Creates monthly rankings of revenue growth
- Calculates weighted average rank using lags 12, 24, 36, 48, 60 months (weights: 5, 4, 3, 2, 1)
- **Note**: `gvkey` is loaded but not used in calculations (only kept for compatibility)

---

## 102. Mom12m.py

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
- Predictor calculates 12-month momentum: compounds monthly returns over months t-11 to t-1 (skipping current month t)
- Uses calendar-aware lag function (`stata_multi_lag`) to create lags 1-11
- Compounds returns: `(1 + ret_lag1) * (1 + ret_lag2) * ... * (1 + ret_lag11) - 1`
- Missing returns are filled with 0 for momentum calculations

---

## 103. Mom12mOffSeason.py

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
- Predictor calculates 12-month momentum excluding same calendar month (off-season momentum)
- Uses lags 1-10, excluding same calendar month as predicted month
- Averages returns from off-season lags: `mean(ret_lag1, ret_lag2, ..., ret_lag10)` excluding same calendar month
- Fills date gaps and missing returns with 0

---

## 104. Mom6m.py

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
- Predictor calculates 6-month momentum: compounds monthly returns from t-5 to t-1 (excludes current month t)
- Uses calendar-aware lag function (`stata_multi_lag`) to create lags 1-5
- Compounds returns: `(1 + ret_lag1) * (1 + ret_lag2) * ... * (1 + ret_lag5) - 1`
- Missing returns are filled with 0 for momentum calculations

---

## 105. Mom6mJunk.py

### Required Columns:
- **m_CIQ_creditratings.parquet**: `gvkey`, `ratingdate`, `source`, `currentratingnum`
- **SignalMasterTable.parquet**: `gvkey`, `permno`, `time_avail_m`, `ret`
- **m_SP_creditratings.parquet**: `gvkey`, `time_avail_m`, `credrat`

### AP File Status:
- ❌ **AP_m_CIQ_creditratings.parquet**: **FILE DOES NOT EXIST YET**
  - Script exists: `CIQCreditRatings.py` (non-AP version downloads from WRDS)
  - AP version would need to be created to download CIQ credit ratings data
  - CIQ credit ratings are typically available from proprietary sources (S&P Capital IQ, WRDS)
  - Would contain `gvkey`, `ratingdate`, `source`, `currentratingnum` (from `CIQCreditRatings.py` lines 165-179)
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `ret` (return) - from `AP_SignalMasterTable.py` line 133
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` line 119)
- ❌ **AP_m_SP_creditratings.parquet**: **FILE DOES NOT EXIST YET**
  - Script exists: `SPCreditRatings.py` (non-AP version downloads from WRDS)
  - AP version would need to be created to download S&P credit ratings data
  - S&P credit ratings are typically available from proprietary sources (S&P Capital IQ, WRDS)
  - Would contain `gvkey`, `time_avail_m`, `credrat` (from `SPCreditRatings.py` lines 78-79)

### Can Be Constructed?
**NO** - Missing both credit ratings files (`AP_m_CIQ_creditratings.parquet` and `AP_m_SP_creditratings.parquet`).

### Additional Work Needed?
**YES** - Need to create scripts to download credit ratings data.

#### What Needs to Be Done:
1. **Create AP_CIQ_creditratings.py**: 
   - Download CIQ credit ratings from proprietary source (S&P Capital IQ, WRDS)
   - Process to create monthly time series with `gvkey`, `ratingdate`, `source`, `currentratingnum`
   - Requires proprietary data access
   - Non-AP version (`CIQCreditRatings.py`) downloads from WRDS `ciq.wrds_erating`, `ciq.wrds_irating`, `ciq.wrds_srating` tables

2. **Create AP_SP_creditratings.py**: 
   - Download S&P credit ratings from proprietary source (S&P Capital IQ, WRDS)
   - Process to create monthly time series with `gvkey`, `time_avail_m`, `credrat`
   - Requires proprietary data access
   - Non-AP version (`SPCreditRatings.py`) downloads from WRDS `comp.adsprate` table

#### Implementation Notes:
- **For Credit Ratings**: S&P and CIQ credit ratings are proprietary data sources that require paid subscriptions
- Without access to these databases, this predictor cannot be constructed
- **Alternative**: Could potentially use free credit rating sources (e.g., SEC filings, company websites), but would require significant data engineering and may have lower coverage/quality
- **Note on gvkey**: Credit ratings data typically uses `gvkey` as identifier. The AP version uses surrogate `gvkey` (CIK/permno), which may not match exactly with credit ratings databases
- Predictor calculates 6-month momentum for junk-rated stocks (credit rating <= 14)
- Uses SP ratings by default, CIQ as fallback
- Forward-fills missing credit ratings with most recent rating

---

## Summary

### Overall Status:
**4 OUT OF 5 PREDICTORS CAN BE CONSTRUCTED** - One predictor (`Mom6mJunk.py`) requires credit ratings data that is not available in AP versions.

### Column Availability:
- ✅ **SignalMasterTable columns**: `permno`, `time_avail_m`, `ret`, `gvkey` (surrogate) - All available
- ✅ **Compustat columns**: `gvkey` (surrogate), `permno`, `time_avail_m`, `revt` - All available
- ❌ **Credit ratings**: `AP_m_CIQ_creditratings.parquet` and `AP_m_SP_creditratings.parquet` - **NOT AVAILABLE** (proprietary data sources)

### Additional Notes:
1. **For MeanRankRevGrowth**: 
   - **VERIFICATION**: Test that `revt` is correctly populated in `AP_m_aCompustat.parquet`
   - Verify sufficient historical coverage for 60-month lag calculation (requires at least 5 years of data)
   - Note: Predictor calculates weighted average rank over past 5 years

2. **For Mom12m, Mom12mOffSeason, Mom6m**: 
   - **VERIFICATION**: Test that `ret` is correctly populated in `AP_SignalMasterTable.parquet`
   - Verify sufficient historical coverage for lag calculations (6-month for Mom6m, 12-month for Mom12m)
   - Note: All use calendar-aware lag functions for proper time-series handling

3. **For Mom6mJunk**: 
   - **CRITICAL**: Cannot be constructed without credit ratings data
   - Requires proprietary access to S&P Capital IQ or WRDS credit ratings databases
   - **Alternative**: Could potentially scrape credit ratings from SEC filings or company websites, but would require significant data engineering

4. **Note on gvkey Surrogate**: 
   - `gvkey` is not directly available from EDGAR (Compustat-specific identifier)
   - AP version uses `cik` (SEC identifier) as surrogate, falls back to `permno` if unavailable
   - For `MeanRankRevGrowth.py`, `gvkey` is loaded but not used in calculations (only kept for compatibility)
   - For `Mom6mJunk.py`, `gvkey` is used for merging with credit ratings data, but surrogate approach may not match credit ratings databases exactly

5. **Note on Credit Ratings Data**: 
   - Credit ratings are proprietary data typically available from S&P Capital IQ or WRDS
   - Free alternatives (SEC filings, company websites) may have lower coverage/quality
   - Would require significant data engineering to create AP versions


# Group 22 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 22.

---

## 106. MomOffSeason.py

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
- Predictor calculates off-season momentum (years 2-5): averages returns from years 2-5 before predicted month, excluding same calendar month
- Uses lags 12-59 months, excluding same calendar month as predicted month
- A lag represents same month when `(lag + 1) % 12 == 0`
- Fills date gaps and missing returns with 0
- Uses calendar-aware lag function (`stata_multi_lag`) for proper time-series handling

---

## 107. MomOffSeason06YrPlus.py

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
- Predictor calculates off-season momentum (years 6-10): averages returns from years 6-10 before predicted month, excluding same calendar month
- Uses lags 60-119 months, excluding same calendar month as predicted month
- A lag represents same month when `(lag + 1) % 12 == 0`
- Fills date gaps and missing returns with 0
- Uses calendar-aware lag function (`stata_multi_lag`) for proper time-series handling

---

## 108. MomOffSeason11YrPlus.py

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
- Predictor calculates off-season momentum (years 11-15): averages returns from years 11-15 before predicted month, excluding same calendar month
- Uses lags 120-179 months, excluding same calendar month as predicted month
- A lag represents same month when `(lag + 1) % 12 == 0`
- Fills date gaps and missing returns with 0
- Uses calendar-aware lag function (`stata_multi_lag`) for proper time-series handling

---

## 109. MomOffSeason16YrPlus.py

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
- Predictor calculates off-season momentum (years 16-20): averages returns from years 16-20 before predicted month, excluding same calendar month
- Uses lags 180-239 months, excluding same calendar month as predicted month
- A lag represents same month when `(lag + 1) % 12 == 0`
- Fills date gaps and missing returns with 0
- Uses calendar-aware lag function (`stata_multi_lag`) for proper time-series handling

---

## 110. MomRev.py

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
- Predictor combines 6-month momentum with 36-month reversal patterns
- Calculates `Mom6m`: compounds returns over months t-5 to t-1 (lags 1-5)
- Calculates `Mom36m`: compounds returns over months t-36 to t-13 (lags 13-36, skips recent 12 months)
- Ranks stocks into quintiles (1-5) within each month based on `Mom6m` and `Mom36m`
- Goes long (`MomRev = 1`): top quintile for 6m momentum AND bottom quintile for 36m momentum
- Goes short (`MomRev = 0`): bottom quintile for 6m momentum AND top quintile for 36m momentum
- Uses calendar-aware lag function (`stata_multi_lag`) for proper time-series handling

---

## Summary

### Overall Status:
**ALL 5 PREDICTORS CAN BE CONSTRUCTED** - All required columns are available in AP data sources.

### Column Availability:
- ✅ **SignalMasterTable columns**: `permno`, `time_avail_m`, `ret` - All available

### Additional Notes:
1. **For MomOffSeason, MomOffSeason06YrPlus, MomOffSeason11YrPlus, MomOffSeason16YrPlus**: 
   - **VERIFICATION**: Test that `ret` is correctly populated in `AP_SignalMasterTable.parquet`
   - Verify sufficient historical coverage for long-term lag calculations:
     - `MomOffSeason`: requires at least 5 years of data (60 months)
     - `MomOffSeason06YrPlus`: requires at least 10 years of data (120 months)
     - `MomOffSeason11YrPlus`: requires at least 15 years of data (180 months)
     - `MomOffSeason16YrPlus`: requires at least 20 years of data (240 months)
   - Note: All use calendar-aware lag functions and exclude same calendar month returns

2. **For MomRev**: 
   - **VERIFICATION**: Test that `ret` is correctly populated in `AP_SignalMasterTable.parquet`
   - Verify sufficient historical coverage for 36-month lag calculation (requires at least 36 months of data)
   - Note: Predictor uses quintile rankings within each month, so needs sufficient cross-sectional coverage

3. **Note on Historical Coverage**: 
   - Predictors requiring very long historical windows (10-20 years) may have limited coverage in AP data sources
   - yfinance data typically has good coverage from ~2000 onwards, but may be incomplete for earlier periods
   - **Recommended**: Verify date range coverage for predictors requiring long historical windows

4. **Note on Off-Season Momentum**: 
   - All off-season momentum predictors exclude same calendar month returns to avoid seasonal effects
   - This is important for capturing true momentum vs. calendar effects
   - Uses `(lag + 1) % 12 == 0` to identify same calendar month


# Group 23 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 23.

---

## 111. MomSeason.py

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
- Predictor calculates seasonal momentum (years 2-5): averages returns from 23, 35, 47, 59 months ago
- Uses specific lag periods: 23, 35, 47, 59 months (same calendar month from previous years)
- Calculates average: `sum(ret_lag23, ret_lag35, ret_lag47, ret_lag59) / count(non-missing)`
- Missing returns are filled with 0 for momentum calculations
- Uses calendar-aware lag function (`stata_multi_lag`) for proper time-series handling

---

## 112. MomSeason06YrPlus.py

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
- Predictor calculates seasonal momentum (years 6-10): averages returns from 71, 83, 95, 107, 119 months ago
- Uses specific lag periods: 71, 83, 95, 107, 119 months (same calendar month from previous years)
- Calculates average: `sum(ret_lag71, ret_lag83, ret_lag95, ret_lag107, ret_lag119) / count(non-missing)`
- Missing returns are filled with 0 for momentum calculations
- Uses calendar-aware lag function (`stata_multi_lag`) for proper time-series handling

---

## 113. MomSeason11YrPlus.py

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
- Predictor calculates seasonal momentum (years 11-15): averages returns from 131, 143, 155, 167, 179 months ago
- Uses specific lag periods: 131, 143, 155, 167, 179 months (same calendar month from previous years)
- Calculates average: `sum(ret_lag131, ret_lag143, ret_lag155, ret_lag167, ret_lag179) / count(non-missing)`
- Missing returns are filled with 0 for momentum calculations
- Uses calendar-aware lag function (`stata_multi_lag`) for proper time-series handling

---

## 114. MomSeason16YrPlus.py

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
- Predictor calculates seasonal momentum (years 16-20): averages returns from 191, 203, 215, 227, 239 months ago
- Uses specific lag periods: 191, 203, 215, 227, 239 months (same calendar month from previous years)
- Calculates average: `sum(ret_lag191, ret_lag203, ret_lag215, ret_lag227, ret_lag239) / count(non-missing)`
- Missing returns are filled with 0 for momentum calculations
- Uses calendar-aware lag function (`stata_multi_lag`) for proper time-series handling

---

## 115. MomSeasonShort.py

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
- Predictor calculates short-term seasonal momentum: uses 11-month lagged return
- Uses lag period: 11 months (same calendar month from previous year)
- Simply assigns: `MomSeasonShort = ret_lag11`
- Missing returns are filled with 0 for momentum calculations
- Uses calendar-aware lag function (`stata_multi_lag`) for proper time-series handling

---

## Summary

### Overall Status:
**ALL 5 PREDICTORS CAN BE CONSTRUCTED** - All required columns are available in AP data sources.

### Column Availability:
- ✅ **SignalMasterTable columns**: `permno`, `time_avail_m`, `ret` - All available

### Additional Notes:
1. **For MomSeason, MomSeason06YrPlus, MomSeason11YrPlus, MomSeason16YrPlus**: 
   - **VERIFICATION**: Test that `ret` is correctly populated in `AP_SignalMasterTable.parquet`
   - Verify sufficient historical coverage for long-term lag calculations:
     - `MomSeason`: requires at least 59 months (~5 years) of data
     - `MomSeason06YrPlus`: requires at least 119 months (~10 years) of data
     - `MomSeason11YrPlus`: requires at least 179 months (~15 years) of data
     - `MomSeason16YrPlus`: requires at least 239 months (~20 years) of data
   - Note: All use specific lag periods that correspond to same calendar month from previous years (seasonal pattern)

2. **For MomSeasonShort**: 
   - **VERIFICATION**: Test that `ret` is correctly populated in `AP_SignalMasterTable.parquet`
   - Verify sufficient historical coverage for 11-month lag calculation (requires at least 11 months of data)
   - Note: Uses single 11-month lag (same calendar month from previous year)

3. **Note on Historical Coverage**: 
   - Predictors requiring very long historical windows (10-20 years) may have limited coverage in AP data sources
   - yfinance data typically has good coverage from ~2000 onwards, but may be incomplete for earlier periods
   - **Recommended**: Verify date range coverage for predictors requiring long historical windows

4. **Note on Seasonal Momentum**: 
   - All seasonal momentum predictors use returns from the same calendar month in previous years
   - This captures calendar/seasonal effects (e.g., January effect, year-end effects)
   - Lag periods are multiples of 12 months (plus small offset) to capture same calendar month
   - Uses average of multiple years to reduce noise and capture persistent seasonal patterns

5. **Note on Calculation Method**: 
   - Seasonal momentum predictors calculate average as: `sum(lagged_returns) / count(non-missing)`
   - This handles missing values gracefully by only averaging available returns
   - If all lagged returns are missing, the result is NaN (not 0)


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


# Group 25 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 25.

---

## 121. NetEquityFinance.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `sstk`, `prstkc`, `at`, `dv`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `sstk` (stock issuance) - mapped in XBRL_TAG_MAP line 222-223
  - Contains `prstkc` (stock repurchases) - mapped in XBRL_TAG_MAP line 217-218
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 84
  - Contains `dv` (dividends) - mapped in XBRL_TAG_MAP line 340

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates net equity financing scaled by average total assets
- Formula: `NetEquityFinance = (sstk - prstkc - dv) / (0.5 * (at + l12_at))`
- Uses 12-month lag of total assets
- Removes extreme values (absolute value greater than 1)
- **Note**: `gvkey` is loaded but not used in calculations (only kept for compatibility)

---

## 122. NetPayoutYield.py

### Required Columns:
- **m_aCompustat.parquet**: `permno`, `time_avail_m`, `dvc`, `prstkc`, `sstk`, `sic`, `ceq`
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `mve_permco`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `dvc` (common dividends) - mapped in XBRL_TAG_MAP line 336
  - Contains `prstkc` (stock repurchases) - mapped in XBRL_TAG_MAP line 217-218
  - Contains `sstk` (stock issuance) - mapped in XBRL_TAG_MAP line 222-223
  - ✅ **`sic`**: **ADDED** - SIC code has been added to `AP_m_aCompustat.parquet`
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `mve_permco` (market value of equity per company) - from `AP_SignalMasterTable.py` line 128

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates net payout yield scaled by lagged market value of equity
- Formula: `NetPayoutYield = (dvc + prstkc - sstk) / mve_permco_l6`
- Uses 6-month calendar-based lag of market value
- Filters out financial firms (SIC 6000-6999) and requires positive book equity
- Requires at least 24 observations per firm for stability

---

## 123. NOA.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `at`, `che`, `dltt`, `mib`, `dc`, `ceq`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 84
  - Contains `che` (cash) - mapped in XBRL_TAG_MAP line 88
  - Contains `dltt` (long-term debt) - mapped in XBRL_TAG_MAP line 142-144
  - Contains `mib` (minority interest) - mapped in XBRL_TAG_MAP line 182
  - ⚠️ **`dc`**: **ZERO-FILLED** - Deferred charges is listed as derived field (line 382: `['dcpstk', 'pstk', 'dcvt']`) but not implemented, currently zero-filled if missing
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate, dc is zero-filled).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates Net Operating Assets: `NOA = (OA - OL) / l12_at` where:
  - `OA = at - che` (Operating Assets)
  - `OL = at - dltt - mib - dc - ceq` (Operating Liabilities)
- Uses 12-month lag of total assets
- **Note**: `dc` is zero-filled, which may affect accuracy for firms with convertible debt, but predictor can still be constructed
- **Note**: `gvkey` is loaded but not used in calculations (only kept for compatibility)

---

## 124. NumEarnIncrease.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `gvkey`, `time_avail_m`
- **m_QCompustat.parquet**: `gvkey`, `time_avail_m`, `ibq`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` line 119)
- ✅ **AP_m_QCompustat.parquet**: 
  - Contains `gvkey` (surrogate: CIK/permno) - from `AP_CompustatQuarterly.py` structure
  - Contains `time_avail_m` (from `AP_CompustatQuarterly.py` structure)
  - Contains `ibq` (income before extraordinary items quarterly) - mapped in XBRL_TAG_MAP line 202

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor counts consecutive quarters with positive earnings growth, up to 8 quarters
- Calculates earnings change: `chearn = ibq - l12_ibq` (year-over-year change)
- Uses calendar-based lags for quarterly data (3, 6, 9, 12, 15, 18, 21, 24 months)
- Sets `NumEarnIncrease` to 1-8 based on consecutive positive earnings growth quarters
- Missing earnings growth is treated as positive (conservative approach)

---

## 125. OperProf.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `gvkey`, `time_avail_m`, `mve_c`
- **m_aCompustat.parquet**: `gvkey`, `time_avail_m`, `revt`, `cogs`, `xsga`, `xint`, `ceq`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `mve_c` (market value of equity) - from `AP_SignalMasterTable.py` line 127
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` line 119)
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `gvkey` (surrogate: CIK/permno) - from `AP_CompustatAnnual.py` structure
  - Contains `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `revt` (total revenue) - mapped in XBRL_TAG_MAP line 232
  - Contains `cogs` (cost of goods sold) - mapped in XBRL_TAG_MAP line 235-236
  - Contains `xsga` (selling, general, administrative expenses) - mapped in XBRL_TAG_MAP line 239
  - Contains `xint` (interest expense) - mapped in XBRL_TAG_MAP line 244-245
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates operating profitability scaled by book equity
- Formula: `OperProf = (revt - cogs - xsga - xint) / ceq`
- Excludes smallest size tercile (simulates NYSE size breakpoints)
- Creates size terciles by `time_avail_m` and sets `OperProf` to missing for smallest tercile

---

## Summary

### Overall Status:
**ALL 5 PREDICTORS CAN BE CONSTRUCTED** - All required columns are available in AP data sources.

### Column Availability:
- ✅ **SignalMasterTable columns**: `permno`, `time_avail_m`, `gvkey` (surrogate), `mve_permco`, `mve_c` - All available
- ✅ **Compustat annual columns**: `gvkey` (surrogate), `permno`, `time_avail_m`, `sstk`, `prstkc`, `at`, `dv`, `dvc`, `sic` (added), `ceq`, `che`, `dltt`, `mib`, `dc` (zero-filled), `revt`, `cogs`, `xsga`, `xint` - All available
- ✅ **Compustat quarterly columns**: `gvkey` (surrogate), `time_avail_m`, `ibq` - All available

### Additional Notes:
1. **For NetEquityFinance**: 
   - **VERIFICATION**: Test that `sstk`, `prstkc`, `dv`, `at` are correctly populated
   - Verify sufficient historical coverage for 12-month lag calculation

2. **For NetPayoutYield**: 
   - **VERIFICATION**: Test that `dvc`, `prstkc`, `sstk`, `sic`, `ceq`, `mve_permco` are correctly populated
   - Verify sufficient historical coverage for 6-month lag calculation
   - Note: Requires at least 24 observations per firm

3. **For NOA**: 
   - **VERIFICATION**: Test that `at`, `che`, `dltt`, `mib`, `dc`, `ceq` are correctly populated
   - Verify sufficient historical coverage for 12-month lag calculation
   - **Note**: `dc` is zero-filled, which may affect accuracy for firms with convertible debt, but predictor can still be constructed

4. **For NumEarnIncrease**: 
   - **VERIFICATION**: Test that `ibq` is correctly populated in `AP_m_QCompustat.parquet`
   - Verify sufficient historical coverage for 24-month lag calculation (requires at least 2 years of quarterly data)
   - Note: Uses quarterly data with calendar-based lags

5. **For OperProf**: 
   - **VERIFICATION**: Test that `revt`, `cogs`, `xsga`, `xint`, `ceq`, `mve_c` are correctly populated
   - Verify sufficient cross-sectional coverage for size tercile calculation

6. **Note on gvkey Surrogate**: 
   - `gvkey` is not directly available from EDGAR (Compustat-specific identifier)
   - AP version uses `cik` (SEC identifier) as surrogate, falls back to `permno` if unavailable
   - For `NetEquityFinance.py` and `NOA.py`, `gvkey` is loaded but not used in calculations (only kept for compatibility)
   - For `NumEarnIncrease.py` and `OperProf.py`, `gvkey` is used for merging with Compustat data, surrogate approach is acceptable

7. **Note on dc (Deferred Charges)**: 
   - `dc` is listed as a derived field in `AP_CompustatAnnual.py` (line 382: `['dcpstk', 'pstk', 'dcvt']`) but derivation logic is not implemented
   - Currently zero-filled if missing (from `zero_fill_vars` list)
   - For `NOA.py`, `dc` is used in calculation: `OL = at - dltt - mib - dc - ceq`
   - Zero-filling `dc` may affect accuracy for firms with convertible debt, but predictor can still be constructed
   - **Optional Enhancement**: Implement derivation logic for `dc` from `dcpstk`, `pstk`, and `dcvt` if available


# Group 26 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 26.

---

## 126. OperProfRD.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `gvkey`, `time_avail_m`, `exchcd`, `sicCRSP`, `mve_c`, `shrcd`
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `xrd`, `revt`, `cogs`, `xsga`, `at`, `ceq`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` line 119)
  - Contains `exchcd` (exchange code) - from `AP_SignalMasterTable.py` line 130
  - Contains `sicCRSP` (SIC code from CRSP) - from `AP_SignalMasterTable.py` line 134
  - Contains `mve_c` (market value of equity) - from `AP_SignalMasterTable.py` line 127
  - Contains `shrcd` (share code) - from `AP_CRSPMonthly.py` line 353, included in `AP_SignalMasterTable.py` line 131
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `gvkey` (surrogate: CIK/permno) - from `AP_CompustatAnnual.py` structure
  - Contains `xrd` (R&D expenses) - mapped in XBRL_TAG_MAP line 250-252
  - Contains `revt` (total revenue) - mapped in XBRL_TAG_MAP line 232
  - Contains `cogs` (cost of goods sold) - mapped in XBRL_TAG_MAP line 235-236
  - Contains `xsga` (selling, general, administrative expenses) - mapped in XBRL_TAG_MAP line 239
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 84
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates R&D-adjusted operating profitability scaled by total assets
- Formula: `OperProfRD = (revt - cogs - xsga + xrd) / at`
- Handles missing R&D by setting to zero
- Filters: `shrcd <= 11`, excludes financial firms (SIC 6000-6999), requires non-missing `mve_c`, `ceq`, `at`

---

## 127. OPLeverage.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `xsga`, `cogs`, `at`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `xsga` (selling, general, administrative expenses) - mapped in XBRL_TAG_MAP line 239
  - Contains `cogs` (cost of goods sold) - mapped in XBRL_TAG_MAP line 235-236
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 84

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates operating leverage: sum of administrative expenses and cost of goods sold, scaled by total assets
- Formula: `OPLeverage = (xsga + cogs) / at`
- Sets missing SGA expenses to zero
- **Note**: `gvkey` is loaded but not used in calculations (only kept for compatibility)

---

## 128. OrderBacklog.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `ob`, `at`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - ⚠️ **`ob`**: **ZERO-FILLED** - Order backlog is not GAAP, not reliably mappable from XBRL (line 185: "Not GAAP, not reliably mappable"), currently zero-filled if missing (from `zero_fill_vars` list line 1032)
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 84

### Can Be Constructed?
**PARTIALLY** - `ob` is zero-filled, which will result in `OrderBacklog = 0` for all observations (since `ob / (0.5 * (at + l12_at))` with `ob = 0` gives 0, and predictor sets to missing if `ob == 0`).

### Additional Work Needed?
**YES** - `ob` (order backlog) is zero-filled, which means the predictor will have no valid observations.

#### What Needs to Be Done:
1. **Extract ob from XBRL**: 
   - Order backlog is not a standard GAAP item and may not be reliably mappable from XBRL
   - May need to search for custom XBRL tags or extract from footnotes/narrative sections
   - Alternative: Extract from MD&A (Management Discussion & Analysis) text sections of 10-K filings

2. **Alternative Data Source**: 
   - Order backlog may be reported in company earnings calls or press releases
   - Would require text mining or manual data collection
   - May have limited historical coverage

#### Implementation Notes:
- Predictor calculates order backlog scaled by average total assets
- Formula: `OrderBacklog = ob / (0.5 * (at + l12_at))`
- Sets to missing if `ob == 0`
- **Current Issue**: Since `ob` is zero-filled, all `OrderBacklog` values will be set to missing (predictor filters out `ob == 0`)
- **Note**: `gvkey` is loaded but not used in calculations (only kept for compatibility)

---

## 129. OrderBacklogChg.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `ob`, `at`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - ⚠️ **`ob`**: **ZERO-FILLED** - Order backlog is not GAAP, not reliably mappable from XBRL (line 185: "Not GAAP, not reliably mappable"), currently zero-filled if missing (from `zero_fill_vars` list line 1032)
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 84

### Can Be Constructed?
**PARTIALLY** - `ob` is zero-filled, which will result in `OrderBacklogChg = 0` for all observations (since `OrderBacklog` will be missing when `ob == 0`).

### Additional Work Needed?
**YES** - `ob` (order backlog) is zero-filled, which means the predictor will have no valid observations.

#### What Needs to Be Done:
1. **Extract ob from XBRL**: 
   - Order backlog is not a standard GAAP item and may not be reliably mappable from XBRL
   - May need to search for custom XBRL tags or extract from footnotes/narrative sections
   - Alternative: Extract from MD&A (Management Discussion & Analysis) text sections of 10-K filings

2. **Alternative Data Source**: 
   - Order backlog may be reported in company earnings calls or press releases
   - Would require text mining or manual data collection
   - May have limited historical coverage

#### Implementation Notes:
- Predictor calculates change in normalized order backlog
- Formula: `OrderBacklogChg = OrderBacklog - OrderBacklog_lag12`
- Where `OrderBacklog = ob / (0.5 * (at + l12_at))`
- Sets to missing if `ob == 0`
- **Current Issue**: Since `ob` is zero-filled, all `OrderBacklog` values will be set to missing, resulting in no valid `OrderBacklogChg` observations
- **Note**: `gvkey` is loaded but not used in calculations (only kept for compatibility)

---

## 130. OScore.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `fopt`, `at`, `lt`, `act`, `lct`, `ib`, `oancf`, `sic`
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `prc`
- **GNPdefl.parquet**: `time_avail_m`, `gnpdefl`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `fopt` (foreign operations income) - mapped in XBRL_TAG_MAP line 291
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 84
  - Contains `lt` (total liabilities) - mapped in XBRL_TAG_MAP line 137
  - Contains `act` (current assets) - mapped in XBRL_TAG_MAP line 90
  - Contains `lct` (current liabilities) - mapped in XBRL_TAG_MAP line 133
  - Contains `ib` (income before extraordinary items) - mapped in XBRL_TAG_MAP line 262-264
  - Contains `oancf` (operating cash flow) - mapped in XBRL_TAG_MAP line 280-282
  - ✅ **`sic`**: **ADDED** - SIC code has been added to `AP_m_aCompustat.parquet`
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `prc` (price) - from `AP_CRSPMonthly.py` line 349, included in `AP_SignalMasterTable.py` line 132
- ✅ **AP_GNPdefl.parquet**: 
  - File exists (from `AP_GNPDeflator.py`)
  - Contains `time_avail_m` (from `AP_GNPDeflator.py` line 181)
  - Contains `gnpdefl` (GNP deflator) - from `AP_GNPDeflator.py` line 178

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates O-Score bankruptcy predictor using Dichev formula
- Uses multiple financial ratios: log(assets/GNP deflator), liabilities/assets, working capital/assets, current ratio, leverage indicator, income/assets, operating income/liabilities, loss indicators
- Replaces missing `fopt` with `oancf`
- Uses 12-month calendar-based lag of `ib`
- Filters out utilities (SIC 4000-4999) and financial firms (SIC 6000+)
- Creates deciles and forms binary signal: long (OScore=1) for top decile, short (OScore=0) for bottom 7 deciles

---

## Summary

### Overall Status:
**3 OUT OF 5 PREDICTORS CAN BE CONSTRUCTED** - Two predictors (`OrderBacklog.py` and `OrderBacklogChg.py`) require `ob` (order backlog) which is zero-filled.

### Column Availability:
- ✅ **SignalMasterTable columns**: `permno`, `time_avail_m`, `gvkey` (surrogate), `exchcd`, `sicCRSP`, `mve_c`, `shrcd`, `prc` - All available
- ✅ **Compustat annual columns**: `gvkey` (surrogate), `permno`, `time_avail_m`, `xrd`, `revt`, `cogs`, `xsga`, `at`, `ceq`, `fopt`, `lt`, `act`, `lct`, `ib`, `oancf`, `sic` (added) - All available
- ⚠️ **`ob`**: **ZERO-FILLED** - Order backlog is not GAAP, not reliably mappable from XBRL, currently zero-filled
- ✅ **GNP deflator**: `time_avail_m`, `gnpdefl` - Available in `AP_GNPdefl.parquet`

### Additional Notes:
1. **For OperProfRD**: 
   - **VERIFICATION**: Test that `xrd`, `revt`, `cogs`, `xsga`, `at`, `ceq`, `mve_c`, `shrcd` are correctly populated
   - Verify sufficient cross-sectional coverage for filtering

2. **For OPLeverage**: 
   - **VERIFICATION**: Test that `xsga`, `cogs`, `at` are correctly populated
   - Note: Missing SGA expenses are set to zero

3. **For OrderBacklog and OrderBacklogChg**: 
   - **CRITICAL**: `ob` (order backlog) is zero-filled, which means these predictors will have no valid observations
   - Order backlog is not a standard GAAP item and is not reliably mappable from XBRL
   - **Recommended**: Extract from MD&A text sections or alternative data sources (earnings calls, press releases)
   - Would require text mining or manual data collection
   - May have limited historical coverage

4. **For OScore**: 
   - **VERIFICATION**: Test that all financial statement columns (`fopt`, `at`, `lt`, `act`, `lct`, `ib`, `oancf`, `sic`), `prc`, and `gnpdefl` are correctly populated
   - Verify sufficient historical coverage for 12-month lag calculation
   - Note: Predictor uses complex formula with multiple financial ratios

5. **Note on gvkey Surrogate**: 
   - `gvkey` is not directly available from EDGAR (Compustat-specific identifier)
   - AP version uses `cik` (SEC identifier) as surrogate, falls back to `permno` if unavailable
   - For `OPLeverage.py`, `OrderBacklog.py`, and `OrderBacklogChg.py`, `gvkey` is loaded but not used in calculations (only kept for compatibility)
   - For `OperProfRD.py` and `OScore.py`, `gvkey` is used for merging with Compustat data, surrogate approach is acceptable

6. **Note on ob (Order Backlog)**: 
   - Order backlog (`ob`) is not a standard GAAP financial statement item
   - It's typically reported in MD&A sections or earnings calls, not in XBRL financial statements
   - Currently zero-filled in `AP_CompustatAnnual.py` (line 1032)
   - **Impact**: `OrderBacklog.py` and `OrderBacklogChg.py` will have no valid observations since predictor sets to missing when `ob == 0`
   - **Recommended**: Extract from MD&A text sections using NLP/text mining, or use alternative data sources
   - May require significant data engineering and may have limited historical coverage


# Group 27 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 27.

---

## 131. PatentsRD.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `gvkey`, `time_avail_m`, `mve_c`, `sicCRSP`, `exchcd`
- **m_aCompustat.parquet**: `permno`, `time_avail_m`, `xrd`, `sich`, `datadate`, `ceq`
- **PatentDataProcessed.parquet**: `gvkey`, `year`, `npat`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` line 119)
  - Contains `mve_c` (market value of equity) - from `AP_SignalMasterTable.py` line 127
  - Contains `sicCRSP` (SIC code from CRSP) - from `AP_SignalMasterTable.py` line 134
  - Contains `exchcd` (exchange code) - from `AP_SignalMasterTable.py` line 130
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `xrd` (R&D expenses) - mapped in XBRL_TAG_MAP line 250-252
  - ⚠️ **`sich`**: **NOT EXTRACTED** - Historical SIC code is not extracted from XBRL in `AP_CompustatAnnual.py`. However, `sich` is loaded but **NOT USED** in the predictor logic (only `sicCRSP` is used for filtering).
  - ✅ **`datadate`**: Available - from `AP_CompustatAnnual.py` line 934 (set from `period_end`)
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184
- ❌ **PatentDataProcessed.parquet**: **FILE MAY NOT EXIST**
  - Script exists: `PatentCitations.py` (generates `PatentDataProcessed.parquet`)
  - Would contain `gvkey`, `year`, `npat` (number of patents)
  - Requires patent citation data from USPTO/patent databases

### Can Be Constructed?
**PARTIALLY** - Missing `PatentDataProcessed.parquet`. Note: `sich` is loaded but not used in calculations.

### Additional Work Needed?
**YES** - Missing patent data:

#### What Needs to Be Done:
1. **Create PatentDataProcessed.parquet**: 
   - Run `PatentCitations.py` to generate `PatentDataProcessed.parquet`
   - Requires patent citation database access (USPTO or proprietary sources)
   - Must contain `gvkey`, `year`, `npat` columns

2. **Note on `sich`**: 
   - `sich` is loaded in the predictor but **NOT USED** in the actual signal construction
   - Only `sicCRSP` from `SignalMasterTable` is used for filtering financial firms (line 119)
   - Can be set to empty/NaN or omitted from the data load without affecting results

#### Implementation Notes:
- Predictor calculates patent efficiency scaled by R&D capital with double-sorted portfolio approach
- Formula: `PatentsRD = npat / RDcap` where `RDcap` is sum of depreciated past R&D
- Portfolios are computed from July of year t to June of year t+1
- Filters: Excludes financial firms (SIC 6000-6999), requires positive `ceq`
- **Note**: `sich` is loaded but not used - can be omitted or set to NaN

---

## 132. PayoutYield.py

### Required Columns:
- **m_aCompustat.parquet**: `permno`, `time_avail_m`, `dvc`, `prstkc`, `pstkrv`, `sstk`, `sic`, `ceq`, `datadate`
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `mve_permco`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `dvc` (cash dividends) - mapped in XBRL_TAG_MAP line 285-286
  - Contains `prstkc` (purchase of common stock) - mapped in XBRL_TAG_MAP line 217-218
  - ❌ **`pstkrv`**: **NOT MAPPABLE** - Preferred stock redemption value is not mappable from XBRL (line 199: "Not mappable (numeric XBRL fact almost never provided)"). Not included in zero_fill_vars, so likely missing from output.
  - Contains `sstk` (sale of common stock) - mapped in XBRL_TAG_MAP line 222-223
  - ✅ **`sic`**: **ADDED** - SIC code has been added to `AP_m_aCompustat.parquet`
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184
  - ✅ **`datadate`**: Available - from `AP_CompustatAnnual.py` line 934 (set from `period_end`)
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `mve_permco` (market value of equity by permco) - from `AP_SignalMasterTable.py` line 128

### Can Be Constructed?
**PARTIALLY** - Missing `pstkrv` (preferred stock redemption value).

### Additional Work Needed?
**YES** - Missing `pstkrv`:

#### What Needs to Be Done:
1. **Handle `pstkrv`**: 
   - `pstkrv` is not mappable from XBRL (line 199: "Not mappable (numeric XBRL fact almost never provided)")
   - **Option 1**: Zero-fill `pstkrv` (add to `zero_fill_vars` list in `AP_CompustatAnnual.py`)
   - **Option 2**: Use `pstkl` (preferred stock liquidation value) as proxy if available
   - **Option 3**: Use `pstk` (preferred stock carrying amount) as proxy
   - **Impact**: Formula is `PayoutYield = (dvc + prstkc + pstkrv) / mve_permco_l6`. If `pstkrv` is zero-filled, it will underestimate payout yield for firms with preferred stock redemptions.

#### Implementation Notes:
- Predictor calculates payout yield scaled by lagged market value of equity
- Formula: `PayoutYield = (dvc + prstkc + pstkrv) / mve_permco_l6`
- Uses 6-month calendar-based lag of `mve_permco`
- Sets negative or zero payout yields to missing
- Filters: Excludes financial companies (SIC 6000-6999), requires positive `ceq`, requires at least 24 historical observations per company
- **Current Issue**: `pstkrv` is missing, which will underestimate payout yield for firms with preferred stock redemptions

---

## 133. PctAcc.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `ib`, `oancf`, `dp`, `act`, `che`, `lct`, `txp`, `dlc`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `ib` (income before extraordinary items) - mapped in XBRL_TAG_MAP line 262-264
  - Contains `oancf` (operating cash flow) - mapped in XBRL_TAG_MAP line 280-282
  - Contains `dp` (depreciation and amortization) - mapped in XBRL_TAG_MAP line 238
  - Contains `act` (current assets) - mapped in XBRL_TAG_MAP line 90
  - Contains `che` (cash and cash equivalents) - mapped in XBRL_TAG_MAP line 88
  - Contains `lct` (current liabilities) - mapped in XBRL_TAG_MAP line 133
  - Contains `txp` (income taxes payable) - mapped in XBRL_TAG_MAP line 163-164
  - Contains `dlc` (debt current) - mapped in XBRL_TAG_MAP line 136-139

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates percent operating accruals: income before extraordinary items minus net cash flow, scaled by absolute income
- Formula: `PctAcc = (ib - oancf) / abs(ib)`
- Uses balance sheet approach when cash flow data is unavailable: `PctAcc = (delta(act) - delta(che) - delta(lct) + delta(dlc) + delta(txp) + dp) / abs(ib)`
- Handles case when `ib == 0` by dividing by 0.01
- Uses 12-month lagged values for balance sheet approach

---

## 134. PctTotAcc.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `ni`, `prstkcc`, `sstk`, `dvt`, `oancf`, `fincf`, `ivncf`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `ni` (net income) - mapped in XBRL_TAG_MAP line 265-266
  - ⚠️ **`prstkcc`**: **ZERO-FILLED** - Preferred stock repurchases are rarely broken out, not mappable from XBRL (line 219: "Rarely broken out, not mappable"), currently zero-filled if missing (from `zero_fill_vars` list line 1035)
  - Contains `sstk` (sale of common stock) - mapped in XBRL_TAG_MAP line 222-223
  - Contains `dvt` (total dividends) - mapped in XBRL_TAG_MAP line 287-288
  - Contains `oancf` (operating cash flow) - mapped in XBRL_TAG_MAP line 280-282
  - Contains `fincf` (financing cash flow) - mapped in XBRL_TAG_MAP line 316
  - Contains `ivncf` (investing cash flow) - mapped in XBRL_TAG_MAP line 311

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate, `prstkcc` is zero-filled).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates percent total accruals: net income minus cash flows from operations, financing and investment, scaled by absolute net income
- Formula: `PctTotAcc = (ni - (prstkcc - sstk + dvt + oancf + fincf + ivncf)) / abs(ni)`
- **Note**: `prstkcc` is zero-filled, which may underestimate total accruals for firms with preferred stock repurchases. However, this is a known limitation and does not prevent construction.
- **Note**: `gvkey` is loaded but not used in calculations (only kept for compatibility)

---

## 135. Price.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `prc`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `prc` (price) - from `AP_CRSPMonthly.py` line 349, included in `AP_SignalMasterTable.py` line 132

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates log of absolute value of stock price
- Formula: `Price = log(abs(prc))`
- Simple transformation of price data from SignalMasterTable

---

## Summary

### Overall Status:
**3 OUT OF 5 PREDICTORS CAN BE CONSTRUCTED** - Two predictors (`PatentsRD.py` and `PayoutYield.py`) have missing columns.

### Column Availability:
- ✅ **SignalMasterTable columns**: `permno`, `time_avail_m`, `gvkey` (surrogate), `mve_c`, `sicCRSP`, `exchcd`, `mve_permco`, `prc` - All available
- ✅ **Compustat annual columns**: `gvkey` (surrogate), `permno`, `time_avail_m`, `xrd`, `datadate`, `ceq`, `dvc`, `prstkc`, `sstk`, `sic` (added), `ib`, `oancf`, `dp`, `act`, `che`, `lct`, `txp`, `dlc`, `ni`, `dvt`, `fincf`, `ivncf` - All available
- ⚠️ **`sich`**: **NOT EXTRACTED** - Historical SIC code is not extracted from XBRL, but is not used in `PatentsRD.py` calculations
- ⚠️ **`prstkcc`**: **ZERO-FILLED** - Preferred stock repurchases are rarely broken out, zero-filled if missing
- ❌ **`pstkrv`**: **NOT MAPPABLE** - Preferred stock redemption value is not mappable from XBRL, likely missing from output
- ❌ **PatentDataProcessed.parquet**: **FILE MAY NOT EXIST** - Requires patent citation database access

### Additional Notes:
1. **For PatentsRD**: 
   - **CRITICAL**: Run `PatentCitations.py` to generate `PatentDataProcessed.parquet`
   - Requires patent citation database access (USPTO, NBER Patent Database, or proprietary sources)
   - **Note on `sich`**: While `sich` is loaded, it is not used in calculations. Can be set to NaN/empty or omitted from data load.
   - If patent data is not available, this predictor cannot be constructed with AP data sources

2. **For PayoutYield**: 
   - **CRITICAL**: Missing `pstkrv` (preferred stock redemption value)
   - **Recommended**: Zero-fill `pstkrv` (add to `zero_fill_vars` list in `AP_CompustatAnnual.py`)
   - **Alternative**: Use `pstkl` (preferred stock liquidation value) or `pstk` (preferred stock carrying amount) as proxy
   - **Impact**: If `pstkrv` is zero-filled, it will underestimate payout yield for firms with preferred stock redemptions

3. **For PctAcc**: 
   - **VERIFICATION**: Test that all financial statement columns (`ib`, `oancf`, `dp`, `act`, `che`, `lct`, `txp`, `dlc`) are correctly populated
   - Verify sufficient historical coverage for 12-month lag calculation

4. **For PctTotAcc**: 
   - **VERIFICATION**: Test that all financial statement columns (`ni`, `prstkcc`, `sstk`, `dvt`, `oancf`, `fincf`, `ivncf`) are correctly populated
   - **Note**: `prstkcc` is zero-filled, which may underestimate total accruals for firms with preferred stock repurchases

5. **For Price**: 
   - **VERIFICATION**: Test that `prc` is correctly populated in `AP_SignalMasterTable.parquet`
   - Simple transformation, should work without issues

6. **Note on gvkey Surrogate**: 
   - `gvkey` is not directly available from EDGAR (Compustat-specific identifier)
   - AP version uses `cik` (SEC identifier) as surrogate, falls back to `permno` if unavailable
   - For `PctAcc.py` and `PctTotAcc.py`, `gvkey` is loaded but not used in calculations (only kept for compatibility)
   - For `PatentsRD.py`, `gvkey` is used for merging with patent data, surrogate approach is acceptable

7. **Note on Preferred Stock Variables**: 
   - `pstkrv` (preferred stock redemption value) is not mappable from XBRL and likely missing from output
   - `prstkcc` (preferred stock repurchases) is zero-filled
   - `pstkl` (preferred stock liquidation value) has low coverage but may be available
   - `pstk` (preferred stock carrying amount) is available
   - **Impact**: Predictors that use preferred stock variables may have limited accuracy for firms with preferred stock


# Group 28 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 28.

---

## 136. ProbInformedTrading.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `gvkey`, `time_avail_m`, `mve_c`
- **pin_monthly.parquet**: `permno`, `time_avail_m`, `a`, `u`, `es`, `eb`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` line 119)
  - Contains `mve_c` (market value of equity) - from `AP_SignalMasterTable.py` line 127
- ❌ **pin_monthly.parquet**: **FILE MAY NOT EXIST**
  - Script exists: `PINData.py` (generates `pin_monthly.parquet`)
  - Would contain `permno`, `time_avail_m`, `a`, `u`, `es`, `eb` (PIN microstructure parameters)
  - Downloads from Dropbox (Easley et al. PIN data)
  - **Note**: This is NOT an AP script - it downloads from a proprietary source (Dropbox)

### Can Be Constructed?
**PARTIALLY** - Missing `pin_monthly.parquet`.

### Additional Work Needed?
**YES** - Missing PIN data:

#### What Needs to Be Done:
1. **Create pin_monthly.parquet**: 
   - Run `PINData.py` to generate `pin_monthly.parquet`
   - Requires Dropbox access to Easley et al. PIN data
   - Converts yearly PIN parameters to monthly data with 11-month availability lag
   - **Note**: This is NOT an AP script - it downloads from a proprietary source

2. **Alternative**: 
   - If PIN data is not available, this predictor cannot be constructed
   - Would require implementing PIN estimation from trade-level data (complex and computationally intensive)

#### Implementation Notes:
- Predictor calculates probability of informed trading from microstructure parameters
- Formula: `ProbInformedTrading = (a * u) / (a * u + es + eb)`
- Sets to missing for large cap stocks (top 50% by market value)
- **Note**: `gvkey` is loaded but not used in calculations (only kept for compatibility)

---

## 137. PS.py

### Required Columns:
- **m_aCompustat.parquet**: `permno`, `time_avail_m`, `fopt`, `oancf`, `ib`, `at`, `dltt`, `act`, `lct`, `txt`, `xint`, `sale`, `ceq`
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `mve_permco`
- **monthlyCRSP.parquet**: `permno`, `time_avail_m`, `shrout`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `fopt` (foreign operations income) - mapped in XBRL_TAG_MAP line 291
  - Contains `oancf` (operating cash flow) - mapped in XBRL_TAG_MAP line 280-282
  - Contains `ib` (income before extraordinary items) - mapped in XBRL_TAG_MAP line 262-264
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 84
  - Contains `dltt` (long-term debt) - mapped in XBRL_TAG_MAP line 142-144
  - Contains `act` (current assets) - mapped in XBRL_TAG_MAP line 90
  - Contains `lct` (current liabilities) - mapped in XBRL_TAG_MAP line 133
  - Contains `txt` (total income taxes) - mapped in XBRL_TAG_MAP line 270-272
  - Contains `xint` (interest expense) - mapped in XBRL_TAG_MAP line 245-246
  - Contains `sale` (sales) - mapped in XBRL_TAG_MAP line 230
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `mve_permco` (market value of equity by permco) - from `AP_SignalMasterTable.py` line 128
- ✅ **AP_monthlyCRSP.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` line 344)
  - Contains `shrout` (shares outstanding) - from `AP_CRSPMonthly.py` line 348

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates Piotroski F-score: nine-factor profitability, efficiency, and leverage score
- Components: p1 (positive net income), p2 (positive operating cash flow), p3 (improvement in ROA), p4 (cash flow exceeds net income), p5 (reduction in leverage), p6 (improvement in current ratio), p7 (improvement in gross margin), p8 (improvement in asset turnover), p9 (no increase in shares outstanding)
- Restricted to highest book-to-market quintile
- Uses 12-month lags for comparison
- Replaces missing `fopt` with `oancf`

---

## 138. RD.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `gvkey`, `time_avail_m`, `mve_permco`
- **m_aCompustat.parquet**: `gvkey`, `time_avail_m`, `xrd`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` line 119)
  - Contains `mve_permco` (market value of equity by permco) - from `AP_SignalMasterTable.py` line 128
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `xrd` (R&D expenses) - mapped in XBRL_TAG_MAP line 250-252

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates R&D intensity: R&D expenditure divided by market value of equity
- Formula: `RD = xrd / mve_permco`
- Simple ratio calculation

---

## 139. RDAbility.py

### Required Columns:
- **a_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `fyear`, `datadate`, `xrd`, `sale`

### AP File Status:
- ✅ **AP_a_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - ✅ **`fyear`**: Available - from `AP_CompustatAnnual.py` line 765 (extracted from `fiscal_year`) and line 935 (renamed to `fyear`)
  - ✅ **`datadate`**: Available - from `AP_CompustatAnnual.py` line 934 (set from `period_end`)
  - Contains `xrd` (R&D expenses) - mapped in XBRL_TAG_MAP line 250-252
  - Contains `sale` (sales) - mapped in XBRL_TAG_MAP line 230

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates R&D ability by regressing sales growth on lagged R&D intensity
- Uses rolling regression with 8-year window, minimum 6 observations
- Calculates R&D ability for lags 1-5 years
- Keeps R&D ability only for firms in highest R&D intensity tercile
- Expands annual observations to monthly frequency
- **Note**: Requires `fyear` for proper sorting and lagging operations

---

## 140. RDcap.py

### Required Columns:
- **m_aCompustat.parquet**: `permno`, `time_avail_m`, `at`, `xrd`
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `mve_c`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 84
  - Contains `xrd` (R&D expenses) - mapped in XBRL_TAG_MAP line 250-252
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `mve_c` (market value of equity) - from `AP_SignalMasterTable.py` line 127

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates R&D capital-to-assets: weighted sum of current and lagged R&D expenditures scaled by total assets
- Formula: `RDcap = (xrd + 0.8*xrd_lag12 + 0.6*xrd_lag24 + 0.4*xrd_lag36 + 0.2*xrd_lag48) / at`
- Uses calendar-based lags (12, 24, 36, 48 months)
- Excludes observations before 1980
- Restricted to small firms only (bottom size tertile)
- Assumes missing R&D values are 0

---

## Summary

### Overall Status:
**4 OUT OF 5 PREDICTORS CAN BE CONSTRUCTED** - One predictor (`ProbInformedTrading.py`) has missing PIN data file.

### Column Availability:
- ✅ **SignalMasterTable columns**: `permno`, `time_avail_m`, `gvkey` (surrogate), `mve_c`, `mve_permco` - All available
- ✅ **Compustat monthly columns**: `permno`, `time_avail_m`, `fopt`, `oancf`, `ib`, `at`, `dltt`, `act`, `lct`, `txt`, `xint`, `sale`, `ceq`, `xrd` - All available
- ✅ **Compustat annual columns**: `permno`, `time_avail_m`, `gvkey` (surrogate), `datadate`, `xrd`, `sale` - All available (with `fyear` verification needed)
- ✅ **CRSP monthly columns**: `permno`, `time_avail_m`, `shrout` - Available in `AP_monthlyCRSP.parquet`
- ❌ **pin_monthly.parquet**: **FILE MAY NOT EXIST** - Requires running `PINData.py` (downloads from Dropbox)

### Additional Notes:
1. **For ProbInformedTrading**: 
   - **CRITICAL**: Run `PINData.py` to generate `pin_monthly.parquet`
   - Requires Dropbox access to Easley et al. PIN data
   - **Note**: This is NOT an AP script - it downloads from a proprietary source
   - If PIN data is not available, this predictor cannot be constructed

2. **For PS**: 
   - **VERIFICATION**: Test that all financial statement columns (`fopt`, `oancf`, `ib`, `at`, `dltt`, `act`, `lct`, `txt`, `xint`, `sale`, `ceq`), `mve_permco`, and `shrout` are correctly populated
   - Verify sufficient historical coverage for 12-month lag calculation

3. **For RD**: 
   - **VERIFICATION**: Test that `xrd` and `mve_permco` are correctly populated
   - Simple ratio calculation, should work without issues

4. **For RDAbility**: 
   - **VERIFICATION**: Test that `gvkey` (surrogate), `permno`, `time_avail_m`, `fyear`, `datadate`, `xrd`, `sale` are correctly populated
   - Verify sufficient historical coverage for rolling regression (8-year window, minimum 6 observations)
   - **Note**: `fyear` is available from `AP_CompustatAnnual.py` (extracted from `fiscal_year`)

5. **For RDcap**: 
   - **VERIFICATION**: Test that `at`, `xrd`, and `mve_c` are correctly populated
   - Verify sufficient historical coverage for 48-month lag calculation
   - Restricted to small firms only (bottom size tertile)

6. **Note on gvkey Surrogate**: 
   - `gvkey` is not directly available from EDGAR (Compustat-specific identifier)
   - AP version uses `cik` (SEC identifier) as surrogate, falls back to `permno` if unavailable
   - For `ProbInformedTrading.py` and `RD.py`, `gvkey` is loaded but not used in calculations (only kept for compatibility)
   - For `RDAbility.py`, `gvkey` is used for grouping and lagging operations, surrogate approach is acceptable

7. **Note on File Names**: 
   - File name mismatches are ignored per user instructions (from Group 19 onwards)
   - Predictors will need to be modified to use AP file names (`AP_monthlyCRSP.parquet`, `AP_a_aCompustat.parquet`) or AP files can be renamed to match expected names


# Group 29 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 29.

---

## 141. RDIPO.py

### Required Columns:
- **m_aCompustat.parquet**: `permno`, `time_avail_m`, `xrd`
- **IPODates.parquet**: `permno`, `IPOdate`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `xrd` (R&D expenses) - mapped in XBRL_TAG_MAP line 250-252
- ✅ **AP_IPODates.parquet**: 
  - File exists (from `AP_IPODates.py`)
  - Contains `permno`, `IPOdate` (from `AP_IPODates.py` structure)

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates binary indicator for IPO firms that have zero R&D expenditures
- Formula: `RDIPO = 1` if IPO period (6-36 months after IPO) AND `xrd == 0`, else `RDIPO = 0`
- Simple indicator calculation

---

## 142. RDS.py

### Required Columns:
- **m_aCompustat.parquet**: `permno`, `gvkey`, `time_avail_m`, `recta`, `ceq`, `ni`, `dvp`, `dvc`, `prcc_f`, `csho`, `msa`
- **CompustatPensions.parquet**: `gvkey`, `year`, `pcupsu`, `paddml`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `recta` (total accounts receivable) - mapped in XBRL_TAG_MAP line 94-95
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184
  - Contains `ni` (net income) - mapped in XBRL_TAG_MAP line 265-266
  - Contains `dvp` (preferred dividends) - mapped in XBRL_TAG_MAP line 283-284
  - Contains `dvc` (cash dividends) - mapped in XBRL_TAG_MAP line 285-286
  - ❌ **`prcc_f`**: **NOT EXTRACTED** - Fiscal year-end stock price is mapped in XBRL_TAG_MAP (line 367) but not extracted from XBRL statements (line 423-427: OTHER_FIELDS not extracted). Market data is rarely in 10-K filings.
  - Contains `csho` (common shares outstanding) - mapped in XBRL_TAG_MAP line 212
  - ⚠️ **`msa`**: **ZERO-FILLED** - Minority interest adjustments are almost never tagged, not mappable from XBRL (line 183: "Almost never tagged, not mappable"), currently zero-filled if missing
- ❌ **CompustatPensions.parquet**: **FILE DOES NOT EXIST YET**
  - Script exists: `CompustatPensions.py` (generates `CompustatPensions.parquet`)
  - Would contain `gvkey`, `year`, `pcupsu`, `paddml` (pension fund data)
  - Requires WRDS Compustat pension fund database access
  - **Note**: This is NOT an AP script - it downloads from WRDS

### Can Be Constructed?
**NO** - Missing `prcc_f` and `CompustatPensions.parquet`.

### Additional Work Needed?
**YES** - Missing `prcc_f` and pension data:

#### What Needs to Be Done:
1. **Handle `prcc_f`**: 
   - `prcc_f` is mapped in XBRL_TAG_MAP but not extracted from XBRL statements
   - Market data is rarely in 10-K filings
   - **Recommended**: Supplement with yfinance data for fiscal year-end prices using `datadate` and `permno`
   - Or modify `AP_CompustatAnnual.py` to fetch fiscal year-end prices from yfinance

2. **Create CompustatPensions.parquet**: 
   - Run `CompustatPensions.py` to generate `CompustatPensions.parquet`
   - Requires WRDS Compustat pension fund database access (`COMP.ACO_PNFNDA`)
   - **Note**: This is NOT an AP script - it downloads from WRDS
   - Would need to create AP version that extracts pension data from SEC filings (complex, may not be feasible)

#### Implementation Notes:
- Predictor calculates real dirty surplus as change in book equity minus dirty surplus minus earnings plus dividends
- Formula: `RDS = (ceq - l12_ceq) - DS - (ni - dvp) + dvc - prcc_f * (csho - l12_csho)`
- Where `DS = (msa - l12_msa) + (recta - l12_recta) + 0.65 * (min(pcupsu - paddml, 0) - min(l12_pcupsu - l12_paddml, 0))`
- Uses 12-month calendar-based lags
- **Current Issues**: 
  - `prcc_f` is missing, which will cause calculation errors
  - `CompustatPensions.parquet` is missing, which will cause `DS` calculation to fail (pension terms will be missing)
  - `msa` is zero-filled, which may underestimate dirty surplus

---

## 143. realestate.py

### Required Columns:
- **m_aCompustat.parquet**: `permno`, `time_avail_m`, `ppenb`, `ppenls`, `fatb`, `fatl`, `ppegt`, `ppent`, `at`
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `sicCRSP`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `ppenb` (buildings and improvements gross) - mapped in XBRL_TAG_MAP line 109-110
  - Contains `ppenls` (finance lease right-of-use asset) - mapped in XBRL_TAG_MAP line 111-112
  - Contains `fatb` (buildings and improvements gross) - mapped in XBRL_TAG_MAP line 130
  - Contains `fatl` (land and land improvements) - mapped in XBRL_TAG_MAP line 132
  - Contains `ppegt` (property, plant, and equipment gross) - mapped in XBRL_TAG_MAP line 107
  - Contains `ppent` (property, plant, and equipment net) - mapped in XBRL_TAG_MAP line 105
  - Contains `at` (total assets) - mapped in XBRL_TAG_MAP line 84
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `sicCRSP` (SIC code from CRSP) - from `AP_SignalMasterTable.py` line 134

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates industry-adjusted real estate holdings
- Uses two methods: old (`re_old = (ppenb + ppenls) / ppent`) and new (`re_new = (fatb + fatl) / ppegt`)
- Falls back to old method if new method is missing
- Industry adjustment: subtracts industry mean (by 2-digit SIC code and time period)
- Requires at least 5 observations per industry-time period

---

## 144. Recomm_ShortInterest.py

### Required Columns:
- **IBES_Recommendations.parquet**: `tickerIBES`, `amaskcd`, `anndats`, `time_avail_m`, `ireccd`
- **SignalMasterTable.parquet**: `permno`, `gvkey`, `tickerIBES`, `time_avail_m`
- **monthlyCRSP.parquet**: `permno`, `time_avail_m`, `shrout`
- **monthlyShortInterest.parquet**: `gvkey`, `time_avail_m`, `shortint`

### AP File Status:
- ❌ **IBES_Recommendations.parquet**: **FILE DOES NOT EXIST YET**
  - Script exists: `AP_IBESRecommendations.py` (would generate `AP_IBES_Recommendations.parquet`)
  - Would contain `tickerIBES`, `amaskcd`, `anndats`, `time_avail_m`, `ireccd` (analyst recommendations)
  - Requires Eikon/LSEG API subscription
  - **Note**: File name mismatch - predictor expects `IBES_Recommendations.parquet`, AP version would be `AP_IBES_Recommendations.parquet`
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` line 119)
  - Contains `tickerIBES` (IBES ticker) - from `AP_SignalMasterTable.py` line 138 (merged from `AP_IBESCRSPLinkingTable.parquet` if available)
- ✅ **AP_monthlyCRSP.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` line 344)
  - Contains `shrout` (shares outstanding) - from `AP_CRSPMonthly.py` line 348
- ✅ **AP_monthlyShortInterest.parquet**: 
  - File exists (from `AP_CompustatShortInterest.py`)
  - Contains `gvkey`, `time_avail_m` (from `AP_CompustatShortInterest.py` structure)
  - Contains `shortint` (short interest) - from `AP_CompustatShortInterest.py` structure
  - **Note**: File name mismatch - predictor expects `monthlyShortInterest.parquet`, AP version is `AP_monthlyShortInterest.parquet`

### Can Be Constructed?
**PARTIALLY** - Missing `IBES_Recommendations.parquet`.

### Additional Work Needed?
**YES** - Missing IBES recommendations data:

#### What Needs to Be Done:
1. **Create IBES_Recommendations.parquet**: 
   - Run `AP_IBESRecommendations.py` to generate `AP_IBES_Recommendations.parquet`
   - Requires Eikon/LSEG API subscription
   - **Note**: File name mismatch - predictor expects `IBES_Recommendations.parquet`, AP version would be `AP_IBES_Recommendations.parquet`
   - If IBES data is not available, this predictor cannot be constructed

2. **File Name Mismatch**: 
   - Predictor expects `monthlyShortInterest.parquet`
   - AP version is `AP_monthlyShortInterest.parquet`
   - **Solution**: Either rename `AP_monthlyShortInterest.parquet` to `monthlyShortInterest.parquet` or modify predictor to use `AP_monthlyShortInterest.parquet`

#### Implementation Notes:
- Predictor calculates binary signal based on quintile rankings of analyst recommendations and short interest
- Formula: `Recomm_ShortInterest = 1` if both `QuintShortInterest == 1` and `QuintConsRecomm == 1` (pessimistic)
- Formula: `Recomm_ShortInterest = 0` if both `QuintShortInterest == 5` and `QuintConsRecomm == 5` (optimistic)
- Uses 5-month forward-fill for recommendations
- Calculates `ShortInterest = shortint / shrout` and `ConsRecomm = 6 - ireccd12`
- **Note**: `gvkey` is used for merging with short interest data, surrogate approach is acceptable

---

## 145. retConglomerate.py

### Required Columns:
- **CCMLinkingTable.parquet**: `gvkey`, `permno`, `timeLinkStart_d`, `timeLinkEnd_d`
- **monthlyCRSP.parquet**: `permno`, `time_avail_m`, `ret`
- **a_aCompustat.parquet**: `gvkey`, `permno`, `sale`, `fyear`
- **CompustatSegments.parquet**: `gvkey`, `datadate`, `stype`, `sics1`, `sales`

### AP File Status:
- ⚠️ **CCMLinkingTable.parquet**: **FILE EXISTS** (WRDS version)
  - File exists: `CCMLinkingTable.parquet` (from `CCMLinkingTable.py`)
  - Contains `gvkey`, `permno`, `timeLinkStart_d`, `timeLinkEnd_d` (CRSP-Compustat linking table)
  - **Note**: This is NOT an AP script - it downloads from WRDS
  - **Issue**: Uses real Compustat `gvkey` values, while AP data uses surrogate `gvkey` (CIK/permno)
  - **Can be used IF**: Using WRDS segment data (real gvkey) and WRDS Compustat annual data (real gvkey)
  - **Cannot be used IF**: Using AP segment data (surrogate gvkey) or AP Compustat annual data (surrogate gvkey)
- ✅ **AP_monthlyCRSP.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` line 344)
  - Contains `ret` (returns) - from `AP_CRSPMonthly.py` line 345
- ✅ **AP_a_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `sale` (sales) - mapped in XBRL_TAG_MAP line 230
  - ✅ **`fyear`**: Available - from `AP_CompustatAnnual.py` line 765 (extracted from `fiscal_year`) and line 935 (renamed to `fyear`)
- ⚠️ **CompustatSegments.parquet**: **FILE EXISTS** (WRDS version)
  - File exists: `CompustatSegments.parquet` (from `CompustatBusinessSegments.py`)
  - Contains `gvkey`, `datadate`, `stype`, `sics1`, `sales` (business segment data)
  - **Note**: This is NOT an AP script - it downloads from WRDS
  - **Issue**: Uses real Compustat `gvkey` values, while AP Compustat annual data uses surrogate `gvkey` (CIK/permno)
  - **Can be used IF**: Using WRDS Compustat annual data (real gvkey) - segments will match on `gvkey` (line 125)
  - **Cannot be used IF**: Using AP Compustat annual data (surrogate gvkey) - segments won't match on `gvkey`

### Can Be Constructed?
**PARTIALLY** - Files exist but have `gvkey` mismatch issues:
- ✅ `CCMLinkingTable.parquet` exists (WRDS version)
- ✅ `CompustatSegments.parquet` exists (WRDS version)
- ⚠️ **Issue**: Both use real Compustat `gvkey` values, while AP Compustat annual data uses surrogate `gvkey` (CIK/permno)
- **Can be constructed IF**: Using WRDS Compustat annual data (`a_aCompustat.parquet` from WRDS) instead of AP version
- **Cannot be constructed IF**: Using AP Compustat annual data (`AP_a_aCompustat.parquet`) - `gvkey` values won't match

### Additional Work Needed?
**YES** - Missing linking table and segment data:

#### What Needs to Be Done:
1. **Option 1: Use WRDS Compustat Annual Data** (Not AP version):
   - Use existing `CCMLinkingTable.parquet` (WRDS version) ✓
   - Use existing `CompustatSegments.parquet` (WRDS version) ✓
   - Use WRDS `a_aCompustat.parquet` instead of `AP_a_aCompustat.parquet`
   - **Result**: Predictor can be constructed, but NOT using AP data sources

2. **Option 2: Create AP Versions** (True AP version):
   - Create `AP_CCMLinkingTable.parquet` from AP data (links surrogate `gvkey` to `permno`)
   - Create `AP_CompustatSegments.parquet` from SEC 10-K filings (extracts segments with surrogate `gvkey`)
   - Use `AP_a_aCompustat.parquet` (surrogate `gvkey`)
   - **Result**: Predictor can be constructed using AP data sources, but requires significant work

#### Implementation Notes:
- Predictor calculates conglomerate returns based on segment-weighted industry returns
- Identifies conglomerates vs. stand-alones based on segment data
- Uses stand-alone firms to calculate industry returns (by 2-digit SIC code)
- Applies industry returns to conglomerates weighted by segment sales
- Requires linking table to match `gvkey` to `permno` with time validity periods
- **Current Issues**: 
  - `CCMLinkingTable.parquet` exists (WRDS version) but uses real Compustat `gvkey` values
  - `CompustatSegments.parquet` exists (WRDS version) but uses real Compustat `gvkey` values
  - AP Compustat annual data uses surrogate `gvkey` (CIK/permno), which won't match WRDS `gvkey` values
  - **Solution**: Either use WRDS Compustat annual data (not AP) OR create AP versions of segments and linking table

---

## Summary

### Overall Status:
**2 OUT OF 5 PREDICTORS CAN BE CONSTRUCTED** - Three predictors have missing files or columns.

### Column Availability:
- ✅ **SignalMasterTable columns**: `permno`, `time_avail_m`, `gvkey` (surrogate), `tickerIBES`, `sicCRSP` - All available
- ✅ **Compustat monthly columns**: `permno`, `time_avail_m`, `xrd`, `recta`, `ceq`, `ni`, `dvp`, `dvc`, `csho`, `ppenb`, `ppenls`, `fatb`, `fatl`, `ppegt`, `ppent`, `at` - All available
- ✅ **Compustat annual columns**: `permno`, `time_avail_m`, `gvkey` (surrogate), `sale`, `fyear` - All available
- ✅ **CRSP monthly columns**: `permno`, `time_avail_m`, `ret`, `shrout` - Available in `AP_monthlyCRSP.parquet`
- ✅ **IPO dates**: `permno`, `IPOdate` - Available in `AP_IPODates.parquet`
- ✅ **Short interest**: `gvkey`, `time_avail_m`, `shortint` - Available in `AP_monthlyShortInterest.parquet`
- ⚠️ **`msa`**: **ZERO-FILLED** - Minority interest adjustments are almost never tagged, zero-filled if missing
- ❌ **`prcc_f`**: **NOT EXTRACTED** - Fiscal year-end stock price is not extracted from XBRL statements
- ❌ **IBES_Recommendations.parquet**: **FILE DOES NOT EXIST** - Requires Eikon/LSEG API subscription
- ❌ **CompustatPensions.parquet**: **FILE DOES NOT EXIST** - Requires WRDS Compustat pension fund database access
- ⚠️ **CCMLinkingTable.parquet**: **FILE EXISTS** (WRDS version) - Uses real Compustat `gvkey`, won't match AP surrogate `gvkey`
- ⚠️ **CompustatSegments.parquet**: **FILE EXISTS** (WRDS version) - Uses real Compustat `gvkey`, won't match AP surrogate `gvkey`

### Additional Notes:
1. **For RDIPO**: 
   - **VERIFICATION**: Test that `xrd` and `IPOdate` are correctly populated
   - Simple indicator calculation, should work without issues

2. **For RDS**: 
   - **CRITICAL**: Missing `prcc_f` (fiscal year-end stock price) and `CompustatPensions.parquet`
   - **Recommended**: Supplement `prcc_f` with yfinance data for fiscal year-end prices
   - **Recommended**: Create AP version of `CompustatPensions.py` that extracts pension data from SEC filings (complex, may not be feasible)
   - **Note**: `msa` is zero-filled, which may underestimate dirty surplus

3. **For realestate**: 
   - **VERIFICATION**: Test that all property/equipment columns (`ppenb`, `ppenls`, `fatb`, `fatl`, `ppegt`, `ppent`, `at`) and `sicCRSP` are correctly populated
   - Uses two methods (old and new) with fallback logic
   - Requires at least 5 observations per industry-time period

4. **For Recomm_ShortInterest**: 
   - **CRITICAL**: Missing `IBES_Recommendations.parquet`
   - **Recommended**: Run `AP_IBESRecommendations.py` to generate `AP_IBES_Recommendations.parquet` (requires Eikon/LSEG API subscription)
   - **File Name Mismatch**: Predictor expects `monthlyShortInterest.parquet`, AP version is `AP_monthlyShortInterest.parquet`
   - If IBES data is not available, this predictor cannot be constructed

5. **For retConglomerate**: 
   - **Files exist but have `gvkey` mismatch**: `CCMLinkingTable.parquet` and `CompustatSegments.parquet` both exist (WRDS versions)
   - **Issue**: Both use real Compustat `gvkey` values, while AP Compustat annual data uses surrogate `gvkey` (CIK/permno)
   - **Option 1**: Use WRDS `a_aCompustat.parquet` instead of `AP_a_aCompustat.parquet` - predictor can be constructed (not AP version)
   - **Option 2**: Create AP versions of segments and linking table - predictor can be constructed (true AP version)
   - **Key constraint**: Segments merge with Compustat annual on `gvkey` and `fyear` (line 125), so `gvkey` values must match

6. **Note on gvkey Surrogate**: 
   - `gvkey` is not directly available from EDGAR (Compustat-specific identifier)
   - AP version uses `cik` (SEC identifier) as surrogate, falls back to `permno` if unavailable
   - For `RDIPO.py`, `gvkey` is not used in calculations
   - For `RDS.py`, `Recomm_ShortInterest.py`, and `retConglomerate.py`, `gvkey` is used for merging, surrogate approach is acceptable

7. **Note on File Names**: 
   - File name mismatches are ignored per user instructions (from Group 19 onwards)
   - Predictors will need to be modified to use AP file names or AP files can be renamed to match expected names


# Group 30 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 30.

---

## 146. ReturnSkew.py

### Required Columns:
- **dailyCRSP.parquet**: `permno`, `time_d`, `ret`

### AP File Status:
- ⚠️ **dailyCRSP.parquet**: **FILE NAME MISMATCH**
  - Predictor expects `dailyCRSP.parquet`
  - AP version is `AP_dailyCRSP.parquet` (from `AP_CRSPDaily.py`)
  - Contains `permno`, `time_d` (from `AP_CRSPDaily.py` structure)
  - Contains `ret` (returns) - from `AP_CRSPDaily.py` structure

### Can Be Constructed?
**YES** - All required columns are present (file name mismatch).

### Additional Work Needed?
**MINOR** - File name mismatch:

#### What Needs to Be Done:
1. **File Name Mismatch**: 
   - Predictor expects `dailyCRSP.parquet`
   - AP version is `AP_dailyCRSP.parquet`
   - **Solution**: Either rename `AP_dailyCRSP.parquet` to `dailyCRSP.parquet` or modify predictor to use `AP_dailyCRSP.parquet`

#### Implementation Notes:
- Predictor calculates skewness of daily returns within each month
- Formula: `ReturnSkew = skewness(ret)` by `permno` and `time_avail_m`
- Requires minimum 15 daily observations per permno-month for valid calculation
- Uses Polars for efficient group-by operations

---

## 147. REV6.py

### Required Columns:
- **IBES_EPS_Unadj.parquet**: `tickerIBES`, `time_avail_m`, `fpi`, `fpedats`, `statpers`, `meanest`
- **SignalMasterTable.parquet**: `permno`, `tickerIBES`, `time_avail_m`, `prc`

### AP File Status:
- ✅ **AP_IBES_EPS_Unadj.parquet**: 
  - File exists (from `AP_IBESEPSUnadjusted.py`)
  - Contains `tickerIBES`, `time_avail_m` (from `AP_IBESEPSUnadjusted.py` structure)
  - Contains `fpi` (forecast period indicator) - from `AP_IBESEPSUnadjusted.py` structure
  - Contains `fpedats` (forecast period end date) - from `AP_IBESEPSUnadjusted.py` structure
  - Contains `statpers` (statement period end date) - from `AP_IBESEPSUnadjusted.py` structure
  - Contains `meanest` (mean estimate) - from `AP_IBESEPSUnadjusted.py` structure
  - **Note**: Predictor filters for `fpi == "1"` (1-year ahead forecasts)
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - Contains `tickerIBES` (IBES ticker) - from `AP_SignalMasterTable.py` line 138 (merged from `AP_IBESCRSPLinkingTable.parquet` if available)
  - Contains `prc` (price) - from `AP_CRSPMonthly.py` line 349, included in `AP_SignalMasterTable.py` line 132

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates 6-month sum of monthly changes in mean earnings estimates scaled by prior month stock price
- Formula: `REV6 = sum(tempRev + l1.tempRev + l2.tempRev + l3.tempRev + l4.tempRev + l5.tempRev + l6.tempRev)`
- Where `tempRev = (meanest - l1.meanest) / abs(l1.prc)`
- Filters for `fpi == "1"` (1-year ahead forecasts)
- Uses conditional fill-forward logic for missing estimates when forecast periods match
- Requires valid `tickerIBES` mapping (from `AP_IBESCRSPLinkingTable.parquet`)

---

## 148. RevenueSurprise.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `gvkey`, `time_avail_m`
- **m_QCompustat.parquet**: `gvkey`, `time_avail_m`, `revtq`, `cshprq`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` line 119)
- ✅ **AP_m_QCompustat.parquet**: 
  - File exists (from `AP_CompustatQuarterly.py`)
  - Contains `time_avail_m` (from `AP_CompustatQuarterly.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `revtq` (quarterly revenue) - mapped in XBRL_TAG_MAP line 167
  - Contains `cshprq` (quarterly shares repurchased) - mapped in XBRL_TAG_MAP line 161-162
  - ✅ **`cshoq`**: Available - quarterly shares outstanding mapped in XBRL_TAG_MAP line 160
  - **Note**: Predictor uses `cshprq` (shares repurchased) in formula: `revps = revtq / cshprq`. This seems unusual as revenue per share typically uses shares outstanding (`cshoq`), not shares repurchased. However, the code explicitly uses `cshprq`, so both columns are available if needed.

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- **Note**: Predictor uses `cshprq` (shares repurchased) in formula: `revps = revtq / cshprq`
- This is unusual as revenue per share typically uses shares outstanding (`cshoq`), not shares repurchased
- However, the code explicitly uses `cshprq`, and both `cshprq` and `cshoq` are available in `AP_m_QCompustat.parquet`
- If the predictor logic is incorrect and should use `cshoq` instead, that would be a predictor code issue, not a data availability issue

#### Implementation Notes:
- Predictor calculates standardized revenue surprise scaled by revenue per share standard deviation
- Formula: `RevenueSurprise = (revps - revps_l12 - Drift) / SD`
- Where `revps = revtq / cshprq` and `Drift` is mean of historical revenue changes
- Uses 3, 6, 9, 12, 15, 18, 21, 24 month lags for drift and volatility calculations
- **Note**: `gvkey` is used for merging, surrogate approach is acceptable
- **Potential Issue**: `cshprq` may be shares repurchased, not shares outstanding - verify column meaning

---

## 149. roaq.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `gvkey`, `time_avail_m`
- **m_QCompustat.parquet**: `gvkey`, `time_avail_m`, `atq`, `ibq`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` lines 124-126)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno` (from `AP_SignalMasterTable.py` line 119)
- ✅ **AP_m_QCompustat.parquet**: 
  - File exists (from `AP_CompustatQuarterly.py`)
  - Contains `time_avail_m` (from `AP_CompustatQuarterly.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `atq` (quarterly total assets) - mapped in XBRL_TAG_MAP line 70
  - Contains `ibq` (quarterly income before extraordinary items) - mapped in XBRL_TAG_MAP line 202

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates quarterly return on assets: quarterly income divided by 3-month lagged quarterly assets
- Formula: `roaq = ibq / atq_lag3`
- Uses calendar-based 3-month lag for exact date matching
- **Note**: `gvkey` is used for merging, surrogate approach is acceptable

---

## 150. RoE.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `ni`, `ceq`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - ⚠️ **`gvkey`**: Not directly available from EDGAR. Uses `cik` as surrogate or falls back to `permno`
  - Contains `ni` (net income) - mapped in XBRL_TAG_MAP line 265-266
  - Contains `ceq` (common equity) - mapped in XBRL_TAG_MAP line 183-184

### Can Be Constructed?
**YES** - All required columns are present (gvkey is surrogate).

### Additional Work Needed?
None. The predictor can be constructed with the available columns.

#### Implementation Notes:
- Predictor calculates return on equity: net income divided by book value of equity
- Formula: `RoE = ni / ceq`
- Simple ratio calculation
- **Note**: `gvkey` is loaded but not used in calculations (only kept for compatibility)

---

## Summary

### Overall Status:
**5 OUT OF 5 PREDICTORS CAN BE CONSTRUCTED** - All predictors can be constructed with available columns.

### Column Availability:
- ✅ **SignalMasterTable columns**: `permno`, `time_avail_m`, `gvkey` (surrogate), `tickerIBES`, `prc` - All available
- ✅ **Compustat monthly columns**: `permno`, `time_avail_m`, `gvkey` (surrogate), `ni`, `ceq` - All available
- ✅ **Compustat quarterly columns**: `gvkey` (surrogate), `time_avail_m`, `atq`, `ibq`, `revtq`, `cshprq`, `cshoq` - All available
- ✅ **CRSP daily columns**: `permno`, `time_d`, `ret` - Available in `AP_dailyCRSP.parquet`
- ✅ **IBES EPS unadjusted**: `tickerIBES`, `time_avail_m`, `fpi`, `fpedats`, `statpers`, `meanest` - Available in `AP_IBES_EPS_Unadj.parquet`

### Additional Notes:
1. **For ReturnSkew**: 
   - **VERIFICATION**: Test that `ret` is correctly populated in `AP_dailyCRSP.parquet`
   - **File Name Mismatch**: Predictor expects `dailyCRSP.parquet`, AP version is `AP_dailyCRSP.parquet`
   - Requires minimum 15 daily observations per permno-month for valid calculation

2. **For REV6**: 
   - **VERIFICATION**: Test that `tickerIBES`, `meanest`, `fpi`, `fpedats`, `statpers`, and `prc` are correctly populated
   - Requires valid `tickerIBES` mapping (from `AP_IBESCRSPLinkingTable.parquet`)
   - Filters for `fpi == "1"` (1-year ahead forecasts)
   - Uses conditional fill-forward logic for missing estimates

3. **For RevenueSurprise**: 
   - **VERIFICATION**: Test that `revtq`, `cshprq`, and `gvkey` (surrogate) are correctly populated
   - **Note**: Predictor uses `cshprq` (shares repurchased) in formula: `revps = revtq / cshprq`
   - This is unusual as revenue per share typically uses shares outstanding (`cshoq`), not shares repurchased
   - However, both `cshprq` and `cshoq` are available in `AP_m_QCompustat.parquet` if needed
   - Uses 3, 6, 9, 12, 15, 18, 21, 24 month lags for drift and volatility calculations
   - **Note**: `gvkey` is used for merging, surrogate approach is acceptable

4. **For roaq**: 
   - **VERIFICATION**: Test that `atq`, `ibq`, and `gvkey` (surrogate) are correctly populated
   - Uses calendar-based 3-month lag for exact date matching
   - **Note**: `gvkey` is used for merging, surrogate approach is acceptable

5. **For RoE**: 
   - **VERIFICATION**: Test that `ni` and `ceq` are correctly populated
   - Simple ratio calculation, should work without issues
   - **Note**: `gvkey` is loaded but not used in calculations

6. **Note on gvkey Surrogate**: 
   - `gvkey` is not directly available from EDGAR (Compustat-specific identifier)
   - AP version uses `cik` (SEC identifier) as surrogate, falls back to `permno` if unavailable
   - For `RevenueSurprise.py` and `roaq.py`, `gvkey` is used for merging, surrogate approach is acceptable
   - For `RoE.py`, `gvkey` is loaded but not used in calculations (only kept for compatibility)

7. **Note on File Names**: 
   - File name mismatches are ignored per user instructions (from Group 19 onwards)
   - Predictors will need to be modified to use AP file names (`AP_dailyCRSP.parquet`) or AP files can be renamed to match expected names

8. **Note on Quarterly Compustat Columns**: 
   - `AP_CompustatQuarterly.py` extracts both `cshprq` (shares repurchased) and `cshoq` (shares outstanding)
   - Verify which column `RevenueSurprise.py` actually needs for the `revps = revtq / cshprq` calculation
   - If `cshoq` is needed, it should be available in `AP_m_QCompustat.parquet`


# Group 31 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 31.

---

## 151. sfe.py

### Required Columns:
- **IBES_EPS_Unadj.parquet**: `fpi`, `statpers`, `fpedats`, `time_avail_m`, `tickerIBES`, `medest`, `numest`
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `tickerIBES`, `prc`
- **m_aCompustat.parquet**: `permno`, `time_avail_m`, `datadate`

### AP File Status:
- ✅ **AP_IBES_EPS_Unadj.parquet**: 
  - File exists (from `AP_IBESEPSUnadjusted.py`)
  - Contains `fpi`, `statpers`, `fpedats`, `time_avail_m`, `tickerIBES` (from `AP_IBESEPSUnadjusted.py` structure)
  - Contains `medest` (median EPS estimate) - mapped from "EPS Median" or "EPS Median Estimate" (line 256-257, 265)
  - Contains `numest` (number of estimates) - mapped from "EPS Number of Estimates" (line 260, 267)
  - All required columns are present

- ✅ **AP_SignalMasterTable.parquet**: 
  - File exists (from `AP_SignalMasterTable.py`)
  - Contains `permno`, `time_avail_m`, `tickerIBES`, `prc` (from `AP_SignalMasterTable.py` structure)
  - All required columns are present

- ✅ **AP_m_aCompustat.parquet**: 
  - File exists (from `AP_CompustatAnnual.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `datadate` (fiscal period end date) - renamed from `period_end` (line 934), included in monthly version (line 1166-1167)
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor filters for `fpi == "1"` (next year forecasts) and March forecasts (`statpers.month == 3`)
- Uses December fiscal year ends only (`datadate.month == 12`)
- Filters for lower analyst coverage (bottom half by `numest`)
- Calculates `sfe = medest / abs(prc)` and holds for 12 months
- **Note**: `tickerIBES` must be populated in `AP_SignalMasterTable.parquet` (requires `AP_IBESCRSPLinkingTable.parquet`)

---

## 152. ShareIss1Y.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`
- **monthlyCRSP.parquet**: `permno`, `time_avail_m`, `shrout`, `cfacshr`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - File exists (from `AP_SignalMasterTable.py`)
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` structure)
  - All required columns are present

- ✅ **AP_monthlyCRSP.parquet**: 
  - File exists (from `AP_CRSPMonthly.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` structure)
  - Contains `shrout` (shares outstanding in millions, back-adjusted by split factor) - line 348
  - Contains `cfacshr` (cumulative share adjustment factor) - line 350
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor calculates growth in adjusted shares outstanding between t-18 and t-6 months
- Formula: `ShareIss1Y = (shrout_6mo * cfacshr_6mo - shrout_18mo * cfacshr_18mo) / (shrout_18mo * cfacshr_18mo)`
- Uses `shrout * cfacshr` to handle stock splits correctly
- **Note**: `cfacshr` is calculated from `facshr` (split factor) in `AP_CRSPMonthly.py` (line 299)

---

## 153. ShareIss5Y.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`
- **monthlyCRSP.parquet**: `permno`, `time_avail_m`, `shrout`, `cfacshr`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - File exists (from `AP_SignalMasterTable.py`)
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` structure)
  - All required columns are present

- ✅ **AP_monthlyCRSP.parquet**: 
  - File exists (from `AP_CRSPMonthly.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` structure)
  - Contains `shrout` (shares outstanding in millions, back-adjusted by split factor) - line 348
  - Contains `cfacshr` (cumulative share adjustment factor) - line 350
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor calculates 5-year growth in adjusted shares outstanding between t-65 and t-5 months
- Formula: `ShareIss5Y = (shrout_5mo * cfacshr_5mo - shrout_65mo * cfacshr_65mo) / (shrout_65mo * cfacshr_65mo)`
- Uses `shrout * cfacshr` to handle stock splits correctly
- **Note**: `cfacshr` is calculated from `facshr` (split factor) in `AP_CRSPMonthly.py` (line 299)

---

## 154. ShareRepurchase.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `prstkc`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - File exists (from `AP_CompustatAnnual.py`)
  - Contains `gvkey` (surrogate: CIK/permno) - from `AP_CompustatAnnual.py` structure
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `prstkc` (payments for repurchase of common stock) - mapped from XBRL tags (line 217, 330)
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor creates binary indicator: `ShareRepurchase = 1` if `prstkc > 0`, `0` otherwise
- Missing `prstkc` values result in missing `ShareRepurchase`
- **Note**: `gvkey` is loaded but not used in calculations (only kept for compatibility)
- **Note**: `prstkc` is extracted from cash flow statement XBRL tags: `PaymentsForRepurchaseOfCommonStock`, `TreasuryStockValueAcquiredCostMethod` (line 217, 330)

---

## 155. ShareVol.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `sicCRSP`, `exchcd`
- **monthlyCRSP.parquet**: `permno`, `time_avail_m`, `shrout`, `vol`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - File exists (from `AP_SignalMasterTable.py`)
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` structure)
  - Contains `sicCRSP` (SIC code from CRSP) - from `AP_SignalMasterTable.py` structure
  - Contains `exchcd` (exchange code) - from `AP_SignalMasterTable.py` structure
  - All required columns are present

- ✅ **AP_monthlyCRSP.parquet**: 
  - File exists (from `AP_CRSPMonthly.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` structure)
  - Contains `shrout` (shares outstanding in millions) - line 348
  - Contains `vol` (monthly volume in 100s of shares) - line 347, 438
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor calculates 3-month rolling share volume: `tempShareVol = (vol + l1_vol + l2_vol) / (3 * shrout) * 100`
- Excludes observations where shares outstanding changed in the past 3 months
- Creates binary signal: `ShareVol = 0` if `tempShareVol < 5`, `1` if `tempShareVol > 10`, missing otherwise
- Missing share volume values are treated as high and assigned `1`
- **Note**: `vol` is extracted from yfinance `Volume` field and converted to 100s of shares (line 321-322)

---

## Summary

### Overall Status:
**5 out of 5 predictors can be constructed** ✅

### Column Availability:
- ✅ All required columns are present in AP data sources
- ✅ No missing columns identified
- ✅ No column-related issues found

### Key Notes:
1. **For sfe**: 
   - **VERIFICATION**: Test that `medest`, `numest`, and `tickerIBES` are correctly populated
   - Requires `AP_IBESCRSPLinkingTable.parquet` to populate `tickerIBES` in `AP_SignalMasterTable.parquet`
   - Filters for December fiscal year ends and lower analyst coverage

2. **For ShareIss1Y and ShareIss5Y**: 
   - **VERIFICATION**: Test that `shrout` and `cfacshr` are correctly calculated
   - Uses adjusted shares (`shrout * cfacshr`) to handle stock splits
   - `cfacshr` is calculated from `facshr` (split factor) in `AP_CRSPMonthly.py`

3. **For ShareRepurchase**: 
   - **VERIFICATION**: Test that `prstkc` is correctly extracted from XBRL cash flow statements
   - Simple binary indicator based on positive `prstkc` values
   - **Note**: `gvkey` is loaded but not used in calculations

4. **For ShareVol**: 
   - **VERIFICATION**: Test that `vol` and `shrout` are correctly populated
   - Calculates 3-month rolling share volume and excludes observations with share changes
   - Creates binary signal based on volume thresholds

5. **Note on gvkey Surrogate**: 
   - `gvkey` is not directly available from EDGAR (Compustat-specific identifier)
   - AP version uses `cik` (SEC identifier) as surrogate, falls back to `permno` if unavailable
   - For `ShareRepurchase.py`, `gvkey` is loaded but not used in calculations (only kept for compatibility)

6. **Note on File Names**: 
   - File name mismatches are ignored per user instructions (from Group 19 onwards)
   - Predictors will need to be modified to use AP file names (`AP_dailyCRSP.parquet`, `AP_monthlyCRSP.parquet`, etc.) or AP files can be renamed to match expected names


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


# Group 34 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 34.

---

## 166. tang.py

### Required Columns:
- **m_aCompustat.parquet**: `permno`, `time_avail_m`, `che`, `rect`, `invt`, `ppegt`, `at`, `sic`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - File exists (from `AP_CompustatAnnual.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `che` (cash and cash equivalents) - mapped from XBRL tags (line 90): `['CashAndCashEquivalentsAtCarryingValue', 'CashAndDueFromBanks', 'Cash']`
  - Contains `rect` (accounts receivable current) - mapped from XBRL tags (line 93): `['AccountsReceivableNetCurrent', 'ReceivablesNetCurrent']`
  - Contains `invt` (inventory net) - mapped from XBRL tags (line 98): `['InventoryNet', 'Inventory']`
  - Contains `ppegt` (PP&E gross) - mapped from XBRL tags (line 107): `['PropertyPlantAndEquipmentGross']`
  - Contains `at` (total assets) - mapped from XBRL tags (line 84): `['Assets']`
  - Contains `sic` (SIC code) - added from static mapping file (line 612-627)
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor calculates tangibility for manufacturing firms (SIC 2000-3999): `tang = (che + 0.715*rect + 0.547*invt + 0.535*ppegt) / at`
- Uses Almeida and Campello (2007) formula for asset tangibility
- Filters for manufacturing firms only (SIC 2000-3999)
- Creates financial constraint measure based on size deciles
- **Note**: `sic` is added from static mapping file (`ticker_sic_mapping.xlsx`) in `AP_CompustatAnnual.py` (line 612-627)

---

## 167. Tax.py

### Required Columns:
- **m_aCompustat.parquet**: `permno`, `time_avail_m`, `txfo`, `txfed`, `ib`, `txt`, `txdi`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - File exists (from `AP_CompustatAnnual.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `txfo` (foreign income tax expense) - mapped from XBRL tags (line 358-359): `['IncomeTaxExpenseBenefitContinuingOperationsForeignIncomeTaxes', ...]`
  - Contains `txfed` (federal income tax expense) - mapped from XBRL tags (line 360-361): `['CurrentFederalTaxExpenseBenefit', 'FederalIncomeTaxExpenseBenefitContinuingOperations', ...]`
  - Contains `ib` (income before extraordinary items) - mapped from XBRL tags (line 269-271): `['NetIncomeLoss', 'ProfitLoss', 'IncomeLossFromContinuingOperations', ...]`
  - Contains `txt` (total income tax expense) - mapped from XBRL tags (line 363): `['IncomeTaxExpenseBenefit', 'IncomeTaxExpenseBenefitContinuingOperations']`
  - Contains `txdi` (deferred income tax expense) - mapped from XBRL tags (line 356-357): `['DeferredIncomeTaxExpenseBenefit', 'DeferredFederalStateAndLocalTaxExpenseBenefit', ...]`
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor calculates taxable income to income ratio: `Tax = ((txfo + txfed) / tr) / ib` or `Tax = ((txt - txdi) / tr) / ib` if foreign/federal taxes missing
- Uses time-varying tax rates (`tr`) based on year (0.48 default, 0.46 for 1979-1986, 0.4 for 1987, 0.34 for 1988-1992, 0.35 for 1993+)
- Handles division by zero cases (sets `Tax = 1` when `ib = 0` and tax activity exists)
- Sets `Tax = 1` when company has tax expense but negative income

---

## 168. TotalAccruals.py

### Required Columns:
- **m_aCompustat.parquet**: `permno`, `time_avail_m`, `ivao`, `ivst`, `dltt`, `dlc`, `pstk`, `sstk`, `prstkc`, `dv`, `act`, `che`, `lct`, `at`, `lt`, `ni`, `oancf`, `ivncf`, `fincf`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - File exists (from `AP_CompustatAnnual.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `ivao` (investments and other noncurrent assets) - mapped from XBRL tags (line 123-126): `['InvestmentsAndOtherNoncurrentAssets', 'OtherLongTermInvestments', ...]`
  - Contains `ivst` (short-term investments) - mapped from XBRL tags (line 122): `['ShortTermInvestments', 'MarketableSecuritiesCurrent', 'AvailableForSaleSecurities']`
  - Contains `dltt` (long-term debt) - mapped from XBRL tags (line 149-151): `['LongTermDebtNoncurrent', 'LongTermDebt', ...]`
  - Contains `dlc` (debt current) - mapped from XBRL tags (line 143-146): `['DebtCurrent', 'ShortTermBorrowings', ...]`
  - Contains `pstk` (preferred stock) - mapped from XBRL tags (line 196): `['PreferredStockValue', 'PreferredStockValueOutstanding', 'PreferredStockCarryingAmount']`
  - Contains `sstk` (proceeds from stock issuance) - mapped from XBRL tags (line 222-223): `['ProceedsFromIssuanceOfCommonStock', 'StockIssuedDuringPeriodValueNewIssues', ...]`
  - Contains `prstkc` (payments for repurchase of common stock) - mapped from XBRL tags (line 217-218): `['PaymentsForRepurchaseOfCommonStock', 'TreasuryStockValueAcquiredCostMethod', ...]`
  - Contains `dv` (dividends) - mapped from XBRL tags (line 340): `['Dividends', 'PaymentsOfDividends', 'PaymentsOfDividendsCommonStock', ...]`
  - Contains `act` (current assets) - mapped from XBRL tags (line 87): `['AssetsCurrent']`
  - Contains `che` (cash) - mapped from XBRL tags (line 90): `['CashAndCashEquivalentsAtCarryingValue', 'CashAndDueFromBanks', 'Cash']`
  - Contains `lct` (current liabilities) - mapped from XBRL tags (line 140): `['LiabilitiesCurrent']`
  - Contains `at` (total assets) - mapped from XBRL tags (line 84): `['Assets']`
  - Contains `lt` (total liabilities) - mapped from XBRL tags (line 137): `['Liabilities']`
  - Contains `ni` (net income) - mapped from XBRL tags (line 272-274): `['NetIncomeLoss', 'ProfitLoss', 'NetIncomeLossAvailableToCommonStockholdersBasic', ...]`
  - Contains `oancf` (operating cash flow) - mapped from XBRL tags (line 306): `['NetCashProvidedByUsedInOperatingActivities', ...]`
  - Contains `ivncf` (investing cash flow) - mapped from XBRL tags (line 311): `['NetCashProvidedByUsedInInvestingActivities', ...]`
  - Contains `fincf` (financing cash flow) - mapped from XBRL tags (line 316): `['NetCashProvidedByUsedInFinancingActivities', ...]`
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor calculates total accruals using balance sheet method (before 1988) or cash flow method (1988+)
- Balance sheet method: `TotalAccruals = (ΔWC + ΔNC + ΔFI) / at_lag12`
- Cash flow method: `TotalAccruals = (ni - (oancf + ivncf + fincf) + (sstk - prstkc - dv)) / at_lag12`
- Uses 12-month lagged assets for scaling
- Missing values set to 0 for temporary variables before calculation

---

## 169. TrendFactor.py

### Required Columns:
- **dailyCRSP.parquet**: `permno`, `time_d`, `prc`, `cfacpr`
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `ret`, `prc`, `exchcd`, `shrcd`, `mve_c`

### AP File Status:
- ✅ **AP_dailyCRSP.parquet**: 
  - File exists (from `AP_CRSPDaily.py`)
  - Contains `permno`, `time_d` (from `AP_CRSPDaily.py` structure)
  - Contains `prc` (stock price) - from `AP_CRSPDaily.py` structure
  - Contains `cfacpr` (cumulative price adjustment factor) - calculated from split factors (line 164 in `AP_CRSPDaily.py`)
  - All required columns are present

- ✅ **AP_SignalMasterTable.parquet**: 
  - File exists (from `AP_SignalMasterTable.py`)
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` structure)
  - Contains `ret` (monthly returns) - comes from `AP_monthlyCRSP.parquet`
  - Contains `prc` (stock price) - comes from `AP_monthlyCRSP.parquet`
  - Contains `exchcd` (exchange code) - from `AP_SignalMasterTable.py` structure
  - Contains `shrcd` (share class code) - from `AP_SignalMasterTable.py` structure
  - Contains `mve_c` (market value of equity) - included in column list (line 127), comes from `AP_monthlyCRSP.parquet`
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor calculates price trend factor using past 20-day returns and volumes
- Computes moving average prices for various lag lengths (3, 5, 10, 20, 50, 100, 200, 400, 600, 800, 1000 days)
- Runs cross-sectional regressions by month: `fRet ~ A_*` (future returns on moving averages)
- Computes 12-month rolling averages of beta coefficients (excludes current month)
- Calculates `TrendFactor` as smoothed regression model predictions: `TrendFactor = sum(EBeta_L * A_L)` for all lag lengths
- Filters for NYSE/AMEX/NASDAQ stocks, common shares only, price >= $5, and size >= NYSE 10th percentile
- **Note**: Uses `asreg_collinear` function for cross-sectional regressions with collinearity handling

---

## 170. UpRecomm.py

### Required Columns:
- **IBES_Recommendations.parquet**: `tickerIBES`, `amaskcd`, `anndats`, `time_avail_m`, `ireccd`
- **SignalMasterTable.parquet**: `permno`, `tickerIBES`, `time_avail_m`

### AP File Status:
- ❌ **AP_IBES_Recommendations.parquet**: **FILE DOES NOT EXIST YET**
  - Script exists: `AP_IBESRecommendations.py` (generates `AP_IBES_Recommendations.parquet`)
  - Would contain `tickerIBES` (IBES ticker) - from `AP_IBESRecommendations.py` structure
  - Would contain `amaskcd` (analyst mask code) - generated from broker_name + analyst_name (line 256-258 in `AP_IBESRecommendations.py`)
  - Would contain `anndats` (announcement date) - mapped from Eikon/LSEG API (line 234-235, 251-252)
  - Would contain `time_avail_m` (monthly availability date) - derived from `anndats` (line 305-306)
  - Would contain `ireccd` (recommendation code) - mapped from Eikon/LSEG API (line 236, 247-248)
  - **Note**: File name mismatch - predictor expects `IBES_Recommendations.parquet`, AP version would be `AP_IBES_Recommendations.parquet`
  - **Note**: Requires Eikon/LSEG API subscription (proprietary data source)

- ✅ **AP_SignalMasterTable.parquet**: 
  - File exists (from `AP_SignalMasterTable.py`)
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` structure)
  - Contains `tickerIBES` (IBES ticker) - populated from `AP_IBESCRSPLinkingTable.parquet` if available (line 99-116)
  - All required columns are present

### Can Be Constructed?
**NO** - Missing required data:
- ❌ `AP_IBES_Recommendations.parquet` does not exist (proprietary IBES recommendations data)

### Additional Work Needed?
**YES** - Missing file:

#### What Needs to Be Done:
1. **Create AP_IBES_Recommendations.parquet**: 
   - Run `AP_IBESRecommendations.py` to generate `AP_IBES_Recommendations.parquet`
   - Requires Eikon/LSEG API subscription (proprietary data source)
   - **Note**: File name mismatch - predictor expects `IBES_Recommendations.parquet`, AP version would be `AP_IBES_Recommendations.parquet`

#### Implementation Notes:
- Predictor creates binary indicator for analyst recommendation upgrades: `UpRecomm = 1` if `ireccd < ireccd_lag`, `0` otherwise
- Aggregates analyst-firm-month recommendations (last non-missing per analyst, then mean across analysts)
- Uses month-over-month changes to identify upgrades (lower values = better recommendations)
- **Note**: Requires `tickerIBES` to be populated in `AP_SignalMasterTable.parquet` (requires `AP_IBESCRSPLinkingTable.parquet`)

---

## Summary

### Overall Status:
**4 out of 5 predictors can be constructed** ✅

### Column Availability:
- ✅ **tang.py**: All required columns are present
- ✅ **Tax.py**: All required columns are present
- ✅ **TotalAccruals.py**: All required columns are present
- ✅ **TrendFactor.py**: All required columns are present
- ❌ **UpRecomm.py**: Missing `AP_IBES_Recommendations.parquet` (proprietary IBES recommendations data)

### Key Notes:
1. **For tang**: 
   - **VERIFICATION**: Test that `che`, `rect`, `invt`, `ppegt`, `at`, and `sic` are correctly populated
   - Uses Almeida and Campello formula for asset tangibility
   - Filters for manufacturing firms only (SIC 2000-3999)
   - **Note**: `sic` is added from static mapping file (`ticker_sic_mapping.xlsx`)

2. **For Tax**: 
   - **VERIFICATION**: Test that `txfo`, `txfed`, `ib`, `txt`, and `txdi` are correctly populated
   - Uses time-varying tax rates based on year
   - Handles division by zero cases and missing tax components
   - **Note**: Complex logic for handling missing foreign/federal taxes

3. **For TotalAccruals**: 
   - **VERIFICATION**: Test that all accrual-related columns are correctly populated
   - Uses balance sheet method (before 1988) or cash flow method (1988+)
   - Requires 12-month lagged assets for scaling
   - **Note**: Many columns required, but all are available in AP data

4. **For TrendFactor**: 
   - **VERIFICATION**: Test that `prc`, `cfacpr`, `ret`, `exchcd`, `shrcd`, and `mve_c` are correctly populated
   - Computes moving averages for 11 different lag lengths
   - Runs cross-sectional regressions by month
   - Computes 12-month rolling averages of beta coefficients
   - **Note**: Complex predictor requiring daily data and cross-sectional regressions

5. **For UpRecomm**: 
   - **VERIFICATION**: Test that `tickerIBES` is correctly populated in `AP_SignalMasterTable.parquet`
   - Requires `AP_IBES_Recommendations.parquet` (does not exist yet)
   - Requires Eikon/LSEG API subscription (proprietary data source)
   - **Note**: File name mismatch - predictor expects `IBES_Recommendations.parquet`, AP version would be `AP_IBES_Recommendations.parquet`

6. **Note on File Names**: 
   - File name mismatches are ignored per user instructions (from Group 19 onwards)
   - Predictors will need to be modified to use AP file names (`AP_dailyCRSP.parquet`, `AP_monthlyCRSP.parquet`, etc.) or AP files can be renamed to match expected names

7. **Note on IBES Recommendations**: 
   - IBES recommendations data is proprietary and requires Eikon/LSEG API subscription
   - `AP_IBESRecommendations.py` script exists but needs to be run to generate `AP_IBES_Recommendations.parquet`
   - This affects `UpRecomm.py` and other predictors that use IBES recommendations data (Groups 5, 8, 12, 29)


# Group 35 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 35.

---

## 171. VarCF.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `mve_permco`
- **m_aCompustat.parquet**: `permno`, `time_avail_m`, `ib`, `dp`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - File exists (from `AP_SignalMasterTable.py`)
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` structure)
  - Contains `mve_permco` (market value of equity at permco level) - included in column list (line 128), comes from `AP_monthlyCRSP.parquet`
  - All required columns are present

- ✅ **AP_m_aCompustat.parquet**: 
  - File exists (from `AP_CompustatAnnual.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `ib` (income before extraordinary items) - mapped from XBRL tags (line 269-271): `['NetIncomeLoss', 'ProfitLoss', 'IncomeLossFromContinuingOperations', ...]`
  - Contains `dp` (depreciation and amortization) - mapped from XBRL tags (line 252-254): `['Depreciation', 'DepreciationAndAmortization', 'DepreciationDepletionAndAmortization', ...]`
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor calculates rolling variance of cash flow to price ratio: `VarCF = var((ib + dp) / mve_permco)` over 60-month window (min 24 periods)
- Uses `asrol` function for rolling standard deviation, then squares to get variance
- **Note**: `mve_permco` comes from `AP_monthlyCRSP.parquet` where it's calculated as `mve_c` (line 370 in `AP_CRSPMonthly.py`)

---

## 172. VolMkt.py

### Required Columns:
- **monthlyCRSP.parquet**: `permno`, `time_avail_m`, `vol`, `prc`, `shrout`

### AP File Status:
- ✅ **AP_monthlyCRSP.parquet**: 
  - File exists (from `AP_CRSPMonthly.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` structure)
  - Contains `vol` (monthly volume in 100s of shares) - line 347, 438
  - Contains `prc` (month-end closing price) - line 346
  - Contains `shrout` (shares outstanding in millions) - line 348
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor calculates volume to market equity ratio: `VolMkt = (12-month avg dollar volume) / mve_c`
- Dollar volume = `vol * abs(prc)`
- Market value = `shrout * abs(prc)`
- Uses 12-month rolling mean of dollar volume (min 10 periods)
- **Note**: `vol` is in 100s of shares, so dollar volume calculation accounts for this

---

## 173. VolSD.py

### Required Columns:
- **monthlyCRSP.parquet**: `permno`, `time_avail_m`, `vol`

### AP File Status:
- ✅ **AP_monthlyCRSP.parquet**: 
  - File exists (from `AP_CRSPMonthly.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` structure)
  - Contains `vol` (monthly volume in 100s of shares) - line 347, 438
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor calculates rolling standard deviation of monthly trading volume: `VolSD = std(vol)` over 36-month window (min 24 periods)
- Uses Polars `rolling_std` function for efficient calculation
- **Note**: `vol` is in 100s of shares

---

## 174. VolumeTrend.py

### Required Columns:
- **monthlyCRSP.parquet**: `permno`, `time_avail_m`, `vol`

### AP File Status:
- ✅ **AP_monthlyCRSP.parquet**: 
  - File exists (from `AP_CRSPMonthly.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` structure)
  - Contains `vol` (monthly volume in 100s of shares) - line 347, 438
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor calculates rolling coefficient from regressing monthly trading volume on linear time trend: `VolumeTrend = beta / meanX`
- Uses 60-month rolling window regression (min 30 periods)
- Time trend: `time_numeric = (year - 1960) * 12 + month - 1`
- Scales coefficient by 60-month average volume (`meanX`)
- Winsorizes at 1st and 99th percentiles
- **Note**: Uses `polars_ols` for rolling OLS regression
- **Note**: `vol` is in 100s of shares

---

## 175. XFIN.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `sstk`, `dv`, `prstkc`, `dltis`, `dltr`, `dlcch`, `at`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - File exists (from `AP_CompustatAnnual.py`)
  - Contains `gvkey` (surrogate: CIK/permno) - from `AP_CompustatAnnual.py` structure
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `sstk` (proceeds from stock issuance) - mapped from XBRL tags (line 222-223): `['ProceedsFromIssuanceOfCommonStock', 'StockIssuedDuringPeriodValueNewIssues', ...]`
  - Contains `dv` (dividends) - mapped from XBRL tags (line 340): `['Dividends', 'PaymentsOfDividends', 'PaymentsOfDividendsCommonStock', ...]`
  - Contains `prstkc` (payments for repurchase of common stock) - mapped from XBRL tags (line 217-218): `['PaymentsForRepurchaseOfCommonStock', 'TreasuryStockValueAcquiredCostMethod', ...]`
  - Contains `dltis` (proceeds from issuance of long-term debt) - mapped from XBRL tags (line 155): `['ProceedsFromIssuanceOfLongTermDebt']`
  - Contains `dltr` (repayments of long-term debt) - mapped from XBRL tags (line 156): `['RepaymentsOfLongTermDebt']`
  - Contains `dlcch` (change in short-term borrowings) - mapped from XBRL tags (line 154): `['IncreaseDecreaseInShortTermBorrowings', 'ProceedsFromRepaymentsOfShortTermDebt']`
  - Contains `at` (total assets) - mapped from XBRL tags (line 84): `['Assets']`
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Predictor calculates net external financing: `XFIN = (sstk - dv - prstkc + dltis - dltr + dlcch) / at`
- Replaces missing `dlcch` with 0 (line 43)
- Scales by total assets (`at`)
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
1. **For VarCF**: 
   - **VERIFICATION**: Test that `ib`, `dp`, and `mve_permco` are correctly populated
   - Calculates rolling variance of cash flow to price ratio over 60-month window
   - Uses `asrol` function for rolling standard deviation
   - **Note**: `mve_permco` comes from `AP_monthlyCRSP.parquet` where it's calculated as `mve_c`

2. **For VolMkt**: 
   - **VERIFICATION**: Test that `vol`, `prc`, and `shrout` are correctly populated
   - Calculates 12-month average dollar volume scaled by market value
   - Uses 12-month rolling mean (min 10 periods)
   - **Note**: `vol` is in 100s of shares

3. **For VolSD**: 
   - **VERIFICATION**: Test that `vol` is correctly populated
   - Calculates rolling standard deviation of monthly trading volume over 36-month window
   - Uses Polars `rolling_std` function
   - **Note**: `vol` is in 100s of shares

4. **For VolumeTrend**: 
   - **VERIFICATION**: Test that `vol` is correctly populated
   - Calculates rolling coefficient from regressing volume on time trend
   - Uses 60-month rolling window regression (min 30 periods)
   - Winsorizes at 1st and 99th percentiles
   - **Note**: Uses `polars_ols` for rolling OLS regression
   - **Note**: `vol` is in 100s of shares

5. **For XFIN**: 
   - **VERIFICATION**: Test that `sstk`, `dv`, `prstkc`, `dltis`, `dltr`, `dlcch`, and `at` are correctly populated
   - Calculates net external financing scaled by total assets
   - Replaces missing `dlcch` with 0
   - **Note**: `gvkey` is loaded but not used in calculations (only kept for compatibility)

6. **Note on gvkey Surrogate**: 
   - `gvkey` is not directly available from EDGAR (Compustat-specific identifier)
   - AP version uses `cik` (SEC identifier) as surrogate, falls back to `permno` if unavailable
   - For `XFIN.py`, `gvkey` is loaded but not used in calculations (only kept for compatibility)

7. **Note on File Names**: 
   - File name mismatches are ignored per user instructions (from Group 19 onwards)
   - Predictors will need to be modified to use AP file names (`AP_dailyCRSP.parquet`, `AP_monthlyCRSP.parquet`, etc.) or AP files can be renamed to match expected names

8. **Note on Volume Units**: 
   - `vol` in `AP_monthlyCRSP.parquet` is in 100s of shares (line 242, 347)
   - This matches the original CRSP format and is accounted for in dollar volume calculations


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


# Group 37 Predictor Analysis: AP Data Column Coverage

## Overview
This document analyzes whether the AP (Alternative Provider) CSV/parquet files contain all required columns to construct the predictors in Group 37.

---

## 181. ZZ1_grcapx_grcapx1y_grcapx3y.py

### Required Columns:
- **m_aCompustat.parquet**: `gvkey`, `permno`, `time_avail_m`, `capx`, `ppent`, `at`
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `exchcd`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - File exists (from `AP_CompustatAnnual.py`)
  - Contains `gvkey` (surrogate: CIK/permno) - from `AP_CompustatAnnual.py` structure
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `capx` (capital expenditures) - mapped from XBRL tags (line 321): `['PaymentsToAcquirePropertyPlantAndEquipment', 'CapitalExpendituresIncurredButNotYetPaid', ...]`
  - Contains `ppent` (property, plant, and equipment net) - mapped from XBRL tags (line 105): `['PropertyPlantAndEquipmentNet']`
  - Contains `at` (total assets) - mapped from XBRL tags (line 84): `['Assets']`
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
- Calculates three capital expenditure growth measures:
  - **grcapx**: 2-year capital expenditure growth = `(capx - l24.capx) / l24.capx`
  - **grcapx1y**: 1-year capital expenditure growth (lagged) = `(l12.capx - l24.capx) / l24.capx`
  - **grcapx3y**: 3-year capital expenditure growth = `capx / (l12.capx + l24.capx + l36.capx) * 3`
- Replaces missing `capx` with change in `ppent` for firms with sufficient age (FirmAge >= 24)
- Requires minimum 24 months of data for `grcapx` and `grcapx1y`, 36 months for `grcapx3y`
- Uses firm age calculation to exclude observations where FirmAge equals time since CRSP start

---

## 182. ZZ1_IntanBM_IntanSP_IntanCFP_IntanEP.py

### Required Columns:
- **m_aCompustat.parquet**: `permno`, `gvkey`, `time_avail_m`, `sale`, `ib`, `dp`, `ni`, `ceq`
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `ret`, `mve_permco`

### AP File Status:
- ✅ **AP_m_aCompustat.parquet**: 
  - File exists (from `AP_CompustatAnnual.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CompustatAnnual.py` structure)
  - Contains `gvkey` (surrogate: CIK/permno) - from `AP_CompustatAnnual.py` structure
  - Contains `sale` (sales revenue) - mapped from XBRL tags (line 230): `['Revenues', 'SalesRevenueNet', ...]`
  - Contains `ib` (income before extraordinary items) - mapped from XBRL tags (line 269-271): `['NetIncomeLoss', 'ProfitLoss', ...]`
  - Contains `dp` (depreciation and amortization) - mapped from XBRL tags (line 293-295): `['DepreciationAndAmortization', 'DepreciationDepletionAndAmortization', ...]`
  - Contains `ni` (net income) - mapped from XBRL tags (line 272-274): `['NetIncomeLoss', 'ProfitLoss', ...]`
  - Contains `ceq` (common equity) - mapped from XBRL tags (line 190-191): `['StockholdersEquity', ...]`
  - All required columns are present

- ✅ **AP_SignalMasterTable.parquet**: 
  - File exists (from `AP_SignalMasterTable.py`)
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` structure)
  - Contains `ret` (monthly return) - comes from `AP_monthlyCRSP.parquet`
  - Contains `mve_permco` (market value of equity at permco level) - included in column list (line 128), comes from `AP_monthlyCRSP.parquet`
  - All required columns are present

### Can Be Constructed?
**YES** - All required columns are present.

### Additional Work Needed?
**NONE** - All columns are available.

#### Implementation Notes:
- Calculates four intangible return predictors using cross-sectional regressions:
  - **IntanBM**: Intangible return from book-to-market ratio
  - **IntanSP**: Intangible return from sales-to-price ratio
  - **IntanCFP**: Intangible return from cash flow-to-price ratio
  - **IntanEP**: Intangible return from earnings-to-price ratio
- Uses 60-month calendar-based lags for cumulative returns and accounting measures
- Runs cross-sectional regressions: `tempRet60 ~ lag60(v) + vRet` for each time period
- Predictors are residuals from these regressions
- Winsorizes forecast errors at 1st and 99th percentiles
- Requires minimum 2 observations per time period for regression

---

## 183. ZZ1_iomom_cust__iomom_supp.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `gvkey`, `time_avail_m`
- **InputOutputMomentumProcessed.parquet**: `gvkey`, `time_avail_m`, `retmatchcustomer`, `portindcustomer`, `retmatchsupplier`, `portindsupplier`
- **InputOutputMomentum_R.csv**: Intermediate file generated by `ZZ1_iomom_cust__iomom_supp.R`

### AP File Status:
- ✅ **AP_SignalMasterTable.parquet**: 
  - File exists (from `AP_SignalMasterTable.py`)
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` structure)
  - Contains `gvkey` (surrogate: CIK/permno) - line 119, filled from `permno` if missing
  - All required columns are present

- ⚠️ **InputOutputMomentumProcessed.parquet**: **FILE GENERATED BY R SCRIPT**
  - File is generated by `ZZ1_iomom_cust__iomom_supp.py` after processing `InputOutputMomentum_R.csv`
  - `InputOutputMomentum_R.csv` is generated by `ZZ1_iomom_cust__iomom_supp.R`
  - See analysis of `ZZ1_iomom_cust__iomom_supp.R` below for dependencies

### Can Be Constructed?
**DEPENDS ON R SCRIPT** - This Python script calls the R script and processes its output. See `ZZ1_iomom_cust__iomom_supp.R` analysis below.

### Additional Work Needed?
**SEE R SCRIPT ANALYSIS BELOW**

#### Implementation Notes:
- Python wrapper script that calls `ZZ1_iomom_cust__iomom_supp.R` to calculate input-output momentum
- Processes R script output (`InputOutputMomentum_R.csv`) to create `InputOutputMomentumProcessed.parquet`
- Collapses data by averaging `retmatch` and `portind` within `gvkey-time_avail_m-type` groups
- Reshapes from long to wide format by type (customer/supplier)
- Creates two predictors:
  - **iomom_cust**: Customer momentum from `retmatchcustomer`
  - **iomom_supp**: Supplier momentum from `retmatchsupplier`

---

## 184. ZZ1_iomom_cust__iomom_supp.R

### Required Columns:
- **CompustatAnnual.csv**: `gvkey`, `datadate`, `naicsh` (NAICS code)
- **monthlyCRSP.parquet**: `permno`, `time_avail_m`, `ret`, `prc`, `shrout`
- **CCMLinkingTable.parquet**: `gvkey`, `permno`, `timeLinkStart_d`, `timeLinkEnd_d`, `linkprim`
- **BEA Input-Output Tables**: 
  - `IOMake_Before_Redefinitions_1963-1996_Summary.xlsx`
  - `IOUse_Before_Redefinitions_PRO_1963-1996_Summary.xlsx`
  - `Supply_Tables_1997-20XX_Summary.xlsx`
  - `Supply-Use_Framework_1997-20XX_Summary.xlsx`

### AP File Status:
- ❌ **CompustatAnnual.csv**: **FILE EXISTS BUT MISSING COLUMN**
  - File exists (from `CompustatAnnual.py` - WRDS version)
  - Contains `gvkey`, `datadate` (from `CompustatAnnual.py` structure)
  - **Missing**: `naicsh` (NAICS code) - NOT extracted in `AP_CompustatAnnual.py`
  - **Note**: This is NOT an AP script - it downloads from WRDS Compustat
  - **Note**: R script requires `naicsh` to map firms to BEA industries (line 259-264)

- ✅ **AP_monthlyCRSP.parquet**: 
  - File exists (from `AP_CRSPMonthly.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` structure)
  - Contains `ret` (monthly return) - from `AP_CRSPMonthly.py` structure
  - Contains `prc` (stock price) - from `AP_CRSPMonthly.py` structure
  - Contains `shrout` (shares outstanding) - line 348
  - All required columns are present

- ⚠️ **CCMLinkingTable.parquet**: **FILE EXISTS** (WRDS version)
  - File exists: `CCMLinkingTable.parquet` (from `CCMLinkingTable.py`)
  - Contains `gvkey`, `permno`, `timeLinkStart_d`, `timeLinkEnd_d`, `linkprim`
  - **Note**: This is NOT an AP script - it downloads from WRDS
  - **Issue**: Uses real Compustat `gvkey` values, while AP Compustat data uses surrogate `gvkey` (CIK/permno)
  - **Can be used IF**: Using WRDS `CompustatAnnual.csv` (real gvkey) - linking table will match
  - **Cannot be used IF**: Using AP Compustat data (surrogate gvkey) - linking table won't match

- ⚠️ **BEA Input-Output Tables**: **FILES EXIST BUT FORMAT MISMATCH**
  - Files exist: Downloaded by `BEAInputOutput.py` (non-AP script) or `AP_BEAInputOutput.py` (AP version)
  - **Non-AP version** (`BEAInputOutput.py`): Downloads Excel files matching expected names:
    - `IOMake_Before_Redefinitions_1963-1996_Summary.xlsx`
    - `IOUse_Before_Redefinitions_PRO_1963-1996_Summary.xlsx`
    - `Supply_Tables_1997-20XX_Summary.xlsx`
    - `Supply-Use_Framework_1997-20XX_Summary.xlsx`
  - **AP version** (`AP_BEAInputOutput.py`): Downloads via BEA API but outputs parquet/CSV files:
    - `AP_BEA_Supply_Table.parquet` / `.csv`
    - `AP_BEA_SupplyUse_Framework.parquet` / `.csv`
  - **Issue**: R script expects specific Excel file names (line 290-296), AP version outputs different format
  - **Can be used IF**: Using non-AP `BEAInputOutput.py` - files match expected names
  - **Cannot be used IF**: Using AP version - file names and format don't match

### Can Be Constructed?
**PARTIALLY** - Multiple issues:
- ❌ `naicsh` missing from `CompustatAnnual.csv` (AP version doesn't extract NAICS codes)
- ⚠️ `CCMLinkingTable.parquet` exists but has `gvkey` mismatch (WRDS version uses real gvkey)
- ⚠️ BEA Input-Output tables format mismatch (AP version outputs different format than expected)

### Additional Work Needed?
**YES** - Multiple issues:

#### What Needs to Be Done:
1. **Extract `naicsh` from XBRL DEI section**: 
   - `naicsh` (NAICS code) is required to map firms to BEA industries
   - Should be extracted from the DEI (Document and Entity Information) section of XBRL filings
   - **Solution**: Modify `AP_CompustatAnnual.py` to extract `naicsh` from XBRL DEI section
   - **Note**: `naicsh` is a 6-digit NAICS code used to match firms to BEA industry codes

2. **Create AP version of CompustatAnnual.csv**: 
   - R script expects `CompustatAnnual.csv` (not parquet) with specific format
   - **Solution**: Modify `AP_CompustatAnnual.py` to also output CSV format matching WRDS version
   - **Note**: CSV format uses Stata-formatted dates (`datadate` as `ddmmmyyyy` lowercase)

3. **Create AP version of CCMLinkingTable.parquet**: 
   - R script uses `CCMLinkingTable.parquet` to link CRSP `permno` to Compustat `gvkey`
   - **Solution**: Create `AP_CCMLinkingTable.parquet` that links `permno` to surrogate `gvkey` (CIK/permno)
   - **Note**: This would allow AP Compustat data to match AP linking table

4. **Modify R script to use AP BEA Input-Output tables**: 
   - R script expects specific Excel file names, AP version outputs parquet/CSV
   - **Solution**: Either modify R script to read AP parquet/CSV files, or modify `AP_BEAInputOutput.py` to output Excel format matching expected names
   - **Note**: R script processes Excel files with specific sheet names and formats (line 45-61)

#### Implementation Notes:
- Calculates input-output momentum following Menzly-Ozbas (2010) methodology
- Uses BEA Input-Output tables to identify customer and supplier relationships
- Maps firms to BEA industries using NAICS codes
- Calculates value-weighted industry returns
- Creates momentum portfolios based on matched industry returns
- Uses 5-year lag from survey to release (line 68)
- Requires NAICS codes available from 1986 onwards (line 171)

---

## 185. ZZ1_OptionVolume1_OptionVolume2.py

### Required Columns:
- **SignalMasterTable.parquet**: `permno`, `time_avail_m`, `secid`, `prc`, `shrcd`
- **monthlyCRSP.parquet**: `permno`, `time_avail_m`, `vol`
- **OptionMetricsVolume.csv**: `secid`, `date`, `optvolume_js12`

### AP File Status:
- ⚠️ **AP_SignalMasterTable.parquet**: **MISSING COLUMN**
  - File exists (from `AP_SignalMasterTable.py`)
  - Contains `permno`, `time_avail_m` (from `AP_SignalMasterTable.py` structure)
  - Contains `prc` (stock price) - comes from `AP_monthlyCRSP.parquet`
  - Contains `shrcd` (share class code) - from `AP_SignalMasterTable.py` structure
  - **Missing**: `secid` (OptionMetrics security identifier) - initialized to `np.nan` in `AP_SignalMasterTable.py` (line 130)
  - **Note**: `secid` is required to link to OptionMetrics data

- ✅ **AP_monthlyCRSP.parquet**: 
  - File exists (from `AP_CRSPMonthly.py`)
  - Contains `permno`, `time_avail_m` (from `AP_CRSPMonthly.py` structure)
  - Contains `vol` (trading volume) - from `AP_CRSPMonthly.py` structure
  - All required columns are present

- ❌ **OptionMetricsVolume.csv**: **FILE DOES NOT EXIST** (proprietary data)
  - File would be generated by `PrepScripts/OptionMetricsVolume.R` (requires WRDS OptionMetrics access)
  - Would contain `secid`, `date`, `optvolume_js12` (option trading volume)
  - **Note**: This is proprietary OptionMetrics data, not available from free sources
  - **Note**: Requires WRDS OptionMetrics database access

### Can Be Constructed?
**NO** - Missing required data:
- ❌ `secid` missing from `AP_SignalMasterTable.parquet`
- ❌ `OptionMetricsVolume.csv` does not exist (proprietary OptionMetrics data)

### Additional Work Needed?
**YES** - Multiple issues:

#### What Needs to Be Done:
1. **Populate `secid` in AP_SignalMasterTable.parquet**: 
   - `secid` is OptionMetrics security identifier, required to link to OptionMetrics data
   - **Solution**: Create mapping from `permno` to `secid` using OptionMetrics database or historical mapping files
   - **Note**: OptionMetrics uses `secid` as primary identifier, CRSP uses `permno` - mapping is complex and time-varying
   - **Alternative**: If OptionMetrics data is not available, predictor cannot be constructed

2. **Obtain OptionMetricsVolume.csv**: 
   - OptionMetrics data is proprietary and requires WRDS subscription
   - **Solution**: Run `PrepScripts/OptionMetricsVolume.R` on WRDS to generate `OptionMetricsVolume.csv`
   - **Note**: This requires WRDS OptionMetrics database access (not available from free sources)
   - **Alternative**: If OptionMetrics data is not available, predictor cannot be constructed

#### Implementation Notes:
- Calculates two option trading volume predictors:
  - **OptionVolume1**: Option-to-stock volume ratio = `optvolume_js12 / vol`
  - **OptionVolume2**: Abnormal option volume = `OptionVolume1 / 6-month average of OptionVolume1`
- Sets `OptionVolume1` to missing if prior period option or stock volume is missing
- Uses 6-month moving average for `OptionVolume2` calculation
- Requires `secid` to link OptionMetrics data to CRSP data
- **Note**: OptionMetrics data is proprietary and not available from free sources

---

## Summary

### Overall Status:
**2 out of 5 predictors can be constructed** ✅

### Column Availability:
- ✅ **ZZ1_grcapx_grcapx1y_grcapx3y.py**: All required columns are present
- ✅ **ZZ1_IntanBM_IntanSP_IntanCFP_IntanEP.py**: All required columns are present
- ⚠️ **ZZ1_iomom_cust__iomom_supp.py**: Depends on R script (see below)
- ⚠️ **ZZ1_iomom_cust__iomom_supp.R**: Partially constructible - missing `naicsh`, `gvkey` mismatch, BEA table format mismatch
- ❌ **ZZ1_OptionVolume1_OptionVolume2.py**: Cannot be constructed - missing `secid` and proprietary OptionMetrics data

### Key Notes:
1. **For ZZ1_grcapx_grcapx1y_grcapx3y**: 
   - **VERIFICATION**: Test that `capx`, `ppent`, `at` are correctly populated
   - Calculates three capital expenditure growth measures using lagged values
   - Replaces missing `capx` with change in `ppent` for firms with sufficient age

2. **For ZZ1_IntanBM_IntanSP_IntanCFP_IntanEP**: 
   - **VERIFICATION**: Test that `sale`, `ib`, `dp`, `ni`, `ceq`, `ret`, `mve_permco` are correctly populated
   - Uses cross-sectional regressions to calculate intangible return predictors
   - Requires 60-month calendar-based lags for cumulative returns and accounting measures

3. **For ZZ1_iomom_cust__iomom_supp (Python and R)**: 
   - **VERIFICATION**: Test that `naicsh` is correctly extracted from XBRL DEI section
   - Requires multiple data sources: CompustatAnnual.csv, CCMLinkingTable.parquet, monthlyCRSP.parquet, BEA Input-Output tables
   - **Critical Issues**:
     - `naicsh` missing from AP Compustat data (needs XBRL DEI extraction)
     - `CCMLinkingTable.parquet` has `gvkey` mismatch (WRDS version uses real gvkey)
     - BEA Input-Output tables format mismatch (AP version outputs different format)
   - **Note**: R script expects specific Excel file names and formats, AP version outputs parquet/CSV

4. **For ZZ1_OptionVolume1_OptionVolume2**: 
   - **VERIFICATION**: Test that `secid` is correctly populated in `AP_SignalMasterTable.parquet`
   - Requires proprietary OptionMetrics data (`OptionMetricsVolume.csv`)
   - **Critical Issues**:
     - `secid` missing from `AP_SignalMasterTable.parquet` (initialized to `np.nan`)
     - `OptionMetricsVolume.csv` does not exist (proprietary OptionMetrics data)
   - **Note**: OptionMetrics data is proprietary and requires WRDS subscription

5. **Note on gvkey Surrogate**: 
   - `gvkey` is not directly available from EDGAR (Compustat-specific identifier)
   - AP version uses `cik` (SEC identifier) as surrogate, falls back to `permno` if unavailable
   - For `ZZ1_iomom_cust__iomom_supp.R`, `gvkey` is used for merging with `CCMLinkingTable.parquet`
   - **Issue**: WRDS `CCMLinkingTable.parquet` uses real Compustat `gvkey`, won't match AP surrogate `gvkey`

6. **Note on File Names**: 
   - File name mismatches are ignored per user instructions (from Group 19 onwards)
   - Predictors will need to be modified to use AP file names (`AP_dailyCRSP.parquet`, `AP_monthlyCRSP.parquet`, etc.) or AP files can be renamed to match expected names

7. **Note on Proprietary Data**: 
   - **OptionMetricsVolume.csv**: Requires proprietary OptionMetrics database access (WRDS subscription)
   - **CCMLinkingTable.parquet**: Exists but is WRDS version (uses real Compustat `gvkey`)
   - **CompustatAnnual.csv**: Exists but is WRDS version (requires `naicsh` extraction for AP version)

8. **Note on BEA Input-Output Tables**: 
   - **Non-AP version** (`BEAInputOutput.py`): Downloads Excel files matching expected names
   - **AP version** (`AP_BEAInputOutput.py`): Downloads via BEA API but outputs parquet/CSV files
   - R script expects specific Excel file names and formats
   - **Solution**: Either modify R script to read AP parquet/CSV files, or modify `AP_BEAInputOutput.py` to output Excel format


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


