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
