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

