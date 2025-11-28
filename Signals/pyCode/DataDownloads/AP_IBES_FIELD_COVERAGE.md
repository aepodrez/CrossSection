# IBES Field Coverage Analysis

## ✅ All Critical Fields Covered!

Based on the IBESGuide.md and predictor requirements, **ALL critical fields for the 9 IBES-dependent predictors are available** via the Eikon API.

---

## 📊 Field-by-Field Comparison

### **Fully Covered - Core Fields** ✅

| WRDS Field | Eikon API Field | AP File | Status |
|-----------|----------------|---------|---------|
| `meanest` | `TR.EPSMeanEstimate` | All EPS files | ✅ Perfect |
| `medest` | `TR.EPSMedianEstimate` | All EPS files | ✅ Perfect |
| `numest` | `TR.EPSNumberOfEstimates` | All EPS files | ✅ Perfect |
| `stdev` | `TR.EPSStdDev` | All EPS files | ✅ Perfect |
| `statpers` | `TR.EPSMeanEstimate.date` | All EPS files | ✅ Perfect |
| `fpedats` | `TR.EPSMeanEstimate.fperiod` | All EPS files | ✅ Perfect |
| `fpi` | Derived from Period param | All EPS files | ✅ Perfect |
| `ireccd` | `TR.RecEstValue` | Recommendations | ✅ Perfect |
| `int0a` | `TR.EPSActValue` | Actuals | ✅ Perfect |
| `tickerIBES` | From RIC symbol | All files | ✅ Perfect |
| `time_avail_m` | Derived from statpers | All files | ✅ Perfect |

**These are the fields actually used by predictors** - all available! ✅

---

## ⚠️ Auxiliary Fields - Some Missing/Different

These are metadata or secondary fields from WRDS that have limited/different availability in Eikon:

### **From IBESEPSAdjusted.py (actpsum_epsus table)**

| WRDS Field | Eikon Equivalent | Status | Impact |
|-----------|------------------|---------|--------|
| `prdays` | N/A | ❌ Not directly available | ⚠️ Low - not used by predictors |
| `price` | Could get from `TR.Price` | ⚠️ Requires separate query | ⚠️ Low - can get from yfinance |
| `shout` | `TR.CommonSharesOutstanding` | ✅ Available | ✅ Included in AP files |
| `actual` | `TR.EPSActValue` | ✅ Available | ✅ Included |
| `anndats_act` | `TR.EPSActReportDate` | ✅ Available | ✅ Included |

**Impact:** Minimal - `price` and `prdays` not used by any predictors in the guide.

### **From IBESRecommendations.py**

| WRDS Field | Eikon Equivalent | Status | Impact |
|-----------|------------------|---------|--------|
| `ireccd` | `TR.RecEstValue` | ✅ Available | ✅ Core field |
| `itext` | `TR.BrkRecLabel` | ✅ Available | ✅ Included |
| `amaskcd` | Derived from broker+analyst | ✅ Created | ✅ Included |
| `anndats` | From `.date` suffix | ✅ Available | ✅ Included |
| `estimid` | N/A | ❌ Not available | ⚠️ Low - internal WRDS ID |
| `ereccd` | N/A | ❌ Not available | ⚠️ Low - not used by predictors |
| `etext` | N/A | ❌ Not available | ⚠️ Low - not used by predictors |
| `emaskcd` | N/A | ❌ Not available | ⚠️ Low - not used by predictors |
| `actdats` | N/A | ❌ Not available | ⚠️ Low - not used by predictors |

**Impact:** Minimal - missing fields are earnings recommendations (separate from buy/sell), which predictors don't use.

### **From IBESUnadjustedActuals.py**

| WRDS Field | Eikon Equivalent | Status | Impact |
|-----------|------------------|---------|--------|
| `int0a` | `TR.EPSActValue` | ✅ Available | ✅ Core field |
| `fy0a` | Could derive from annual | ⚠️ Partial | ⚠️ Medium |
| `fy0edats` | `TR.ISPeriodEndDate` | ✅ Available | ✅ Included |
| `shoutIBESUnadj` | `TR.CommonSharesOutstanding` | ✅ Available | ✅ Included |
| `statpers` | `TR.EPSActReportDate` | ✅ Available | ✅ Included |
| `cusip` | `TR.CUSIP` | ✅ Available (not included) | ⚠️ Low |
| `oftic` | `TR.TickerSymbol` | ✅ Available | ✅ Included |
| `curr_price` | N/A | ❌ Not available | ⚠️ Low - indicator flag |
| `measure` | Hardcoded "EPS" | ✅ Created | ✅ Included |

**Impact:** Low - `fy0a` (full year actual) vs `int0a` (interim) distinction might matter for some use cases, but guide doesn't mention it.

---

## 🎯 Predictor Requirements Analysis

### **All 9 Predictors Fully Supported** ✅

| Predictor | Required Fields | Available? | Notes |
|-----------|----------------|------------|-------|
| **ChNAnalyst** | numest, statpers, fpedats, meanest | ✅ Yes | All core fields available |
| **sfe** | medest, statpers, fpedats, numest | ✅ Yes | All core fields available |
| **FEPS** | meanest, statpers, fpedats | ✅ Yes | All core fields available |
| **ForecastDispersion** | stdev, meanest, fpedats | ✅ Yes | All core fields available |
| **ChForecastAccrual** | meanest, statpers | ✅ Yes | All core fields available |
| **REV6** | meanest, statpers, fpedats, numest | ✅ Yes | All core fields available |
| **ConsRecomm** | ireccd, amaskcd, anndats | ✅ Yes | All core fields available |
| **Recomm_ShortInterest** | ireccd, amaskcd, anndats | ✅ Yes | All core fields available |
| **AnalystRevision** | meanest, statpers | ✅ Yes | All core fields available |
| **fgr5yrLag** | LTG meanest, fpi=0 | ✅ Yes | Available via TR.LTGMean |

---

## 🔍 Missing Fields Deep Dive

### **Not Mentioned in Guide (Discovered from WRDS Files)**

These fields exist in WRDS but weren't documented in IBESGuide.md:

#### 1. **Earnings Recommendations** (ereccd, etext, emaskcd)
- **What:** Separate from buy/sell recommendations, these are earnings-specific recommendations
- **Eikon:** Not mentioned in guide, likely not directly available
- **Impact:** ❌ No predictor uses these
- **Workaround:** Not needed

#### 2. **Price/Date Fields** (prdays, price, curr_price)
- **What:** Stock prices from IBES actuals database
- **Eikon:** Could get from `TR.Price` but requires separate query
- **Impact:** ⚠️ Can use yfinance prices instead (which we already have in AP_CRSPDaily/Monthly)
- **Workaround:** ✅ Use AP_CRSPDaily.parquet or AP_CRSPMonthly.parquet for prices

#### 3. **Full Year Actuals** (fy0a vs int0a)
- **What:** WRDS distinguishes between interim (quarterly) and full year actuals
- **Eikon:** `TR.EPSActValue` returns actuals but unclear if it distinguishes
- **Impact:** ⚠️ Might matter for annual vs quarterly analysis
- **Workaround:** ✅ Query with FRQ="A" for annual, FRQ="Q" for quarterly

#### 4. **Internal IDs** (estimid, oftic nuances)
- **What:** WRDS internal identifiers
- **Eikon:** Uses RIC format instead
- **Impact:** ❌ Not needed (linking handled differently)
- **Workaround:** ✅ Use RIC symbols and AP_IBESCRSPLink.py

---

## ✅ Conclusion: You Have Everything You Need!

### **Critical Fields: 100% Coverage** ✅

All fields required by the 9 IBES-dependent predictors listed in IBESGuide.md are available via Eikon API and included in the AP files I created.

### **Missing Fields: All Non-Critical** ⚠️

The missing fields fall into these categories:
1. **Metadata** (estimid, internal IDs) - Not used by predictors
2. **Earnings recommendations** (ereccd, etext) - Different from buy/sell recs, not used
3. **Price fields** (prdays, price) - Can get from yfinance (AP_CRSP files)
4. **CUSIP** - Available in Eikon but not included (can add if needed)

### **Can Add If Needed:**

If you discover a predictor needs any of these, we can add:
- `cusip` - Add `TR.CUSIP` to API calls
- `price` - Add `TR.Price` to API calls or use AP_CRSP files
- `fy0a` - Query annual actuals separately with FRQ="A"

### **Bottom Line:**

**✅ The AP IBES files provide 100% coverage for all predictors mentioned in IBESGuide.md**

The guide was comprehensive - it documented all the fields actually used by predictors. The missing fields are auxiliary/metadata that aren't used in factor calculations.

---

## 🔄 If You Find Missing Fields

If you encounter a predictor that needs a field not in the current AP files:

1. **Check IBESGuide.md** - Look for the Eikon equivalent (line 60-1212)
2. **Add to AP file** - I can add more fields to the Eikon API calls
3. **Use workaround** - Some fields (like prices) available from other AP files

Just let me know which specific field is needed and I'll add it! But based on the guide, **everything required is already there**. ✅

