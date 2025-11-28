# IBES AP Files Implementation Summary

## ⚠️ Important: IBES Data Requires Paid Subscription

**IBES data is ONLY available through Eikon/LSEG API** - it is NOT available from free sources like SEC EDGAR or Yahoo Finance.

**Cost:** Included in Eikon/Refinitiv subscription (~$20,000-30,000/year per user)

---

## ✅ Files Created (2 of 4)

### 1. **AP_IBESEPSAdjusted.py** ✅
- **Source:** Eikon API (TR.EPSMeanEstimate, TR.EPSNumberOfEstimates, etc.)
- **Output:** `AP_IBES_EPS_Adj.parquet`
- **Data:** EPS estimates, analyst coverage, forecast dispersion
- **Frequency:** Monthly (aggregated from daily)
- **Fields:** meanest, medest, numest, stdev, actual, fpedats

### 2. **AP_IBESRecommendations.py** ✅
- **Source:** Eikon API (TR.RecEstValue, TR.BrkRecLabel, etc.)
- **Output:** `AP_IBES_Recommendations.parquet`
- **Data:** Analyst recommendations (1-5 scale)
- **Frequency:** Daily
- **Fields:** ireccd (recommendation code), broker_name, analyst_name

---

## 🔴 Not Implemented (Would Require Same Eikon Subscription)

### 3. AP_IBESUnadjustedActuals.py (Could be created)
- **Source:** Eikon API (TR.EPSActValue, TR.EPSActReportDate, etc.)
- **Data:** Actual reported earnings (unadjusted for splits)
- **Note:** Less critical - actual earnings also available from SEC EDGAR

### 4. AP_IBESLongTermGrowth.py (Could be created)
- **Source:** Eikon API (TR.LTGMean, TR.LTGMedian, etc.)
- **Data:** 5-year forward growth forecasts
- **Note:** Used by fgr5yrLag predictor

---

## 📋 Setup Requirements

### 1. Eikon Subscription
- Contact Refinitiv/LSEG for subscription
- Cost: ~$20,000-30,000/year
- Alternative: Bloomberg Terminal (similar cost)

### 2. Eikon Desktop Application
- Must be installed and running
- Required for API authentication

### 3. API Key
```bash
# Get from Eikon Desktop:
# 1. Type "APPKEY" in Eikon search
# 2. Generate App Key
# 3. Add to .env file:
echo "EIKON_APP_KEY=your_key_here" >> .env
```

### 4. Python Package
```bash
pip install eikon
```

---

## 🚀 Quick Start (If You Have Eikon Subscription)

```bash
cd /Users/alexpodrez/Documents/CrossSection/Signals/pyCode/DataDownloads

# 1. Ensure Eikon Desktop is running

# 2. Test with DEBUG_MODE
# Edit files: Set DEBUG_MODE = True (line ~50)

# 3. Run scripts
python AP_IBESEPSAdjusted.py      # ~5-10 min in DEBUG, ~1-2 hours full
python AP_IBESRecommendations.py   # ~5-10 min in DEBUG, ~1-2 hours full

# 4. Verify outputs
ls -lh ../pyData/Intermediate/AP_IBES_*
```

---

## ⚡ Performance & Rate Limits

### Eikon API Rate Limits
- **5 requests/second**
- **300 requests/minute**
- **10,000 data points per request**

### Expected Runtimes (500 tickers)

| Script | Debug Mode | Production | Records |
|--------|-----------|-----------|----------|
| EPS Estimates | ~5 min | ~1-2 hours | ~50,000-100,000 |
| Recommendations | ~5 min | ~1-2 hours | ~100,000-500,000 |

**Note:** First run is slow due to historical data download. No caching implemented yet (could be added).

---

## 💰 Cost Analysis

### Option 1: Use IBES via Eikon (Current AP Implementation)
- **Cost:** $20,000-30,000/year (Eikon subscription)
- **Coverage:** Complete IBES data
- **Latency:** Real-time to 1-day lag
- **Historical:** 1990s-present

### Option 2: Use WRDS IBES (Original Implementation)
- **Cost:** $2,000-5,000/year (WRDS subscription)
- **Coverage:** Complete IBES data
- **Latency:** ~1 week lag
- **Historical:** 1990s-present

### Option 3: Free Alternatives (LIMITED)
**Analyst Estimates:**
- Alpha Vantage: $50/month (limited coverage)
- Financial Modeling Prep: $30/month (limited history)
- Yahoo Finance: Free but unreliable

**Recommendations:**
- Yahoo Finance: Free, limited historical
- Finviz: Free, current only
- TipRanks: Paid, $30-100/month

**❌ No true free alternative for complete IBES data**

---

## 🎯 Which Predictors Need IBES?

### Critical (Need IBES)
1. **ChNAnalyst** - Decline in analyst coverage
2. **sfe** - Earnings forecast to price
3. **FEPS** - Earnings forecast
4. **ForecastDispersion** - Forecast dispersion
5. **REV6** - 6-month analyst revision
6. **ConsRecomm** - Consensus recommendation
7. **Recomm_ShortInterest** - Recommendations + short interest
8. **AnalystRevision** - 1-month analyst revision
9. **fgr5yrLag** - Long-term growth forecast

### Can Work Without IBES
- Most other predictors use fundamentals from SEC EDGAR or prices from yfinance

---

## 📊 Data Quality Comparison

| Feature | IBES (Eikon/WRDS) | Free Alternatives |
|---------|-------------------|-------------------|
| **Analyst Coverage** | ✅ Complete | ⚠️ Top stocks only |
| **Historical Depth** | ✅ 1990s+ | ❌ 2010s+ |
| **Forecast Detail** | ✅ Mean, median, std | ⚠️ Mean only |
| **Recommendations** | ✅ Individual analysts | ❌ Consensus only |
| **Long-term Growth** | ✅ Yes | ❌ No |
| **Unadjusted Data** | ✅ Yes | ❌ No |
| **Update Frequency** | ✅ Daily | ⚠️ Weekly/monthly |

---

## 🔄 Integration

### If You Have Eikon:

```python
# Replace in your analysis code:
# Old:
ibes_eps = pd.read_parquet('../pyData/Intermediate/IBES_EPS_Adj.parquet')
ibes_rec = pd.read_parquet('../pyData/Intermediate/IBES_Recommendations.parquet')

# New:
ibes_eps = pd.read_parquet('../pyData/Intermediate/AP_IBES_EPS_Adj.parquet')
ibes_rec = pd.read_parquet('../pyData/Intermediate/AP_IBES_Recommendations.parquet')
```

### If You DON'T Have Eikon:

**Option 1:** Skip IBES-dependent predictors
- Focus on ~80% of predictors that don't need IBES
- Use fundamentals (Compustat) + prices (CRSP) only

**Option 2:** Use free alternatives (limited quality)
- Implement AP_IBESAlternative.py using Alpha Vantage or Yahoo Finance
- Expect lower coverage and accuracy

**Option 3:** Get WRDS subscription
- Cheaper than Eikon ($2,000 vs $25,000)
- Use original WRDS-based scripts

---

## 📝 Summary

**What I Created:**
- ✅ AP_IBESEPSAdjusted.py (EPS estimates from Eikon)
- ✅ AP_IBESRecommendations.py (Recommendations from Eikon)
- ✅ Complete RIC format conversion
- ✅ Rate limiting and error handling
- ✅ Data validation

**What's Needed:**
- ⚠️ Active Eikon/Refinitiv subscription ($20K-30K/year)
- ⚠️ Eikon Desktop application running
- ⚠️ API key configured in .env

**What's Missing:**
- 🔴 Free alternative (doesn't exist for full IBES data)
- 🔴 Unadjusted actuals script (could create if needed)
- 🔴 Long-term growth script (could create if needed)

**Recommendation:**
- If you have Eikon: Use these AP files ✅
- If you have WRDS: Use original WRDS files ✅
- If you have neither: Skip IBES predictors or use limited free alternatives ⚠️

---

## 🎯 Next Steps

### If You Have Eikon Subscription:
1. ✅ Ensure Eikon Desktop is running
2. ✅ Get App Key and add to .env
3. ✅ Run AP_IBESEPSAdjusted.py
4. ✅ Run AP_IBESRecommendations.py
5. ✅ Integrate with predictors

### If You DON'T Have Eikon:
1. Consider WRDS subscription (cheaper alternative)
2. OR focus on non-IBES predictors (still 80%+ coverage)
3. OR implement limited free alternatives

---

**Bottom Line:** IBES data requires paid subscription. No complete free alternative exists. The AP files I created will work if you have Eikon access, but this is a $20K-30K/year cost.

