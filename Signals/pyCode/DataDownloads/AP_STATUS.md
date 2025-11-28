# AP Data Download Files - Status

## ✅ COMPLETED (12 files)

### Core Data Sources (6)
1. **AP_CompustatAnnual.py** ✅ (Replaces: CompustatAnnual.py)
   - Source: SEC EDGAR (10-K filings)
   - Cost: $0 vs. WRDS $2,000+/year

2. **AP_CRSPDaily.py** ✅ (Replaces: CRSPDaily.py)
   - Source: yfinance
   - Cost: $0 vs. CRSP $2,000+/year

3. **AP_CRSPMonthly.py** ✅ (Replaces: CRSPMonthly.py)
   - Source: yfinance
   - Cost: $0 vs. CRSP $2,000+/year

4. **AP_CRSPDistributions.py** ✅ (Replaces: CRSPDistributions.py)
   - Source: yfinance
   - Cost: $0 vs. CRSP $2,000+/year

5. **AP_InstitutionalHoldings13F.py** ✅ (Replaces: InstitutionalHoldings13F.py)
   - Source: SEC 13F filings (edgartools)
   - Cost: $0 vs. WRDS included

6. **AP_GNPDeflator.py** ✅ (Replaces: GNPDeflator.py)
   - Source: FRED API
   - Cost: $0 (already free)

### Market Data (3 - NEW!)
7. **AP_TreasuryBill3M.py** ✅ (Replaces: TreasuryBill3M.py)
   - Source: FRED API (TB3MS)
   - Cost: $0 (already free)

8. **AP_VIX.py** ✅ (Replaces: VIX.py)
   - Source: FRED API (VXOCLS + VIXCLS)
   - Cost: $0 (already free)

9. **AP_MarketReturns.py** ✅ (Replaces: MarketReturns.py)
   - Source: Yahoo Finance (S&P 500)
   - Cost: $0 vs. CRSP $2,000+/year

### Factor Data (3 - Already existed!)
10. **AP_FamaFrenchDaily.py** ✅ (Already exists)
11. **Ap_FamaFrenchMonthly.py** ✅ (Already exists)
12. **AP_BuildFFPortfolios.py** ✅ (Already exists)

---

## 🔴 NO AP VERSION YET (27 files)

### High Priority - WRDS Data (can be replaced with free sources)

#### Compustat Data
10. **CompustatQuarterly.py** ⭐⭐⭐
    - Original: WRDS Compustat Quarterly
    - Possible AP: SEC 10-Q filings (edgartools)
    - Same approach as AP_CompustatAnnual

11. **CompustatBusinessSegments.py** ⭐⭐
    - Original: WRDS Compustat Segments
    - Possible AP: SEC 10-K/10-Q segment disclosures
    - More complex extraction

12. **CompustatCustomerSegments.py** ⭐⭐
    - Original: WRDS Compustat Customer segments
    - Possible AP: SEC filings (text extraction)
    - Complex, low priority

13. **CompustatPensions.py** ⭐
    - Original: WRDS Compustat Pensions
    - Possible AP: SEC 10-K footnotes
    - Complex extraction

14. **CompustatShortInterest.py** ⭐⭐
    - Original: WRDS Compustat
    - Possible AP: FINRA short interest data (free)
    - URL: https://www.finra.org/finra-data/browse-catalog/short-sale-volume-data

#### IBES Data (Earnings Estimates)
15. **IBESEPSAdjusted.py** ⭐⭐⭐
    - Original: WRDS IBES
    - Possible AP: Alpha Vantage, Seeking Alpha, or scraping
    - Cost: Some APIs available

16. **IBESEPSUnadjusted.py** ⭐⭐
    - Original: WRDS IBES
    - Possible AP: Same as above

17. **IBESRecommendations.py** ⭐⭐
    - Original: WRDS IBES analyst recommendations
    - Possible AP: Yahoo Finance, Finviz
    - Limited historical depth

18. **IBESUnadjustedActuals.py** ⭐
    - Original: WRDS IBES
    - Possible AP: Company earnings releases

19. **IBESCRSPLink.py** ⭐
    - Original: WRDS linking table
    - AP: Would derive from ticker mappings

#### CRSP Extensions
20. **CRSPAcquisitions.py** ⭐⭐
    - Original: CRSP M&A events
    - Possible AP: SEC 8-K filings for M&A events
    - edgartools can access 8-K

#### Linking Tables
21. **CCMLinkingTable.py** ⭐⭐⭐
    - Original: WRDS CCM (CRSP-Compustat Merged)
    - Possible AP: Build from ticker mappings
    - Critical for merging data

22. **OptionMetricsCRSPLink.py** ⭐
    - Original: WRDS linking table
    - AP: Would derive from ticker mappings

### Medium Priority - Free but needs API

#### Market Data (Free APIs Available)
23. **VIX.py** ⭐⭐⭐
    - Original: CBOE VIX
    - Possible AP: Yahoo Finance, FRED (VIXCLS)
    - Easy, free

24. **TreasuryBill3M.py** ⭐⭐⭐
    - Original: FRED or Treasury
    - Possible AP: FRED API (DTB3, DGS3MO)
    - Already free, just needs AP version

25. **MarketReturns.py** ⭐⭐⭐
    - Original: CRSP market returns
    - Possible AP: S&P 500 from yfinance + calculate
    - Easy

26. **LiquidityFactor.py** ⭐⭐
    - Original: Pastor-Stambaugh liquidity factor
    - Possible AP: Calculate from trade data or use published data
    - May need construction

27. **QFactorModel.py** ⭐⭐
    - Original: Hou-Xue-Zhang q-factors
    - Possible AP: Authors publish factors online
    - URL: http://global-q.org/factors.html

#### Credit Ratings
28. **SPCreditRatings.py** ⭐⭐
    - Original: S&P Credit Ratings (Capital IQ)
    - Possible AP: FRED has some credit spread data
    - Limited free access to ratings

29. **CIQCreditRatings.py** ⭐⭐
    - Original: Capital IQ ratings
    - Same as above

### Low Priority - Specialized/Difficult

30. **GovernanceIndex.py** ⭐
    - Original: ISS Governance data
    - Possible AP: No good free source
    - Proprietary data

31. **BidAskSpreads.py** ⭐
    - Original: TAQ or CRSP
    - Possible AP: Could calculate from yfinance bid/ask if available
    - Limited historical data

32. **BrokerDealerLeverage.py** ⭐
    - Original: Fed/FINRA data
    - Possible AP: FRED may have aggregate series
    - Not stock-specific

33. **IPODates.py** ⭐⭐
    - Original: Multiple sources
    - Possible AP: SEC S-1 filings via edgartools
    - Doable but complex

34. **PatentCitations.py** ⭐
    - Original: USPTO data
    - Possible AP: Google Patents API or USPTO bulk data
    - Very specialized

35. **PINData.py** ⭐
    - Original: Probability of Informed Trading
    - Possible AP: Would need to calculate from trade data
    - Very complex

36. **BEAInputOutput.py** ⭐
    - Original: Bureau of Economic Analysis
    - Possible AP: BEA website has free data
    - Not urgent

---

## 📊 Summary Statistics

| Status | Count | Percentage |
|--------|-------|------------|
| ✅ **Completed (AP versions exist)** | 9 | 25% |
| 🔴 **No AP version yet** | 27 | 75% |
| **TOTAL** | 36 | 100% |

### Priority Breakdown (of 27 remaining)

| Priority | Count | Files |
|----------|-------|-------|
| ⭐⭐⭐ **High** (Should do next) | 7 | CompustatQuarterly, IBESEPSAdjusted, CCMLinkingTable, VIX, TreasuryBill3M, MarketReturns |
| ⭐⭐ **Medium** (Nice to have) | 13 | Most others |
| ⭐ **Low** (Specialized/Difficult) | 7 | Governance, Patents, PIN, etc. |

---

## 🎯 Recommended Next Steps (Priority Order)

### Tier 1: Critical & Easy (Do First)
1. **AP_TreasuryBill3M.py** ⭐⭐⭐ (FRED API, 10 min)
2. **AP_VIX.py** ⭐⭐⭐ (FRED or yfinance, 10 min)
3. **AP_MarketReturns.py** ⭐⭐⭐ (yfinance SPY, 15 min)
4. **AP_QFactorModel.py** ⭐⭐ (Download from website, 15 min)

### Tier 2: Important but More Work
5. **AP_CompustatQuarterly.py** ⭐⭐⭐ (SEC 10-Q, 60 min)
   - Same as AP_CompustatAnnual but quarterly
6. **AP_CCMLinkingTable.py** ⭐⭐⭐ (Build from mappings, 30 min)
   - Critical for merging CRSP/Compustat

### Tier 3: IBES Alternatives
7. **AP_IBESEPSAdjusted.py** ⭐⭐⭐ (Need earnings estimate source)
   - Options: Alpha Vantage, Seeking Alpha, FMP
   - May require paid API

### Tier 4: Everything Else
8. Others as needed for specific factors

---

## 💰 Cost Savings Summary

### Already Completed (6 core files)
- **Savings: $4,000-5,000/year**
- Compustat: $2,000+
- CRSP: $2,000+
- TR 13F: Included in WRDS

### Potential Additional Savings (if all completed)
- **CompustatQuarterly:** Same as annual (already counted)
- **IBES Estimates:** $1,000-2,000/year
- **Credit Ratings:** $500-1,000/year
- **Options/Other:** $1,000+/year

**Total Potential Savings: $6,000-8,000/year**

---

## 🚀 Quick Wins (Can do in next hour)

These are trivial because data is already free and easily accessible:

```bash
# 1. Treasury Bill (FRED API) - 10 min
python AP_TreasuryBill3M.py  

# 2. VIX (FRED or yfinance) - 10 min
python AP_VIX.py

# 3. Market Returns (SPY from yfinance) - 15 min
python AP_MarketReturns.py

# 4. Q-Factors (download from website) - 15 min
python AP_QFactorModel.py

# Total: 50 minutes for 4 more files ✅
```

---

## 📋 Files Requiring Paid APIs (Limited Free Options)

These may be difficult to replace completely for free:
1. **IBES Estimates** (IBESEPSAdjusted, etc.)
   - Free: Limited (Yahoo Finance has some)
   - Paid: Alpha Vantage, FMP, Seeking Alpha
   - Cost: $50-200/month

2. **Credit Ratings** (SPCreditRatings, CIQCreditRatings)
   - Free: Very limited
   - Paid: S&P, Moody's feeds
   - Cost: $500-1,000/month

3. **Governance Data** (GovernanceIndex)
   - Free: None
   - Paid: ISS, GMI
   - Cost: $1,000+/year

4. **PIN Data** (PINData)
   - Free: None (requires calculation)
   - Would need tick-by-tick trade data

---

## 🎯 Conclusion

**Current Status:**
- ✅ 9/36 files have AP versions (25%)
- ✅ Core data pipeline complete (fundamentals, prices, dividends, institutional)
- ✅ $4,000-5,000/year saved

**Next Steps:**
- 🎯 4 "quick wins" in next hour (VIX, Treasury, Market Returns, Q-Factors)
- 🎯 CompustatQuarterly & CCMLinkingTable for completeness
- 🎯 IBES estimates (may need paid API for quality data)

**Bottom Line:**
- Most critical files are DONE ✅
- Remaining files are either easy (market data) or specialized (governance, patents)
- Can achieve 80% coverage with 13-15 AP files total

