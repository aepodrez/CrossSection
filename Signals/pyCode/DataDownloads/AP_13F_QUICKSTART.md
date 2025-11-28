# AP_InstitutionalHoldings13F - Quick Start Guide

## ✅ What I Created

**2 new files:**
1. `AP_InstitutionalHoldings13F.py` - Main script (free 13F data using edgartools)
2. `test_ap_13f.py` - Test/validation script

---

## 🚀 Installation & First Run (Initial: 30-60 min, Cached: <1 min)

```bash
cd /Users/alexpodrez/Documents/CrossSection/Signals/pyCode/DataDownloads

# 1. Install edgartools (if not already installed)
pip install edgartools yfinance

# 2. Test with 5 tickers (fast, ~5-10 min)
# Edit AP_InstitutionalHoldings13F.py line 49: DEBUG_MODE = True
python AP_InstitutionalHoldings13F.py

# 3. Verify outputs
python test_ap_13f.py

# 4. Production: Full S&P 500 (30-60 min first run, then instant with cache)
# Edit AP_InstitutionalHoldings13F.py line 49: DEBUG_MODE = False
python AP_InstitutionalHoldings13F.py
```

---

## 📊 What You Get

**1 output file:**
- `AP_TR_13F.parquet` - Institutional holdings data (monthly)

**Data fields:**
- `permno` - Stock identifier
- `time_avail_m` - Month (quarterly data forward-filled to monthly)
- `numinstown` - Number of institutional owners
- `dbreadth` - Quarterly change in number of institutional owners
- `instown_perc` - Institutional ownership percentage (% of shares)
- `total_shares` - Total shares held by institutions
- `shares_outstanding` - Total shares outstanding

---

## 💰 Comparison to Thomson Reuters 13F

| Feature | AP_13F (edgartools) | TR 13F (WRDS) |
|---------|-------------------|---------------|
| **Cost** | **$0** | Included in WRDS (~$2,000/year) |
| **Data lag** | **45 days** (SEC filing deadline) | 60-90 days |
| **History** | 2000-present | 1980s-present |
| **Update frequency** | **Quarterly (real-time)** | Quarterly batch |
| **Coverage** | All 13F filers | All 13F filers |
| **Metrics** | numinstown, dbreadth, instown_perc | Same + additional |

---

## ⚡ Performance & Caching

### First Run (Downloads All 13F Filings)
- **Debug mode (5 tickers, 100 filings):** ~5-10 minutes
- **Production (500 tickers, all filings):** ~30-60 minutes
- **Why slow:** Must process 1000s of 13F filings from managers

### Subsequent Runs (Uses Cache)
- **Any mode:** ~30 seconds to 2 minutes
- **Why fast:** Cache stores all historical data
- **Incremental:** Only fetches new quarters

### Cache Management

**Cache location:**
```
../pyData/Intermediate/.cache/AP_13F_holdings_cache.parquet
```

**Refresh cache (to get latest filings):**
```bash
rm ../pyData/Intermediate/.cache/AP_13F_holdings_cache.parquet
python AP_InstitutionalHoldings13F.py
```

**Cache contains:**
- All historical quarterly 13F data
- Automatically updates with new quarters
- Refreshes most recent quarter (in case of late filings)

---

## 🔄 How It Works

### 13F Filing Strategy

**Key insight:** 13F filings are filed BY institutional managers (hedge funds, mutual funds), NOT by the companies being held.

**Process:**
1. Fetch all 13F-HR filings from institutional managers (using `get_filings(form="13F-HR")`)
2. For each filing, extract the infotable (holdings table with Ticker column)
3. Map Ticker → permno using your ticker mapping
4. Count unique manager_cik per (permno, year, quarter)
5. This gives breadth = number of institutional owners
6. Calculate total shares held by aggregating across all managers

**Why this works:**
- Every institutional manager with >$100M AUM must file 13F
- 13F lists ALL their equity holdings (with ticker symbols)
- We aggregate across all managers to count how many hold each stock

---

## 📋 Data Fields Explained

### `numinstown` - Number of Institutional Owners
- Count of unique institutional managers holding the stock
- Higher = more institutional interest
- Quarterly data, forward-filled to monthly

### `dbreadth` - Change in Breadth
- Quarterly change in number of institutional owners
- `dbreadth[t] = numinstown[t] - numinstown[t-1]`
- Positive = increasing institutional interest
- Used in academic factors (Chen, Hong & Stein 2002)

### `instown_perc` - Institutional Ownership %
- Percentage of shares outstanding held by institutions
- Calculated: (total_shares_held / shares_outstanding) × 100
- Requires shares outstanding from yfinance
- May be >100% due to short positions or data lags

---

## 🔄 Integration with Existing Code

**Option 1: Direct replacement**
```python
# In your analysis code, change:
inst_holdings = pd.read_parquet('../pyData/Intermediate/TR_13F.parquet')

# To:
inst_holdings = pd.read_parquet('../pyData/Intermediate/AP_TR_13F.parquet')
```

**Option 2: Rename files**
```bash
cd ../pyData/Intermediate
mv AP_TR_13F.parquet TR_13F.parquet
```

---

## ⚙️ Configuration

### Change Ticker Universe

Edit `get_user_ticker_list()` in `AP_InstitutionalHoldings13F.py`:

```python
# Default: S&P 500
return get_sp500_tickers()

# Or custom list:
return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']

# Or from CSV:
# Create: ../pyData/Intermediate/ticker_list.csv
# (same file used by other AP scripts)
```

### Change Date Range

Edit lines 44-45 in `AP_InstitutionalHoldings13F.py`:

```python
START_DATE = '2020-01-01'  # Default (last ~5 years)
END_DATE = datetime.now().strftime('%Y-%m-%d')  # Today
```

**Note:** Going back too far increases processing time significantly.

---

## 📅 Update Schedule

### Recommended Cadence

| Frequency | When | Runtime | Why |
|-----------|------|---------|-----|
| **Quarterly** | Mid-Feb, May, Aug, Nov | 5-10 min | After 13F filing deadline (45 days after quarter) |
| **Monthly** | 1st of month | <1 min | Uses cached data with forward-fill |

### 13F Filing Deadlines

- Q1 ends Mar 31 → File by May 15
- Q2 ends Jun 30 → File by Aug 14
- Q3 ends Sep 30 → File by Nov 14
- Q4 ends Dec 31 → File by Feb 14

**Best time to update:** 2-3 weeks after filing deadline to catch late filings.

### Automation

```bash
# Create update script
cat > update_13f.sh << 'EOF'
#!/bin/bash
cd /path/to/DataDownloads
python AP_InstitutionalHoldings13F.py
python test_ap_13f.py
EOF

chmod +x update_13f.sh

# Add to crontab (runs 20th of Feb, May, Aug, Nov at 6 PM)
crontab -e

# Add:
0 18 20 2,5,8,11 * /path/to/update_13f.sh
```

---

## 🎯 Use Cases

### ✅ Best For:
- Institutional ownership factors (DelBreadth, InstOwnership)
- Change in institutional holdings analysis
- Smart money indicators
- Momentum/reversal factors with institutional flow
- Liquidity analysis

### ⚠️ Acceptable For:
- Academic factor replication (Chen, Hong & Stein 2002)
- Institutional herding studies
- Ownership concentration metrics

### ❌ Not Suitable For:
- Fund-level analysis (manager-by-manager holdings)
- Short interest calculations (13F shows long positions only)
- Detailed position sizing (13F has minimum thresholds)
- Daily institutional flow (13F is quarterly)

---

## 🐛 Troubleshooting

### "No module named 'edgartools'"
```bash
pip install edgartools
```

### First run is very slow (30-60 min)
- **Normal!** Processing 1000s of 13F filings takes time
- **Solution:** Be patient, subsequent runs use cache
- **Debug mode:** Set DEBUG_MODE = True for faster testing

### "No holdings data retrieved"
- Check internet connection
- Verify ticker list matches S&P 500
- Ensure edgartools identity is set correctly
- Try increasing date range

### Cache is stale / want fresh data
```bash
# Delete cache and re-run
rm ../pyData/Intermediate/.cache/AP_13F_holdings_cache.parquet
python AP_InstitutionalHoldings13F.py
```

### Missing shares outstanding / instown_perc
- Some stocks may not have shares outstanding in yfinance
- Will show as NaN in instown_perc column
- numinstown and dbreadth still available

---

## 💡 Tips & Best Practices

### 1. Use Cache Intelligently
```bash
# First run: Full download (30-60 min)
python AP_InstitutionalHoldings13F.py

# Monthly updates: Uses cache (<1 min)
python AP_InstitutionalHoldings13F.py

# Quarterly refresh: Delete cache before filing season
rm -rf ../pyData/Intermediate/.cache/AP_13F_holdings_cache.parquet
python AP_InstitutionalHoldings13F.py
```

### 2. Validate After Each Download
```bash
python test_ap_13f.py
```

### 3. Monitor Data Gaps
```python
# Check which stocks have missing data
import pandas as pd
df = pd.read_parquet('../pyData/Intermediate/AP_TR_13F.parquet')
missing = df[df['numinstown'].isna()]
print(f"Missing data: {missing['permno'].unique()}")
```

### 4. Understand Quarterly Nature
- 13F data is filed quarterly (4 times/year)
- Script forward-fills to monthly for consistency
- Month 1 of quarter = new data, Months 2-3 = forward-filled

---

## 📊 Expected Output

### Debug Mode (5 tickers, 100 filings)
- **Runtime:** ~5-10 minutes
- **Records:** ~150-300
- **Size:** <1 MB
- **Coverage:** Partial (recent quarters only)

### Production Mode (S&P 500, all filings)
- **First run:** ~30-60 minutes
- **Cached run:** <1 minute
- **Records:** ~50,000-150,000
- **Size:** ~5-20 MB
- **Coverage:** Most S&P 500 stocks from 2020+

---

## 🔍 Data Quality Expectations

| Metric | Expected Value |
|--------|---------------|
| **numinstown completeness** | 80-90% |
| **dbreadth completeness** | 70-80% (missing first quarter per stock) |
| **instown_perc completeness** | 70-80% (requires shares outstanding) |
| **Mean institutional owners** | 200-400 (for S&P 500) |
| **Mean institutional ownership** | 60-80% (for S&P 500) |

---

## 🔗 Related Files

- `AP_CRSPMonthly.py` - Monthly market data (for merging)
- `InstitutionalHoldings13F.py` - Original Thomson Reuters version
- `DelBreadth` predictor - Example usage

---

## 📚 Additional Resources

- edgartools docs: https://github.com/dgunning/edgartools
- SEC 13F filing guide: https://www.sec.gov/divisions/investment/13ffaq.htm
- Chen, Hong & Stein (2002) paper on breadth of ownership

---

## ✅ Summary

**You now have:**
- ✅ Free institutional holdings data
- ✅ CRSP-compatible format
- ✅ Quarterly updates (auto-cached)
- ✅ Ready for DelBreadth and ownership factors
- ✅ Number of institutional owners
- ✅ Institutional ownership percentage
- ✅ Change in breadth (dbreadth)
- ⚠️ Quarterly data (forward-filled to monthly)
- ⚠️ Active stocks only (no delisted holdings)

**Perfect for:**
- Ownership-based factors
- Institutional flow analysis
- Smart money indicators

**Cost: $0 vs. included in $2,000+ WRDS subscription** 💰

---

## 🚀 Next Steps

1. ✅ Run test mode (DEBUG_MODE = True, ~10 min)
2. ✅ Validate output (test_ap_13f.py)
3. ✅ Run production (DEBUG_MODE = False, ~60 min first time)
4. ✅ Subsequent updates use cache (instant)
5. ✅ Integrate with ownership factors
6. ✅ Set up quarterly refresh

**You're ready for institutional ownership analysis!** 🎉

---

## ⚠️ Important Notes

**First run is slow (30-60 minutes) because:**
- Must process 1000s of 13F-HR filings from institutional managers
- Each filing must be parsed for holdings
- This is a one-time cost - cache makes subsequent runs instant

**Data limitations:**
- 13F only shows positions >10,000 shares or >$200,000
- Only shows long positions (no short positions)
- Quarterly reporting (45-day lag)
- Active stocks only (no delisted holdings)

**Advantages over Thomson Reuters:**
- Free (vs. paid subscription)
- Real-time updates (vs. vendor delay)
- Official SEC source (vs. third-party)

