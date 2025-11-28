# AP_CRSPDistributions - Quick Start Guide

## ✅ What I Created

**2 new files:**
1. `AP_CRSPDistributions.py` - Main script (free CRSP Distributions alternative using yfinance)
2. `test_ap_distributions.py` - Test/validation script

---

## 🚀 Installation & First Run (5 minutes)

```bash
cd /Users/alexpodrez/Documents/CrossSection/Signals/pyCode/DataDownloads

# 1. Install yfinance (if not already installed)
pip install yfinance

# 2. Test with 10 tickers (fast, ~30 seconds)
# Edit AP_CRSPDistributions.py line 46: DEBUG_MODE = True
python AP_CRSPDistributions.py

# 3. Verify outputs
python test_ap_distributions.py

# 4. Production: Full S&P 500 (~15-20 minutes)
# Edit AP_CRSPDistributions.py line 46: DEBUG_MODE = False
python AP_CRSPDistributions.py
```

---

## 📊 What You Get

**1 output file:**
- `AP_CRSPdistributions.parquet` - Dividend & distribution data (CRSP-compatible)

**Data fields (matching CRSP Distributions):**
- `permno` - Numeric stock ID
- `divamt` - Dividend/distribution amount per share
- `distcd` - Distribution code (1232=cash div, 5523=split, etc.)
- `facshr` - Share adjustment factor (for splits)
- `rcrddt` - Record date (estimated)
- `exdt` - Ex-dividend/distribution date
- `paydt` - Payment date (estimated)
- `cd1`, `cd2`, `cd3`, `cd4` - Individual digits of distribution code

---

## 📋 Distribution Codes

**CRSP-like codes (approximated):**

| Code | Description | Source |
|------|-------------|--------|
| 1232 | Regular cash dividend | yfinance dividends |
| 1262 | Special cash dividend | Large dividend (>5% of price) |
| 5523 | Stock split (forward) | yfinance splits (ratio > 1) |
| 5533 | Reverse stock split | yfinance splits (ratio < 1) |

**Note:** Original CRSP has 100+ distribution codes. We approximate the most common ones.

---

## 🔄 Integration with Existing Code

**Option 1: Direct replacement**
```python
# In your analysis code, change:
distributions = pd.read_parquet('../pyData/Intermediate/CRSPdistributions.parquet')

# To:
distributions = pd.read_parquet('../pyData/Intermediate/AP_CRSPdistributions.parquet')
```

**Option 2: Rename files**
```bash
cd ../pyData/Intermediate
mv AP_CRSPdistributions.parquet CRSPdistributions.parquet
```

---

## 💰 Comparison to CRSP Distributions

| Feature | AP_CRSPDistributions | CRSP (WRDS) |
|---------|---------------------|-------------|
| **Cost** | **$0** | $2,000+/year |
| **Data lag** | **Real-time** | ~1 week |
| **History** | 2000-present | 1926-present |
| **Dividends** | ✅ Yes | ✅ Yes |
| **Stock splits** | ✅ Yes | ✅ Yes |
| **Distribution codes** | ⚠️ Approximated (4 types) | ✅ Precise (100+ types) |
| **Record dates** | ⚠️ Estimated | ✅ Actual |
| **Payment dates** | ⚠️ Estimated | ✅ Actual |
| **Special distributions** | ❌ Limited | ✅ Complete |
| **Delisted stocks** | ❌ No | ✅ Yes |

---

## ⚠️ Key Differences from CRSP

### ✅ What Works Well
- Regular cash dividends (accurate amounts and ex-dates)
- Stock splits (accurate ratios and dates)
- Recent data (2000+)
- Real-time updates

### ⚠️ What's Approximated
- **Distribution codes** - Simplified to 4 main types (vs. 100+ in CRSP)
- **Record dates** - Estimated as ex-date - 1 day
- **Payment dates** - Estimated as ex-date + 14 days
- **Special dividends** - Heuristic: >5% of price

### ❌ What's Missing
- Special distributions (spinoffs, rights offerings, liquidations)
- Delisted stock distributions
- Precise distribution code classifications
- Exact record/payment dates

---

## 🎯 Use Cases

### ✅ Best For:
- Dividend yield calculations
- Dividend growth analysis
- Stock split tracking
- Monthly/quarterly rebalancing strategies
- Cost basis adjustments
- Live trading with dividend-adjusted returns

### ⚠️ Acceptable For:
- Factor models using dividend data
- Payout ratio analysis
- Dividend screen strategies

### ❌ Not Suitable For:
- Research requiring precise distribution codes
- Analysis of special distributions
- Studies of delisted stock dividends
- Corporate actions research (spinoffs, rights)

---

## 🔍 Data Quality Validation

Run after each download:

```bash
python test_ap_distributions.py
```

**Expected output:**
```
✓ AP_CRSPdistributions.parquet (5-50 MB)

Total records: 10,000 - 100,000
Unique stocks: 500
Date range: 2000-01 to 2024-11

Distribution breakdown:
  Cash Dividend (1232): 95%
  Stock Split (5523): 4%
  Special Dividend (1262): <1%
  Reverse Split (5533): <1%
```

---

## 📅 Update Schedule

### Recommended Cadence

| Frequency | When | Why |
|-----------|------|-----|
| **Weekly** | Every Monday | Capture recent dividends |
| **Monthly** | 1st of month | Sufficient for most strategies |
| **Quarterly** | Earnings season | Match fundamental updates |

### Automation

**Create: `update_distributions.sh`**
```bash
#!/bin/bash
cd /Users/alexpodrez/Documents/CrossSection/Signals/pyCode/DataDownloads
python AP_CRSPDistributions.py
python test_ap_distributions.py
```

**Add to crontab (weekly update):**
```bash
crontab -e

# Add (runs every Monday at 6 PM):
0 18 * * 1 /path/to/update_distributions.sh
```

---

## 💡 Usage Examples

### Example 1: Calculate Dividend Yield

```python
import pandas as pd

# Load data
distributions = pd.read_parquet('../pyData/Intermediate/AP_CRSPdistributions.parquet')
prices = pd.read_parquet('../pyData/Intermediate/AP_dailyCRSP.parquet')

# Get cash dividends only
dividends = distributions[distributions['distcd'] == 1232]

# Calculate trailing 12-month dividend yield
def calc_dividend_yield(permno, date):
    # Get dividends in past 12 months
    start = date - pd.DateOffset(months=12)
    divs = dividends[
        (dividends['permno'] == permno) &
        (dividends['exdt'] >= start) &
        (dividends['exdt'] <= date)
    ]
    
    # Get price on date
    price = prices[
        (prices['permno'] == permno) &
        (prices['time_d'] == date)
    ]['prc'].iloc[0]
    
    # Calculate yield
    div_sum = divs['divamt'].sum()
    return (div_sum / price) * 100 if price > 0 else 0

# Example usage
yield_aapl = calc_dividend_yield(permno=1, date='2024-01-01')
print(f"AAPL Dividend Yield: {yield_aapl:.2f}%")
```

### Example 2: Identify High-Yielders

```python
import pandas as pd

distributions = pd.read_parquet('../pyData/Intermediate/AP_CRSPdistributions.parquet')
prices = pd.read_parquet('../pyData/Intermediate/AP_dailyCRSP.parquet')

# Get recent dividends (last year)
recent_divs = distributions[
    (distributions['distcd'] == 1232) &
    (distributions['exdt'] >= '2023-01-01')
]

# Sum by permno
annual_divs = recent_divs.groupby('permno')['divamt'].sum().reset_index()
annual_divs.columns = ['permno', 'annual_div']

# Get latest prices
latest_prices = prices.sort_values('time_d').groupby('permno').last()[['prc']].reset_index()

# Calculate yields
yields = annual_divs.merge(latest_prices, on='permno')
yields['yield_pct'] = (yields['annual_div'] / yields['prc']) * 100

# Top 10 yielders
top_yielders = yields.nlargest(10, 'yield_pct')
print(top_yielders)
```

### Example 3: Track Split-Adjusted Returns

```python
import pandas as pd

distributions = pd.read_parquet('../pyData/Intermediate/AP_CRSPdistributions.parquet')

# Get stock splits
splits = distributions[distributions['distcd'].isin([5523, 5533])]

# Calculate cumulative split adjustment for a stock
def get_split_factor(permno, start_date, end_date):
    stock_splits = splits[
        (splits['permno'] == permno) &
        (splits['exdt'] >= start_date) &
        (splits['exdt'] <= end_date)
    ]
    
    # Cumulative product of split ratios
    cumulative_factor = stock_splits['facshr'].prod()
    return cumulative_factor

# Example: AAPL splits from 2000-2024
factor = get_split_factor(permno=1, start_date='2000-01-01', end_date='2024-12-31')
print(f"AAPL cumulative split factor: {factor:.4f}")
```

---

## 🐛 Troubleshooting

### "No module named 'yfinance'"
```bash
pip install yfinance
```

### Empty data / No distributions found
- Check date range (may not have dividends in period)
- Some tickers don't pay dividends (growth stocks)
- Verify ticker list is correct
- Check internet connection

### Rate limiting
```python
# In AP_CRSPDistributions.py, increase sleep time around line 286:
time.sleep(3)  # Increase from 2 to 3 seconds
```

### Missing permno mapping
- Script will create mapping if not found
- Or run AP_CRSPDaily.py or AP_CRSPMonthly.py first

---

## 📊 Distribution Code Reference

### Cash Dividends (1232)
- Regular quarterly/monthly dividends
- Most common type (95% of records)
- Accurate amounts from yfinance

### Special Dividends (1262)
- Large one-time cash payments
- Detected when dividend > 5% of stock price
- May have false positives/negatives

### Stock Splits (5523)
- Forward splits (e.g., 2-for-1, 3-for-1)
- Accurate split ratios from yfinance
- Adjust historical prices accordingly

### Reverse Splits (5533)
- Consolidation of shares (e.g., 1-for-10)
- Less common
- Important for penny stocks

---

## 🎓 Best Practices

### 1. Validate After Download
```bash
python test_ap_distributions.py
```

### 2. Cross-Check Major Events
```python
# Verify known splits/dividends for major stocks
# Example: AAPL 4-for-1 split on 2020-08-31
```

### 3. Use with Price Data
Always combine with price data for yield calculations:
```python
# Don't use dividend amount alone
# Always calculate yield = dividend / price
```

### 4. Document Limitations
When using for research:
```
"Dividend data from yfinance (2000-2024). Distribution codes 
approximated. Record/payment dates estimated. Special distributions 
may not be comprehensive."
```

---

## 🔗 Related Files

- `AP_CRSPDaily.py` - Daily prices (for yield calculations)
- `AP_CRSPMonthly.py` - Monthly data (for dividend screens)
- `CRSPDistributions.py` - Original CRSP version

---

## ✅ Summary

**You now have:**
- ✅ Free dividend/distribution data
- ✅ CRSP-compatible format
- ✅ Real-time updates
- ✅ Stock split tracking
- ✅ Ready for dividend strategies
- ⚠️ Approximated distribution codes
- ⚠️ Estimated record/payment dates
- ❌ No special distributions

**Perfect for:**
- Dividend yield calculations
- Dividend growth strategies
- Stock split adjustments
- Live trading with dividends

**Not perfect for:**
- Complex corporate actions
- Precise distribution classification
- Special distribution research

**Cost: $0 vs. $2,000+/year** 💰

---

## 🚀 Next Steps

1. ✅ Run test mode (DEBUG_MODE = True)
2. ✅ Validate output (test_ap_distributions.py)
3. ✅ Run production (DEBUG_MODE = False)
4. ✅ Integrate with dividend strategies
5. ✅ Set up weekly updates

**You're ready for dividend-based quantitative trading!** 🎉

