# AP_CRSPDaily - Quick Start Guide

## ✅ What I Created For You

**3 new files:**
1. `AP_CRSPDaily.py` - Main script (free CRSP alternative using yfinance)
2. `test_ap_crsp.py` - Test/validation script
3. `AP_CRSP_README.md` - Full documentation

---

## 🚀 Installation & First Run (5 minutes)

```bash
cd /Users/alexpodrez/Documents/CrossSection/Signals/pyCode/DataDownloads

# 1. Install yfinance
pip install yfinance

# 2. Test with 10 tickers (fast)
# Edit AP_CRSPDaily.py line 59: DEBUG_MODE = True
python AP_CRSPDaily.py

# 3. Verify outputs
python test_ap_crsp.py

# 4. Production: Full S&P 500
# Edit AP_CRSPDaily.py line 59: DEBUG_MODE = False
python AP_CRSPDaily.py  # Takes ~15-20 minutes
```

---

## 📊 What You Get

**3 output files:**
1. `AP_dailyCRSP.parquet` - Full daily data (returns, volume, prices)
2. `AP_dailyCRSPprc.parquet` - Price-only version
3. `AP_ticker_to_permno.csv` - Ticker → numeric ID mapping

**Data fields (CRSP-compatible):**
- `permno` - Numeric stock ID
- `time_d` - Trading date
- `ret` - Daily return
- `vol` - Trading volume
- `prc` - Closing price (adjusted)
- `cfacpr` - Price adjustment factor
- `shrout` - Shares outstanding (thousands)

---

## 🔄 Integration with Existing Code

**Option 1: Rename files (easiest)**
```bash
cd ../pyData/Intermediate
mv AP_dailyCRSP.parquet dailyCRSP.parquet
mv AP_dailyCRSPprc.parquet dailyCRSPprc.parquet
```

**Option 2: Update SignalMasterTable.py**
```python
# Change:
crsp = pd.read_parquet('../pyData/Intermediate/dailyCRSP.parquet')
# To:
crsp = pd.read_parquet('../pyData/Intermediate/AP_dailyCRSP.parquet')
```

**Option 3: Symlink (Mac/Linux)**
```bash
cd ../pyData/Intermediate
ln -sf AP_dailyCRSP.parquet dailyCRSP.parquet
ln -sf AP_dailyCRSPprc.parquet dailyCRSPprc.parquet
```

---

## 💰 Cost Comparison

| Feature | AP_CRSPDaily | CRSP (WRDS) |
|---------|-------------|-------------|
| **Cost** | **$0** | $2,000+/year |
| **Data lag** | **15 minutes** | End of day |
| **History** | 2000-present | 1926-present |
| **Live updates** | **Yes** | No |
| **Delisted stocks** | No | Yes |

---

## ⚙️ Configuration

### Change Ticker Universe

Edit `get_user_ticker_list()` in `AP_CRSPDaily.py`:

```python
# Default: S&P 500 (~500 tickers)
return get_sp500_tickers()

# Or custom list:
return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']

# Or from CSV:
# Create: ../pyData/Intermediate/ticker_list.csv
# Script auto-detects and uses it
```

### Change Date Range

Edit lines 54-55 in `AP_CRSPDaily.py`:

```python
START_DATE = '2000-01-01'  # Default
END_DATE = datetime.now().strftime('%Y-%m-%d')  # Today
```

---

## 🎯 Key Differences from CRSP

### ✅ Advantages
- **Free** (vs. $2,000+/year)
- **Real-time** (~15-min delay vs. end-of-day)
- **Easy to update** (run script daily)
- **No WRDS account needed**

### ⚠️ Limitations
- History only to ~2000 (vs. 1926)
- No delisted stocks (survivor bias)
- Shares outstanding is current value only
- ~500 stocks (S&P 500) vs. 20,000+ in CRSP

### 💡 When to Use Each

**Use AP_CRSPDaily for:**
- ✅ Live trading strategies
- ✅ Testing with recent data
- ✅ Cost-sensitive projects
- ✅ Factors using active stocks only

**Use CRSP (original) for:**
- ✅ Long-term historical backtests (pre-2000)
- ✅ Research requiring delisted stocks
- ✅ Comprehensive survivorship-bias-free analysis
- ✅ Academic factor replication

---

## 🔍 Data Quality Check

Run after each download:

```bash
python test_ap_crsp.py
```

**Expected output:**
```
✓ AP_dailyCRSP.parquet (50-500 MB)
✓ AP_dailyCRSPprc.parquet (30-300 MB)
✓ AP_ticker_to_permno.csv (<1 MB)

Total records: 1,000,000+
Unique tickers: 500
Date range: 2000-01-01 to 2024-11-24
Returns completeness: 99%+
```

---

## 📅 Daily Update Workflow

### Manual
```bash
# Run daily after market close
cd /Users/alexpodrez/Documents/CrossSection/Signals/pyCode/DataDownloads
python AP_CRSPDaily.py
```

### Automated (cron job on Mac/Linux)
```bash
# Edit crontab
crontab -e

# Add line (runs at 6 PM daily)
0 18 * * * cd /Users/alexpodrez/Documents/CrossSection/Signals/pyCode/DataDownloads && python AP_CRSPDaily.py
```

---

## 🐛 Troubleshooting

### "No module named 'yfinance'"
```bash
pip install yfinance
```

### "Rate limit exceeded"
```python
# In AP_CRSPDaily.py, increase sleep time:
time.sleep(2)  # Line ~142
```

### Empty data for some tickers
- Ticker may be delisted
- Check spelling
- Verify on Yahoo Finance

### Slow download
- Normal for 500 tickers (~15-20 min)
- Use DEBUG_MODE for testing (10 tickers, <1 min)

---

## 🚀 Next Steps

1. **Test run** (5 min):
   ```bash
   # Set DEBUG_MODE = True in AP_CRSPDaily.py
   python AP_CRSPDaily.py
   python test_ap_crsp.py
   ```

2. **Production run** (20 min):
   ```bash
   # Set DEBUG_MODE = False
   python AP_CRSPDaily.py
   ```

3. **Integrate with factors**:
   - Update `SignalMasterTable.py` to use `AP_dailyCRSP.parquet`
   - Or rename files to replace CRSP data

4. **Set up daily updates**:
   - Manual: Run script daily
   - Auto: Set up cron job

---

## 📖 More Information

- Full documentation: `AP_CRSP_README.md`
- yfinance docs: https://github.com/ranaroussi/yfinance
- Comparison: See README for detailed CRSP vs. AP_CRSP comparison

---

## ✅ Summary

**You now have:**
- ✅ Free daily stock data (price, return, volume)
- ✅ CRSP-compatible format
- ✅ Ready to integrate with existing factor code
- ✅ ~15-minute lag for live trading
- ✅ $0 cost vs. $2,000+/year

**Perfect for live trading strategies!** 🎉

