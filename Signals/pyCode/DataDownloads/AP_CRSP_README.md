# AP_CRSPDaily.py - Live Daily Stock Data

## Overview

`AP_CRSPDaily.py` is a **free, live alternative** to `CRSPDaily.py` that uses **yfinance** instead of WRDS/CRSP subscription data.

### What It Does

- Downloads daily stock prices, returns, and volume
- Creates CRSP-compatible output format
- Maps tickers to numeric IDs (permno substitute)
- Generates two output files (full data + price-only)

### Cost Comparison

| Source | AP_CRSPDaily (yfinance) | CRSPDaily (WRDS) |
|--------|------------------------|------------------|
| **Cost** | **$0** | $2,000+/year |
| **Data lag** | **~15 minutes** | End of day |
| **History** | ~2000 onwards | 1926 onwards |
| **Update frequency** | **Real-time** | Daily batch |
| **Coverage** | Active stocks only | Includes delisted |

---

## Installation

```bash
cd /Users/alexpodrez/Documents/CrossSection/Signals/pyCode/DataDownloads

# Install yfinance
pip install yfinance

# Optional: for faster downloads
pip install requests-cache
```

---

## Quick Start

### 1. Test Mode (10 tickers, 2023-2024)

```bash
# Edit AP_CRSPDaily.py and set DEBUG_MODE = True
python AP_CRSPDaily.py

# Check outputs
python test_ap_crsp.py
```

### 2. Production Mode (S&P 500, 2000-present)

```bash
# Edit AP_CRSPDaily.py and set DEBUG_MODE = False
python AP_CRSPDaily.py
```

Expected runtime:
- **Debug mode (10 tickers):** ~30 seconds
- **S&P 500 (500 tickers):** ~15-20 minutes
- **Russell 3000:** ~1-2 hours

---

## Output Files

### 1. `AP_dailyCRSP.parquet` (Full Daily Data)

**Columns:**
- `permno` (int32) - Numeric ID (ticker mapping in separate file)
- `time_d` (datetime64) - Trading date
- `ret` (float32) - Daily return (calculated from adjusted close)
- `vol` (float64) - Trading volume
- `prc` (float32) - Closing price (adjusted for splits/dividends)
- `cfacpr` (float32) - Cumulative price adjustment factor (always 1.0 with adjusted prices)
- `shrout` (float32) - Shares outstanding (in thousands)

**Size:** ~50-500 MB depending on number of tickers and date range

**Use for:** Factor calculations requiring returns, volume, liquidity metrics

---

### 2. `AP_dailyCRSPprc.parquet` (Price-Only)

**Columns:**
- `permno` (int32) - Numeric ID
- `time_d` (datetime64) - Trading date
- `prc` (float32) - Closing price
- `cfacpr` (float32) - Price adjustment factor
- `shrout` (float32) - Shares outstanding (thousands)

**Size:** ~30-300 MB

**Use for:** Price-based calculations (momentum, trends), when returns/volume not needed

---

### 3. `AP_ticker_to_permno.csv` (Mapping)

Maps ticker symbols to numeric permno IDs.

**Columns:**
- `ticker` (str) - Stock ticker symbol
- `permno` (int) - Numeric identifier (1, 2, 3, ...)

**Example:**
```
ticker,permno
AAPL,1
AMZN,2
GOOGL,3
MSFT,4
```

---

## Configuration Options

### Ticker Universe

Edit `get_user_ticker_list()` in `AP_CRSPDaily.py`:

**Option 1: S&P 500 (Default)**
```python
def get_user_ticker_list():
    return get_sp500_tickers()  # ~500 tickers
```

**Option 2: Custom List**
```python
def get_user_ticker_list():
    return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA']
```

**Option 3: From CSV File**
```python
# Create: ../pyData/Intermediate/ticker_list.csv
# with column: ticker
def get_user_ticker_list():
    # Already implemented, just create the CSV file
    pass
```

**Option 4: Russell 3000**
```python
def get_user_ticker_list():
    # Use a package like fmpsdk or download from FTSE Russell
    import ftse_russell  # hypothetical package
    return ftse_russell.get_russell_3000()
```

---

### Date Range

Edit configuration in `AP_CRSPDaily.py`:

```python
START_DATE = '2000-01-01'  # Earliest available date
END_DATE = datetime.now().strftime('%Y-%m-%d')  # Today
```

**Available history by source:**
- **yfinance:** Typically 2000-present (some go back further)
- **CRSP (comparison):** 1926-present

---

## Data Quality & Limitations

### ✅ What Works Well

1. **Recent data (2000+):** Excellent coverage and quality
2. **Active stocks:** Near-complete data for S&P 500, Russell 1000
3. **Prices & returns:** Reliable, adjusted for splits/dividends
4. **Volume:** Complete for liquid stocks
5. **Real-time updates:** 15-minute delayed (vs. CRSP's end-of-day)

### ⚠️ Known Limitations

1. **Historical depth:** Only goes back to ~2000 (vs. CRSP's 1926)
2. **Delisted stocks:** Not included (survivor bias)
3. **Ticker changes:** Doesn't track historical ticker changes
4. **Shares outstanding:** Point-in-time only (not historical adjustments)
5. **Corporate actions:** Some complex events may not be perfectly adjusted
6. **Penny stocks:** Coverage may be spotty

### 🔧 Comparison to CRSP

| Feature | AP_CRSPDaily (yfinance) | CRSP |
|---------|------------------------|------|
| **Price adjustments** | ✅ Yes (splits, dividends) | ✅ Yes |
| **Returns** | ✅ Calculated | ✅ Provided |
| **Volume** | ✅ Yes | ✅ Yes |
| **Shares outstanding** | ⚠️ Current value only | ✅ Historical |
| **Delisted stocks** | ❌ No | ✅ Yes |
| **Ticker changes** | ❌ No tracking | ✅ Tracked |
| **Historical depth** | ⚠️ 2000+ | ✅ 1926+ |
| **Survivor bias** | ⚠️ Yes | ✅ No |
| **Cost** | ✅ Free | ❌ $2,000+/year |
| **Latency** | ✅ 15-min delayed | ⚠️ End of day |

---

## Integration with Existing Code

### Replace CRSP in SignalMasterTable

**Option 1: Use AP files directly**
```python
# In SignalMasterTable.py, change:
crsp_daily = pd.read_parquet('../pyData/Intermediate/dailyCRSP.parquet')

# To:
crsp_daily = pd.read_parquet('../pyData/Intermediate/AP_dailyCRSP.parquet')
```

**Option 2: Create symlink (Unix/Mac)**
```bash
cd ../pyData/Intermediate
ln -s AP_dailyCRSP.parquet dailyCRSP.parquet
ln -s AP_dailyCRSPprc.parquet dailyCRSPprc.parquet
```

**Option 3: Modify to use AP prefix globally**
```python
# Add to config.py
USE_AP_DATA = True  # Toggle between CRSP and AP data

# In data loading functions:
if USE_AP_DATA:
    prefix = 'AP_'
else:
    prefix = ''
    
crsp = pd.read_parquet(f'../pyData/Intermediate/{prefix}dailyCRSP.parquet')
```

---

## Advanced Features

### Custom Ticker Universe

Create `ticker_list.csv`:
```csv
ticker
AAPL
MSFT
GOOGL
AMZN
META
TSLA
NVDA
JPM
V
WMT
```

Then run:
```bash
python AP_CRSPDaily.py
```

The script will automatically detect and use your custom list.

---

### Incremental Updates

To update daily without re-downloading everything:

```python
# In AP_CRSPDaily.py, modify:
# Check existing file and only download new dates
existing_file = OUTPUT_DIR / "AP_dailyCRSP.parquet"
if existing_file.exists():
    existing = pd.read_parquet(existing_file)
    last_date = existing['time_d'].max()
    START_DATE = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
    print(f"📥 Incremental update from {START_DATE}")
```

---

### Handle Ticker Changes

For stocks that changed tickers (e.g., Facebook → Meta):

```python
TICKER_HISTORY = {
    'META': ['FB'],  # Meta was formerly FB
    'GOOGL': ['GOOG'],  # Alphabet
}

def download_with_history(ticker, ...):
    # Try current ticker
    df = download_ticker_data(ticker, ...)
    
    # If empty, try historical tickers
    if df.empty and ticker in TICKER_HISTORY:
        for old_ticker in TICKER_HISTORY[ticker]:
            df_old = download_ticker_data(old_ticker, ...)
            df = pd.concat([df_old, df])
    
    return df
```

---

## Performance Optimization

### 1. Enable Caching
```python
import yfinance as yf
import requests_cache

# Cache for 1 day
session = requests_cache.CachedSession('yfinance.cache', expire_after=86400)
```

### 2. Parallel Downloads
```python
from concurrent.futures import ThreadPoolExecutor

def download_parallel(tickers, max_workers=10):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = executor.map(download_ticker_data, tickers)
    return list(results)
```

### 3. Batch Processing
```python
# Process in chunks to manage memory
CHUNK_SIZE = 100
for i in range(0, len(tickers), CHUNK_SIZE):
    chunk = tickers[i:i+CHUNK_SIZE]
    process_chunk(chunk)
```

---

## Troubleshooting

### Error: "No module named 'yfinance'"
```bash
pip install yfinance
```

### Error: "Too many requests" or rate limiting
```python
# Increase delays between requests
time.sleep(1)  # Wait 1 second between tickers

# Or use caching (see Performance section)
```

### Error: "No data for ticker XXX"
- Ticker may be delisted
- Ticker may have changed symbols
- Check ticker spelling

### Empty/Missing Data
- Check internet connection
- Verify ticker list is correct
- Some tickers may not have data in requested date range

### Shares Outstanding Missing
- yfinance doesn't always provide `sharesOutstanding`
- Will show as NaN in output
- Can supplement with quarterly filings (10-Q) if needed

---

## Extending to Other Data Sources

### Add Polygon.io (Paid, Better Coverage)

```python
from polygon import RESTClient

def download_with_polygon(ticker, api_key):
    client = RESTClient(api_key)
    aggs = client.list_aggs(
        ticker=ticker,
        multiplier=1,
        timespan='day',
        from_=start_date,
        to=end_date
    )
    return process_polygon_data(aggs)
```

### Add Alpha Vantage (Free Alternative)

```python
from alpha_vantage.timeseries import TimeSeries

def download_with_alphavantage(ticker, api_key):
    ts = TimeSeries(key=api_key, output_format='pandas')
    data, meta = ts.get_daily_adjusted(symbol=ticker, outputsize='full')
    return process_alpha_data(data)
```

---

## Best Practices

### 1. Run Regularly
```bash
# Set up cron job (Unix/Mac) to run daily
0 18 * * * cd /path/to/CrossSection/Signals/pyCode/DataDownloads && python AP_CRSPDaily.py
```

### 2. Validate Data
```bash
# Always run test after download
python test_ap_crsp.py
```

### 3. Monitor Quality
- Check for missing dates
- Verify return distributions
- Compare with known benchmarks

### 4. Backup Old Data
```bash
# Before re-downloading
cp AP_dailyCRSP.parquet AP_dailyCRSP_backup.parquet
```

---

## FAQ

**Q: Can I use this for live trading?**  
A: Yes, but with 15-minute delay. Upgrade yfinance or use paid APIs for real-time.

**Q: Why 50% coverage for shares outstanding?**  
A: yfinance doesn't always provide this field. Supplement with SEC 10-Q data if needed.

**Q: How do I handle delisted stocks?**  
A: Use CRSP for backtests requiring delisted stocks. For live trading, delisted stocks aren't tradeable anyway.

**Q: What about dividend adjustments?**  
A: yfinance provides adjusted prices. The `ret` field accounts for dividends.

**Q: Can I download more than S&P 500?**  
A: Yes, edit `get_user_ticker_list()` or provide CSV with tickers.

**Q: How often should I update?**  
A: Daily for live trading, weekly for research.

---

## Support & Contribution

**Issues:** 
- yfinance documentation: https://github.com/ranaroussi/yfinance
- Check ticker validity: https://finance.yahoo.com

**Performance:**
- Typical download: ~50 tickers/minute
- Parallel: ~200 tickers/minute

---

## Summary

✅ **Use AP_CRSPDaily when:**
- You need free data
- You're trading live (want real-time updates)
- Your strategy uses active stocks only
- Historical depth to ~2000 is sufficient

❌ **Use CRSP (original) when:**
- You need data before 2000
- Delisted stocks are critical (survivor bias matters)
- You need comprehensive corporate action tracking
- Budget allows for institutional data

**For most live trading strategies, AP_CRSPDaily is sufficient and actually better due to lower latency!**

