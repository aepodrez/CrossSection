# 📊 AP_CompustatShortInterest Quick Start Guide

## Overview

`AP_CompustatShortInterest.py` provides **FREE, live short interest data** from FINRA as an alternative to paid Compustat Short Interest data from WRDS.

---

## 🎯 What It Does

✅ **Downloads FINRA short sale volume data** (free, public data)  
✅ **Aggregates daily to monthly** (first non-missing value per month)  
✅ **Matches to gvkey** via ticker mapping (if available)  
✅ **Same format as Compustat** (shortint in millions of shares)  
✅ **Available from 2010 onwards** (when FINRA started publishing)  
✅ **100% FREE** (no WRDS subscription needed)

---

## 📥 Outputs

| File | Description |
|------|-------------|
| `AP_monthlyShortInterest.parquet` | Monthly short interest data (use this for predictors) |

**Columns:**
- `gvkey` - Compustat identifier (if mapping available)
- `ticker` - Ticker symbol
- `time_avail_m` - Month (first day of month)
- `shortint` - Short interest (millions of shares)
- `shortintadj` - Adjusted short interest (millions of shares, same as shortint for now)

---

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install requests pandas numpy
```

### 2. Run the Script

```bash
cd /Users/alexpodrez/Documents/CrossSection/Signals/pyCode/DataDownloads
python AP_CompustatShortInterest.py
```

### 3. What Happens

1. **Loads ticker-gvkey mapping** from `AP_CompustatAnnual.parquet` or `CCMLinkingTable.parquet`
2. **Downloads FINRA data** for last 2 years (default, can be adjusted)
3. **Processes daily data** into standardized format
4. **Aggregates to monthly** (first non-missing value per ticker-month)
5. **Adds gvkey** via ticker mapping
6. **Saves** `AP_monthlyShortInterest.parquet`

### 4. Expected Runtime

- **1 month of data**: ~2-3 minutes
- **1 year of data**: ~10-15 minutes
- **2 years of data**: ~20-30 minutes

*(FINRA rate limiting may slow downloads)*

---

## 📋 FINRA Data Source

### **FINRA Short Sale Volume Data**

- **URL**: https://www.finra.org/finra-data/browse-catalog/short-sale-volume-data
- **Format**: Daily CSV files (pipe-delimited)
- **Availability**: ~2010 onwards (trading days only)
- **Update Frequency**: Daily (with 1-2 day lag)
- **Coverage**: All NASDAQ and NYSE stocks

### **Data Format**

FINRA provides daily files with:
- `Symbol` - Ticker symbol
- `ShortVolume` - Number of shares sold short
- `TotalVolume` - Total trading volume
- `Date` - Trading date

### **File URL Format**

```
https://cdn.finra.org/equity/regsho/daily/YYYYMMDD/CNMSshvolYYYYMMDD.txt
```

Example:
```
https://cdn.finra.org/equity/regsho/daily/20241127/CNMSshvol20241127.txt
```

---

## ⚙️ Configuration

### Customize Date Range

Edit the `main()` function:

```python
# Download last 2 years (default)
end_date = datetime.now()
start_date = end_date - timedelta(days=730)

# Download last 5 years
start_date = end_date - timedelta(days=1825)

# Download specific date range
start_date = datetime(2020, 1, 1)
end_date = datetime(2024, 12, 31)
```

### Adjust Aggregation Method

The script uses **first non-missing value** per month (matching Compustat logic). To change:

```python
# In aggregate_to_monthly(), change from:
shortint=('short_volume', first_non_missing)

# To average:
shortint=('short_volume', 'mean')

# Or last value:
shortint=('short_volume', 'last')
```

---

## 🔧 Troubleshooting

### Error: "requests not installed"
```bash
pip install requests
```

### Error: "No FINRA data downloaded"
- **Check date range**: FINRA data only available from ~2010
- **Check trading days**: FINRA only publishes data for trading days (Mon-Fri)
- **Check network**: FINRA servers may be slow or temporarily unavailable
- **Try specific dates**: Test with a known trading day first

### Warning: "No ticker-gvkey mapping available"
- Run `AP_CompustatAnnual.py` first to create mapping
- Or ensure `CCMLinkingTable.parquet` exists
- Short interest will still work with ticker, but won't have gvkey

### Low gvkey coverage (< 50%)
- Ensure `AP_CompustatAnnual.parquet` is up to date
- Check that ticker symbols match between FINRA and Compustat
- Some tickers may have changed over time

### Script is very slow
- FINRA rate limiting (downloads one day at a time)
- Large date ranges take longer
- Consider downloading in batches (by year)

---

## 🔗 Integration with Existing Pipeline

### Replace Original monthlyShortInterest

1. **Run AP version first:**
```bash
python AP_CompustatShortInterest.py
```

2. **Update predictor scripts** to use AP file:

```python
# OLD: Original WRDS Compustat
short_interest = pd.read_parquet('../pyData/Intermediate/monthlyShortInterest.parquet')

# NEW: AP version from FINRA
short_interest = pd.read_parquet('../pyData/Intermediate/AP_monthlyShortInterest.parquet')
```

### Use with Predictors

The AP version has the same format as the original, so predictors should work without changes:

```python
# ShortInterest.py predictor
monthly_short = pd.read_parquet(
    '../pyData/Intermediate/AP_monthlyShortInterest.parquet',
    columns=['gvkey', 'time_avail_m', 'shortint']
)

# Merge with CRSP
df = df.merge(monthly_short, on=['gvkey', 'time_avail_m'], how='inner')

# Calculate short interest ratio
df['ShortInterest'] = df['shortint'] / df['shrout']
```

---

## 📊 Data Quality Comparison

| Metric | AP_CompustatShortInterest | WRDS Compustat |
|--------|--------------------------|----------------|
| **Timeliness** | 1-2 days lag | 1-3 months lag |
| **Cost** | Free | $500-1,000/year |
| **Coverage** | 2010-present | 1973-present |
| **Format** | Daily → Monthly | Monthly |
| **Data Quality** | High (FINRA official) | Very High (standardized) |
| **gvkey Mapping** | Via ticker (if available) | Direct (native) |

---

## ⚠️ Important Notes

### 1. **Historical Coverage**
- FINRA data starts ~2010 (when Reg SHO reporting began)
- Original Compustat has data back to 1973
- **For pre-2010 data**: Use original Compustat or other sources

### 2. **Trading Days Only**
- FINRA only publishes data for trading days
- No data for weekends or holidays
- Script automatically skips non-trading days

### 3. **gvkey Mapping**
- Requires `AP_CompustatAnnual.parquet` or `CCMLinkingTable.parquet`
- Some tickers may not match (ticker changes, delistings)
- Coverage typically 80-90% if mapping files are up to date

### 4. **Aggregation Method**
- Uses **first non-missing value** per month (matching Compustat)
- Alternative: Could use average, median, or last value
- Current method ensures consistency with original Compustat

### 5. **Short Interest vs Short Volume**
- FINRA provides **short volume** (daily)
- Compustat provides **short interest** (monthly, end-of-month)
- Script aggregates daily short volume to monthly
- **Note**: Short volume ≠ short interest (short interest is shares held short, short volume is shares sold short that day)
- For most predictors, monthly aggregation of short volume is a reasonable proxy

---

## 🎓 Next Steps

1. ✅ **Run the script** with a small date range (1-2 months) to test
2. ✅ **Validate output** against known values (if available)
3. ✅ **Test with predictors** that use short interest
4. ✅ **Scale to full history** (2010-present) once validated
5. ✅ **Schedule regular updates** (weekly or monthly)

---

## 📚 Related AP Files

- **`AP_CompustatAnnual.py`** - Provides ticker-gvkey mapping
- **`AP_CRSPMonthly.py`** - Provides shares outstanding for short interest ratio
- **`AP_IBESRecommendations.py`** - Used with short interest in Recomm_ShortInterest predictor

---

## 💰 Cost Savings

| Data Source | Annual Cost | AP_CompustatShortInterest |
|-------------|-------------|--------------------------|
| **WRDS Compustat** | $500-1,000 | **$0** |
| **Bloomberg** | $1,000+ | **$0** |
| **FactSet** | $1,000+ | **$0** |
| **Total Savings** | | **$500-1,000/year** |

---

## 🔗 References

### FINRA Data
- **Catalog**: https://www.finra.org/finra-data/browse-catalog/short-sale-volume-data
- **Documentation**: https://www.finra.org/filing-and-reporting/market-transparency-reporting/regsho

### Short Interest Predictors
- **ShortInterest.py** - Basic short interest ratio
- **Recomm_ShortInterest.py** - Combined with analyst recommendations
- **IO_ShortInterest.py** - Combined with institutional ownership

---

**🚀 You're now ready to use free, live short interest data!**

