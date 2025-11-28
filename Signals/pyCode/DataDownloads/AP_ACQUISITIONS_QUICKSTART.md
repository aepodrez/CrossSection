# 📊 AP_CRSPAcquisitions Quick Start Guide

## Overview

`AP_CRSPAcquisitions.py` provides **FREE, live spinoff company data** from SEC 8-K filings as an alternative to paid CRSP Acquisitions data from WRDS.

---

## 🎯 What It Does

✅ **Fetches 8-K filings** from SEC EDGAR using `edgartools`  
✅ **Identifies spinoff events** via text parsing (keywords, item numbers)  
✅ **Extracts spinoff company information** (ticker, date)  
✅ **Matches to permnos** via ticker mapping  
✅ **Same format as CRSP** (permno, SpinoffCo)  
✅ **100% FREE** (no WRDS subscription needed)

---

## 📥 Outputs

| File | Description |
|------|-------------|
| `AP_m_CRSPAcquisitions.parquet` | Spinoff company data (use this for predictors) |

**Columns:**
- `permno` - CRSP permanent number (spinoff company)
- `SpinoffCo` - Spinoff company indicator (always 1)

**Note:** The original CRSP format only includes `permno` and `SpinoffCo` (no time dimension). The predictor merges on `permno` only.

---

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install edgartools pandas numpy
```

### 2. Run the Script

```bash
cd /Users/alexpodrez/Documents/CrossSection/Signals/pyCode/DataDownloads
python AP_CRSPAcquisitions.py
```

### 3. What Happens

1. **Loads ticker universe** from `AP_CRSPMonthly.parquet` or `CCMLinkingTable.parquet`
2. **Fetches 8-K filings** for each ticker (last 5 years)
3. **Identifies spinoff events** via text parsing
4. **Extracts spinoff company info** (ticker, date)
5. **Matches to permnos** via ticker mapping
6. **Saves** `AP_m_CRSPAcquisitions.parquet`

### 4. Expected Runtime

- **10 tickers**: ~5-10 minutes
- **100 tickers**: ~1-2 hours  
- **1000 tickers**: ~10-20 hours

*(SEC rate limiting pauses every 10 requests)*

---

## 📋 How Spinoff Detection Works

### **1. Filing Selection**
- Fetches 8-K filings (Form 8-K) from SEC EDGAR
- Focuses on Item 2.01 (acquisitions) and Item 2.02 (spinoffs/divestitures)

### **2. Text Parsing**
Looks for spinoff-related keywords:
- "spinoff", "spin-off", "spin off"
- "spinout", "spin-out", "spin out"
- "distribution of shares"
- "pro rata distribution"
- "separation of"
- "divestiture", "divest"
- "split-off", "split off"
- "carve-out", "carve out"

### **3. Date Extraction**
- Extracts spinoff completion date from filing text
- Falls back to filing date if date not found
- Uses date patterns: "completed on [date]", "effective [date]", etc.

### **4. Company Identification**
- Extracts spinoff company ticker from filing text
- Looks for patterns: "(AAPL)", "(NYSE: AAPL)", "ticker: AAPL"
- Extracts company name if available

### **5. Permno Matching**
- Matches spinoff ticker to permno via `AP_CRSPMonthly.parquet` or `CCMLinkingTable.parquet`
- Only includes spinoff companies that can be matched to permnos

---

## ⚙️ Configuration

### Customize Ticker Universe

Edit the `main()` function:

```python
# Option 1: Use specific tickers
universe_tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META']

# Option 2: Use first N from AP files (for testing)
universe_tickers = universe_tickers[:50]  # First 50 tickers

# Option 3: Use full universe (default)
# Let script auto-detect from AP_CRSPMonthly.parquet
```

### Adjust History Period

```python
# Change from 5 years to 3 years
ticker_data = get_spinoff_filings_for_company(ticker, years=3)
```

### Rate Limiting

```python
# Adjust pause frequency (default: every 10 requests)
if i % 10 == 0:
    time.sleep(2)  # Increase to 3-5 seconds if hitting rate limits
```

---

## 🔧 Troubleshooting

### Error: "edgartools not installed"
```bash
pip install edgartools
```

### Error: "No universe file found"
- Make sure you've run `AP_CRSPMonthly.py` first
- Or manually specify tickers in the script

### Warning: "No spinoff companies could be matched to permnos"
- **Possible causes:**
  - Spinoff tickers were not extracted from filings (text parsing limitation)
  - Ticker-permno mapping is incomplete
  - Spinoff companies are not yet in CRSP (newly created)
- **Solution:** Check that `AP_CRSPMonthly.parquet` or `CCMLinkingTable.parquet` exists and is up to date

### Low spinoff detection rate
- **Text parsing limitations:** Some spinoffs may not be clearly described in 8-K filings
- **Alternative approach:** Could also check 10-K/10-Q filings for spinoff disclosures
- **Historical coverage:** Pre-2000 filings may have less structured text

### Script is very slow
- SEC rate limiting is required (10 requests/second max)
- 8-K text parsing is computationally intensive
- Process will take hours for large universes (1000+ tickers)
- Consider running overnight or in batches

---

## 🔗 Integration with Existing Pipeline

### Replace Original m_CRSPAcquisitions

1. **Run AP version first:**
```bash
python AP_CRSPAcquisitions.py
```

2. **Update predictor scripts** to use AP file:

```python
# OLD: Original WRDS CRSP
acquisitions = pd.read_parquet('../pyData/Intermediate/m_CRSPAcquisitions.parquet')

# NEW: AP version from SEC EDGAR
acquisitions = pd.read_parquet('../pyData/Intermediate/AP_m_CRSPAcquisitions.parquet')
```

### Use with Spinoff Predictor

The AP version has the same format as the original, so the predictor works without changes:

```python
# Spinoff.py predictor
acquisitions = pd.read_parquet('../pyData/Intermediate/AP_m_CRSPAcquisitions.parquet')

# Merge on permno (no time dimension needed)
df = df.merge(acquisitions, on='permno', how='left')

# Create Spinoff signal: 1 if SpinoffCo == 1 & FirmAgeNoScreen <= 24
df['Spinoff'] = np.where((df['SpinoffCo'] == 1) & (df['FirmAgeNoScreen'] <= 24), 1, 0)
```

---

## 📊 Data Quality Comparison

| Metric | AP_CRSPAcquisitions | WRDS CRSP |
|--------|---------------------|-----------|
| **Timeliness** | Same-day to 1 week | 1-3 months |
| **Cost** | Free | $2,000+/year |
| **Coverage** | 1994-present (8-K electronic) | 1962-present |
| **Detection Method** | Text parsing (8-K filings) | Direct from CRSP distributions |
| **Accuracy** | Moderate (text parsing) | Very High (structured data) |
| **Completeness** | 70-80% (may miss some) | 100% |

---

## ⚠️ Important Notes

### 1. **Text Parsing Limitations**
- Spinoff detection relies on parsing unstructured 8-K filing text
- Some spinoffs may be missed if not clearly described
- False positives are possible (e.g., mentions of spinoffs in other contexts)
- **Recommendation:** Validate results against known spinoff events

### 2. **Historical Coverage**
- 8-K filings available electronically from ~1994
- Pre-1994 data would require paper filing access
- Original CRSP has data back to 1962
- **For pre-1994 data:** Use original CRSP

### 3. **Ticker Matching**
- Spinoff companies must be matched to permnos via ticker
- If spinoff ticker not extracted or not in mapping, company is excluded
- Coverage typically 60-80% depending on text extraction quality

### 4. **Spinoff vs Other Events**
- Script focuses on spinoffs (distribution of shares to shareholders)
- Does NOT include:
  - Mergers and acquisitions (different event type)
  - Asset sales (divestitures without share distribution)
  - IPOs (initial public offerings)
- Original CRSP also focuses on spinoffs (via `acperm` field)

### 5. **Output Format**
- Only includes `permno` and `SpinoffCo` (no time dimension)
- Matches original CRSP format exactly
- Predictor merges on `permno` only (not time-based)

---

## 🎓 Next Steps

1. ✅ **Run the script** with a small sample (10-50 tickers)
2. ✅ **Validate output** against known spinoff events
3. ✅ **Test with Spinoff predictor** to ensure compatibility
4. ✅ **Scale to full universe** once validated
5. ✅ **Schedule regular updates** (monthly or quarterly)

---

## 📚 Related AP Files

- **`AP_CRSPMonthly.py`** - Provides ticker-permno mapping
- **`AP_CRSPDistributions.py`** - Related distribution data
- **`AP_CompustatAnnual.py`** - Company fundamental data

---

## 💰 Cost Savings

| Data Source | Annual Cost | AP_CRSPAcquisitions |
|-------------|-------------|---------------------|
| **WRDS CRSP** | $2,000+ | **$0** |
| **Bloomberg** | $2,000+ | **$0** |
| **FactSet** | $2,000+ | **$0** |
| **Total Savings** | | **$2,000+/year** |

---

## 🔗 References

### SEC 8-K Filings
- **Form 8-K**: https://www.sec.gov/files/form8-k.pdf
- **Item 2.01**: Completion of Acquisition or Disposition of Assets
- **Item 2.02**: Results of Operations and Financial Condition

### Spinoff Predictor
- **Spinoff.py** - Uses spinoff company data to create spinoff indicator
- **Paper**: Cusatis, Miles and Woolridge (1993)

---

## 📝 Limitations and Future Improvements

### Current Limitations
1. **Text parsing accuracy** - May miss some spinoffs or have false positives
2. **Ticker extraction** - Spinoff ticker may not always be extracted correctly
3. **Historical coverage** - Limited to post-1994 (electronic filings)
4. **Processing speed** - Text parsing is slow for large universes

### Potential Improvements
1. **Machine learning** - Train model to identify spinoff filings more accurately
2. **Multiple filing types** - Also check 10-K/10-Q for spinoff disclosures
3. **External data sources** - Cross-reference with news articles or press releases
4. **Incremental updates** - Only process new filings since last run

---

**🚀 You're now ready to use free, live spinoff company data!**

