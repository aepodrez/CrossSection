# IBES Data Comprehensive Guide - DataIngressModel Repository

## Table of Contents
1. [Overview](#overview)
2. [Data Source Architecture](#data-source-architecture)
3. [IBES Data Types](#ibes-data-types)
4. [Predictor-by-Predictor IBES Usage](#predictor-by-predictor-ibes-usage)
5. [Bulk Download Specifications](#bulk-download-specifications)

---

## Overview

This repository uses **IBES (Institutional Brokers' Estimate System)** data exclusively through the **Eikon/LSEG (London Stock Exchange Group) API**. IBES data is NOT available from SEC EDGAR or Yahoo Finance - it is a proprietary dataset that requires an Eikon/Refinitiv subscription.

### Key Points
- **Primary Data Source**: Eikon/LSEG API (via Python `eikon` package)
- **No Fallback Sources**: IBES data is ONLY available through Eikon
- **Authentication**: Requires Eikon App Key configured in `config.yaml`
- **Data Connector**: `predictor_ingress/data_sources/eikon.py`
- **Manager Layer**: `predictor_ingress/data_sources/manager.py`

---

## Data Source Architecture

### Connection Flow
```
Predictor
    ↓
DataSourceManager (manager.py)
    ↓
EikonConnector (eikon.py)
    ↓
Eikon Python API (eikon package)
    ↓
LSEG Eikon Desktop/API
```

### EikonConnector Configuration
**File**: `predictor_ingress/data_sources/eikon.py`

**Initialization Parameters**:
```python
self.est_period = "FY1"      # FY1, FY2, etc. (fiscal year ahead)
self.currency = "USD"        # Currency for estimates
self.freq = "M"              # Monthly frequency
self.scale = 6               # Scale factor (millions)
self.start_date = "2020-01-01"  # Default historical start
```

**Connection Method**:
```python
async def connect(self) -> bool:
    import eikon as ek
    ek.set_app_key(self.app_key)
    # Test connection with simple request
    test_response = ek.get_data(["AAPL.O"], ["TR.CompanyName"])
```

---

## IBES Data Types

The repository accesses **FOUR distinct types** of IBES data through Eikon:

### 1. IBES EPS Estimates (Forward-Looking Forecasts)
**Method**: `get_ibes_eps_estimates()`
**File**: `eikon.py` lines 290-450

#### Eikon API Fields
```python
fields = [
    "TR.EPSMeanEstimate.date",       # statpers - statement period date
    "TR.EPSMeanEstimate.fperiod",    # fpedats - forecast period end date  
    "TR.EPSMeanEstimate",            # meanest - EPS mean estimate
    "TR.EPSNumberOfEstimates",       # numest - number of analysts
    "TR.EPSStdDev",                  # stdev - EPS estimate standard deviation
]
```

#### Eikon API Parameters
```python
params = {
    "Scale": 6,              # Millions
    "SDate": "2020-01-01",   # Start date
    "EDate": "2024-12-31",   # End date
    "FRQ": "M",              # Monthly frequency
    "Curn": "USD",           # Currency
    "Period": "FY1",         # Next fiscal year (FY1), or FY2, etc.
    "RH": "date",            # Return history by date
}
```

#### API Call Pattern
```python
# For each symbol (e.g., "AAPL.O"):
df, err = ek.get_data(
    ["AAPL.O"],                    # Single RIC symbol
    fields,                        # List of TR fields above
    params                         # Parameter dictionary
)
```

#### Output Columns (Normalized)
```python
# Column mapping to academic paper format:
{
    "Instrument": "tickerIBES",     # Original symbol (e.g., "AAPL")
    "statpers": datetime,            # Statement period date
    "fpedats": datetime,             # Forecast period end date
    "meanest": float,                # Mean EPS estimate
    "numest": int,                   # Number of analysts
    "stdev": float,                  # Standard deviation of estimates
    "fpi": "1"                       # Forecast period indicator (hardcoded to "1" for FY1)
}
```

#### Data Hygiene Steps
1. Convert symbols to RIC format (e.g., "AAPL" → "AAPL.O" for NYSE)
2. Parse dates with timezone handling (convert to timezone-naive)
3. Drop rows with missing `meanest`, `numest`, or `stdev`
4. Add `fpi` field = "1" to indicate FY1 forecasts
5. Create `time_avail_m` from `statpers` (monthly timestamp)

---

### 2. IBES Actual Earnings (Historical Results)
**Method**: `get_ibes_actual_earnings()`
**File**: `eikon.py` lines 452-620

#### Eikon API Fields
```python
fields = [
    "TR.EPSActValue",                # int0a - IBES actual EPS (unadjusted)
    "TR.EPSActReportDate",           # statpers - announcement/report date
    "TR.ISPeriodEndDate",            # period_end_date - fiscal period end
    "TR.TickerSymbol",               # ticker symbol
    "TR.CompanyName",                # company name
    "TR.FinancialPeriodAbsolute",    # fiscal period (e.g., FY2025)
    "TR.FinancialPeriodRelative",    # relative period (e.g., FY1)
    "TR.Currency"                    # currency
]
```

#### Eikon API Parameters
```python
params = {
    "Scale": 6,              # Millions
    "SDate": "2020-01-01",   # Start date
    "EDate": "2024-12-31",   # End date
    "FRQ": "Q",              # Quarterly frequency for actual earnings
    "Curn": "USD",           # Currency
    "RH": "date",            # Return history by date
}
```

#### Output Columns (Normalized)
```python
{
    "tickerIBES": str,           # Ticker symbol (e.g., "AAPL")
    "int0a": float,              # Actual EPS value
    "statpers": datetime,        # Announcement/report date
    "period_end_date": datetime, # Fiscal period end
    "time_avail_m": datetime,    # Monthly timestamp
    "fiscal_period": str,        # e.g., "FY2025"
    "currency": str              # Currency code
}
```

---

### 3. IBES Analyst Recommendations
**Method**: `get_ibes_recommendations()`
**File**: `eikon.py` lines 620-801

#### Eikon API Fields
```python
fields = [
    "TR.RecEstValue",              # ireccd - recommendation code (1-5 scale)
    "TR.BrkRecLabel",              # itext - recommendation text label
    "TR.RecLabelEstBrokerName",    # broker_name - broker name
    "TR.AnalystName"               # analyst_name - analyst name
]
```

#### Eikon API Parameters
```python
params = {
    "Scale": 6,              # Millions
    "SDate": "2020-01-01",   # Start date
    "EDate": "2024-12-31",   # End date
    "FRQ": "D",              # Daily frequency for recommendations
    "Curn": "USD",           # Currency
    "RH": "date",            # Return history by date
}
```

#### Recommendation Scale
```
1 = Strong Buy
2 = Buy
3 = Hold
4 = Sell
5 = Strong Sell
```

#### Output Columns (Normalized)
```python
{
    "tickerIBES": str,           # Ticker symbol (e.g., "AAPL")
    "amaskcd": str,              # Analyst mask code (broker_name + analyst_name)
    "anndats": datetime,         # Announcement date
    "time_avail_m": datetime,    # Monthly timestamp
    "ireccd": int,               # Recommendation code (1-5)
}
```

---

### 4. IBES Long-Term Growth Forecasts
**Method**: `get_ibes_long_term_growth()`
**File**: `eikon.py` lines 1357-1603

#### Eikon API Fields
```python
fields = [
    "TR.LTGMean.date",               # statpers - statistical period date
    "TR.LTGMean",                    # meanest - long-term growth mean estimate
    "TR.LTGMedian",                  # median - long-term growth median estimate
    "TR.LTGNumberOfEstimates",       # numest - number of analysts
]
```

#### Eikon API Parameters
```python
params = {
    "Scale": 6,              # Millions
    "SDate": "2020-01-01",   # Start date
    "EDate": "0",            # Use "0" for current (per LSEG docs)
    "Frq": "M",              # Monthly frequency (note: "Frq" not "FRQ")
    "Curn": "USD",           # Currency
    "RH": "date",            # Return history by date
}
```

#### Output Columns (Normalized)
```python
{
    "tickerIBES": str,           # Ticker symbol (e.g., "AAPL")
    "statpers": datetime,        # Statistical period date
    "meanest": float,            # Long-term growth mean estimate (5-year)
    "median": float,             # Long-term growth median estimate
    "fpi": "0",                  # Forecast period indicator (hardcoded to "0" for LTG)
    "numest": int,               # Number of analysts
    "time_avail_m": datetime     # Monthly timestamp
}
```

**Note**: LTG forecasts represent 5-year forward growth expectations and correspond to the IBES `FPI=0` concept in academic papers.

---

## Predictor-by-Predictor IBES Usage

### 1. ChNAnalyst (Decline in Analyst Coverage)
**File**: `predictor_ingress/predictors/ChNAnalyst.py`
**Paper**: Scherbina 2008, Table 2

#### IBES Data Used
- **Method**: `get_ibes_eps_estimates()`
- **Fields Required**: `numest`, `statpers`, `fpedats`, `meanest`
- **Period**: FY1 (next year estimates)
- **Date Range**: 6 months historical

#### Data Access Pattern
```python
ibes_df = await self.data_manager.get_ibes_eps_estimates(
    ric_symbols,    # List of RIC symbols (e.g., ["AAPL.O"])
    start_dt,       # 6 months ago
    end_str         # Today
)
```

#### IBES Fields Usage
- **numest**: Number of analyst estimates (primary metric)
- **statpers**: Statement period date (for monthly aggregation)
- **fpedats**: Forecast period end date (for validity filter)
- **meanest**: Mean EPS estimate (for fill-forward logic)
- **fpi**: Filter to "1" (annual forecasts only)

#### Paper Logic
1. Filter to annual forecasts (`fpi == 1`)
2. Create indicator for valid forecasts (`fpedats > statpers + 30 days`)
3. For invalid forecasts with same end date, use previous mean estimate
4. Calculate 3-month lagged analyst coverage using calendar-based lag
5. Set indicator = 1 if current analyst count < 3 months ago
6. Exclude July-September 1987 (data quality issues)
7. Keep only smallest two quintiles (small firms)

---

### 2. sfe (Earnings Forecast to Price)
**File**: `predictor_ingress/predictors/sfe.py`
**Paper**: Elgers, Lo and Pfeiffer 2001, Table 4A

#### IBES Data Used
- **Method**: `get_ibes_eps_estimates()`
- **Fields Required**: `medest`, `statpers`, `fpedats`, `numest`
- **Period**: FY1 (next year estimates)
- **Special Filter**: March forecasts only (`statpers.month == 3`)

#### Data Access Pattern
```python
ibes_data = await self.data_manager.get_ibes_eps_estimates(symbols)
```

#### IBES Fields Usage
- **medest**: Median EPS estimate (if unavailable, use `meanest`)
- **statpers**: Statement period date (filter for March only)
- **fpedats**: Forecast period end date (validity: `> statpers + 90 days`)
- **numest**: Number of analysts (for coverage ranking)
- **fpi**: Filter to "1" (1-year ahead forecasts)

#### Paper Logic
1. Filter for FPI=1 (next year estimates)
2. Use March forecasts only (`statpers.month == 3`)
3. Filter for valid forecast dates (`fpedats > statpers + 90 days`)
4. Merge with stock prices from 3 months earlier
5. Filter to December fiscal year ends only
6. Lower analyst coverage only (median split by month - **cross-sectional**)
7. Calculate `sfe = medest / abs(prc)`
8. Hold for one year (expand to 12 months)

**Current Status**: NOT IMPLEMENTABLE - Eikon subscription does not include IBES `statpers` field

---

### 3. FEPS (Earnings Forecast)
**File**: `predictor_ingress/predictors/FEPS.py`

#### IBES Data Used
- **Method**: `get_ibes_eps_estimates()`
- **Fields Required**: `meanest`, `statpers`, `fpedats`
- **Period**: FY1 (next year estimates)

#### Data Access Pattern
```python
ibes_data = await self.data_manager.get_ibes_eps_estimates(symbols)
```

#### IBES Fields Usage
- **meanest**: Mean EPS estimate (primary metric)
- **statpers**: Statement period date
- **fpedats**: Forecast period end date
- **fpi**: Set to "1" explicitly (FY1 equivalent)

#### Paper Logic
1. Get IBES EPS estimates (Period=FY1)
2. Add `fpi = "1"` to match paper logic
3. Create `tickerIBES` from Instrument field
4. Create `time_avail_m` from `statpers` (monthly timestamp)
5. Return processed IBES data

---

### 4. ForecastDispersion (Analyst Forecast Dispersion)
**File**: `predictor_ingress/predictors/ForecastDispersion.py`

#### IBES Data Used
- **Method**: `get_eikon_data()` with custom fields
- **Fields Required**: `TR.EPSStdDev`, `TR.EPSMean`
- **Period**: FY1

#### Data Access Pattern
```python
eikon_data = await self.data_manager.get_eikon_data(
    symbols, 
    ["TR.EPSStdDev", "TR.EPSMean"], 
    period="FY1"
)
```

#### IBES Fields Usage
- **stdev**: EPS estimate standard deviation (from `TR.EPSStdDev`)
- **meanest**: Mean EPS estimate (from `TR.EPSMean`)
- **fpi**: Set to "1" (FY1 equivalent)
- **fpedats**: Forecast period end date

#### Paper Logic
1. Keep if `fpi == "1"` (next year estimates)
2. Keep if `fpedats != missing` (forecast period end date exists)
3. Calculate: **ForecastDispersion = stdev / abs(meanest)**
4. Sort by `tickerIBES` and `time_avail_m`

---

### 5. ChForecastAccrual (Change in Forecast and Accrual)
**File**: `predictor_ingress/predictors/ChForecastAccrual.py`
**Paper**: Barth and Hutton 2004 RAS Table 3B

#### IBES Data Used
- **Method**: `get_ibes_eps_estimates()`
- **Fields Required**: `meanest`, `statpers`
- **Period**: FY1 (next year estimates)

#### Data Access Pattern
```python
ibes_data = await self.data_manager.get_ibes_eps_estimates(symbols)
```

#### IBES Fields Usage
- **meanest**: Mean EPS estimate (for month-over-month changes)
- **statpers**: Statement period date (for monthly aggregation)
- **fpi**: Set to "1" (FY1 equivalent)

#### Paper Logic
1. Get IBES EPS estimates (Period=FY1)
2. Create forecast change indicator (`meanest_t - meanest_{t-1}`)
3. Calculate working capital accruals from SEC EDGAR
4. Create binary accruals ranking (median split by month - **cross-sectional**)
5. Mark forecast increases (1) and decreases (0)
6. Exclude lower half of accruals distribution

---

### 6. REV6 (6-Month Analyst Revision)
**File**: `predictor_ingress/predictors/REV6.py`
**Paper**: Chan, Jegadeesh & Lakonishok 1996

#### IBES Data Used
- **Method**: `get_ibes_eps_estimates()`
- **Fields Required**: `Instrument`, `statpers`, `fpedats`, `meanest`, `numest`
- **Period**: FY1

#### Data Access Pattern
```python
ibes_raw = await self.data_manager.get_ibes_eps_estimates(
    symbols, 
    start_date, 
    end_date
)
```

#### IBES Fields Usage
- **Instrument**: Symbol identifier
- **statpers**: Statement period date (for monthly aggregation)
- **fpedats**: Forecast period end date (validity: `> statpers + 30 days`)
- **meanest**: Mean EPS estimate (for revision calculation)
- **numest**: Number of analysts

#### Paper Logic
1. Filter for FY1 (paper `fpi=1`)
2. Validity: keep if `fpedats > statpers + 30 days` OR `fpedats` is NaN
3. Fill-forward `meanest` when `fpedats` unchanged and validity missing
4. Create monthly key `time_avail_m` from `statpers`
5. Merge with monthly prices
6. Calculate: **tempRev_t = (meanest_t - meanest_{t-1}) / |prc_{t-1}|**
7. Sum current + 6 lags: **REV6_t = Σ(tempRev_{t-i})** for i=0 to 6

---

### 7. ConsRecomm (Consensus Recommendation)
**File**: `predictor_ingress/predictors/ConsRecomm.py`
**Paper**: Barber et al. 2001, Table 3A

#### IBES Data Used
- **Method**: `get_ibes_recommendations()`
- **Fields Required**: `tickerIBES`, `amaskcd`, `anndats`, `time_avail_m`, `ireccd`
- **Frequency**: Daily

#### Data Access Pattern
```python
ibes_data = await self.data_manager.get_ibes_recommendations(
    symbols, 
    start_date, 
    end_date
)
```

#### IBES Fields Usage
- **tickerIBES**: Symbol identifier
- **amaskcd**: Analyst mask code (identifies unique analyst)
- **anndats**: Announcement date
- **time_avail_m**: Monthly timestamp
- **ireccd**: Recommendation code (1-5 scale)

#### Paper Logic
1. Collapse to firm-month level:
   - Take last non-missing `ireccd` by ticker-analyst-month
   - Take mean across all analysts for each ticker-month
2. Create binary recommendation signal:
   - **ConsRecomm = 1** if mean `ireccd > 3` (sell recommendations)
   - **ConsRecomm = 0** if mean `ireccd <= 3` (buy/hold recommendations)

---

### 8. Recomm_ShortInterest (Analyst Recommendations and Short Interest)
**File**: `predictor_ingress/predictors/Recomm_ShortInterest.py`
**Paper**: Drake, Rees and Swanson 2011, Table 7b

#### IBES Data Used
- **Method**: `get_ibes_recommendations()`
- **Fields Required**: `tickerIBES`, `amaskcd`, `anndats`, `ireccd`
- **Frequency**: Daily
- **Date Range**: 2 years historical

#### Data Access Pattern
```python
ibes_data = await self.data_manager.get_ibes_recommendations(
    symbols, 
    start_date, 
    end_date
)
```

#### IBES Fields Usage
- **tickerIBES**: Symbol identifier
- **amaskcd**: Analyst mask code
- **anndats**: Announcement date
- **ireccd**: Recommendation code (1-5 scale)

#### Paper Logic
1. For duplicate ticker-analyst-date, take mean recommendation
2. Drop if `anndats` is after IBES consensus date (~17th of month)
3. Keep last recommendation for each ticker-analyst-month
4. Extend recommendations for 5 months after `anndats`
5. Take mean across analysts for each stock-month
6. Create **ConsRecomm = 6 - ireccd** (to align with paper coding)
7. Merge with short interest data from Eikon
8. Create quintiles for ShortInterest and ConsRecomm (**cross-sectional** by month)
9. Binary signal:
   - **1** if both quintiles == 1 (pessimistic)
   - **0** if both quintiles == 5 (optimistic)

---

### 9. AnalystRevision (1-Month Analyst Revision)
**File**: `predictor_ingress/predictors/AnalystRevision.py`

#### IBES Data Used
- **Method**: `get_ibes_eps_estimates()`
- **Fields Required**: `Instrument`, `statpers`, `meanest`
- **Period**: FY1

#### Data Access Pattern
```python
df = await eikon.get_ibes_eps_estimates(
    [symbol], 
    start_date=start_date, 
    end_date=end_date
)
```

#### IBES Fields Usage
- **Instrument**: Symbol identifier
- **statpers**: Statement period date (for monthly aggregation)
- **meanest**: Mean EPS estimate (for revision calculation)

#### Paper Logic
1. Fetch FY1 mean EPS history
2. Collapse to monthly: keep last observation within each month
3. Calculate: **AnalystRevision = meanest_t / meanest_{t-1}** within each symbol
4. Return latest month only (cross-sectional characteristic)

---

### 10. fgr5yrLag (Long-Term EPS Forecast)
**File**: `predictor_ingress/predictors/fgr5yrLag.py`
**Paper**: La Porta 1996

#### IBES Data Used
- **Method**: NOT CURRENTLY USING IBES - Uses simulated data
- **Expected Method**: `get_ibes_long_term_growth()`
- **Fields Required**: `meanest`, `statpers`, `fpi` (where `fpi = "0"` for LTG)

#### Expected Data Access Pattern (when implemented)
```python
ibes_data = await self.data_manager.get_ibes_long_term_growth(
    symbols, 
    start_date, 
    end_date
)
```

#### IBES Fields Usage (Expected)
- **meanest**: Mean long-term growth estimate (5-year forward)
- **statpers**: Statement period date
- **fpi**: Forecast period indicator (set to "0" for long-term growth)

#### Paper Logic
1. Apply 6-month calendar-based lag to long-term forecasts
2. Use June observations only
3. Return most recent value per symbol

**Current Status**: Uses simulated LTG data based on historical financial performance (SEC EDGAR). Real IBES LTG data from Eikon is available via `get_ibes_long_term_growth()`.

---

## Bulk Download Specifications

### Context for AI Bulk Download Tool

To recreate mass data download files that pull these IBES data fields in bulk, the AI needs the following specifications:

---

### 1. Authentication & Connection
```python
# Required Package
import eikon as ek

# Authentication
eikon_app_key = "YOUR_APP_KEY_HERE"
ek.set_app_key(eikon_app_key)

# Test connection
test_df, test_err = ek.get_data(["AAPL.O"], ["TR.CompanyName"])
if test_err:
    raise Exception("Eikon connection failed")
```

---

### 2. Symbol Format (RIC - Reuters Instrument Code)
```python
def convert_to_ric(symbol: str, exchange: str = "NYSE") -> str:
    """
    Convert simple ticker to RIC format.
    
    Exchange suffixes:
    - NYSE: .N
    - NASDAQ: .OQ  
    - AMEX: .A
    - LSE: .L
    - TSX: .TO
    """
    if "." in symbol:
        return symbol  # Already in RIC format
    
    exchange_map = {
        "NYSE": ".N",
        "NASDAQ": ".OQ",
        "AMEX": ".A",
        "LSE": ".L",
        "TSX": ".TO"
    }
    
    suffix = exchange_map.get(exchange, ".N")
    return f"{symbol}{suffix}"

# Example: "AAPL" → "AAPL.O" (NASDAQ: O is alternate)
# Standard: "AAPL.N" for NYSE, "MSFT.OQ" for NASDAQ
```

---

### 3. Bulk Download Template: IBES EPS Estimates

```python
import eikon as ek
import pandas as pd
from datetime import datetime
from typing import List

def bulk_download_ibes_eps_estimates(
    symbols: List[str],
    start_date: str = "2020-01-01",
    end_date: str = None
) -> pd.DataFrame:
    """
    Bulk download IBES EPS estimates for multiple symbols.
    
    Args:
        symbols: List of RIC symbols (e.g., ["AAPL.O", "MSFT.OQ"])
        start_date: Start date (YYYY-MM-DD format)
        end_date: End date (YYYY-MM-DD format, defaults to today)
    
    Returns:
        Combined DataFrame with all IBES EPS estimate data
    """
    
    end_date = end_date or datetime.now().strftime("%Y-%m-%d")
    
    # Eikon fields for IBES EPS estimates
    fields = [
        "TR.EPSMeanEstimate.date",       # Statement period date
        "TR.EPSMeanEstimate.fperiod",    # Forecast period end date  
        "TR.EPSMeanEstimate",            # Mean EPS estimate
        "TR.EPSNumberOfEstimates",       # Number of analysts
        "TR.EPSStdDev",                  # Standard deviation of estimates
        "TR.EPSMedianEstimate",          # Median EPS estimate (bonus field)
    ]
    
    # Parameters
    params = {
        "Scale": 6,              # Millions
        "SDate": start_date,
        "EDate": end_date,
        "FRQ": "M",              # Monthly frequency
        "Curn": "USD",
        "Period": "FY1",         # Next fiscal year
        "RH": "date",            # Return history
    }
    
    all_data = []
    
    # Eikon has rate limits - process in batches
    batch_size = 50  # Adjust based on Eikon limits
    
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        
        print(f"Processing batch {i//batch_size + 1}/{(len(symbols)-1)//batch_size + 1}")
        
        for symbol in batch:
            try:
                df, err = ek.get_data([symbol], fields, params)
                
                if err:
                    print(f"Warning for {symbol}: {err}")
                    continue
                
                if df is None or df.empty:
                    print(f"No data for {symbol}")
                    continue
                
                # Add symbol column
                df["Symbol"] = symbol
                
                # Normalize column names
                df = df.rename(columns={
                    "TR.EPSMeanEstimate.date": "StatementDate",
                    "TR.EPSMeanEstimate.fperiod": "ForecastPeriodEnd",
                    "TR.EPSMeanEstimate": "MeanEstimate",
                    "TR.EPSNumberOfEstimates": "NumberOfEstimates",
                    "TR.EPSStdDev": "StandardDeviation",
                    "TR.EPSMedianEstimate": "MedianEstimate",
                })
                
                # Parse dates
                df["StatementDate"] = pd.to_datetime(df["StatementDate"], errors="coerce")
                df["ForecastPeriodEnd"] = pd.to_datetime(df["ForecastPeriodEnd"], errors="coerce")
                
                all_data.append(df)
                
            except Exception as e:
                print(f"Error processing {symbol}: {e}")
                continue
    
    if not all_data:
        return pd.DataFrame()
    
    # Combine all data
    result = pd.concat(all_data, ignore_index=True)
    
    return result

# Example usage
symbols = ["AAPL.O", "MSFT.OQ", "GOOGL.OQ", "AMZN.OQ", "TSLA.OQ"]
data = bulk_download_ibes_eps_estimates(symbols, start_date="2020-01-01")
data.to_csv("ibes_eps_estimates_bulk.csv", index=False)
```

---

### 4. Bulk Download Template: IBES Actual Earnings

```python
def bulk_download_ibes_actual_earnings(
    symbols: List[str],
    start_date: str = "2020-01-01",
    end_date: str = None
) -> pd.DataFrame:
    """
    Bulk download IBES actual earnings for multiple symbols.
    """
    
    end_date = end_date or datetime.now().strftime("%Y-%m-%d")
    
    fields = [
        "TR.EPSActValue",                # Actual EPS (unadjusted)
        "TR.EPSActReportDate",           # Announcement date
        "TR.ISPeriodEndDate",            # Fiscal period end
        "TR.FinancialPeriodAbsolute",    # Fiscal period (e.g., FY2025)
        "TR.FinancialPeriodRelative",    # Relative period (e.g., FY1)
    ]
    
    params = {
        "Scale": 6,
        "SDate": start_date,
        "EDate": end_date,
        "FRQ": "Q",              # Quarterly frequency
        "Curn": "USD",
        "RH": "date",
    }
    
    all_data = []
    batch_size = 50
    
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        
        for symbol in batch:
            try:
                df, err = ek.get_data([symbol], fields, params)
                
                if err:
                    print(f"Warning for {symbol}: {err}")
                    continue
                
                if df is not None and not df.empty:
                    df["Symbol"] = symbol
                    df = df.rename(columns={
                        "TR.EPSActValue": "ActualEPS",
                        "TR.EPSActReportDate": "ReportDate",
                        "TR.ISPeriodEndDate": "PeriodEnd",
                        "TR.FinancialPeriodAbsolute": "FiscalPeriod",
                        "TR.FinancialPeriodRelative": "RelativePeriod",
                    })
                    df["ReportDate"] = pd.to_datetime(df["ReportDate"], errors="coerce")
                    df["PeriodEnd"] = pd.to_datetime(df["PeriodEnd"], errors="coerce")
                    all_data.append(df)
                
            except Exception as e:
                print(f"Error processing {symbol}: {e}")
                continue
    
    if not all_data:
        return pd.DataFrame()
    
    return pd.concat(all_data, ignore_index=True)
```

---

### 5. Bulk Download Template: IBES Recommendations

```python
def bulk_download_ibes_recommendations(
    symbols: List[str],
    start_date: str = "2020-01-01",
    end_date: str = None
) -> pd.DataFrame:
    """
    Bulk download IBES analyst recommendations for multiple symbols.
    """
    
    end_date = end_date or datetime.now().strftime("%Y-%m-%d")
    
    fields = [
        "TR.RecEstValue",              # Recommendation code (1-5)
        "TR.BrkRecLabel",              # Recommendation text
        "TR.RecLabelEstBrokerName",    # Broker name
        "TR.AnalystName"               # Analyst name
    ]
    
    params = {
        "Scale": 6,
        "SDate": start_date,
        "EDate": end_date,
        "FRQ": "D",              # Daily frequency
        "Curn": "USD",
        "RH": "date",
    }
    
    all_data = []
    batch_size = 50
    
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        
        for symbol in batch:
            try:
                df, err = ek.get_data([symbol], fields, params)
                
                if err:
                    print(f"Warning for {symbol}: {err}")
                    continue
                
                if df is not None and not df.empty:
                    df["Symbol"] = symbol
                    
                    # Rename columns (handle Eikon's actual column names)
                    rename_map = {}
                    for col in df.columns:
                        if "Standard Rec" in col or "RecEstValue" in col:
                            rename_map[col] = "RecommendationCode"
                        elif "Broker Rec Descr" in col or "BrkRecLabel" in col:
                            rename_map[col] = "RecommendationText"
                        elif "Broker Name" in col:
                            rename_map[col] = "BrokerName"
                        elif "Analyst" in col:
                            rename_map[col] = "AnalystName"
                    
                    df = df.rename(columns=rename_map)
                    
                    # Convert recommendation code to numeric
                    if "RecommendationCode" in df.columns:
                        df["RecommendationCode"] = pd.to_numeric(df["RecommendationCode"], errors="coerce")
                    
                    all_data.append(df)
                
            except Exception as e:
                print(f"Error processing {symbol}: {e}")
                continue
    
    if not all_data:
        return pd.DataFrame()
    
    return pd.concat(all_data, ignore_index=True)
```

---

### 6. Bulk Download Template: IBES Long-Term Growth

```python
def bulk_download_ibes_long_term_growth(
    symbols: List[str],
    start_date: str = "2020-01-01",
    end_date: str = None
) -> pd.DataFrame:
    """
    Bulk download IBES long-term growth forecasts for multiple symbols.
    """
    
    end_date = end_date or datetime.now().strftime("%Y-%m-%d")
    
    fields = [
        "TR.LTGMean.date",               # Statement date
        "TR.LTGMean",                    # Mean LTG estimate
        "TR.LTGMedian",                  # Median LTG estimate
        "TR.LTGNumberOfEstimates",       # Number of analysts
    ]
    
    params = {
        "Scale": 6,
        "SDate": start_date,
        "EDate": "0",            # Use "0" for current (per LSEG docs)
        "Frq": "M",              # Monthly (note: "Frq" not "FRQ")
        "Curn": "USD",
        "RH": "date",
    }
    
    all_data = []
    batch_size = 50
    
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        
        for symbol in batch:
            try:
                df, err = ek.get_data([symbol], fields, params)
                
                if err:
                    print(f"Warning for {symbol}: {err}")
                    continue
                
                if df is not None and not df.empty:
                    df["Symbol"] = symbol
                    
                    # Rename columns (handle Eikon's actual column names)
                    rename_map = {}
                    for col in df.columns:
                        if "LTG" in col and "Mean" in col and "date" in col:
                            rename_map[col] = "StatementDate"
                        elif "LTG" in col and "Mean" in col:
                            rename_map[col] = "MeanLTG"
                        elif "LTG" in col and "Median" in col:
                            rename_map[col] = "MedianLTG"
                        elif "LTG" in col and "Number" in col:
                            rename_map[col] = "NumberOfEstimates"
                    
                    df = df.rename(columns=rename_map)
                    
                    # Parse dates
                    if "StatementDate" in df.columns:
                        df["StatementDate"] = pd.to_datetime(df["StatementDate"], errors="coerce")
                    
                    all_data.append(df)
                
            except Exception as e:
                print(f"Error processing {symbol}: {e}")
                continue
    
    if not all_data:
        return pd.DataFrame()
    
    return pd.concat(all_data, ignore_index=True)
```

---

### 7. Complete Bulk Download Script

```python
"""
Complete IBES Bulk Download Script
Downloads all IBES data types for a list of symbols
"""

import eikon as ek
import pandas as pd
from datetime import datetime
from typing import List, Dict
import time

# Authentication
EIKON_APP_KEY = "YOUR_APP_KEY_HERE"
ek.set_app_key(EIKON_APP_KEY)

# Test connection
print("Testing Eikon connection...")
test_df, test_err = ek.get_data(["AAPL.O"], ["TR.CompanyName"])
if test_err:
    raise Exception(f"Eikon connection failed: {test_err}")
print("✓ Eikon connection successful")

# Universe of symbols (S&P 500 example)
SYMBOLS = [
    "AAPL.O", "MSFT.OQ", "GOOGL.OQ", "AMZN.OQ", "TSLA.OQ",
    "META.OQ", "NVDA.OQ", "JPM.N", "V.N", "WMT.N"
    # Add all S&P 500 symbols here...
]

# Date range
START_DATE = "2020-01-01"
END_DATE = datetime.now().strftime("%Y-%m-%d")

# Output directory
OUTPUT_DIR = "./ibes_bulk_download/"

print(f"\nStarting bulk download for {len(SYMBOLS)} symbols")
print(f"Date range: {START_DATE} to {END_DATE}")
print(f"Output directory: {OUTPUT_DIR}\n")

# 1. Download IBES EPS Estimates
print("1/4 Downloading IBES EPS Estimates...")
eps_estimates = bulk_download_ibes_eps_estimates(SYMBOLS, START_DATE, END_DATE)
eps_estimates.to_csv(f"{OUTPUT_DIR}ibes_eps_estimates.csv", index=False)
print(f"✓ Saved {len(eps_estimates)} EPS estimate records\n")

time.sleep(2)  # Rate limiting

# 2. Download IBES Actual Earnings
print("2/4 Downloading IBES Actual Earnings...")
actual_earnings = bulk_download_ibes_actual_earnings(SYMBOLS, START_DATE, END_DATE)
actual_earnings.to_csv(f"{OUTPUT_DIR}ibes_actual_earnings.csv", index=False)
print(f"✓ Saved {len(actual_earnings)} actual earnings records\n")

time.sleep(2)  # Rate limiting

# 3. Download IBES Recommendations
print("3/4 Downloading IBES Recommendations...")
recommendations = bulk_download_ibes_recommendations(SYMBOLS, START_DATE, END_DATE)
recommendations.to_csv(f"{OUTPUT_DIR}ibes_recommendations.csv", index=False)
print(f"✓ Saved {len(recommendations)} recommendation records\n")

time.sleep(2)  # Rate limiting

# 4. Download IBES Long-Term Growth
print("4/4 Downloading IBES Long-Term Growth...")
ltg_forecasts = bulk_download_ibes_long_term_growth(SYMBOLS, START_DATE, END_DATE)
ltg_forecasts.to_csv(f"{OUTPUT_DIR}ibes_long_term_growth.csv", index=False)
print(f"✓ Saved {len(ltg_forecasts)} long-term growth records\n")

print("\n=== Bulk Download Complete ===")
print(f"Total records downloaded: {len(eps_estimates) + len(actual_earnings) + len(recommendations) + len(ltg_forecasts)}")
```

---

### 8. Rate Limiting & Best Practices

```python
# Eikon Rate Limits (as of 2024)
RATE_LIMITS = {
    "requests_per_second": 5,        # Max 5 requests/second
    "requests_per_minute": 300,      # Max 300 requests/minute
    "data_points_per_request": 10000 # Max 10,000 data points/request
}

# Recommended delays
DELAY_BETWEEN_SYMBOLS = 0.2  # 200ms between symbols
DELAY_BETWEEN_BATCHES = 2.0  # 2 seconds between batches
DELAY_BETWEEN_DATA_TYPES = 5.0  # 5 seconds between different data types

# Error handling and retry logic
import time
from typing import Tuple, Optional

def safe_eikon_call(
    symbols: List[str], 
    fields: List[str], 
    params: Dict,
    max_retries: int = 3
) -> Tuple[Optional[pd.DataFrame], Optional[List]]:
    """
    Make Eikon API call with retry logic and rate limiting.
    """
    
    for attempt in range(max_retries):
        try:
            df, err = ek.get_data(symbols, fields, params)
            
            if err:
                # Check if it's a rate limit error
                error_str = str(err)
                if "429" in error_str or "rate limit" in error_str.lower():
                    wait_time = (attempt + 1) * 5  # Exponential backoff
                    print(f"Rate limited, waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                
                # Return error for logging
                return df, err
            
            # Success - add small delay before next call
            time.sleep(DELAY_BETWEEN_SYMBOLS)
            return df, None
            
        except Exception as e:
            print(f"Exception on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                time.sleep((attempt + 1) * 2)
                continue
            else:
                return None, [{"code": "EXCEPTION", "message": str(e)}]
    
    return None, [{"code": "MAX_RETRIES", "message": "Max retries exceeded"}]
```

---

### 9. Data Validation & Quality Checks

```python
def validate_ibes_data(df: pd.DataFrame, data_type: str) -> Dict:
    """
    Validate IBES data quality and completeness.
    
    Returns:
        Dictionary with validation results
    """
    
    validation = {
        "data_type": data_type,
        "total_records": len(df),
        "symbols_count": df["Symbol"].nunique() if "Symbol" in df.columns else 0,
        "date_range": None,
        "missing_values": {},
        "warnings": []
    }
    
    if df.empty:
        validation["warnings"].append("Empty DataFrame")
        return validation
    
    # Check date range
    date_cols = [col for col in df.columns if "Date" in col or "date" in col]
    if date_cols:
        first_date_col = date_cols[0]
        min_date = df[first_date_col].min()
        max_date = df[first_date_col].max()
        validation["date_range"] = f"{min_date} to {max_date}"
    
    # Check missing values
    for col in df.columns:
        missing_count = df[col].isna().sum()
        missing_pct = (missing_count / len(df)) * 100
        if missing_pct > 0:
            validation["missing_values"][col] = f"{missing_count} ({missing_pct:.1f}%)"
    
    # Data-type specific checks
    if data_type == "eps_estimates":
        if "MeanEstimate" in df.columns:
            zero_count = (df["MeanEstimate"] == 0).sum()
            if zero_count > 0:
                validation["warnings"].append(f"{zero_count} zero mean estimates")
        
        if "NumberOfEstimates" in df.columns:
            avg_analysts = df["NumberOfEstimates"].mean()
            validation["avg_analysts"] = round(avg_analysts, 1)
    
    elif data_type == "recommendations":
        if "RecommendationCode" in df.columns:
            rec_dist = df["RecommendationCode"].value_counts().to_dict()
            validation["recommendation_distribution"] = rec_dist
    
    return validation

# Example usage
validation_results = validate_ibes_data(eps_estimates, "eps_estimates")
print(json.dumps(validation_results, indent=2))
```

---

### 10. Summary: Key IBES Fields by Use Case

| Use Case | Eikon Fields | Output Columns | Predictors Using This |
|----------|-------------|----------------|----------------------|
| **Analyst Coverage** | `TR.EPSNumberOfEstimates` | `numest` | ChNAnalyst |
| **Mean EPS Forecast** | `TR.EPSMeanEstimate` | `meanest` | FEPS, REV6, ChForecastAccrual, AnalystRevision |
| **Median EPS Forecast** | `TR.EPSMedianEstimate` | `medest` | sfe |
| **Forecast Dispersion** | `TR.EPSStdDev`, `TR.EPSMeanEstimate` | `stdev`, `meanest` | ForecastDispersion |
| **Forecast Timing** | `TR.EPSMeanEstimate.date`, `TR.EPSMeanEstimate.fperiod` | `statpers`, `fpedats` | All predictors (for filtering/validity) |
| **Actual Earnings** | `TR.EPSActValue`, `TR.EPSActReportDate` | `int0a`, `statpers` | (Not currently used) |
| **Analyst Recommendations** | `TR.RecEstValue`, `TR.BrkRecLabel`, `TR.AnalystName` | `ireccd`, `itext`, `analyst_name` | ConsRecomm, Recomm_ShortInterest |
| **Long-Term Growth** | `TR.LTGMean`, `TR.LTGMedian` | `meanest`, `median`, `fpi="0"` | fgr5yrLag (expected) |

---

### 11. Critical Implementation Notes

1. **RIC Format Required**: All symbols MUST be in RIC format (e.g., "AAPL.O" not "AAPL")

2. **Period Parameter**: 
   - Use `"Period": "FY1"` for next fiscal year estimates
   - Use `"Period": "FY2"` for two years ahead
   - LTG fields don't use Period parameter (use dedicated TR.LTG* fields)

3. **Date Handling**:
   - Eikon returns dates in ISO 8601 format: "2024-01-15T00:00:00Z"
   - Always parse with timezone handling: `pd.to_datetime(...).dt.tz_localize(None)`

4. **Column Name Variations**:
   - Eikon may return different column names depending on subscription
   - Always implement flexible column mapping (exact match → partial match → keyword search)

5. **Data Availability**:
   - Not all symbols have IBES coverage
   - Small-cap stocks often have no analyst coverage
   - Expect 20-30% of requests to return no data

6. **Quarterly vs Annual**:
   - Use `"FRQ": "Q"` for quarterly actual earnings
   - Use `"FRQ": "M"` for monthly EPS estimates
   - Use `"FRQ": "D"` for daily recommendations

7. **Historical Depth**:
   - IBES data typically available from 1990s onward
   - Older data may have gaps or lower quality
   - Recommend starting from 2010 or later for robust coverage

---

## End of IBES Data Comprehensive Guide

This document provides complete specifications for all IBES data fields used in the DataIngressModel repository. For bulk downloads, implement the provided templates with proper rate limiting, error handling, and data validation.

**Last Updated**: November 27, 2025
**Repository**: DataIngressModel (Euclidean Research)

