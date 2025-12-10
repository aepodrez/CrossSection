# ABOUTME: Downloads IBES EPS estimates (unadjusted for splits) from Refinitiv Platform
# ABOUTME: Covers multiple forecast periods (current quarter, next year, year after, long-term) to mirror WRDS pull
"""
Inputs:
- Refinitiv Platform credentials (via .env REFINITIV_APP_KEY)

Outputs:
- ../pyData/Intermediate/AP_IBES_EPS_Unadj.parquet

How to run: python3 AP_IBESEPSUnadjusted.py
"""

import os
from pathlib import Path
from datetime import datetime
import pandas as pd
import refinitiv.data as rd
from refinitiv.data import _configure as rd_config
from dotenv import load_dotenv
import warnings

warnings.filterwarnings('ignore')

# Load environment variables
load_dotenv()

# Configuration
OUTPUT_DIR = Path("../pyData/Intermediate")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "AP_IBES_EPS_Unadj.parquet"
DEBUG_MODE = False  # Set to True to limit the number of instruments
MAX_INSTRUMENTS = 100  # Only used if DEBUG_MODE is True
BATCH_SIZE = int(os.getenv("IBES_BATCH_SIZE", "20"))  # Smaller batches to avoid timeouts
HTTP_REQUEST_TIMEOUT = int(os.getenv("RD_HTTP_TIMEOUT", "60"))  # seconds


def initialize_refinitiv_session():
    """
    Initialize Refinitiv Data session using the Platform (RDP) with REFINITIV_APP_KEY.
    """
    app_key = os.getenv("REFINITIV_APP_KEY")
    username = os.getenv("REFINITIV_USERNAME")
    password = os.getenv("REFINITIV_PASSWORD")

    if not app_key or not username or not password:
        raise Exception("REFINITIV_APP_KEY, REFINITIV_USERNAME, and REFINITIV_PASSWORD must be set in your .env file.")

    session_name = "rdp"
    session_path = f"sessions.platform.{session_name}"

    rd_config.set_param(f"{session_path}.app-key", app_key, auto_create=True)
    rd_config.set_param(f"{session_path}.username", username, auto_create=True)
    rd_config.set_param(f"{session_path}.password", password, auto_create=True)
    rd_config.set_param("sessions.default", f"platform.{session_name}", auto_create=True)
    rd_config.set_param("http.request-timeout", HTTP_REQUEST_TIMEOUT, auto_create=True)

    try:
        rd.open_session(f"platform.{session_name}")
        test_df = rd.get_data(universe=["AAPL.O"], fields=["TR.CompanyName"])
        if test_df is None:
            raise Exception("Platform session opened but test request returned no data.")
        print("Connected to Refinitiv Platform session")
    except Exception as e:
        try:
            rd.close_session()
        except Exception:
            pass
        raise Exception(f"Could not connect to Refinitiv Platform with APP_KEY. {e}")


def load_sp500_universe():
    """
    Load S&P 500 ticker universe from pickle file.
    Returns list of ticker strings.
    """
    import pickle
    universe_path = Path("../pyData/Static/sp500_universe.pkl")
    
    if universe_path.exists():
        try:
            with open(universe_path, 'rb') as f:
                tickers = pickle.load(f)
            print(f"✓ Loaded {len(tickers)} tickers from sp500_universe.pkl")
            return tickers
        except Exception as e:
            print(f"⚠️  Could not load sp500_universe.pkl: {e}")
    
    # Fallback: Try to load from AP_CRSPMonthly
    ap_crsp_path = Path("../pyData/Intermediate/AP_monthlyCRSP.parquet")
    if ap_crsp_path.exists():
        try:
            print("Loading tickers from AP_monthlyCRSP.parquet...")
            crsp_df = pd.read_parquet(ap_crsp_path, columns=['ticker'])
            tickers = crsp_df['ticker'].dropna().unique().tolist()
            print(f"✓ Found {len(tickers)} unique tickers from AP_CRSPMonthly")
            return tickers
        except Exception as e:
            print(f"⚠️  Could not load from AP_CRSPMonthly: {e}")
    
    # Final fallback: sample list
    print("⚠️  No universe file found. Using sample tickers.")
    return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NVDA', 'JPM', 'V', 'JNJ']


def convert_tickers_to_rics(tickers):
    """
    Convert ticker symbols to Refinitiv RIC format.
    Uses heuristic mapping based on common exchange patterns.
    """
    print(f"\nConverting {len(tickers)} tickers to RIC format...")
    
    # Common NYSE tickers (financials, consumer staples, etc.)
    nyse_tickers = {
        'JPM', 'V', 'JNJ', 'WMT', 'PG', 'UNH', 'HD', 'DIS', 'BAC', 'MA',
        'XOM', 'CVX', 'KO', 'PEP', 'T', 'VZ', 'MRK', 'ABT', 'TMO', 'DHR'
    }
    
    rics = []
    for ticker in tickers:
        if ticker.upper() in nyse_tickers:
            rics.append(f"{ticker.upper()}.N")
        else:
            rics.append(f"{ticker.upper()}.O")
    
    print(f"✓ Converted to {len(rics)} RICs")
    return rics


def get_active_universe():
    """
    Get a universe of US stocks to download IBES data for.
    Loads from SP500 universe pickle file and converts to RIC format.
    """
    print("\n" + "="*60)
    print("📋 Loading ticker universe...")
    print("="*60)
    
    # Load tickers from SP500 universe
    tickers = load_sp500_universe()
    
    # Limit for debug mode
    if DEBUG_MODE:
        tickers = tickers[:MAX_INSTRUMENTS]
        print(f"DEBUG MODE: Limited to {len(tickers)} tickers")
    
    # Convert tickers to RIC format
    rics = convert_tickers_to_rics(tickers)
    
    print(f"\n📊 Processing {len(rics)} instruments")
    return rics


def get_ibes_unadjusted_data(rics, start_date="2000-01-01", end_date=None):
    """
    Download IBES unadjusted estimates data from Refinitiv.

    Forecast periods:
    - FPI 0: Long-term growth (Period=LTG)
    - FPI 1: Next fiscal year (Period=FY1)
    - FPI 2: Year after next (Period=FY2)
    - FPI 6: Current fiscal quarter (Period=FQ0)
    """
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    print(f"\nDownloading IBES Unadjusted data for {len(rics)} instruments...")
    print(f"Date range: {start_date} to {end_date}")
    print("Forecast periods: Current Quarter (FQ0), Next Year (FY1), Year After (FY2), Long-Term")

    forecast_periods = [
        ("FQ0", "6"),  # Current quarter
        ("FY1", "1"),  # Next fiscal year
        ("FY2", "2"),  # Year after next
        ("LTG", "0"),  # Long-term growth
    ]

    all_data = []

    for period_code, fpi_code in forecast_periods:
        print(f"\n--- Processing Forecast Period: {period_code} (FPI={fpi_code}) ---")

        if period_code == "LTG":
            fields = [
                "TR.EPSLTGMeanEst",
                "TR.EPSLTGMeanEst.date",
                "TR.EPSLTGMedianEst",
                "TR.EPSLTGEstStdDev",
                "TR.EPSLTGNumOfEst",
                "TR.EPSLTGMeanEst.fperiodenddate",
            ]
        else:
            fields = [
                "TR.EPSMeanEst",
                "TR.EPSMeanEst.date",
                "TR.EPSMedianEst",
                "TR.EPSEstStdDev",
                "TR.EPSNumOfEst",
                "TR.EPSMeanEst.fperiodenddate",
            ]

        parameters = {
            "SDate": start_date,
            "EDate": end_date,
            "Frq": "M",
        }

        if period_code != "LTG":
            parameters["Period"] = period_code

        period_data = []

        for i in range(0, len(rics), BATCH_SIZE):
            batch_rics = rics[i : i + BATCH_SIZE]
            print(
                f"  Batch {i//BATCH_SIZE + 1}/{(len(rics)-1)//BATCH_SIZE + 1} ({len(batch_rics)} instruments)...",
                end=" ",
            )

            try:
                df_batch = rd.get_data(universe=batch_rics, fields=fields, parameters=parameters)

                if df_batch is not None and len(df_batch) > 0:
                    df_batch["fpi"] = fpi_code
                    period_data.append(df_batch)
                    print(f"Retrieved {len(df_batch)} records")
                else:
                    print("No data")

            except Exception as e:
                print(f"Error: {e}")
                continue

        if len(period_data) > 0:
            df_period = pd.concat(period_data, ignore_index=True)
            all_data.append(df_period)
            print(f"  Total for {period_code}: {len(df_period)} records")

    if len(all_data) == 0:
        print("\nWarning: No data retrieved!")
        return pd.DataFrame()

    df = pd.concat(all_data, ignore_index=True)
    print(f"\nTotal records downloaded across all periods: {len(df)}")
    return df


def process_ibes_unadjusted_data(df):
    """Process and clean the IBES unadjusted data to match the WRDS script format."""
    print("\nProcessing IBES Unadjusted data...")
    print(f"Columns received: {list(df.columns)}")

    column_mapping = {
        "Instrument": "tickerIBES",
        "Earnings Per Share - Mean Estimate": "meanest",
        "EPS Mean Estimate": "meanest",
        "EPS Median": "medest",
        "EPS Median Estimate": "medest",
        "EPS Standard Deviation": "stdev",
        "EPS Estimate Standard Deviation": "stdev",
        "EPS Number of Estimates": "numest",
        "Date": "statpers",
        "Period End Date": "fpedats",
        # Long-term growth specific fields
        "EPS Long-Term Growth Mean Estimate": "meanest",
        "EPS Long-Term Growth Median Estimate": "medest",
        "EPS Long-Term Growth Estimate Standard Deviation": "stdev",
        "EPS Long-Term Growth Number of Estimates": "numest",
    }

    df = df.rename(columns=column_mapping)

    for col in ["statpers", "fpedats"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    if "statpers" in df.columns:
        df["time_avail_m"] = df["statpers"].dt.to_period("M").dt.to_timestamp()
        df["time_avail_m"] = pd.to_datetime(df["time_avail_m"])

    if "meanest" in df.columns:
        initial_count = len(df)
        df = df.dropna(subset=["meanest"])
        print(f"Removed {initial_count - len(df)} records with missing meanest")
    else:
        print("Warning: 'meanest' column not found")

    if len(df) > 0 and {"tickerIBES", "fpi", "time_avail_m"}.issubset(df.columns):
        df = df.sort_values(["tickerIBES", "fpi", "time_avail_m", "statpers"])
        df = df.drop_duplicates(["tickerIBES", "fpi", "time_avail_m"], keep="last")
        print(f"After keeping last obs per month: {len(df)} records")

    output_cols = [
        "tickerIBES",
        "statpers",
        "fpi",
        "numest",
        "medest",
        "meanest",
        "stdev",
        "fpedats",
        "time_avail_m",
    ]
    available_cols = [col for col in output_cols if col in df.columns]
    return df[available_cols]


def main():
    print("=" * 70)
    print("IBES EPS Unadjusted Data Download from Refinitiv Platform")
    print("=" * 70)

    initialize_refinitiv_session()
    rics = get_active_universe()

    if DEBUG_MODE:
        print(f"\nDEBUG MODE: Limiting to {len(rics)} instruments")

    df_ibes = get_ibes_unadjusted_data(rics)

    if len(df_ibes) == 0:
        print("\nNo data downloaded. Exiting.")
        rd.close_session()
        return

    df_processed = process_ibes_unadjusted_data(df_ibes)

    if len(df_processed) > 0:
        df_processed.to_parquet(OUTPUT_FILE, index=False)
        print("\n" + "=" * 70)
        print(f"IBES EPS Unadjusted data saved to: {OUTPUT_FILE}")
        print(f"Total records: {len(df_processed)}")

        if "time_avail_m" in df_processed.columns:
            print(f"Date range: {df_processed['time_avail_m'].min()} to {df_processed['time_avail_m'].max()}")

        print("\nRecords by FPI (Forecast Period Indicator):")
        if "fpi" in df_processed.columns:
            fpi_counts = df_processed["fpi"].value_counts().sort_index()
            fpi_labels = {
                "0": "Long-term growth",
                "1": "Next fiscal year",
                "2": "Year after next",
                "6": "Current fiscal quarter",
            }
            for fpi, count in fpi_counts.items():
                label = fpi_labels.get(str(fpi), "Unknown")
                print(f"  FPI {fpi} ({label}): {count} records")

        print("\nColumn names:")
        print(list(df_processed.columns))

        print("\nSample data (first 10 rows):")
        print(df_processed.head(10))

        print("\nData summary:")
        print(df_processed.describe())
    else:
        print("\nNo data to save after processing.")

    rd.close_session()
    print("\n" + "=" * 70)
    print("Download complete!")


if __name__ == "__main__":
    main()
