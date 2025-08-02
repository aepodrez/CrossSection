"""
Python equivalent of ZC_GovernanceIndex.do
Generated from: ZC_GovernanceIndex.do

Original Stata file: ZC_GovernanceIndex.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime
import requests
import tempfile
import os

logger = logging.getLogger(__name__)

def zc_governanceindex():
    """
    Python equivalent of ZC_GovernanceIndex.do
    
    Downloads and processes governance index data from S3-hosted Excel file
    """
    logger.info("Downloading governance index data from S3...")
    
    try:
        # URL from original Stata file
        url = "https://spinup-000d1a-wp-offload-media.s3.amazonaws.com/faculty/wp-content/uploads/sites/7/2019/06/Governance.xlsx"
        
        logger.info(f"Downloading governance index data from: {url}")
        
        # Download the Excel file
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()  # Raise an exception for bad status codes
            
            # Create a temporary file to store the downloaded data
            with tempfile.NamedTemporaryFile(mode='wb', suffix='.xlsx', delete=False) as temp_file:
                temp_file.write(response.content)
                temp_file_path = temp_file.name
            
            # Read the Excel file with specific sheet and range (equivalent to Stata's cellrange)
            data = pd.read_excel(
                temp_file_path, 
                sheet_name="governance index", 
                skiprows=23,  # Skip first 23 rows to start from row 24
                nrows=14000   # Read up to 14000 rows (A24:F14024)
            )
            
            # Clean up temporary file
            os.unlink(temp_file_path)
            
            logger.info(f"Successfully downloaded {len(data)} governance index records")
            
        except requests.RequestException as e:
            logger.error(f"Failed to download governance index data: {e}")
            return False
        
        # Clean ticker column (equivalent to Stata's "replace ticker = strtrim(ticker)")
        if 'ticker' in data.columns:
            data['ticker'] = data['ticker'].astype(str).str.strip()
            logger.info("Cleaned ticker column")
        
        # Keep only first observation per ticker-year (equivalent to Stata's "bys ticker year: keep if _n == 1")
        if 'ticker' in data.columns and 'year' in data.columns:
            data = data.drop_duplicates(subset=['ticker', 'year'], keep='first')
            logger.info(f"After keeping first observation per ticker-year: {len(data)} records")
        
        # Replace year 2000 with 1999 (equivalent to Stata's "replace year = 1999 if year == 2000")
        if 'year' in data.columns:
            data.loc[data['year'] == 2000, 'year'] = 1999
            logger.info("Replaced year 2000 with 1999")
        
        # Generate month based on year (equivalent to Stata's month assignment)
        data['month'] = np.nan
        
        if 'year' in data.columns:
            # Apply month assignments based on year
            data.loc[data['year'] == 1990, 'month'] = 9
            data.loc[data['year'] == 1993, 'month'] = 7
            data.loc[data['year'] == 1995, 'month'] = 7
            data.loc[data['year'] == 1998, 'month'] = 2
            data.loc[data['year'] == 1999, 'month'] = 11
            data.loc[data['year'] >= 2002, 'month'] = 1
            
            logger.info("Assigned months based on year")
        
        # Create time_avail_m (equivalent to Stata's "gen time_avail_m = ym(year, month)")
        data['time_avail_m'] = pd.to_datetime(data[['year', 'month']].assign(day=1)).dt.to_period('M')
        
        # Interpolate missing dates and extend one year beyond end of data
        logger.info("Interpolating missing dates and extending data...")
        
        # Group by ticker and process each group
        processed_data = []
        
        for ticker in data['ticker'].unique():
            ticker_data = data[data['ticker'] == ticker].copy()
            ticker_data = ticker_data.sort_values('time_avail_m')
            
            # Extend one year beyond end of data (equivalent to Stata's expand logic)
            last_row = ticker_data.iloc[-1].copy()
            last_row['time_avail_m'] = pd.Period(year=2007, month=1, freq='M')
            ticker_data = pd.concat([ticker_data, pd.DataFrame([last_row])], ignore_index=True)
            
            # Create complete time series for this ticker
            min_date = ticker_data['time_avail_m'].min()
            max_date = ticker_data['time_avail_m'].max()
            
            # Generate complete monthly time series
            complete_dates = pd.date_range(
                start=min_date.to_timestamp(),
                end=max_date.to_timestamp(),
                freq='M'
            )
            complete_dates = [pd.Period(date, freq='M') for date in complete_dates]
            
            # Create complete time series dataframe
            complete_series = pd.DataFrame({
                'ticker': ticker,
                'time_avail_m': complete_dates
            })
            
            # Merge with original data to get G values
            complete_series = complete_series.merge(
                ticker_data[['time_avail_m', 'G']], 
                on='time_avail_m', 
                how='left'
            )
            
            # Forward fill missing values (equivalent to Stata's forward fill)
            complete_series = complete_series.sort_values('time_avail_m')
            complete_series['G'] = complete_series['G'].fillna(method='ffill')
            complete_series['ticker'] = complete_series['ticker'].fillna(method='ffill')
            
            processed_data.append(complete_series)
        
        # Combine all processed data
        data = pd.concat(processed_data, ignore_index=True)
        logger.info(f"After interpolation and extension: {len(data)} records")
        
        # Keep only essential columns
        data = data[['ticker', 'time_avail_m', 'G']].copy()
        
        # Save to intermediate file
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/GovIndex.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved governance index data to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/GovIndex.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log some summary statistics
        logger.info("Governance index summary:")
        logger.info(f"  Total records: {len(data)}")
        
        # Time range analysis
        if 'time_avail_m' in data.columns:
            min_date = data['time_avail_m'].min()
            max_date = data['time_avail_m'].max()
            logger.info(f"  Time range: {min_date} to {max_date}")
        
        # Company analysis
        if 'ticker' in data.columns:
            unique_companies = data['ticker'].nunique()
            logger.info(f"  Unique companies: {unique_companies}")
            
            # Companies with most observations
            company_counts = data['ticker'].value_counts().head(10)
            logger.info(f"  Companies with most observations:")
            for ticker, count in company_counts.items():
                logger.info(f"    {ticker}: {count} observations")
        
        # Governance index analysis
        if 'G' in data.columns:
            g_data = data['G'].dropna()
            if len(g_data) > 0:
                logger.info("  Governance index analysis:")
                logger.info(f"    Non-missing observations: {len(g_data)}")
                logger.info(f"    Missing observations: {data['G'].isna().sum()}")
                
                # Governance index statistics
                mean_val = g_data.mean()
                std_val = g_data.std()
                min_val = g_data.min()
                max_val = g_data.max()
                logger.info(f"    Mean: {mean_val:.4f}")
                logger.info(f"    Std: {std_val:.4f}")
                logger.info(f"    Range: [{min_val:.0f}, {max_val:.0f}]")
                
                # Governance index distribution analysis
                logger.info("    Governance index distribution:")
                
                # Count unique values
                unique_values = g_data.nunique()
                logger.info(f"      Unique governance index values: {unique_values}")
                
                # Show distribution of values
                value_counts = g_data.value_counts().sort_index()
                logger.info("      Governance index value counts:")
                for value, count in value_counts.head(10).items():
                    percentage = count / len(g_data) * 100
                    logger.info(f"        {value:.0f}: {count} ({percentage:.1f}%)")
                
                # Governance quality analysis
                logger.info("    Governance quality analysis:")
                low_governance = (g_data <= 5).sum()
                medium_governance = ((g_data > 5) & (g_data <= 10)).sum()
                high_governance = (g_data > 10).sum()
                
                total_obs = len(g_data)
                logger.info(f"      Low governance (≤5): {low_governance} ({low_governance/total_obs*100:.1f}%)")
                logger.info(f"      Medium governance (6-10): {medium_governance} ({medium_governance/total_obs*100:.1f}%)")
                logger.info(f"      High governance (>10): {high_governance} ({high_governance/total_obs*100:.1f}%)")
        
        # Monthly frequency analysis
        logger.info("  Monthly frequency analysis:")
        logger.info(f"    Total monthly observations: {len(data)}")
        
        # Check for any gaps in monthly data
        if 'time_avail_m' in data.columns:
            expected_months = pd.date_range(
                start=data['time_avail_m'].min().to_timestamp(),
                end=data['time_avail_m'].max().to_timestamp(),
                freq='M'
            )
            actual_months = data['time_avail_m'].dt.to_timestamp().sort_values()
            missing_months = set(expected_months) - set(actual_months)
            
            if missing_months:
                logger.warning(f"    Missing months: {len(missing_months)}")
                logger.warning(f"    First few missing months: {sorted(list(missing_months))[:5]}")
            else:
                logger.info("    No missing months detected")
        
        # Data processing explanation
        logger.info("  Data processing explanation:")
        logger.info("    - Original data: Governance index from S3-hosted Excel file")
        logger.info("    - Sheet: 'governance index' with specific cell range")
        logger.info("    - Time assignment: Months assigned based on year")
        logger.info("    - Interpolation: Missing dates filled with forward fill")
        logger.info("    - Extension: Data extended one year beyond original end")
        logger.info("    - Usage: Corporate governance analysis and research")
        
        # Governance index interpretation
        logger.info("  Governance index interpretation:")
        logger.info("    Governance Index (G-Index):")
        logger.info("    - Measures corporate governance quality")
        logger.info("    - Higher values = More anti-takeover provisions (worse governance)")
        logger.info("    - Lower values = Fewer anti-takeover provisions (better governance)")
        logger.info("    - Based on Gompers, Ishii, and Metrick (2003)")
        logger.info("    - Used in corporate governance and firm performance studies")
        
        logger.info("Successfully downloaded and processed governance index data")
        logger.info("Note: Monthly governance index data for corporate governance analysis")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download governance index data: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the download function
    zc_governanceindex()
