"""
Python equivalent of L_IBES_EPS_Unadj.do
Generated from: L_IBES_EPS_Unadj.do

Original Stata file: L_IBES_EPS_Unadj.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def l_ibes_eps_unadj(wrds_conn=None):
    """
    Python equivalent of L_IBES_EPS_Unadj.do
    
    Downloads and processes IBES EPS unadjusted data from WRDS
    """
    logger.info("Downloading IBES EPS unadjusted data...")
    
    try:
        # Check if WRDS connection is provided
        if wrds_conn is None:
            logger.error("WRDS connection not available. Please run master.py")
            return False
        conn = wrds_conn
        
        # SQL query from original Stata file
        query = """
        SELECT 
            a.ticker, a.statpers, a.measure, a.fpi, a.numest, a.medest,
            a.meanest, a.stdev, a.fpedats
        FROM ibes.statsumu_epsus as a
        WHERE a.fpi = '0' OR a.fpi = '1' OR a.fpi = '2' OR a.fpi = '6'
        """
        
        # Execute query
        data = conn.raw_sql(query, date_cols=['statpers', 'fpedats'])
        logger.info(f"Downloaded {len(data)} IBES EPS unadjusted records")
        
        # Set up linking variables
        data['time_avail_m'] = pd.to_datetime(data['statpers']).dt.to_period('M')
        
        # Rename ticker to tickerIBES
        data = data.rename(columns={'ticker': 'tickerIBES'})
        
        # Drop measure column
        data = data.drop('measure', axis=1)
        
        # Keep last observation each month (drop if meanest is missing for sanity)
        data = data.dropna(subset=['meanest'])
        logger.info(f"After dropping missing meanest: {len(data)} records")
        
        # Sort and keep last observation per ticker-fpi-month
        data = data.sort_values(['tickerIBES', 'fpi', 'time_avail_m', 'statpers'])
        data = data.drop_duplicates(subset=['tickerIBES', 'fpi', 'time_avail_m'], keep='last')
        logger.info(f"After keeping last observation per month: {len(data)} records")
        
        # Save to intermediate file
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/IBES_EPS_Unadj.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved IBES EPS unadjusted data to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/IBES_EPS_Unadj.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log some summary statistics
        logger.info("IBES EPS unadjusted data summary:")
        logger.info(f"  Total records: {len(data)}")
        logger.info(f"  Unique tickers: {data['tickerIBES'].nunique()}")
        logger.info(f"  Time range: {data['time_avail_m'].min()} to {data['time_avail_m'].max()}")
        
        # FPI distribution
        if 'fpi' in data.columns:
            fpi_counts = data['fpi'].value_counts()
            logger.info("  FPI distribution:")
            for fpi, count in fpi_counts.items():
                logger.info(f"    FPI {fpi}: {count} records")
        
        # Check data availability
        if 'meanest' in data.columns:
            non_missing_meanest = data['meanest'].notna().sum()
            logger.info(f"  Non-missing mean estimates: {non_missing_meanest} ({non_missing_meanest/len(data)*100:.1f}%)")
        
        if 'numest' in data.columns:
            non_missing_numest = data['numest'].notna().sum()
            logger.info(f"  Non-missing number of estimates: {non_missing_numest} ({non_missing_numest/len(data)*100:.1f}%)")
        
        logger.info("Successfully downloaded and processed IBES EPS unadjusted data")
        logger.info("Note: Unadjusted for splits, FPI=0 (LTG), 1 (1yr), 2 (2yr), 6 (current quarter)")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download IBES EPS unadjusted data: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the download function
    l_ibes_eps_unadj()
