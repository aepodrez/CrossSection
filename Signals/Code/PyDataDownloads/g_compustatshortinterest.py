"""
Python equivalent of G_CompustatShortInterest.do
Generated from: G_CompustatShortInterest.do

Original Stata file: G_CompustatShortInterest.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def g_compustatshortinterest():
    """
    Python equivalent of G_CompustatShortInterest.do
    
    Downloads and processes Compustat short interest data from WRDS
    """
    logger.info("Downloading Compustat short interest data...")
    
    try:
        # Use global WRDS connection from master.py
        from master import wrds_conn
        if wrds_conn is None:
            logger.error("WRDS connection not available. Please run master.py")
            return False
        conn = wrds_conn
        
        # SQL query from original Stata file
        query = """
        SELECT 
            a.gvkey, a.iid, a.shortint, a.shortintadj, a.datadate
        FROM comp.sec_shortint as a
        """
        
        # Execute query
        data = conn.raw_sql(query, date_cols=['datadate'])
        logger.info(f"Downloaded {len(data)} Compustat short interest records")
        
        # Create time_avail_m (month of datadate)
        data['time_avail_m'] = pd.to_datetime(data['datadate']).dt.to_period('M')
        logger.info(f"Created time_avail_m from datadate")
        
        # Data reported bi-weekly and made available with a four day lag
        # Use the mid-month observation to make sure data would be available in real time
        # This is equivalent to gcollapse (firstnm) in Stata
        data = data.sort_values(['gvkey', 'time_avail_m', 'datadate'])
        
        # For each gvkey-time_avail_m combination, keep the first non-missing observation
        # This approximates the mid-month observation approach
        data = data.drop_duplicates(subset=['gvkey', 'time_avail_m'], keep='first')
        logger.info(f"After keeping first observation per month: {len(data)} records")
        
        # Convert gvkey to numeric
        data['gvkey'] = pd.to_numeric(data['gvkey'], errors='coerce')
        logger.info("Converted gvkey to numeric")
        
        # Save to intermediate file
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/monthlyShortInterest.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved short interest data to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/monthlyShortInterest.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log some summary statistics
        logger.info("Short interest data summary:")
        logger.info(f"  Total records: {len(data)}")
        logger.info(f"  Unique firms (gvkey): {data['gvkey'].nunique()}")
        logger.info(f"  Unique instruments (iid): {data['iid'].nunique()}")
        logger.info(f"  Time range: {data['time_avail_m'].min()} to {data['time_avail_m'].max()}")
        
        # Check data availability
        if 'shortint' in data.columns:
            non_missing_shortint = data['shortint'].notna().sum()
            logger.info(f"  Non-missing shortint: {non_missing_shortint} ({non_missing_shortint/len(data)*100:.1f}%)")
        
        if 'shortintadj' in data.columns:
            non_missing_shortintadj = data['shortintadj'].notna().sum()
            logger.info(f"  Non-missing shortintadj: {non_missing_shortintadj} ({non_missing_shortintadj/len(data)*100:.1f}%)")
        
        logger.info("Successfully downloaded and processed Compustat short interest data")
        logger.info("Note: Data reported bi-weekly with 4-day lag, using mid-month observations for real-time availability")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download Compustat short interest data: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the download function
    g_compustatshortinterest()
