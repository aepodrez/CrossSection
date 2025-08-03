"""
Python equivalent of F_CompustatCustomerSegments.do
Generated from: F_CompustatCustomerSegments.do

Original Stata file: F_CompustatCustomerSegments.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def f_compustatcustomersegments(wrds_conn=None):
    """
    Python equivalent of F_CompustatCustomerSegments.do
    
    Downloads and processes Compustat customer segments data from WRDS
    """
    logger.info("Downloading Compustat customer segments data...")
    
    try:
        # Check if WRDS connection is provided
        if wrds_conn is None:
            logger.error("WRDS connection not available. Please run master.py")
            return False
        conn = wrds_conn
        
        # SQL query from original Stata file
        query = """
        SELECT a.*
        FROM compseg.wrds_seg_customer as a
        """
        
        # Execute query
        data = conn.raw_sql(query)
        logger.info(f"Downloaded {len(data)} Compustat customer segments records")
        
        # Rename srcdate to datadate (equivalent to Stata rename)
        if 'srcdate' in data.columns:
            data = data.rename(columns={'srcdate': 'datadate'})
            logger.info("Renamed srcdate to datadate")
        
        # Save to intermediate file (CSV format)
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/CompustatSegmentDataCustomers.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved customer segments data to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/CompustatSegmentDataCustomers.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log some summary statistics
        logger.info("Customer segments data summary:")
        logger.info(f"  Total records: {len(data)}")
        if 'gvkey' in data.columns:
            logger.info(f"  Unique firms (gvkey): {data['gvkey'].nunique()}")
        if 'datadate' in data.columns:
            logger.info(f"  Date range: {data['datadate'].min()} to {data['datadate'].max()}")
        
        # Show column names for reference
        logger.info(f"  Available columns: {list(data.columns)}")
        
        logger.info("Successfully downloaded and processed Compustat customer segments data")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download Compustat customer segments data: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the download function
    f_compustatcustomersegments()
