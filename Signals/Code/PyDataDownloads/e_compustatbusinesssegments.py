"""
Python equivalent of E_CompustatBusinessSegments.do
Generated from: E_CompustatBusinessSegments.do

Original Stata file: E_CompustatBusinessSegments.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def e_compustatbusinesssegments(wrds_conn=None):
    """
    Python equivalent of E_CompustatBusinessSegments.do
    
    Downloads and processes Compustat business segments data from WRDS
    """
    logger.info("Downloading Compustat business segments data...")
    
    try:
        # Check if WRDS connection is provided
        if wrds_conn is None:
            logger.error("WRDS connection not available. Please run master.py")
            return False
        conn = wrds_conn
        
        # SQL query from original Stata file
        query = """
        SELECT 
            a.gvkey, a.datadate, a.stype, a.sid, a.sales, a.srcdate, a.naicsh, a.sics1, a.snms
        FROM compseg.wrds_segmerged as a
        """
        
        # Execute query
        data = conn.raw_sql(query, date_cols=['datadate', 'srcdate'])
        logger.info(f"Downloaded {len(data)} Compustat business segments records")
        
        # Convert string columns to numeric (equivalent to destring in Stata)
        numeric_columns = ['gvkey', 'sics1', 'naicsh']
        for col in numeric_columns:
            if col in data.columns:
                data[col] = pd.to_numeric(data[col], errors='coerce')
                logger.info(f"Converted {col} to numeric")
        
        # Save to intermediate file
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/CompustatSegments.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved business segments data to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/CompustatSegments.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log some summary statistics
        logger.info("Business segments data summary:")
        logger.info(f"  Total records: {len(data)}")
        logger.info(f"  Unique firms (gvkey): {data['gvkey'].nunique()}")
        logger.info(f"  Unique segments (sid): {data['sid'].nunique()}")
        if 'stype' in data.columns:
            logger.info(f"  Segment types: {data['stype'].value_counts().to_dict()}")
        
        logger.info("Successfully downloaded and processed Compustat business segments data")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download Compustat business segments data: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the download function
    e_compustatbusinesssegments()
