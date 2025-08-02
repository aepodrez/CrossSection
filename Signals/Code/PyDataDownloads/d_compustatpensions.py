"""
Python equivalent of D_CompustatPensions.do
Generated from: D_CompustatPensions.do

Original Stata file: D_CompustatPensions.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def d_compustatpensions():
    """
    Python equivalent of D_CompustatPensions.do
    
    Downloads and processes Compustat pension data from WRDS
    """
    logger.info("Downloading Compustat pension data...")
    
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
            a.gvkey, a.datadate, a.paddml, a.pbnaa, a.pbnvv, a.pbpro, 
            a.pbpru, a.pcupsu, a.pplao, a.pplau
        FROM COMP.ACO_PNFNDA as a
        WHERE a.consol = 'C'
        AND a.popsrc = 'D'
        AND a.datafmt = 'STD'
        AND a.indfmt = 'INDL'
        """
        
        # Execute query
        data = conn.raw_sql(query, date_cols=['datadate'])
        logger.info(f"Downloaded {len(data)} Compustat pension records")
        
        # Create year variable and assume data available with a lag of one year
        data['year'] = pd.to_datetime(data['datadate']).dt.year
        data['year'] = data['year'] + 1  # Assume data available with a lag of one year
        
        # Keep only the first observation for each gvkey-year combination
        data = data.sort_values(['gvkey', 'year', 'datadate'])
        data = data.drop_duplicates(subset=['gvkey', 'year'], keep='first')
        logger.info(f"After keeping first per year: {len(data)} records")
        
        # Drop datadate column
        data = data.drop('datadate', axis=1)
        
        # Check missing data (similar to mdesc in Stata)
        missing_summary = data.isnull().sum()
        total_records = len(data)
        missing_percentage = (missing_summary / total_records) * 100
        
        logger.info("Missing data summary:")
        for col in data.columns:
            if col != 'gvkey' and col != 'year':
                missing_count = missing_summary[col]
                missing_pct = missing_percentage[col]
                logger.info(f"  {col}: {missing_count} missing ({missing_pct:.1f}%)")
        
        # Convert gvkey to numeric
        data['gvkey'] = pd.to_numeric(data['gvkey'], errors='coerce')
        
        # Save to intermediate file
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/CompustatPensions.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved pension data to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/CompustatPensions.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        logger.info("Successfully downloaded and processed Compustat pension data")
        logger.info(f"Note: Missing data for about 80% of firm-years in all but two variables (as expected)")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download Compustat pension data: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the download function
    d_compustatpensions()
