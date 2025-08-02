"""
Python equivalent of H_CRSPDistributions.do
Generated from: H_CRSPDistributions.do

Original Stata file: H_CRSPDistributions.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def h_crspdistributions():
    """
    Python equivalent of H_CRSPDistributions.do
    
    Downloads and processes CRSP distributions data from WRDS
    """
    logger.info("Downloading CRSP distributions data...")
    
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
            d.permno, d.divamt, d.distcd, d.facshr, d.rcrddt, d.exdt, d.paydt
        FROM crsp.msedist as d
        """
        
        # Execute query
        data = conn.raw_sql(query, date_cols=['rcrddt', 'exdt', 'paydt'])
        logger.info(f"Downloaded {len(data)} CRSP distributions records")
        
        # Remove duplicates (seems like these are data errors, e.g. see permno 93338 or 93223)
        # Keep first observation for each permno-distcd-paydt combination
        data = data.sort_values(['permno', 'distcd', 'paydt'])
        data = data.drop_duplicates(subset=['permno', 'distcd', 'paydt'], keep='first')
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # For convenience, extract components of distribution code
        # Convert distcd to string first
        data['distcd'] = data['distcd'].astype(str)
        
        # Extract individual digits from distribution code
        data['cd1'] = data['distcd'].str[0].astype(float)
        data['cd2'] = data['distcd'].str[1].astype(float)
        data['cd3'] = data['distcd'].str[2].astype(float)
        data['cd4'] = data['distcd'].str[3].astype(float)
        
        logger.info("Extracted distribution code components (cd1, cd2, cd3, cd4)")
        
        # Save to intermediate file
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/CRSPdistributions.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved distributions data to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/CRSPdistributions.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log some summary statistics
        logger.info("CRSP distributions data summary:")
        logger.info(f"  Total records: {len(data)}")
        logger.info(f"  Unique firms (permno): {data['permno'].nunique()}")
        logger.info(f"  Unique distribution codes: {data['distcd'].nunique()}")
        logger.info(f"  Date range: {data['paydt'].min()} to {data['paydt'].max()}")
        
        # Distribution code summary
        if 'distcd' in data.columns:
            distcd_counts = data['distcd'].value_counts().head(10)
            logger.info("  Top 10 distribution codes:")
            for code, count in distcd_counts.items():
                logger.info(f"    {code}: {count} records")
        
        # Check data availability
        if 'divamt' in data.columns:
            non_missing_divamt = data['divamt'].notna().sum()
            logger.info(f"  Non-missing dividend amounts: {non_missing_divamt} ({non_missing_divamt/len(data)*100:.1f}%)")
        
        logger.info("Successfully downloaded and processed CRSP distributions data")
        logger.info("Note: Distribution codes extracted into cd1, cd2, cd3, cd4 for analysis")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download CRSP distributions data: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the download function
    h_crspdistributions()
