"""
Python equivalent of K_CRSPAcquisitions.do
Generated from: K_CRSPAcquisitions.do

Original Stata file: K_CRSPAcquisitions.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def k_crspacquisitions(wrds_conn=None):
    """
    Python equivalent of K_CRSPAcquisitions.do
    
    Downloads and processes CRSP acquisitions/spinoffs data from WRDS
    """
    logger.info("Downloading CRSP acquisitions/spinoffs data...")
    
    try:
        # Check if WRDS connection is provided
        if wrds_conn is None:
            logger.error("WRDS connection not available. Please run master.py")
            return False
        conn = wrds_conn
        
        # SQL query from original Stata file
        query = """
        SELECT 
            a.permno, a.distcd, a.exdt, a.acperm
        FROM crsp.msedist as a
        """
        
        # Execute query
        data = conn.raw_sql(query, date_cols=['exdt'])
        logger.info(f"Downloaded {len(data)} CRSP acquisitions records")
        
        # Keep only records where acperm > 999 and not missing
        data = data[(data['acperm'] > 999) & (data['acperm'].notna())]
        logger.info(f"After filtering acperm > 999: {len(data)} records")
        
        # Rename exdt to time_d
        data = data.rename(columns={'exdt': 'time_d'})
        
        # Drop records with missing time_d
        data = data.dropna(subset=['time_d'])
        logger.info(f"After dropping missing time_d: {len(data)} records")
        
        # Create time_avail_m (month of time_d)
        data['time_avail_m'] = pd.to_datetime(data['time_d']).dt.to_period('M')
        
        # Drop time_d column
        data = data.drop('time_d', axis=1)
        
        # According to CRSP documentation:
        # distcd identifies true spinoffs using keep if distcd >= 3762 & distcd <= 3764
        # But MP don't use it, and it results in a large share of months with no spinoffs.
        # So we follow the original approach and don't filter by distcd
        
        # Turn into list of permnos which were created in spinoffs
        data['SpinoffCo'] = 1
        
        # Drop original permno and rename acperm to permno
        data = data.drop('permno', axis=1)
        data = data.rename(columns={'acperm': 'permno'})
        
        # Keep only permno and SpinoffCo columns
        data = data[['permno', 'SpinoffCo']]
        
        # Remove spinoffs which had multi-stock parents (duplicates)
        data = data.drop_duplicates()
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # Save to intermediate file
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_CRSPAcquisitions.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved acquisitions data to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/m_CRSPAcquisitions.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log some summary statistics
        logger.info("CRSP acquisitions/spinoffs data summary:")
        logger.info(f"  Total spinoff companies: {len(data)}")
        logger.info(f"  Unique spinoff permnos: {data['permno'].nunique()}")
        
        # Show some example spinoff permnos
        if len(data) > 0:
            sample_permnos = data['permno'].head(10).tolist()
            logger.info(f"  Sample spinoff permnos: {sample_permnos}")
        
        logger.info("Successfully downloaded and processed CRSP acquisitions/spinoffs data")
        logger.info("Note: Focuses on spinoff companies (acperm > 999), not distribution codes")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download CRSP acquisitions data: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the download function
    k_crspacquisitions()
