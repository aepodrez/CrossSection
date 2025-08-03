"""
Python equivalent of XFIN.do
Generated from: XFIN.do

Original Stata file: XFIN.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def xfin():
    """
    Python equivalent of XFIN.do
    
    Constructs the XFIN predictor signal for net external financing.
    """
    logger.info("Constructing predictor signal: XFIN...")
    
    try:
        # DATA LOAD
        # Load annual Compustat data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading annual Compustat data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"m_aCompustat not found: {compustat_path}")
            logger.error("Please run the annual Compustat data creation script first")
            return False
        
        # Load required variables
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'sstk', 'dv', 'prstkc', 'dltis', 'dltr', 'dlcch', 'at']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating XFIN signal...")
        
        # Replace missing dlcch with 0 (equivalent to Stata's "replace dlcch = 0 if mi(dlcch)")
        data['dlcch'] = data['dlcch'].fillna(0)
        
        # Calculate net external financing (equivalent to Stata's "gen XFIN = (sstk - dv - prstkc + dltis - dltr + dlcch)/at")
        data['XFIN'] = (data['sstk'] - data['dv'] - data['prstkc'] + data['dltis'] - data['dltr'] + data['dlcch']) / data['at']
        
        logger.info("Successfully calculated XFIN signal")
        
        # SAVE RESULTS
        logger.info("Saving XFIN predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'XFIN']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['XFIN'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "XFIN.csv"
        csv_data = output_data[['permno', 'yyyymm', 'XFIN']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved XFIN predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed XFIN predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct XFIN predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    xfin()
