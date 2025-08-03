"""
Python equivalent of NetEquityFinance.do
Generated from: NetEquityFinance.do

Original Stata file: NetEquityFinance.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def netequityfinance():
    """
    Python equivalent of NetEquityFinance.do
    
    Constructs the NetEquityFinance predictor signal for net equity financing.
    """
    logger.info("Constructing predictor signal: NetEquityFinance...")
    
    try:
        # DATA LOAD
        # Load Compustat data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading Compustat data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"m_aCompustat not found: {compustat_path}")
            logger.error("Please run the Compustat data creation script first")
            return False
        
        # Load required variables
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'sstk', 'prstkc', 'at', 'dv']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating NetEquityFinance signal...")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'])
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate 12-month lag of total assets (equivalent to Stata's "l12.at")
        data['at_lag12'] = data.groupby('permno')['at'].shift(12)
        
        # Calculate NetEquityFinance (equivalent to Stata's "gen NetEquityFinance = (sstk - prstkc - dv)/(.5*(at + l12.at))")
        data['NetEquityFinance'] = (data['sstk'] - data['prstkc'] - data['dv']) / (0.5 * (data['at'] + data['at_lag12']))
        
        # Replace extreme values with missing (equivalent to Stata's "replace NetEquityFinance = . if abs(NetEquityFinance) > 1")
        data.loc[abs(data['NetEquityFinance']) > 1, 'NetEquityFinance'] = np.nan
        
        logger.info("Successfully calculated NetEquityFinance signal")
        
        # SAVE RESULTS
        logger.info("Saving NetEquityFinance predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'NetEquityFinance']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['NetEquityFinance'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "NetEquityFinance.csv"
        csv_data = output_data[['permno', 'yyyymm', 'NetEquityFinance']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved NetEquityFinance predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed NetEquityFinance predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct NetEquityFinance predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    netequityfinance()
