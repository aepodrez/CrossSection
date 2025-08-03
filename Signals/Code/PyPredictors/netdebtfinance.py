"""
Python equivalent of NetDebtFinance.do
Generated from: NetDebtFinance.do

Original Stata file: NetDebtFinance.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def netdebtfinance():
    """
    Python equivalent of NetDebtFinance.do
    
    Constructs the NetDebtFinance predictor signal for net debt financing.
    """
    logger.info("Constructing predictor signal: NetDebtFinance...")
    
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
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'dlcch', 'dltis', 'dltr', 'at']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating NetDebtFinance signal...")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'])
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Replace missing dlcch with 0 (equivalent to Stata's "replace dlcch = 0 if mi(dlcch)")
        data['dlcch'] = data['dlcch'].fillna(0)
        
        # Calculate 12-month lag of total assets (equivalent to Stata's "l12.at")
        data['at_lag12'] = data.groupby('permno')['at'].shift(12)
        
        # Calculate NetDebtFinance (equivalent to Stata's "gen NetDebtFinance = (dltis - dltr + dlcch)/(.5*(at + l12.at))")
        data['NetDebtFinance'] = (data['dltis'] - data['dltr'] + data['dlcch']) / (0.5 * (data['at'] + data['at_lag12']))
        
        # Replace extreme values with missing (equivalent to Stata's "replace NetDebtFinance = . if abs(NetDebtFinance) > 1")
        data.loc[abs(data['NetDebtFinance']) > 1, 'NetDebtFinance'] = np.nan
        
        logger.info("Successfully calculated NetDebtFinance signal")
        
        # SAVE RESULTS
        logger.info("Saving NetDebtFinance predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'NetDebtFinance']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['NetDebtFinance'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "NetDebtFinance.csv"
        csv_data = output_data[['permno', 'yyyymm', 'NetDebtFinance']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved NetDebtFinance predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed NetDebtFinance predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct NetDebtFinance predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    netdebtfinance()
