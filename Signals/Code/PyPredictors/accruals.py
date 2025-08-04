"""
Python equivalent of Accruals.do
Generated from: Accruals.do

Original Stata file: Accruals.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def accruals():
    """
    Python equivalent of Accruals.do
    
    Constructs the Accruals predictor signal based on Sloan (1996) methodology.
    """
    logger.info("Constructing predictor signal: Accruals...")
    
    try:
        # DATA LOAD
        # Load Compustat monthly data (equivalent to Stata's "use ... using m_aCompustat")
        data_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading Compustat monthly data from: {data_path}")
        
        if not data_path.exists():
            logger.error(f"Input file not found: {data_path}")
            logger.error("Please run the Compustat data download scripts first")
            return False
        
        # Load the required variables
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'txp', 'act', 'che', 'lct', 'dlc', 'at', 'dp']
        
        data = pd.read_csv(data_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # DATA CLEANING
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        initial_count = len(data)
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        logger.info(f"Removed {initial_count - len(data)} duplicate observations")
        
        # Sort by permno and time_avail_m for lag calculations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Convert time_avail_m to datetime for proper lagging
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Constructing Accruals signal...")
        
        # Create temporary TXP variable (equivalent to Stata's "gen tempTXP = txp; replace tempTXP = 0 if mi(txp)")
        data['tempTXP'] = data['txp'].fillna(0)
        
        # Calculate 12-month lags for all required variables (equivalent to Stata's l12. prefix)
        lag_vars = ['act', 'che', 'lct', 'dlc', 'tempTXP', 'at']
        
        for var in lag_vars:
            data[f'{var}_lag12'] = data.groupby('permno')[var].shift(12)
        
        # Construct Accruals signal according to Sloan (1996) equation 1, page 6
        # Accruals = ((act - act_lag12) - (che - che_lag12) - 
        #            ((lct - lct_lag12) - (dlc - dlc_lag12) - (tempTXP - tempTXP_lag12)) - dp) / 
        #            ((at + at_lag12)/2)
        
        # Calculate numerator components
        change_act = data['act'] - data['act_lag12']
        change_che = data['che'] - data['che_lag12']
        change_lct = data['lct'] - data['lct_lag12']
        change_dlc = data['dlc'] - data['dlc_lag12']
        change_tempTXP = data['tempTXP'] - data['tempTXP_lag12']
        
        # Calculate numerator: (change_act - change_che - (change_lct - change_dlc - change_tempTXP) - dp)
        numerator = (change_act - change_che - (change_lct - change_dlc - change_tempTXP) - data['dp'])
        
        # Calculate denominator: (at + at_lag12) / 2
        denominator = (data['at'] + data['at_lag12']) / 2
        
        # Calculate Accruals
        data['Accruals'] = numerator / denominator
        
        logger.info("Successfully calculated Accruals signal")
        
        # SAVE RESULTS
        logger.info("Saving Accruals predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'Accruals']].copy()
        
        # Remove missing values (equivalent to Stata's "drop if Accruals == .")
        output_data = output_data.dropna(subset=['Accruals'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output (equivalent to Stata's "gen yyyymm = year(dofm(time_avail_m))*100 + month(dofm(time_avail_m))")
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file (main output)
        csv_output_path = predictors_dir / "Accruals.csv"
        csv_data = output_data[['permno', 'yyyymm', 'Accruals']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved Accruals predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed Accruals predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct Accruals predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    accruals()
