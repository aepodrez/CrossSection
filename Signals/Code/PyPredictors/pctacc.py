"""
Python equivalent of PctAcc.do
Generated from: PctAcc.do

Original Stata file: PctAcc.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def pctacc():
    """
    Python equivalent of PctAcc.do
    
    Constructs the PctAcc predictor signal for percent accruals.
    """
    logger.info("Constructing predictor signal: PctAcc...")
    
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
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'ib', 'oancf', 'dp', 'act', 'che', 'lct', 'txp', 'dlc']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating PctAcc signal...")
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate 12-month lags (equivalent to Stata's "l12." variables)
        data['ib_lag12'] = data.groupby('permno')['ib'].shift(12)
        data['act_lag12'] = data.groupby('permno')['act'].shift(12)
        data['che_lag12'] = data.groupby('permno')['che'].shift(12)
        data['lct_lag12'] = data.groupby('permno')['lct'].shift(12)
        data['dlc_lag12'] = data.groupby('permno')['dlc'].shift(12)
        data['txp_lag12'] = data.groupby('permno')['txp'].shift(12)
        
        # Calculate PctAcc (equivalent to Stata's complex formula)
        # Base case: PctAcc = (ib - oancf) / abs(ib)
        data['PctAcc'] = (data['ib'] - data['oancf']) / np.abs(data['ib'])
        
        # Handle case where ib == 0 (equivalent to Stata's "replace PctAcc = (ib - oancf)/.01 if ib == 0")
        data.loc[data['ib'] == 0, 'PctAcc'] = (data.loc[data['ib'] == 0, 'ib'] - data.loc[data['ib'] == 0, 'oancf']) / 0.01
        
        # Handle case where oancf is missing (equivalent to Stata's complex replacement)
        # Calculate working capital accruals when oancf is missing
        missing_oancf_mask = data['oancf'].isna()
        
        # Calculate working capital accruals: (act - act_lag12) - (che - che_lag12) - ((lct - lct_lag12) - (dlc - dlc_lag12) - (txp - txp_lag12) - dp)
        wc_accruals = ((data['act'] - data['act_lag12']) - 
                       (data['che'] - data['che_lag12']) - 
                       ((data['lct'] - data['lct_lag12']) - 
                        (data['dlc'] - data['dlc_lag12']) - 
                        (data['txp'] - data['txp_lag12']) - 
                        data['dp']))
        
        # Replace PctAcc when oancf is missing and ib != 0
        data.loc[missing_oancf_mask & (data['ib'] != 0), 'PctAcc'] = wc_accruals.loc[missing_oancf_mask & (data['ib'] != 0)] / np.abs(data.loc[missing_oancf_mask & (data['ib'] != 0), 'ib'])
        
        # Replace PctAcc when oancf is missing and ib == 0
        data.loc[missing_oancf_mask & (data['ib'] == 0), 'PctAcc'] = wc_accruals.loc[missing_oancf_mask & (data['ib'] == 0)] / 0.01
        
        logger.info("Successfully calculated PctAcc signal")
        
        # SAVE RESULTS
        logger.info("Saving PctAcc predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'PctAcc']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['PctAcc'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "PctAcc.csv"
        csv_data = output_data[['permno', 'yyyymm', 'PctAcc']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved PctAcc predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed PctAcc predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct PctAcc predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    pctacc()
