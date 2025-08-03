"""
Python equivalent of MRreversal.do
Generated from: MRreversal.do

Original Stata file: MRreversal.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def mrreversal():
    """
    Python equivalent of MRreversal.do
    
    Constructs the MRreversal predictor signal for momentum-reversal.
    """
    logger.info("Constructing predictor signal: MRreversal...")
    
    try:
        # DATA LOAD
        # Load SignalMasterTable data
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load the required variables
        required_vars = ['permno', 'time_avail_m', 'ret']
        
        data = pd.read_csv(master_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating MRreversal signal...")
        
        # Replace missing returns with 0 (equivalent to Stata's "replace ret = 0 if mi(ret)")
        data['ret'] = data['ret'].fillna(0)
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate lags of returns (13-18 months for MRreversal)
        for lag in range(13, 19):
            data[f'ret_lag{lag}'] = data.groupby('permno')['ret'].shift(lag)
        
        # Calculate MRreversal (equivalent to Stata's "gen MRreversal = ( (1+l13.ret)*(1+l14.ret)*(1+l15.ret)*(1+l16.ret)*(1+l17.ret)*(1+l18.ret) ) - 1")
        data['MRreversal'] = ((1 + data['ret_lag13']) * (1 + data['ret_lag14']) * 
                              (1 + data['ret_lag15']) * (1 + data['ret_lag16']) * 
                              (1 + data['ret_lag17']) * (1 + data['ret_lag18'])) - 1
        
        logger.info("Successfully calculated MRreversal signal")
        
        # SAVE RESULTS
        logger.info("Saving MRreversal predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'MRreversal']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['MRreversal'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "MRreversal.csv"
        csv_data = output_data[['permno', 'yyyymm', 'MRreversal']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved MRreversal predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed MRreversal predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct MRreversal predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    mrreversal()
