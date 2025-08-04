"""
Python equivalent of Mom12m.do
Generated from: Mom12m.do

Original Stata file: Mom12m.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def mom12m():
    """
    Python equivalent of Mom12m.do
    
    Constructs the Mom12m predictor signal for twelve month momentum.
    """
    logger.info("Constructing predictor signal: Mom12m...")
    
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
        logger.info("Calculating Mom12m signal...")
        
        # Replace missing returns with 0 (equivalent to Stata's "replace ret = 0 if mi(ret)")
        data['ret'] = data['ret'].fillna(0)
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate lags of returns (1-11 months)
        for lag in range(1, 12):
            data[f'ret_lag{lag}'] = data.groupby('permno')['ret'].shift(lag)
        
        # Calculate 12-month momentum (equivalent to Stata's complex multiplication)
        # Mom12m = (1+lag1)*(1+lag2)*(1+lag3)*(1+lag4)*(1+lag5)*(1+lag6)*(1+lag7)*(1+lag8)*(1+lag9)*(1+lag10)*(1+lag11) - 1
        data['Mom12m'] = 1
        for lag in range(1, 12):
            data['Mom12m'] = data['Mom12m'] * (1 + data[f'ret_lag{lag}'])
        
        # Subtract 1 to get the cumulative return
        data['Mom12m'] = data['Mom12m'] - 1
        
        logger.info("Successfully calculated Mom12m signal")
        
        # SAVE RESULTS
        logger.info("Saving Mom12m predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'Mom12m']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['Mom12m'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "Mom12m.csv"
        csv_data = output_data[['permno', 'yyyymm', 'Mom12m']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved Mom12m predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed Mom12m predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct Mom12m predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    mom12m()
