"""
Python equivalent of Mom6m.do
Generated from: Mom6m.do

Original Stata file: Mom6m.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def mom6m():
    """
    Python equivalent of Mom6m.do
    
    Constructs the Mom6m predictor signal for six month momentum.
    """
    logger.info("Constructing predictor signal: Mom6m...")
    
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
        logger.info("Calculating Mom6m signal...")
        
        # Replace missing returns with 0 (equivalent to Stata's "replace ret = 0 if mi(ret)")
        data['ret'] = data['ret'].fillna(0)
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate lags of returns (1-5 months)
        for lag in range(1, 6):
            data[f'ret_lag{lag}'] = data.groupby('permno')['ret'].shift(lag)
        
        # Calculate 6-month momentum (equivalent to Stata's "gen Mom6m = ( (1+l.ret)*(1+l2.ret)*(1+l3.ret)*(1+l4.ret)*(1+l5.ret)) - 1")
        data['Mom6m'] = ((1 + data['ret_lag1']) * (1 + data['ret_lag2']) * 
                         (1 + data['ret_lag3']) * (1 + data['ret_lag4']) * 
                         (1 + data['ret_lag5'])) - 1
        
        logger.info("Successfully calculated Mom6m signal")
        
        # SAVE RESULTS
        logger.info("Saving Mom6m predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'Mom6m']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['Mom6m'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "Mom6m.csv"
        csv_data = output_data[['permno', 'yyyymm', 'Mom6m']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved Mom6m predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed Mom6m predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct Mom6m predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    mom6m()
