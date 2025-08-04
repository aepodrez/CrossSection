"""
Python equivalent of IntMom.do
Generated from: IntMom.do

Original Stata file: IntMom.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def intmom():
    """
    Python equivalent of IntMom.do
    
    Constructs the IntMom predictor signal for intermediate momentum.
    """
    logger.info("Constructing predictor signal: IntMom...")
    
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
        logger.info("Calculating IntMom signal...")
        
        # Replace missing returns with 0 (equivalent to Stata's "replace ret = 0 if mi(ret)")
        data['ret'] = data['ret'].fillna(0)
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate lags of returns (7-12 months)
        for lag in range(7, 13):
            data[f'ret_lag{lag}'] = data.groupby('permno')['ret'].shift(lag)
        
        # Calculate intermediate momentum (equivalent to Stata's "gen IntMom = ( (1+l7.ret)*(1+l8.ret)*(1+l9.ret)*(1+l10.ret)*(1+l11.ret)*(1+l12.ret) ) - 1")
        data['IntMom'] = ((1 + data['ret_lag7']) * (1 + data['ret_lag8']) * 
                          (1 + data['ret_lag9']) * (1 + data['ret_lag10']) * 
                          (1 + data['ret_lag11']) * (1 + data['ret_lag12'])) - 1
        
        logger.info("Successfully calculated IntMom signal")
        
        # SAVE RESULTS
        logger.info("Saving IntMom predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'IntMom']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['IntMom'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "IntMom.csv"
        csv_data = output_data[['permno', 'yyyymm', 'IntMom']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved IntMom predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed IntMom predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct IntMom predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    intmom()
