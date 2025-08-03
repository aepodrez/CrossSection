"""
Python equivalent of LRreversal.do
Generated from: LRreversal.do

Original Stata file: LRreversal.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def lrreversal():
    """
    Python equivalent of LRreversal.do
    
    Constructs the LRreversal predictor signal for long-term reversal.
    """
    logger.info("Constructing predictor signal: LRreversal...")
    
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
        logger.info("Calculating LRreversal signal...")
        
        # Replace missing returns with 0 (equivalent to Stata's "replace ret = 0 if mi(ret)")
        data['ret'] = data['ret'].fillna(0)
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate lags of returns (13-36 months)
        for lag in range(13, 37):
            data[f'ret_lag{lag}'] = data.groupby('permno')['ret'].shift(lag)
        
        # Calculate long-term reversal (equivalent to Stata's complex multiplication)
        # This is the cumulative return over months 13-36 (24 months)
        lrreversal_terms = []
        for lag in range(13, 37):
            lrreversal_terms.append(f"(1 + data['ret_lag{lag}'])")
        
        # Evaluate the expression
        data['LRreversal'] = 1
        for lag in range(13, 37):
            data['LRreversal'] = data['LRreversal'] * (1 + data[f'ret_lag{lag}'])
        
        # Subtract 1 to get the cumulative return
        data['LRreversal'] = data['LRreversal'] - 1
        
        logger.info("Successfully calculated LRreversal signal")
        
        # SAVE RESULTS
        logger.info("Saving LRreversal predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'LRreversal']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['LRreversal'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "LRreversal.csv"
        csv_data = output_data[['permno', 'yyyymm', 'LRreversal']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved LRreversal predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed LRreversal predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct LRreversal predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    lrreversal()
