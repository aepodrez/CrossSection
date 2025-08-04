"""
Python equivalent of ExchSwitch.do
Generated from: ExchSwitch.do

Original Stata file: ExchSwitch.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def exchswitch():
    """
    Python equivalent of ExchSwitch.do
    
    Constructs the ExchSwitch predictor signal for exchange switches.
    """
    logger.info("Constructing predictor signal: ExchSwitch...")
    
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
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'exchcd']
        
        data = pd.read_csv(master_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating ExchSwitch signal...")
        
        # Sort data for lag calculations (equivalent to Stata's "xtset permno time_avail_m")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate lags of exchcd (1-12 months)
        for lag in range(1, 13):
            data[f'exchcd_lag{lag}'] = data.groupby('permno')['exchcd'].shift(lag)
        
        # Calculate ExchSwitch (equivalent to Stata's complex condition)
        # Case 1: Currently on NYSE (exchcd == 1) and was on AMEX/NASDAQ in past 12 months
        nyse_condition = (
            (data['exchcd'] == 1) & 
            ((data['exchcd_lag1'] == 2) | (data['exchcd_lag2'] == 2) | (data['exchcd_lag3'] == 2) |
             (data['exchcd_lag4'] == 2) | (data['exchcd_lag5'] == 2) | (data['exchcd_lag6'] == 2) |
             (data['exchcd_lag7'] == 2) | (data['exchcd_lag8'] == 2) | (data['exchcd_lag9'] == 2) |
             (data['exchcd_lag10'] == 2) | (data['exchcd_lag11'] == 2) | (data['exchcd_lag12'] == 2) |
             (data['exchcd_lag1'] == 3) | (data['exchcd_lag2'] == 3) | (data['exchcd_lag3'] == 3) |
             (data['exchcd_lag4'] == 3) | (data['exchcd_lag5'] == 3) | (data['exchcd_lag6'] == 3) |
             (data['exchcd_lag7'] == 3) | (data['exchcd_lag8'] == 3) | (data['exchcd_lag9'] == 3) |
             (data['exchcd_lag10'] == 3) | (data['exchcd_lag11'] == 3) | (data['exchcd_lag12'] == 3))
        )
        
        # Case 2: Currently on AMEX (exchcd == 2) and was on NASDAQ in past 12 months
        amex_condition = (
            (data['exchcd'] == 2) & 
            ((data['exchcd_lag1'] == 3) | (data['exchcd_lag2'] == 3) | (data['exchcd_lag3'] == 3) |
             (data['exchcd_lag4'] == 3) | (data['exchcd_lag5'] == 3) | (data['exchcd_lag6'] == 3) |
             (data['exchcd_lag7'] == 3) | (data['exchcd_lag8'] == 3) | (data['exchcd_lag9'] == 3) |
             (data['exchcd_lag10'] == 3) | (data['exchcd_lag11'] == 3) | (data['exchcd_lag12'] == 3))
        )
        
        # Combine conditions
        data['ExchSwitch'] = (nyse_condition | amex_condition).astype(int)
        
        logger.info("Successfully calculated ExchSwitch signal")
        
        # SAVE RESULTS
        logger.info("Saving ExchSwitch predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'ExchSwitch']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['ExchSwitch'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "ExchSwitch.csv"
        csv_data = output_data[['permno', 'yyyymm', 'ExchSwitch']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved ExchSwitch predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed ExchSwitch predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct ExchSwitch predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    exchswitch()
