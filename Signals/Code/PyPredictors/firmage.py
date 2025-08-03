"""
Python equivalent of FirmAge.do
Generated from: FirmAge.do

Original Stata file: FirmAge.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def firmage():
    """
    Python equivalent of FirmAge.do
    
    Constructs the FirmAge predictor signal for firm age.
    """
    logger.info("Constructing predictor signal: FirmAge...")
    
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
        logger.info("Calculating FirmAge signal...")
        
        # Sort data for time series operations (equivalent to Stata's "xtset permno time_avail_m")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate FirmAge as observation number within each permno (equivalent to Stata's "bys permno (time_avail_m): gen FirmAge = _n")
        data['FirmAge'] = data.groupby('permno').cumcount() + 1
        
        # Calculate tempcrsptime (equivalent to Stata's "gen tempcrsptime = time_avail_m - mofd(mdy(7,1,1926)) + 1")
        # Convert time_avail_m to datetime if it's not already
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        
        # Calculate months since July 1926
        base_date = pd.to_datetime('1926-07-01')
        data['tempcrsptime'] = ((data['time_avail_m'].dt.year - base_date.year) * 12 + 
                               (data['time_avail_m'].dt.month - base_date.month) + 1)
        
        # Set FirmAge to missing if tempcrsptime equals FirmAge (equivalent to Stata's "replace FirmAge = . if tempcrsptime == FirmAge")
        data.loc[data['tempcrsptime'] == data['FirmAge'], 'FirmAge'] = np.nan
        
        logger.info("Successfully calculated FirmAge signal")
        
        # SAVE RESULTS
        logger.info("Saving FirmAge predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'FirmAge']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['FirmAge'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "FirmAge.csv"
        csv_data = output_data[['permno', 'yyyymm', 'FirmAge']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved FirmAge predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed FirmAge predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct FirmAge predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    firmage()
