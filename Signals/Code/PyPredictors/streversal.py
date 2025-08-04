"""
Python equivalent of STreversal.do
Generated from: STreversal.do

Original Stata file: STreversal.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def streversal():
    """
    Python equivalent of STreversal.do
    
    Constructs the STreversal predictor signal for short-term reversal.
    """
    logger.info("Constructing predictor signal: STreversal...")
    
    try:
        # DATA LOAD
        # Load SignalMasterTable data
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load required variables
        required_vars = ['permno', 'time_avail_m', 'ret']
        
        data = pd.read_csv(master_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating STreversal signal...")
        
        # Replace missing returns with 0 (equivalent to Stata's "replace ret = 0 if mi(ret)")
        data['ret'] = data['ret'].fillna(0)
        
        # Create STreversal signal (equivalent to Stata's "gen STreversal = ret")
        data['STreversal'] = data['ret']
        
        logger.info("Successfully calculated STreversal signal")
        
        # SAVE RESULTS
        logger.info("Saving STreversal predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'STreversal']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['STreversal'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "STreversal.csv"
        csv_data = output_data[['permno', 'yyyymm', 'STreversal']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved STreversal predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed STreversal predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct STreversal predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    streversal()
