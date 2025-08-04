"""
Python equivalent of MomSeasonShort.do
Generated from: MomSeasonShort.do

Original Stata file: MomSeasonShort.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def momseasonshort():
    """
    Python equivalent of MomSeasonShort.do
    
    Constructs the MomSeasonShort predictor signal for return seasonality last year.
    """
    logger.info("Constructing predictor signal: MomSeasonShort...")
    
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
        logger.info("Calculating MomSeasonShort signal...")
        
        # Replace missing returns with 0 (equivalent to Stata's "replace ret = 0 if mi(ret)")
        data['ret'] = data['ret'].fillna(0)
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate 11-month lag of returns (equivalent to Stata's "gen MomSeasonShort = l11.ret")
        # This represents the return from the same month last year
        data['MomSeasonShort'] = data.groupby('permno')['ret'].shift(11)
        
        logger.info("Successfully calculated MomSeasonShort signal")
        
        # SAVE RESULTS
        logger.info("Saving MomSeasonShort predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'MomSeasonShort']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['MomSeasonShort'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "MomSeasonShort.csv"
        csv_data = output_data[['permno', 'yyyymm', 'MomSeasonShort']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved MomSeasonShort predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed MomSeasonShort predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct MomSeasonShort predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    momseasonshort()
