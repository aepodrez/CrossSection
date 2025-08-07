"""
Python equivalent of MaxRet.do
Generated from: MaxRet.do

Original Stata file: MaxRet.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def maxret():
    """
    Python equivalent of MaxRet.do
    
    Constructs the MaxRet predictor signal for maximum return over month.
    """
    logger.info("Constructing predictor signal: MaxRet...")
    
    try:
        # DATA LOAD
        # Load daily CRSP data
        crsp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/dailyCRSP.csv")
        
        logger.info(f"Loading daily CRSP data from: {crsp_path}")
        
        if not crsp_path.exists():
            logger.error(f"Daily CRSP file not found: {crsp_path}")
            logger.error("Please run the CRSP data download scripts first")
            return False
        
        # Load the required variables
        required_vars = ['permno', 'time_d', 'ret']
        
        data = pd.read_csv(crsp_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating MaxRet signal...")
        
        # Create time_avail_m column (equivalent to Stata's "gen time_avail_m = mofd(time_d)")
        data['time_d'] = pd.to_datetime(data['time_d'])
        # Convert time_d to datetime if needed for period conversion
        if not pd.api.types.is_datetime64_any_dtype(data['time_d']):
            data['time_d'] = pd.to_datetime(data['time_d'])
        
        data['time_avail_m'] = data['time_d'].dt.to_period('M').dt.to_timestamp()
        
        # Calculate maximum return by permno and time_avail_m (equivalent to Stata's "gcollapse (max) MaxRet = ret, by(permno time_avail_m)")
        data['MaxRet'] = data.groupby(['permno', 'time_avail_m'])['ret'].transform('max')
        
        # Keep only one observation per permno-time_avail_m combination (equivalent to gcollapse)
        data = data.groupby(['permno', 'time_avail_m']).first().reset_index()
        
        logger.info("Successfully calculated MaxRet signal")
        
        # SAVE RESULTS
        logger.info("Saving MaxRet predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'MaxRet']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['MaxRet'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "maxret.csv"
        csv_data = output_data[['permno', 'yyyymm', 'MaxRet']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved MaxRet predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed MaxRet predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct MaxRet predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    maxret()
