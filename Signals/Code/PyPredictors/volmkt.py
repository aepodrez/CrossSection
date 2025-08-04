"""
Python equivalent of VolMkt.do
Generated from: VolMkt.do

Original Stata file: VolMkt.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def volmkt():
    """
    Python equivalent of VolMkt.do
    
    Constructs the VolMkt predictor signal for volume to market equity ratio.
    """
    logger.info("Constructing predictor signal: VolMkt...")
    
    try:
        # DATA LOAD
        # Load monthly CRSP data
        crsp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/monthlyCRSP.csv")
        
        logger.info(f"Loading monthly CRSP data from: {crsp_path}")
        
        if not crsp_path.exists():
            logger.error(f"monthlyCRSP not found: {crsp_path}")
            logger.error("Please run the monthly CRSP data creation script first")
            return False
        
        # Load required variables
        required_vars = ['permno', 'time_avail_m', 'vol', 'prc', 'shrout']
        
        data = pd.read_csv(crsp_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating VolMkt signal...")
        
        # Calculate market value of equity (equivalent to Stata's "gen mve_c = (shrout * abs(prc))")
        data['mve_c'] = data['shrout'] * np.abs(data['prc'])
        
        # Calculate temporary volume measure (equivalent to Stata's "gen temp = vol*abs(prc)")
        data['temp'] = data['vol'] * np.abs(data['prc'])
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate rolling mean of temp (equivalent to Stata's "asrol temp, gen(tempMean) stat(mean) window(time_avail_m 12) min(10)")
        data['tempMean'] = data.groupby('permno')['temp'].rolling(
            window=12, 
            min_periods=10
        ).mean().reset_index(0, drop=True)
        
        # Calculate volume to market equity ratio (equivalent to Stata's "gen VolMkt = tempMean/mve_c")
        data['VolMkt'] = data['tempMean'] / data['mve_c']
        
        logger.info("Successfully calculated VolMkt signal")
        
        # SAVE RESULTS
        logger.info("Saving VolMkt predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'VolMkt']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['VolMkt'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "VolMkt.csv"
        csv_data = output_data[['permno', 'yyyymm', 'VolMkt']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved VolMkt predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed VolMkt predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct VolMkt predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    volmkt()
