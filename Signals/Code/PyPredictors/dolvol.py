"""
Python equivalent of DolVol.do
Generated from: DolVol.do

Original Stata file: DolVol.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def dolvol():
    """
    Python equivalent of DolVol.do
    
    Constructs the DolVol predictor signal for past trading volume.
    """
    logger.info("Constructing predictor signal: DolVol...")
    
    try:
        # DATA LOAD
        # Load monthly CRSP data
        crsp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/monthlyCRSP.csv")
        
        logger.info(f"Loading monthly CRSP data from: {crsp_path}")
        
        if not crsp_path.exists():
            logger.error(f"Monthly CRSP file not found: {crsp_path}")
            logger.error("Please run the CRSP data download script first")
            return False
        
        # Load the required variables
        required_vars = ['permno', 'time_avail_m', 'vol', 'prc']
        
        data = pd.read_csv(crsp_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Sort for lag calculations (equivalent to Stata's "xtset permno time_avail_m")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating DolVol signal...")
        
        # Calculate 2-month lags
        data['vol_lag2'] = data.groupby('permno')['vol'].shift(2)
        data['prc_lag2'] = data.groupby('permno')['prc'].shift(2)
        
        # Calculate DolVol (equivalent to Stata's "gen DolVol = log(l2.vol*abs(l2.prc))")
        data['DolVol'] = np.log(data['vol_lag2'] * data['prc_lag2'].abs())
        
        logger.info("Successfully calculated DolVol signal")
        
        # SAVE RESULTS
        logger.info("Saving DolVol predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'DolVol']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['DolVol'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "DolVol.csv"
        csv_data = output_data[['permno', 'yyyymm', 'DolVol']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved DolVol predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed DolVol predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct DolVol predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    dolvol()
