"""
Python equivalent of VolSD.do
Generated from: VolSD.do

Original Stata file: VolSD.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def volsd():
    """
    Python equivalent of VolSD.do
    
    Constructs the VolSD predictor signal for volume variance.
    """
    logger.info("Constructing predictor signal: VolSD...")
    
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
        required_vars = ['permno', 'time_avail_m', 'vol']
        
        data = pd.read_csv(crsp_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating VolSD signal...")
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate rolling standard deviation of volume (equivalent to Stata's "asrol vol, gen(VolSD) stat(sd) window(time_avail_m 36) min(24)")
        data['VolSD'] = data.groupby('permno')['vol'].rolling(
            window=36, 
            min_periods=24
        ).std().reset_index(0, drop=True)
        
        logger.info("Successfully calculated VolSD signal")
        
        # SAVE RESULTS
        logger.info("Saving VolSD predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'VolSD']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['VolSD'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "VolSD.csv"
        csv_data = output_data[['permno', 'yyyymm', 'VolSD']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved VolSD predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed VolSD predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct VolSD predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    volsd()
