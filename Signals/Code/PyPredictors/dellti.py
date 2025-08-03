"""
Python equivalent of DelLTI.do
Generated from: DelLTI.do

Original Stata file: DelLTI.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def dellti():
    """
    Python equivalent of DelLTI.do
    
    Constructs the DelLTI predictor signal for change in long-term investment.
    """
    logger.info("Constructing predictor signal: DelLTI...")
    
    try:
        # DATA LOAD
        # Load Compustat annual data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading Compustat annual data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"Input file not found: {compustat_path}")
            logger.error("Please run the Compustat data download scripts first")
            return False
        
        # Load the required variables
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'at', 'ivao']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # Sort for lag calculations (equivalent to Stata's "xtset permno time_avail_m")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating DelLTI signal...")
        
        # Calculate 12-month lags
        data['at_lag12'] = data.groupby('permno')['at'].shift(12)
        data['ivao_lag12'] = data.groupby('permno')['ivao'].shift(12)
        
        # Calculate average total assets (equivalent to Stata's "gen tempAvAT = .5*(at + l12.at)")
        data['tempAvAT'] = 0.5 * (data['at'] + data['at_lag12'])
        
        # Calculate change in long-term investment (equivalent to Stata's "gen DelLTI = ivao - l12.ivao")
        data['DelLTI'] = data['ivao'] - data['ivao_lag12']
        
        # Scale by average total assets (equivalent to Stata's "replace DelLTI = DelLTI/tempAvAT")
        data['DelLTI'] = data['DelLTI'] / data['tempAvAT']
        
        logger.info("Successfully calculated DelLTI signal")
        
        # SAVE RESULTS
        logger.info("Saving DelLTI predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'DelLTI']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['DelLTI'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "DelLTI.csv"
        csv_data = output_data[['permno', 'yyyymm', 'DelLTI']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved DelLTI predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed DelLTI predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct DelLTI predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    dellti()
