"""
Python equivalent of DelCOA.do
Generated from: DelCOA.do

Original Stata file: DelCOA.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def delcoa():
    """
    Python equivalent of DelCOA.do
    
    Constructs the DelCOA predictor signal for change in current operating assets.
    """
    logger.info("Constructing predictor signal: DelCOA...")
    
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
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'at', 'act', 'che']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # Sort for lag calculations (equivalent to Stata's "xtset permno time_avail_m")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating DelCOA signal...")
        
        # Calculate 12-month lags
        data['at_lag12'] = data.groupby('permno')['at'].shift(12)
        data['act_lag12'] = data.groupby('permno')['act'].shift(12)
        data['che_lag12'] = data.groupby('permno')['che'].shift(12)
        
        # Calculate average total assets (equivalent to Stata's "gen tempAvAT = .5*(at + l12.at)")
        data['tempAvAT'] = 0.5 * (data['at'] + data['at_lag12'])
        
        # Calculate change in current operating assets (equivalent to Stata's "gen DelCOA = (act - che) - (l12.act - l12.che)")
        data['DelCOA'] = (data['act'] - data['che']) - (data['act_lag12'] - data['che_lag12'])
        
        # Scale by average total assets (equivalent to Stata's "replace DelCOA = DelCOA/tempAvAT")
        data['DelCOA'] = data['DelCOA'] / data['tempAvAT']
        
        logger.info("Successfully calculated DelCOA signal")
        
        # SAVE RESULTS
        logger.info("Saving DelCOA predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'DelCOA']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['DelCOA'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "DelCOA.csv"
        csv_data = output_data[['permno', 'yyyymm', 'DelCOA']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved DelCOA predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed DelCOA predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct DelCOA predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    delcoa()
