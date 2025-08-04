"""
Python equivalent of DelDRC.do
Generated from: DelDRC.do

Original Stata file: DelDRC.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def deldrc():
    """
    Python equivalent of DelDRC.do
    
    Constructs the DelDRC predictor signal for deferred revenue.
    """
    logger.info("Constructing predictor signal: DelDRC...")
    
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
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'drc', 'at', 'ceq', 'sale', 'sic']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # Sort for lag calculations (equivalent to Stata's "xtset permno time_avail_m")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Convert SIC to numeric (equivalent to Stata's "destring sic, replace")
        data['sic'] = pd.to_numeric(data['sic'], errors='coerce')
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating DelDRC signal...")
        
        # Calculate 12-month lags
        data['drc_lag12'] = data.groupby('permno')['drc'].shift(12)
        data['at_lag12'] = data.groupby('permno')['at'].shift(12)
        
        # Calculate DelDRC (equivalent to Stata's "gen DelDRC = (drc - l12.drc)/(.5*(at + l12.at))")
        data['DelDRC'] = (data['drc'] - data['drc_lag12']) / (0.5 * (data['at'] + data['at_lag12']))
        
        # Apply filters (equivalent to Stata's replace conditions)
        # Set to missing if ceq <= 0 or (drc == 0 & DelDRC == 0) or sale < 5 or (sic >= 6000 & sic < 7000)
        data.loc[
            (data['ceq'] <= 0) | 
            ((data['drc'] == 0) & (data['DelDRC'] == 0)) | 
            (data['sale'] < 5) | 
            ((data['sic'] >= 6000) & (data['sic'] < 7000)),
            'DelDRC'
        ] = np.nan
        
        logger.info("Successfully calculated DelDRC signal")
        
        # SAVE RESULTS
        logger.info("Saving DelDRC predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'DelDRC']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['DelDRC'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "DelDRC.csv"
        csv_data = output_data[['permno', 'yyyymm', 'DelDRC']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved DelDRC predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed DelDRC predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct DelDRC predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    deldrc()
