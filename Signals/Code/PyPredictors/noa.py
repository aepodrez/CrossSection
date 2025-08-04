"""
Python equivalent of NOA.do
Generated from: NOA.do

Original Stata file: NOA.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def noa():
    """
    Python equivalent of NOA.do
    
    Constructs the NOA predictor signal for net operating assets.
    """
    logger.info("Constructing predictor signal: NOA...")
    
    try:
        # DATA LOAD
        # Load Compustat data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading Compustat data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"m_aCompustat not found: {compustat_path}")
            logger.error("Please run the Compustat data creation script first")
            return False
        
        # Load required variables
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'at', 'che', 'dltt', 'mib', 'dc', 'ceq']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating NOA signal...")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'])
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate operating assets (equivalent to Stata's "gen OA = at - che")
        data['OA'] = data['at'] - data['che']
        
        # Calculate operating liabilities (equivalent to Stata's "gen OL = at - dltt - mib - dc - ceq")
        data['OL'] = data['at'] - data['dltt'] - data['mib'] - data['dc'] - data['ceq']
        
        # Calculate 12-month lag of total assets (equivalent to Stata's "l12.at")
        data['at_lag12'] = data.groupby('permno')['at'].shift(12)
        
        # Calculate NOA (equivalent to Stata's "gen NOA = (OA - OL)/l12.at")
        data['NOA'] = (data['OA'] - data['OL']) / data['at_lag12']
        
        logger.info("Successfully calculated NOA signal")
        
        # SAVE RESULTS
        logger.info("Saving NOA predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'NOA']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['NOA'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "NOA.csv"
        csv_data = output_data[['permno', 'yyyymm', 'NOA']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved NOA predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed NOA predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct NOA predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    noa()
