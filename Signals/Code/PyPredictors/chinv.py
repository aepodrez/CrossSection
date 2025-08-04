"""
Python equivalent of ChInv.do
Generated from: ChInv.do

Original Stata file: ChInv.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def chinv():
    """
    Python equivalent of ChInv.do
    
    Constructs the ChInv predictor signal for change in inventory.
    """
    logger.info("Constructing predictor signal: ChInv...")
    
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
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'at', 'invt']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates: keep first observation per permno-time_avail_m
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        
        # Sort by permno and time_avail_m for lag calculations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating ChInv signal...")
        
        # Calculate 12-month lags for required variables
        data['at_lag12'] = data.groupby('permno')['at'].shift(12)
        data['invt_lag12'] = data.groupby('permno')['invt'].shift(12)
        
        # Calculate ChInv (equivalent to Stata's "gen ChInv = (invt-l12.invt)/((at+l12.at)/2)")
        data['ChInv'] = (data['invt'] - data['invt_lag12']) / ((data['at'] + data['at_lag12']) / 2)
        
        logger.info("Successfully calculated ChInv signal")
        
        # SAVE RESULTS
        logger.info("Saving ChInv predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'ChInv']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['ChInv'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "ChInv.csv"
        csv_data = output_data[['permno', 'yyyymm', 'ChInv']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved ChInv predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed ChInv predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct ChInv predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    chinv()
