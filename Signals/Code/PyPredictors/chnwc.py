"""
Python equivalent of ChNWC.do
Generated from: ChNWC.do

Original Stata file: ChNWC.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def chnwc():
    """
    Python equivalent of ChNWC.do
    
    Constructs the ChNWC predictor signal for change in net working capital.
    """
    logger.info("Constructing predictor signal: ChNWC...")
    
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
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'act', 'che', 'lct', 'dlc', 'at']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates: keep first observation per permno-time_avail_m
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        
        # Sort by permno and time_avail_m for lag calculations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating ChNWC signal...")
        
        # Calculate temp variable (equivalent to Stata's "gen temp = ( (act - che) - (lct - dlc) )/at")
        data['temp'] = ((data['act'] - data['che']) - (data['lct'] - data['dlc'])) / data['at']
        
        # Calculate 12-month lag of temp
        data['temp_lag12'] = data.groupby('permno')['temp'].shift(12)
        
        # Calculate ChNWC (equivalent to Stata's "gen ChNWC = temp - l12.temp")
        data['ChNWC'] = data['temp'] - data['temp_lag12']
        
        logger.info("Successfully calculated ChNWC signal")
        
        # SAVE RESULTS
        logger.info("Saving ChNWC predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'ChNWC']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['ChNWC'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "ChNWC.csv"
        csv_data = output_data[['permno', 'yyyymm', 'ChNWC']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved ChNWC predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed ChNWC predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct ChNWC predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    chnwc()
