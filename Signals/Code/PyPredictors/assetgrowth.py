"""
Python equivalent of AssetGrowth.do
Generated from: AssetGrowth.do

Original Stata file: AssetGrowth.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def assetgrowth():
    """
    Python equivalent of AssetGrowth.do
    
    Constructs the AssetGrowth predictor signal for asset growth rate.
    """
    logger.info("Constructing predictor signal: AssetGrowth...")
    
    try:
        # DATA LOAD
        # Load Compustat monthly data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading Compustat monthly data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"Input file not found: {compustat_path}")
            logger.error("Please run the Compustat data download scripts first")
            return False
        
        # Load the required variables
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'at']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Sort by permno and time_avail_m for lag calculations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Convert time_avail_m to datetime for proper lagging
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Constructing AssetGrowth signal...")
        
        # Calculate 12-month lag for total assets (equivalent to Stata's "l12.at")
        data['at_lag12'] = data.groupby('permno')['at'].shift(12)
        
        # Calculate AssetGrowth (equivalent to Stata's "gen AssetGrowth = (at - l12.at)/l12.at")
        data['AssetGrowth'] = (data['at'] - data['at_lag12']) / data['at_lag12']
        
        logger.info("Successfully calculated AssetGrowth signal")
        
        # SAVE RESULTS
        logger.info("Saving AssetGrowth predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'AssetGrowth']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['AssetGrowth'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "AssetGrowth.csv"
        csv_data = output_data[['permno', 'yyyymm', 'AssetGrowth']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved AssetGrowth predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed AssetGrowth predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct AssetGrowth predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    assetgrowth()
