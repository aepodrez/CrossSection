"""
Python equivalent of Price.do
Generated from: Price.do

Original Stata file: Price.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def price():
    """
    Python equivalent of Price.do
    
    Constructs the Price predictor signal for log price.
    """
    logger.info("Constructing predictor signal: Price...")
    
    try:
        # DATA LOAD
        # Load SignalMasterTable data
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load required variables
        required_vars = ['permno', 'time_avail_m', 'prc']
        
        data = pd.read_csv(master_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating Price signal...")
        
        # Calculate Price (equivalent to Stata's "gen Price = log(abs(prc))")
        data['Price'] = np.log(np.abs(data['prc']))
        
        logger.info("Successfully calculated Price signal")
        
        # SAVE RESULTS
        logger.info("Saving Price predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'Price']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['Price'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "Price.csv"
        csv_data = output_data[['permno', 'yyyymm', 'Price']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved Price predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed Price predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct Price predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    price()
