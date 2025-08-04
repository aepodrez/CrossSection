"""
Python equivalent of RoE.do
Generated from: RoE.do

Original Stata file: RoE.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def roe():
    """
    Python equivalent of RoE.do
    
    Constructs the RoE predictor signal for return on equity.
    """
    logger.info("Constructing predictor signal: RoE...")
    
    try:
        # DATA LOAD
        # Load annual Compustat data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading annual Compustat data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"m_aCompustat not found: {compustat_path}")
            logger.error("Please run the annual Compustat data creation script first")
            return False
        
        # Load required variables
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'ni', 'ceq']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating RoE signal...")
        
        # Calculate RoE (equivalent to Stata's "gen RoE = ni/ceq")
        data['RoE'] = data['ni'] / data['ceq']
        
        logger.info("Successfully calculated RoE signal")
        
        # SAVE RESULTS
        logger.info("Saving RoE predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'RoE']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['RoE'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "RoE.csv"
        csv_data = output_data[['permno', 'yyyymm', 'RoE']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved RoE predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed RoE predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct RoE predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    roe()
