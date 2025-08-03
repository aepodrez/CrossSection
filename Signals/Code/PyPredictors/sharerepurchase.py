"""
Python equivalent of ShareRepurchase.do
Generated from: ShareRepurchase.do

Original Stata file: ShareRepurchase.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def sharerepurchase():
    """
    Python equivalent of ShareRepurchase.do
    
    Constructs the ShareRepurchase predictor signal for share repurchase.
    """
    logger.info("Constructing predictor signal: ShareRepurchase...")
    
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
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'prstkc']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating ShareRepurchase signal...")
        
        # Calculate ShareRepurchase (equivalent to Stata's "gen ShareRepurchase = (prstkc > 0 & !mi(prstkc))")
        data['ShareRepurchase'] = (data['prstkc'] > 0) & (~data['prstkc'].isna())
        
        # Replace with missing if prstkc is missing (equivalent to Stata's "replace ShareRepurchase = . if mi(prstkc)")
        data.loc[data['prstkc'].isna(), 'ShareRepurchase'] = np.nan
        
        # Convert boolean to integer (0/1)
        data['ShareRepurchase'] = data['ShareRepurchase'].astype(int)
        
        logger.info("Successfully calculated ShareRepurchase signal")
        
        # SAVE RESULTS
        logger.info("Saving ShareRepurchase predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'ShareRepurchase']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['ShareRepurchase'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "ShareRepurchase.csv"
        csv_data = output_data[['permno', 'yyyymm', 'ShareRepurchase']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved ShareRepurchase predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed ShareRepurchase predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct ShareRepurchase predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    sharerepurchase()
