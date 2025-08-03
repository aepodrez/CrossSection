"""
Python equivalent of BookLeverage.do
Generated from: BookLeverage.do

Original Stata file: BookLeverage.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def bookleverage():
    """
    Python equivalent of BookLeverage.do
    
    Constructs the BookLeverage predictor signal for book leverage ratio.
    """
    logger.info("Constructing predictor signal: BookLeverage...")
    
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
        required_vars = ['permno', 'time_avail_m', 'at', 'lt', 'txditc', 'pstk', 'pstkrv', 'pstkl', 'seq', 'ceq']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        initial_count = len(data)
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        logger.info(f"Removed {initial_count - len(data)} duplicate observations")
        
        # SIGNAL CONSTRUCTION
        logger.info("Constructing BookLeverage signal...")
        
        # Set txditc to 0 if missing (equivalent to Stata's "replace txditc = 0 if mi(txditc)")
        data['txditc'] = data['txditc'].fillna(0)
        
        # Calculate preferred stock (equivalent to Stata's preferred stock logic)
        data['tempPS'] = data['pstk']
        data.loc[data['tempPS'].isna(), 'tempPS'] = data.loc[data['tempPS'].isna(), 'pstkrv']
        data.loc[data['tempPS'].isna(), 'tempPS'] = data.loc[data['tempPS'].isna(), 'pstkl']
        
        # Calculate stockholders' equity (equivalent to Stata's stockholders' equity logic)
        data['tempSE'] = data['seq']
        data.loc[data['tempSE'].isna(), 'tempSE'] = data.loc[data['tempSE'].isna(), 'ceq'] + data.loc[data['tempSE'].isna(), 'tempPS']
        data.loc[data['tempSE'].isna(), 'tempSE'] = data.loc[data['tempSE'].isna(), 'at'] - data.loc[data['tempSE'].isna(), 'lt']
        
        # Calculate BookLeverage (equivalent to Stata's "gen BookLeverage = at/(tempSE + txditc - tempPS)")
        data['BookLeverage'] = data['at'] / (data['tempSE'] + data['txditc'] - data['tempPS'])
        
        logger.info("Successfully calculated BookLeverage signal")
        
        # SAVE RESULTS
        logger.info("Saving BookLeverage predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'BookLeverage']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['BookLeverage'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "BookLeverage.csv"
        csv_data = output_data[['permno', 'yyyymm', 'BookLeverage']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved BookLeverage predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed BookLeverage predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct BookLeverage predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    bookleverage()
