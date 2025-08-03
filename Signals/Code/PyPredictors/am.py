"""
Python equivalent of AM.do
Generated from: AM.do

Original Stata file: AM.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def am():
    """
    Python equivalent of AM.do
    
    Constructs the AM predictor signal for total assets to market value ratio.
    """
    logger.info("Constructing predictor signal: AM...")
    
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
        required_vars = ['permno', 'time_avail_m', 'at']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        initial_count = len(data)
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        logger.info(f"Removed {initial_count - len(data)} duplicate observations")
        
        # Load SignalMasterTable for market value
        signal_master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {signal_master_path}")
        
        if not signal_master_path.exists():
            logger.error(f"SignalMasterTable not found: {signal_master_path}")
            return False
        
        signal_master = pd.read_csv(signal_master_path, usecols=['permno', 'time_avail_m', 'mve_c'])
        logger.info(f"Successfully loaded SignalMasterTable with {len(signal_master)} records")
        
        # Merge with SignalMasterTable (equivalent to Stata's "merge 1:1 permno time_avail_m using SignalMasterTable")
        data = data.merge(signal_master, on=['permno', 'time_avail_m'], how='inner')
        logger.info(f"After merge: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Constructing AM signal...")
        
        # Calculate AM (equivalent to Stata's "gen AM = at/mve_c")
        data['AM'] = data['at'] / data['mve_c']
        
        logger.info("Successfully calculated AM signal")
        
        # SAVE RESULTS
        logger.info("Saving AM predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'AM']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['AM'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "AM.csv"
        csv_data = output_data[['permno', 'yyyymm', 'AM']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved AM predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed AM predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct AM predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    am()
