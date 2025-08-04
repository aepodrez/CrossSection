"""
Python equivalent of AdExp.do
Generated from: AdExp.do

Original Stata file: AdExp.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def adexp():
    """
    Python equivalent of AdExp.do
    
    Constructs the AdExp predictor signal for advertising expenses.
    """
    logger.info("Constructing predictor signal: AdExp...")
    
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
        required_vars = ['permno', 'time_avail_m', 'xad']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
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
        logger.info("Constructing AdExp signal...")
        
        # Calculate AdExp (equivalent to Stata's "gen AdExp = xad/mve_c")
        data['AdExp'] = data['xad'] / data['mve_c']
        
        # Set to missing if xad <= 0 (equivalent to Stata's "replace AdExp = . if xad <= 0")
        data.loc[data['xad'] <= 0, 'AdExp'] = np.nan
        
        logger.info("Successfully calculated AdExp signal")
        
        # SAVE RESULTS
        logger.info("Saving AdExp predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'AdExp']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['AdExp'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "AdExp.csv"
        csv_data = output_data[['permno', 'yyyymm', 'AdExp']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved AdExp predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed AdExp predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct AdExp predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    adexp()
