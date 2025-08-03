"""
Python equivalent of EP.do
Generated from: EP.do

Original Stata file: EP.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def ep():
    """
    Python equivalent of EP.do
    
    Constructs the EP predictor signal for earnings-to-price ratio.
    """
    logger.info("Constructing predictor signal: EP...")
    
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
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'ib']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # Merge with SignalMasterTable to get mve_c
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load required variables from SignalMasterTable
        master_vars = ['permno', 'time_avail_m', 'mve_c']
        master_data = pd.read_csv(master_path, usecols=master_vars)
        
        # Merge data (equivalent to Stata's "merge 1:1 permno time_avail_m using "$pathDataIntermediate/SignalMasterTable", keep(using match)")
        data = data.merge(
            master_data,
            on=['permno', 'time_avail_m'],
            how='inner'  # keep(using match)
        )
        
        logger.info(f"Successfully merged data: {len(data)} observations")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating EP signal...")
        
        # Sort data for lag calculations (equivalent to Stata's "xtset permno time_avail_m")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate 6-month lag of mve_c (equivalent to Stata's "l6.mve_c")
        data['mve_c_lag6'] = data.groupby('permno')['mve_c'].shift(6)
        
        # Calculate EP (equivalent to Stata's "gen EP = ib/l6.mve_c")
        data['EP'] = data['ib'] / data['mve_c_lag6']
        
        # Apply filter (equivalent to Stata's "replace EP = . if EP < 0")
        data.loc[data['EP'] < 0, 'EP'] = np.nan
        
        logger.info("Successfully calculated EP signal")
        
        # SAVE RESULTS
        logger.info("Saving EP predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'EP']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['EP'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "EP.csv"
        csv_data = output_data[['permno', 'yyyymm', 'EP']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved EP predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed EP predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct EP predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    ep()
