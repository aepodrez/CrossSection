"""
Python equivalent of SmileSlope.do
Generated from: SmileSlope.do

Original Stata file: SmileSlope.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def smileslope():
    """
    Python equivalent of SmileSlope.do
    
    Constructs the SmileSlope predictor signal for average jump size.
    """
    logger.info("Constructing predictor signal: SmileSlope...")
    
    try:
        # Data Prep
        # Load OptionMetricsVolSurf data
        volsurf_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/OptionMetricsVolSurf.csv")
        
        logger.info(f"Loading OptionMetricsVolSurf data from: {volsurf_path}")
        
        if not volsurf_path.exists():
            logger.error(f"OptionMetricsVolSurf not found: {volsurf_path}")
            logger.error("Please run the OptionMetricsVolSurf data creation script first")
            return False
        
        data = pd.read_csv(volsurf_path)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Filter data (equivalent to Stata's "keep if days == 30 & abs(delta) == 50")
        data = data[(data['days'] == 30) & (np.abs(data['delta']) == 50)]
        logger.info(f"After filtering for days=30 and abs(delta)=50: {len(data)} records")
        
        # Keep required variables
        data = data[['secid', 'time_avail_m', 'cp_flag', 'impl_vol']]
        
        # Reshape data (equivalent to Stata's "reshape wide impl_vol, i(secid time_avail_m) j(cp_flag) string")
        data_wide = data.pivot_table(
            index=['secid', 'time_avail_m'],
            columns='cp_flag',
            values='impl_vol',
            aggfunc='first'
        ).reset_index()
        
        # Rename columns to match Stata output
        data_wide.columns.name = None
        data_wide = data_wide.rename(columns={'C': 'impl_volC', 'P': 'impl_volP'})
        
        logger.info(f"After reshaping: {len(data_wide)} records")
        
        # Calculate SmileSlope (equivalent to Stata's "gen SmileSlope = impl_volP - impl_volC")
        data_wide['SmileSlope'] = data_wide['impl_volP'] - data_wide['impl_volC']
        
        logger.info("Successfully calculated SmileSlope signal")
        
        # Save temporary data (equivalent to Stata's "save "$pathtemp/temp", replace")
        temp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Temp/temp_smileslope.csv")
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        data_wide.to_csv(temp_path, index=False)
        logger.info(f"Saved temporary data to: {temp_path}")
        
        # Merge onto master table
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        master_data = pd.read_csv(master_path, usecols=['permno', 'time_avail_m', 'secid'])
        logger.info(f"Successfully loaded {len(master_data)} master records")
        
        # Merge with temporary data (equivalent to Stata's "merge m:1 secid time_avail_m using "$pathtemp/temp", keep(master match) nogenerate")
        final_data = master_data.merge(data_wide, on=['secid', 'time_avail_m'], how='left')
        logger.info(f"After merging with temporary data: {len(final_data)} records")
        
        # Keep only non-missing SmileSlope (equivalent to Stata's "keep if SmileSlope != .")
        final_data = final_data.dropna(subset=['SmileSlope'])
        logger.info(f"After dropping missing SmileSlope: {len(final_data)} records")
        
        # SAVE RESULTS
        logger.info("Saving SmileSlope predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = final_data[['permno', 'time_avail_m', 'SmileSlope']].copy()
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "SmileSlope.csv"
        csv_data = output_data[['permno', 'yyyymm', 'SmileSlope']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved SmileSlope predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed SmileSlope predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct SmileSlope predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    smileslope()
