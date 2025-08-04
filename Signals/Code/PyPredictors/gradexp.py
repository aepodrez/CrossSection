"""
Python equivalent of GrAdExp.do
Generated from: GrAdExp.do

Original Stata file: GrAdExp.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def gradexp():
    """
    Python equivalent of GrAdExp.do
    
    Constructs the GrAdExp predictor signal for growth in advertising expenses.
    """
    logger.info("Constructing predictor signal: GrAdExp...")
    
    try:
        # DATA LOAD
        # Load Compustat annual data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading Compustat annual data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"Compustat annual file not found: {compustat_path}")
            logger.error("Please run the Compustat data download scripts first")
            return False
        
        # Load the required variables
        required_vars = ['permno', 'time_avail_m', 'at', 'xad']
        
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
        
        # Merge data (equivalent to Stata's "merge 1:1 permno time_avail_m using "$pathDataIntermediate/SignalMasterTable", keep(master match) nogenerate keepusing(mve_c)")
        data = data.merge(
            master_data,
            on=['permno', 'time_avail_m'],
            how='inner'  # keep(master match)
        )
        
        logger.info(f"After merging with SignalMasterTable: {len(data)} observations")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating GrAdExp signal...")
        
        # Sort data for time series operations (equivalent to Stata's "xtset permno time_avail_m")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate 12-month lag of xad (equivalent to Stata's "l12.xad")
        data['xad_lag12'] = data.groupby('permno')['xad'].shift(12)
        
        # Calculate GrAdExp (equivalent to Stata's "gen GrAdExp = log(xad) - log(l12.xad)")
        data['GrAdExp'] = np.log(data['xad']) - np.log(data['xad_lag12'])
        
        # Create size deciles (equivalent to Stata's "egen tempSize = fastxtile(mve_c), n(10) by(time_avail)")
        data['tempSize'] = data.groupby('time_avail_m')['mve_c'].transform(
            lambda x: pd.qcut(x, q=10, labels=False, duplicates='drop') + 1
        )
        
        # Apply filters (equivalent to Stata's "replace GrAdExp = . if xad < .1 | tempSize == 1")
        data.loc[(data['xad'] < 0.1) | (data['tempSize'] == 1), 'GrAdExp'] = np.nan
        
        logger.info("Successfully calculated GrAdExp signal")
        
        # SAVE RESULTS
        logger.info("Saving GrAdExp predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'GrAdExp']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['GrAdExp'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "GrAdExp.csv"
        csv_data = output_data[['permno', 'yyyymm', 'GrAdExp']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved GrAdExp predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed GrAdExp predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct GrAdExp predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    gradexp()
