"""
Python equivalent of FEPS.do
Generated from: FEPS.do

Original Stata file: FEPS.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def feps():
    """
    Python equivalent of FEPS.do
    
    Constructs the FEPS predictor signal for forecasted EPS.
    """
    logger.info("Constructing predictor signal: FEPS...")
    
    try:
        # Prep IBES data
        logger.info("Preparing IBES data...")
        
        # Load IBES EPS unadjusted data
        ibes_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/IBES_EPS_Unadj.csv")
        
        if not ibes_path.exists():
            logger.error(f"IBES EPS unadjusted file not found: {ibes_path}")
            logger.error("Please run the IBES data download scripts first")
            return False
        
        # Load and filter IBES data (equivalent to Stata's "keep if fpi == '1'")
        ibes_data = pd.read_csv(ibes_path)
        ibes_data = ibes_data[ibes_data['fpi'] == '1']
        
        # Keep required variables (equivalent to Stata's "keep tickerIBES time_avail_m meanest")
        ibes_data = ibes_data[['tickerIBES', 'time_avail_m', 'meanest']]
        
        logger.info(f"Prepared IBES data: {len(ibes_data)} records")
        
        # DATA LOAD
        # Load SignalMasterTable data
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load the required variables
        required_vars = ['permno', 'tickerIBES', 'time_avail_m']
        
        data = pd.read_csv(master_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Merge with prepared IBES data (equivalent to Stata's "merge m:1 tickerIBES time_avail_m using "$pathtemp/temp", keep(master match) nogenerate")
        data = data.merge(
            ibes_data,
            on=['tickerIBES', 'time_avail_m'],
            how='inner'  # keep(master match)
        )
        
        logger.info(f"After merging with IBES: {len(data)} observations")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating FEPS signal...")
        
        # Sort data for time series operations (equivalent to Stata's "xtset permno time_avail_m")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate FEPS (equivalent to Stata's "gen FEPS = meanest")
        data['FEPS'] = data['meanest']
        
        logger.info("Successfully calculated FEPS signal")
        
        # SAVE RESULTS
        logger.info("Saving FEPS predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'FEPS']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['FEPS'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "FEPS.csv"
        csv_data = output_data[['permno', 'yyyymm', 'FEPS']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved FEPS predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed FEPS predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct FEPS predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    feps()
