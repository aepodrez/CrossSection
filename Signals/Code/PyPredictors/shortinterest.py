"""
Python equivalent of ShortInterest.do
Generated from: ShortInterest.do

Original Stata file: ShortInterest.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def shortinterest():
    """
    Python equivalent of ShortInterest.do
    
    Constructs the ShortInterest predictor signal for short interest.
    """
    logger.info("Constructing predictor signal: ShortInterest...")
    
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
        required_vars = ['permno', 'gvkey', 'time_avail_m']
        
        data = pd.read_csv(master_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Drop if gvkey is missing (equivalent to Stata's "drop if mi(gvkey)")
        data = data.dropna(subset=['gvkey'])
        logger.info(f"After dropping missing gvkey: {len(data)} records")
        
        # Load monthly CRSP data for shares outstanding
        crsp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/monthlyCRSP.csv")
        
        logger.info(f"Loading monthly CRSP data from: {crsp_path}")
        
        if not crsp_path.exists():
            logger.error(f"monthlyCRSP not found: {crsp_path}")
            logger.error("Please run the monthly CRSP data creation script first")
            return False
        
        crsp_data = pd.read_csv(crsp_path, usecols=['permno', 'time_avail_m', 'shrout'])
        
        # Merge with CRSP data (equivalent to Stata's "merge 1:1 permno time_avail_m using "$pathDataIntermediate/monthlyCRSP", keep(match) nogenerate keepusing(shrout)")
        data = data.merge(crsp_data, on=['permno', 'time_avail_m'], how='inner')
        logger.info(f"After merging with CRSP data: {len(data)} records")
        
        # Load monthly Short Interest data
        shortint_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/monthlyShortInterest.csv")
        
        logger.info(f"Loading monthly Short Interest data from: {shortint_path}")
        
        if not shortint_path.exists():
            logger.error(f"monthlyShortInterest not found: {shortint_path}")
            logger.error("Please run the monthly Short Interest data creation script first")
            return False
        
        shortint_data = pd.read_csv(shortint_path, usecols=['gvkey', 'time_avail_m', 'shortint'])
        
        # Merge with Short Interest data (equivalent to Stata's "merge 1:1 gvkey time_avail_m using "$pathDataIntermediate/monthlyShortInterest", keep(match) nogenerate keepusing(shortint)")
        data = data.merge(shortint_data, on=['gvkey', 'time_avail_m'], how='inner')
        logger.info(f"After merging with Short Interest data: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating ShortInterest signal...")
        
        # Calculate ShortInterest (equivalent to Stata's "gen ShortInterest = shortint/shrout")
        data['ShortInterest'] = data['shortint'] / data['shrout']
        
        logger.info("Successfully calculated ShortInterest signal")
        
        # SAVE RESULTS
        logger.info("Saving ShortInterest predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'ShortInterest']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['ShortInterest'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "ShortInterest.csv"
        csv_data = output_data[['permno', 'yyyymm', 'ShortInterest']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved ShortInterest predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed ShortInterest predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct ShortInterest predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    shortinterest()
