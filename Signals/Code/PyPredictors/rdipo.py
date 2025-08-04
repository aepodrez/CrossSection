"""
Python equivalent of RDIPO.do
Generated from: RDIPO.do

Original Stata file: RDIPO.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def rdipo():
    """
    Python equivalent of RDIPO.do
    
    Constructs the RDIPO predictor signal for IPO without R&D.
    """
    logger.info("Constructing predictor signal: RDIPO...")
    
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
        required_vars = ['permno', 'time_avail_m', 'xrd']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # Load IPO dates data
        ipo_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/IPODates.csv")
        
        logger.info(f"Loading IPO dates from: {ipo_path}")
        
        if not ipo_path.exists():
            logger.error(f"IPODates not found: {ipo_path}")
            logger.error("Please run the IPO dates creation script first")
            return False
        
        ipo_data = pd.read_csv(ipo_path, usecols=['permno', 'IPOdate'])
        
        # Merge with IPO data
        data = data.merge(ipo_data, on='permno', how='left')
        logger.info(f"After merging with IPO data: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating RDIPO signal...")
        
        # Convert dates to datetime
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        data['IPOdate'] = pd.to_datetime(data['IPOdate'])
        
        # Calculate months since IPO (equivalent to Stata's "time_avail_m - IPOdate")
        data['months_since_ipo'] = ((data['time_avail_m'].dt.year - data['IPOdate'].dt.year) * 12 + 
                                   (data['time_avail_m'].dt.month - data['IPOdate'].dt.month))
        
        # Create tempipo indicator (equivalent to Stata's "gen tempipo = (time_avail_m - IPOdate <= 36) & (time_avail_m - IPOdate > 6)")
        data['tempipo'] = ((data['months_since_ipo'] <= 36) & (data['months_since_ipo'] > 6)).astype(int)
        
        # Set tempipo to 0 if IPOdate is missing (equivalent to Stata's "replace tempipo = 0 if IPOdate == .")
        data.loc[data['IPOdate'].isna(), 'tempipo'] = 0
        
        # Initialize RDIPO (equivalent to Stata's "gen RDIPO = 0")
        data['RDIPO'] = 0
        
        # Set RDIPO to 1 if tempipo == 1 and xrd == 0 (equivalent to Stata's "replace RDIPO = 1 if tempipo == 1 & xrd == 0")
        data.loc[(data['tempipo'] == 1) & (data['xrd'] == 0), 'RDIPO'] = 1
        
        logger.info("Successfully calculated RDIPO signal")
        
        # SAVE RESULTS
        logger.info("Saving RDIPO predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'RDIPO']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['RDIPO'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "RDIPO.csv"
        csv_data = output_data[['permno', 'yyyymm', 'RDIPO']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved RDIPO predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed RDIPO predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct RDIPO predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    rdipo()
