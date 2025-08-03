"""
Python equivalent of SurpriseRD.do
Generated from: SurpriseRD.do

Original Stata file: SurpriseRD.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def surprisedr():
    """
    Python equivalent of SurpriseRD.do
    
    Constructs the SurpriseRD predictor signal for unexpected R&D increase.
    """
    logger.info("Constructing predictor signal: SurpriseRD...")
    
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
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'xrd', 'revt', 'at']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # Sort data for time series operations (equivalent to Stata's "xtset permno time_avail_m")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate lagged values (equivalent to Stata's "l12." variables)
        data['xrd_lag12'] = data.groupby('permno')['xrd'].shift(12)
        data['at_lag12'] = data.groupby('permno')['at'].shift(12)
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating SurpriseRD signal...")
        
        # Create SurpriseRD indicator (equivalent to Stata's logic)
        data['SurpriseRD'] = np.nan
        
        # Condition for SurpriseRD = 1
        surprise_condition = (
            (data['xrd'] / data['revt'] > 0) &
            (data['xrd'] / data['at'] > 0) &
            (data['xrd'] / data['xrd_lag12'] > 1.05) &
            ((data['xrd'] / data['at']) / (data['xrd_lag12'] / data['at_lag12']) > 1.05) &
            (data['xrd'].notna()) &
            (data['xrd_lag12'].notna())
        )
        
        data.loc[surprise_condition, 'SurpriseRD'] = 1
        
        # Set to 0 for observations with non-missing R&D data but not meeting surprise conditions
        # (equivalent to Stata's "replace SurpriseRD = 0 if SurpriseRD==. & (xrd !=. & l12.xrd !=.)")
        zero_condition = (
            (data['SurpriseRD'].isna()) &
            (data['xrd'].notna()) &
            (data['xrd_lag12'].notna())
        )
        
        data.loc[zero_condition, 'SurpriseRD'] = 0
        
        logger.info("Successfully calculated SurpriseRD signal")
        
        # SAVE RESULTS
        logger.info("Saving SurpriseRD predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'SurpriseRD']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['SurpriseRD'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "SurpriseRD.csv"
        csv_data = output_data[['permno', 'yyyymm', 'SurpriseRD']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved SurpriseRD predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed SurpriseRD predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct SurpriseRD predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    surprisedr() 