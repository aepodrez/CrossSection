"""
Python equivalent of OPLeverage.do
Generated from: OPLeverage.do

Original Stata file: OPLeverage.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def opleverage():
    """
    Python equivalent of OPLeverage.do
    
    Constructs the OPLeverage predictor signal for operating leverage.
    """
    logger.info("Constructing predictor signal: OPLeverage...")
    
    try:
        # DATA LOAD
        # Load Compustat data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading Compustat data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"m_aCompustat not found: {compustat_path}")
            logger.error("Please run the Compustat data creation script first")
            return False
        
        # Load required variables
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'xsga', 'cogs', 'at']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating OPLeverage signal...")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'])
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Create temporary SG&A variable (equivalent to Stata's "gen tempxsga = 0" and "replace tempxsga = xsga if xsga !=.")
        data['tempxsga'] = 0
        data.loc[data['xsga'].notna(), 'tempxsga'] = data.loc[data['xsga'].notna(), 'xsga']
        
        # Calculate OPLeverage (equivalent to Stata's "gen OPLeverage = (tempxsga + cogs)/at")
        data['OPLeverage'] = (data['tempxsga'] + data['cogs']) / data['at']
        
        logger.info("Successfully calculated OPLeverage signal")
        
        # SAVE RESULTS
        logger.info("Saving OPLeverage predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'OPLeverage']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['OPLeverage'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "OPLeverage.csv"
        csv_data = output_data[['permno', 'yyyymm', 'OPLeverage']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved OPLeverage predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed OPLeverage predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct OPLeverage predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    opleverage()
