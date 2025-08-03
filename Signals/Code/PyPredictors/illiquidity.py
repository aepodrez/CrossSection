"""
Python equivalent of Illiquidity.do
Generated from: Illiquidity.do

Original Stata file: Illiquidity.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def illiquidity():
    """
    Python equivalent of Illiquidity.do
    
    Constructs the Illiquidity predictor signal.
    """
    logger.info("Constructing predictor signal: Illiquidity...")
    
    try:
        # DATA LOAD
        # Load daily CRSP data
        crsp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/dailyCRSP.csv")
        
        logger.info(f"Loading daily CRSP data from: {crsp_path}")
        
        if not crsp_path.exists():
            logger.error(f"Daily CRSP file not found: {crsp_path}")
            logger.error("Please run the CRSP data download scripts first")
            return False
        
        # Load the required variables
        required_vars = ['permno', 'time_d', 'ret', 'prc', 'vol']
        
        data = pd.read_csv(crsp_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating Illiquidity signal...")
        
        # Create time_avail_m (equivalent to Stata's "gen time_avail_m = mofd(time_d)")
        data['time_d'] = pd.to_datetime(data['time_d'])
        data['time_avail_m'] = data['time_d'].dt.to_period('M').dt.to_timestamp()
        
        # Calculate daily illiquidity (equivalent to Stata's "gen double ill = abs(ret)/(abs(prc)*vol)")
        data['ill'] = data['ret'].abs() / (data['prc'].abs() * data['vol'])
        
        # Collapse to monthly by taking mean (equivalent to Stata's "gcollapse (mean) ill, by(permno time_avail_m)")
        data = data.groupby(['permno', 'time_avail_m'])['ill'].mean().reset_index()
        
        logger.info(f"After collapsing to monthly: {len(data)} observations")
        
        # Sort data for time series operations (equivalent to Stata's "xtset permno time_avail_m")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate 12-month moving average (equivalent to Stata's "gen Illiquidity = (ill + l.ill + l2.ill + ... + l11.ill)/12")
        data['Illiquidity'] = data.groupby('permno')['ill'].rolling(
            window=12, min_periods=1
        ).mean().reset_index(0, drop=True)
        
        logger.info("Successfully calculated Illiquidity signal")
        
        # SAVE RESULTS
        logger.info("Saving Illiquidity predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'Illiquidity']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['Illiquidity'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "Illiquidity.csv"
        csv_data = output_data[['permno', 'yyyymm', 'Illiquidity']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved Illiquidity predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed Illiquidity predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct Illiquidity predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    illiquidity()
