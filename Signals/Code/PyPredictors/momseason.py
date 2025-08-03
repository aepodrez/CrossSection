"""
Python equivalent of MomSeason.do
Generated from: MomSeason.do

Original Stata file: MomSeason.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def momseason():
    """
    Python equivalent of MomSeason.do
    
    Constructs the MomSeason predictor signal for return seasonality (years 2 to 5).
    """
    logger.info("Constructing predictor signal: MomSeason...")
    
    try:
        # DATA LOAD
        # Load SignalMasterTable data
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load the required variables
        required_vars = ['permno', 'time_avail_m', 'ret']
        
        data = pd.read_csv(master_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating MomSeason signal...")
        
        # Replace missing returns with 0 (equivalent to Stata's "replace ret = 0 if mi(ret)")
        data['ret'] = data['ret'].fillna(0)
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate specific seasonal lags (equivalent to Stata's "foreach n of numlist 23(12)59")
        # This creates lags at 23, 35, 47, 59 months (every 12 months starting from 23)
        seasonal_lags = [23, 35, 47, 59]
        
        for lag in seasonal_lags:
            data[f'temp{lag}'] = data.groupby('permno')['ret'].shift(lag)
        
        # Calculate sum of seasonal returns (equivalent to Stata's "egen retTemp1 = rowtotal(temp*), missing")
        # This sums across the seasonal lag columns, treating missing values appropriately
        temp_cols = [f'temp{lag}' for lag in seasonal_lags]
        data['retTemp1'] = data[temp_cols].sum(axis=1)
        
        # Calculate count of non-missing seasonal returns (equivalent to Stata's "egen retTemp2 = rownonmiss(temp*)")
        data['retTemp2'] = data[temp_cols].count(axis=1)
        
        # Calculate MomSeason as the mean of seasonal returns (equivalent to Stata's "gen MomSeason = retTemp1/retTemp2")
        data['MomSeason'] = data['retTemp1'] / data['retTemp2']
        
        logger.info("Successfully calculated MomSeason signal")
        
        # SAVE RESULTS
        logger.info("Saving MomSeason predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'MomSeason']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['MomSeason'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "MomSeason.csv"
        csv_data = output_data[['permno', 'yyyymm', 'MomSeason']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved MomSeason predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed MomSeason predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct MomSeason predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    momseason()
