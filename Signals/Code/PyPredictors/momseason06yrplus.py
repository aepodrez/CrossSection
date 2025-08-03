"""
Python equivalent of MomSeason06YrPlus.do
Generated from: MomSeason06YrPlus.do

Original Stata file: MomSeason06YrPlus.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def momseason06yrplus():
    """
    Python equivalent of MomSeason06YrPlus.do
    
    Constructs the MomSeason06YrPlus predictor signal for return seasonality (years 6-10).
    """
    logger.info("Constructing predictor signal: MomSeason06YrPlus...")
    
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
        logger.info("Calculating MomSeason06YrPlus signal...")
        
        # Replace missing returns with 0 (equivalent to Stata's "replace ret = 0 if mi(ret)")
        data['ret'] = data['ret'].fillna(0)
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate specific seasonal lags (equivalent to Stata's "foreach n of numlist 71(12)120")
        # This creates lags at 71, 83, 95, 107, 119 months (every 12 months starting from 71)
        seasonal_lags = [71, 83, 95, 107, 119]
        
        for lag in seasonal_lags:
            data[f'temp{lag}'] = data.groupby('permno')['ret'].shift(lag)
        
        # Calculate sum of seasonal returns (equivalent to Stata's "egen retTemp1 = rowtotal(temp*), missing")
        # This sums across the seasonal lag columns, treating missing values appropriately
        temp_cols = [f'temp{lag}' for lag in seasonal_lags]
        data['retTemp1'] = data[temp_cols].sum(axis=1)
        
        # Calculate count of non-missing seasonal returns (equivalent to Stata's "egen retTemp2 = rownonmiss(temp*)")
        data['retTemp2'] = data[temp_cols].count(axis=1)
        
        # Calculate MomSeason06YrPlus as the mean of seasonal returns (equivalent to Stata's "gen MomSeason06YrPlus = retTemp1/retTemp2")
        data['MomSeason06YrPlus'] = data['retTemp1'] / data['retTemp2']
        
        logger.info("Successfully calculated MomSeason06YrPlus signal")
        
        # SAVE RESULTS
        logger.info("Saving MomSeason06YrPlus predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'MomSeason06YrPlus']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['MomSeason06YrPlus'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "MomSeason06YrPlus.csv"
        csv_data = output_data[['permno', 'yyyymm', 'MomSeason06YrPlus']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved MomSeason06YrPlus predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed MomSeason06YrPlus predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct MomSeason06YrPlus predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    momseason06yrplus()
