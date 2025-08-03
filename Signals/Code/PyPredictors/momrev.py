"""
Python equivalent of MomRev.do
Generated from: MomRev.do

Original Stata file: MomRev.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def momrev():
    """
    Python equivalent of MomRev.do
    
    Constructs the MomRev predictor signal for momentum and long-term reversal.
    """
    logger.info("Constructing predictor signal: MomRev...")
    
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
        logger.info("Calculating MomRev signal...")
        
        # Replace missing returns with 0 (equivalent to Stata's "replace ret = 0 if mi(ret)")
        data['ret'] = data['ret'].fillna(0)
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate lags of returns (1-5 months for Mom6m)
        for lag in range(1, 6):
            data[f'ret_lag{lag}'] = data.groupby('permno')['ret'].shift(lag)
        
        # Calculate 6-month momentum (equivalent to Stata's "gen Mom6m = ( (1+l.ret)*(1+l2.ret)*(1+l3.ret)*(1+l4.ret)*(1+l5.ret)) - 1")
        data['Mom6m'] = ((1 + data['ret_lag1']) * (1 + data['ret_lag2']) * 
                         (1 + data['ret_lag3']) * (1 + data['ret_lag4']) * 
                         (1 + data['ret_lag5'])) - 1
        
        # Calculate lags of returns (13-36 months for Mom36m)
        for lag in range(13, 37):
            data[f'ret_lag{lag}'] = data.groupby('permno')['ret'].shift(lag)
        
        # Calculate 36-month momentum (equivalent to Stata's complex multiplication)
        # Mom36m = (1+lag13)*(1+lag14)*...*(1+lag36) - 1
        data['Mom36m'] = 1
        for lag in range(13, 37):
            data['Mom36m'] = data['Mom36m'] * (1 + data[f'ret_lag{lag}'])
        
        # Subtract 1 to get the cumulative return
        data['Mom36m'] = data['Mom36m'] - 1
        
        # Create quintiles for Mom6m (equivalent to Stata's "egen tempMom6 = fastxtile(Mom6m), by(time_avail_m) n(5)")
        data['tempMom6'] = data.groupby('time_avail_m')['Mom6m'].transform(
            lambda x: pd.qcut(x, q=5, labels=False, duplicates='drop') + 1
        )
        
        # Create quintiles for Mom36m (equivalent to Stata's "egen tempMom36 = fastxtile(Mom36m), by(time_avail_m) n(5)")
        data['tempMom36'] = data.groupby('time_avail_m')['Mom36m'].transform(
            lambda x: pd.qcut(x, q=5, labels=False, duplicates='drop') + 1
        )
        
        # Create MomRev signal (equivalent to Stata's logic)
        # MomRev = 1 if tempMom6 == 5 & tempMom36 == 1 (high short-term momentum, low long-term momentum)
        # MomRev = 0 if tempMom6 == 1 & tempMom36 == 5 (low short-term momentum, high long-term momentum)
        data['MomRev'] = np.nan
        data.loc[(data['tempMom6'] == 5) & (data['tempMom36'] == 1), 'MomRev'] = 1
        data.loc[(data['tempMom6'] == 1) & (data['tempMom36'] == 5), 'MomRev'] = 0
        
        logger.info("Successfully calculated MomRev signal")
        
        # SAVE RESULTS
        logger.info("Saving MomRev predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'MomRev']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['MomRev'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "MomRev.csv"
        csv_data = output_data[['permno', 'yyyymm', 'MomRev']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved MomRev predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed MomRev predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct MomRev predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    momrev()
