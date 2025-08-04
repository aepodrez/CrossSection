"""
Python equivalent of FirmAgeMom.do
Generated from: FirmAgeMom.do

Original Stata file: FirmAgeMom.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def firmagemom():
    """
    Python equivalent of FirmAgeMom.do
    
    Constructs the FirmAgeMom predictor signal for firm age momentum.
    """
    logger.info("Constructing predictor signal: FirmAgeMom...")
    
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
        required_vars = ['permno', 'time_avail_m', 'ret', 'prc']
        
        data = pd.read_csv(master_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating FirmAgeMom signal...")
        
        # Replace missing returns with 0 (equivalent to Stata's "replace ret = 0 if mi(ret)")
        data['ret'] = data['ret'].fillna(0)
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate tempage as observation number within each permno (equivalent to Stata's "bys permno (time_avail_m): gen tempage = _n")
        data['tempage'] = data.groupby('permno').cumcount() + 1
        
        # Drop observations with price < 5 or age < 12 (equivalent to Stata's "drop if abs(prc) < 5 | tempage < 12")
        data = data[(data['prc'].abs() >= 5) & (data['tempage'] >= 12)]
        logger.info(f"After filtering for price >= 5 and age >= 12: {len(data)} observations")
        
        # Calculate lags of returns (1-5 months)
        for lag in range(1, 6):
            data[f'ret_lag{lag}'] = data.groupby('permno')['ret'].shift(lag)
        
        # Calculate FirmAgeMom (equivalent to Stata's "gen FirmAgeMom = ( (1+l.ret)*(1+l2.ret)*(1+l3.ret)*(1+l4.ret)*(1+l5.ret)) - 1")
        data['FirmAgeMom'] = ((1 + data['ret_lag1']) * (1 + data['ret_lag2']) * 
                             (1 + data['ret_lag3']) * (1 + data['ret_lag4']) * 
                             (1 + data['ret_lag5'])) - 1
        
        # Create quintiles of tempage by time_avail_m (equivalent to Stata's "egen temp = fastxtile(tempage), by(time_avail_m) n(5)")
        data['temp'] = data.groupby('time_avail_m')['tempage'].transform(
            lambda x: pd.qcut(x, q=5, labels=False, duplicates='drop') + 1
        )
        
        # Set FirmAgeMom to missing if not in bottom quintile (equivalent to Stata's "replace FirmAgeMom =. if temp > 1 & temp !=.")
        data.loc[(data['temp'] > 1) & (data['temp'].notna()), 'FirmAgeMom'] = np.nan
        
        logger.info("Successfully calculated FirmAgeMom signal")
        
        # SAVE RESULTS
        logger.info("Saving FirmAgeMom predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'FirmAgeMom']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['FirmAgeMom'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "FirmAgeMom.csv"
        csv_data = output_data[['permno', 'yyyymm', 'FirmAgeMom']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved FirmAgeMom predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed FirmAgeMom predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct FirmAgeMom predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    firmagemom()
