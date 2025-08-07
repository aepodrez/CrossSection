"""
Python equivalent of NumEarnIncrease.do
Generated from: NumEarnIncrease.do

Original Stata file: NumEarnIncrease.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def numearnincrease():
    """
    Python equivalent of NumEarnIncrease.do
    
    Constructs the NumEarnIncrease predictor signal for number of consecutive earnings increases.
    """
    logger.info("Constructing predictor signal: NumEarnIncrease...")
    
    try:
        # DATA LOAD
        # Load SignalMasterTable data
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable data from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load required variables from SignalMasterTable
        required_vars = ['permno', 'gvkey', 'time_avail_m']
        
        logger.info("Reading SignalMasterTable data...")
        data = pd.read_csv(master_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records from SignalMasterTable")
        
        # Keep only observations with non-missing gvkey (equivalent to Stata's "keep if !mi(gvkey)")
        data = data.dropna(subset=['gvkey'])
        logger.info(f"After filtering for non-missing gvkey: {len(data)} records")
        
        # Load quarterly Compustat data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_QCompustat.csv")
        
        logger.info(f"Loading quarterly Compustat data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"Quarterly Compustat data not found: {compustat_path}")
            logger.error("Please run the quarterly Compustat data download scripts first")
            return False
        
        # Load quarterly Compustat data with ibq variable
        compustat_data = pd.read_csv(compustat_path, usecols=['gvkey', 'time_avail_m', 'ibq'])
        logger.info(f"Successfully loaded {len(compustat_data)} quarterly Compustat records")
        
        # Merge with quarterly Compustat data (equivalent to Stata's "merge 1:1 gvkey time_avail_m using ...")
        logger.info("Merging with quarterly Compustat data...")
        data = data.merge(compustat_data, on=['gvkey', 'time_avail_m'], how='inner')
        logger.info(f"After merging with quarterly Compustat data: {len(data)} records")
        
        # Sort by permno and time_avail_m for lag calculations (equivalent to Stata's "xtset permno time_avail_m")
        logger.info("Sorting data by permno and time_avail_m...")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating NumEarnIncrease signal...")
        
        # Calculate change in earnings (equivalent to Stata's "gen chearn = ibq - l12.ibq")
        data['ibq_lag12'] = data.groupby('permno')['ibq'].shift(12)
        data['chearn'] = data['ibq'] - data['ibq_lag12']
        
        # Calculate lagged values of chearn for consecutive increase logic
        data['chearn_lag3'] = data.groupby('permno')['chearn'].shift(3)
        data['chearn_lag6'] = data.groupby('permno')['chearn'].shift(6)
        data['chearn_lag9'] = data.groupby('permno')['chearn'].shift(9)
        data['chearn_lag12'] = data.groupby('permno')['chearn'].shift(12)
        data['chearn_lag15'] = data.groupby('permno')['chearn'].shift(15)
        data['chearn_lag18'] = data.groupby('permno')['chearn'].shift(18)
        data['chearn_lag21'] = data.groupby('permno')['chearn'].shift(21)
        data['chearn_lag24'] = data.groupby('permno')['chearn'].shift(24)
        
        # Initialize NumEarnIncrease (equivalent to Stata's "gen nincr = 0")
        data['NumEarnIncrease'] = 0
        
        # Apply consecutive earnings increase logic (equivalent to Stata's replace statements)
        # 1 quarter increase
        data.loc[(data['chearn'] > 0) & (data['chearn_lag3'] <= 0), 'NumEarnIncrease'] = 1
        
        # 2 quarters increase
        data.loc[(data['chearn'] > 0) & (data['chearn_lag3'] > 0) & (data['chearn_lag6'] <= 0), 'NumEarnIncrease'] = 2
        
        # 3 quarters increase
        data.loc[(data['chearn'] > 0) & (data['chearn_lag3'] > 0) & (data['chearn_lag6'] > 0) & (data['chearn_lag9'] <= 0), 'NumEarnIncrease'] = 3
        
        # 4 quarters increase
        data.loc[(data['chearn'] > 0) & (data['chearn_lag3'] > 0) & (data['chearn_lag6'] > 0) & (data['chearn_lag9'] > 0) & (data['chearn_lag12'] <= 0), 'NumEarnIncrease'] = 4
        
        # 5 quarters increase
        data.loc[(data['chearn'] > 0) & (data['chearn_lag3'] > 0) & (data['chearn_lag6'] > 0) & (data['chearn_lag9'] > 0) & (data['chearn_lag12'] > 0) & (data['chearn_lag15'] <= 0), 'NumEarnIncrease'] = 5
        
        # 6 quarters increase
        data.loc[(data['chearn'] > 0) & (data['chearn_lag3'] > 0) & (data['chearn_lag6'] > 0) & (data['chearn_lag9'] > 0) & (data['chearn_lag12'] > 0) & (data['chearn_lag15'] > 0) & (data['chearn_lag18'] <= 0), 'NumEarnIncrease'] = 6
        
        # 7 quarters increase
        data.loc[(data['chearn'] > 0) & (data['chearn_lag3'] > 0) & (data['chearn_lag6'] > 0) & (data['chearn_lag9'] > 0) & (data['chearn_lag12'] > 0) & (data['chearn_lag15'] > 0) & (data['chearn_lag18'] > 0) & (data['chearn_lag21'] <= 0), 'NumEarnIncrease'] = 7
        
        # 8 quarters increase
        data.loc[(data['chearn'] > 0) & (data['chearn_lag3'] > 0) & (data['chearn_lag6'] > 0) & (data['chearn_lag9'] > 0) & (data['chearn_lag12'] > 0) & (data['chearn_lag15'] > 0) & (data['chearn_lag18'] > 0) & (data['chearn_lag21'] > 0) & (data['chearn_lag24'] <= 0), 'NumEarnIncrease'] = 8
        
        logger.info("Successfully calculated NumEarnIncrease signal")
        
        # SAVE RESULTS
        logger.info("Saving NumEarnIncrease predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'NumEarnIncrease']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['NumEarnIncrease'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "numearnincrease.csv"
        csv_data = output_data[['permno', 'yyyymm', 'NumEarnIncrease']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved NumEarnIncrease predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed NumEarnIncrease predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct NumEarnIncrease predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    numearnincrease()
