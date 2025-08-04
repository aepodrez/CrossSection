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

def numeracrincrease():
    """
    Python equivalent of NumEarnIncrease.do
    
    Constructs the NumEarnIncrease predictor signal for number of consecutive earnings increases.
    """
    logger.info("Constructing predictor signal: NumEarnIncrease...")
    
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
        
        # Keep only observations with non-missing gvkey (equivalent to Stata's "keep if !mi(gvkey)")
        data = data.dropna(subset=['gvkey'])
        logger.info(f"After dropping missing gvkey: {len(data)} records")
        
        # Load quarterly Compustat data
        qcompustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_QCompustat.csv")
        
        logger.info(f"Loading quarterly Compustat data from: {qcompustat_path}")
        
        if not qcompustat_path.exists():
            logger.error(f"m_QCompustat not found: {qcompustat_path}")
            logger.error("Please run the quarterly Compustat data creation script first")
            return False
        
        qcompustat_data = pd.read_csv(qcompustat_path, usecols=['gvkey', 'time_avail_m', 'ibq'])
        
        # Merge with quarterly data
        data = data.merge(qcompustat_data, on=['gvkey', 'time_avail_m'], how='inner')
        logger.info(f"After merging with quarterly data: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating NumEarnIncrease signal...")
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate change in earnings (equivalent to Stata's "gen chearn = ibq - l12.ibq")
        data['chearn'] = data['ibq'] - data.groupby('permno')['ibq'].shift(12)
        
        # Calculate various lags of chearn for the consecutive increase logic
        for lag in [3, 6, 9, 12, 15, 18, 21, 24]:
            data[f'chearn_lag{lag}'] = data.groupby('permno')['chearn'].shift(lag)
        
        # Initialize nincr to 0 (equivalent to Stata's "gen nincr = 0")
        data['nincr'] = 0
        
        # Apply the consecutive earnings increase logic (equivalent to Stata's multiple replace statements)
        # nincr = 1 if chearn > 0 & l3.chearn <= 0
        data.loc[(data['chearn'] > 0) & (data['chearn_lag3'] <= 0), 'nincr'] = 1
        
        # nincr = 2 if chearn > 0 & l3.chearn > 0 & l6.chearn <= 0
        data.loc[(data['chearn'] > 0) & (data['chearn_lag3'] > 0) & (data['chearn_lag6'] <= 0), 'nincr'] = 2
        
        # nincr = 3 if chearn > 0 & l3.chearn > 0 & l6.chearn > 0 & l9.chearn <= 0
        data.loc[(data['chearn'] > 0) & (data['chearn_lag3'] > 0) & (data['chearn_lag6'] > 0) & (data['chearn_lag9'] <= 0), 'nincr'] = 3
        
        # nincr = 4 if chearn > 0 & l3.chearn > 0 & l6.chearn > 0 & l9.chearn > 0 & l12.chearn <= 0
        data.loc[(data['chearn'] > 0) & (data['chearn_lag3'] > 0) & (data['chearn_lag6'] > 0) & (data['chearn_lag9'] > 0) & (data['chearn_lag12'] <= 0), 'nincr'] = 4
        
        # nincr = 5 if chearn > 0 & l3.chearn > 0 & l6.chearn > 0 & l9.chearn > 0 & l12.chearn > 0 & l15.chearn <= 0
        data.loc[(data['chearn'] > 0) & (data['chearn_lag3'] > 0) & (data['chearn_lag6'] > 0) & (data['chearn_lag9'] > 0) & (data['chearn_lag12'] > 0) & (data['chearn_lag15'] <= 0), 'nincr'] = 5
        
        # nincr = 6 if chearn > 0 & l3.chearn > 0 & l6.chearn > 0 & l9.chearn > 0 & l12.chearn > 0 & l15.chearn > 0 & l18.chearn <= 0
        data.loc[(data['chearn'] > 0) & (data['chearn_lag3'] > 0) & (data['chearn_lag6'] > 0) & (data['chearn_lag9'] > 0) & (data['chearn_lag12'] > 0) & (data['chearn_lag15'] > 0) & (data['chearn_lag18'] <= 0), 'nincr'] = 6
        
        # nincr = 7 if chearn > 0 & l3.chearn > 0 & l6.chearn > 0 & l9.chearn > 0 & l12.chearn > 0 & l15.chearn > 0 & l18.chearn > 0 & l21.chearn <= 0
        data.loc[(data['chearn'] > 0) & (data['chearn_lag3'] > 0) & (data['chearn_lag6'] > 0) & (data['chearn_lag9'] > 0) & (data['chearn_lag12'] > 0) & (data['chearn_lag15'] > 0) & (data['chearn_lag18'] > 0) & (data['chearn_lag21'] <= 0), 'nincr'] = 7
        
        # nincr = 8 if chearn > 0 & l3.chearn > 0 & l6.chearn > 0 & l9.chearn > 0 & l12.chearn > 0 & l15.chearn > 0 & l18.chearn > 0 & l21.chearn > 0 & l24.chearn <= 0
        data.loc[(data['chearn'] > 0) & (data['chearn_lag3'] > 0) & (data['chearn_lag6'] > 0) & (data['chearn_lag9'] > 0) & (data['chearn_lag12'] > 0) & (data['chearn_lag15'] > 0) & (data['chearn_lag18'] > 0) & (data['chearn_lag21'] > 0) & (data['chearn_lag24'] <= 0), 'nincr'] = 8
        
        # Rename to NumEarnIncrease (equivalent to Stata's "rename nincr NumEarnIncrease")
        data['NumEarnIncrease'] = data['nincr']
        
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
        csv_output_path = predictors_dir / "NumEarnIncrease.csv"
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
    numeracrincrease() 