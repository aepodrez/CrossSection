"""
Python equivalent of CompEquIss.do
Generated from: CompEquIss.do

Original Stata file: CompEquIss.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def compequiss():
    """
    Python equivalent of CompEquIss.do
    
    Constructs the CompEquIss predictor signal for composite equity issuance.
    """
    logger.info("Constructing predictor signal: CompEquIss...")
    
    try:
        # DATA LOAD
        # Load SignalMasterTable
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load required variables from SignalMasterTable
        master_vars = ['permno', 'time_avail_m', 'ret', 'mve_c']
        data = pd.read_csv(master_path, usecols=master_vars)
        
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Sort by permno and time_avail_m for calculations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating CompEquIss signal...")
        
        # Calculate tempIdx (equivalent to Stata's cumulative return index)
        data['tempIdx'] = np.nan
        
        for permno in data['permno'].unique():
            permno_data = data[data['permno'] == permno].copy()
            permno_data = permno_data.sort_values('time_avail_m')
            
            # Set first observation to 1
            permno_data.iloc[0, permno_data.columns.get_loc('tempIdx')] = 1
            
            # Calculate cumulative return index
            for i in range(1, len(permno_data)):
                prev_idx = permno_data.iloc[i-1]['tempIdx']
                curr_ret = permno_data.iloc[i]['ret']
                permno_data.iloc[i, permno_data.columns.get_loc('tempIdx')] = (1 + curr_ret) * prev_idx
            
            data.loc[data['permno'] == permno, 'tempIdx'] = permno_data['tempIdx'].values
        
        # Calculate 60-month lag of tempIdx
        data['tempIdx_lag60'] = data.groupby('permno')['tempIdx'].shift(60)
        
        # Calculate tempBH (equivalent to Stata's "gen tempBH = (tempIdx - l60.tempIdx)/l60.tempIdx")
        data['tempBH'] = (data['tempIdx'] - data['tempIdx_lag60']) / data['tempIdx_lag60']
        
        # Calculate 60-month lag of mve_c
        data['mve_c_lag60'] = data.groupby('permno')['mve_c'].shift(60)
        
        # Calculate CompEquIss (equivalent to Stata's "gen CompEquIss = log(mve_c/l60.mve_c) - tempBH")
        data['CompEquIss'] = np.log(data['mve_c'] / data['mve_c_lag60']) - data['tempBH']
        
        logger.info("Successfully calculated CompEquIss signal")
        
        # SAVE RESULTS
        logger.info("Saving CompEquIss predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'CompEquIss']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['CompEquIss'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "compequiss.csv"
        csv_data = output_data[['permno', 'yyyymm', 'CompEquIss']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved CompEquIss predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed CompEquIss predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct CompEquIss predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    compequiss()
