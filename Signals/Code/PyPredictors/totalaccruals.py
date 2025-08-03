"""
Python equivalent of TotalAccruals.do
Generated from: TotalAccruals.do

Original Stata file: TotalAccruals.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def totalaccruals():
    """
    Python equivalent of TotalAccruals.do
    
    Constructs the TotalAccruals predictor signal for total accruals.
    """
    logger.info("Constructing predictor signal: TotalAccruals...")
    
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
        required_vars = ['permno', 'time_avail_m', 'ivao', 'ivst', 'dltt', 'dlc', 'pstk', 'sstk', 'prstkc', 'dv', 
                        'act', 'che', 'lct', 'at', 'lt', 'ni', 'oancf', 'ivncf', 'fincf']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # Sort data for time series operations (equivalent to Stata's "xtset permno time_avail_m")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating TotalAccruals signal...")
        
        # Create temporary variables with missing values replaced by 0 (equivalent to Stata's foreach loop)
        variables = ['ivao', 'ivst', 'dltt', 'dlc', 'pstk', 'sstk', 'prstkc', 'dv']
        
        for var in variables:
            data[f'temp{var}'] = data[var].fillna(0)
        
        # Calculate working capital accruals (equivalent to Stata's "gen tempWc = (act - che) - (lct - tempdlc)")
        data['tempWc'] = (data['act'] - data['che']) - (data['lct'] - data['tempdlc'])
        
        # Calculate non-current accruals (equivalent to Stata's "gen tempNc = (at - act - tempivao) - (lt - tempdlc - tempdltt)")
        data['tempNc'] = (data['at'] - data['act'] - data['tempivao']) - (data['lt'] - data['tempdlc'] - data['tempdltt'])
        
        # Calculate financial accruals (equivalent to Stata's "gen tempFi = (tempivst + tempivao) - (tempdltt + tempdlc + temppstk)")
        data['tempFi'] = (data['tempivst'] + data['tempivao']) - (data['tempdltt'] + data['tempdlc'] + data['temppstk'])
        
        # Convert time_avail_m to year (equivalent to Stata's "gen year = yofd(dofm(time_avail_m))")
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        data['year'] = data['time_avail_m'].dt.year
        
        # Calculate lagged values (equivalent to Stata's "l12." variables)
        data['tempWc_lag12'] = data.groupby('permno')['tempWc'].shift(12)
        data['tempNc_lag12'] = data.groupby('permno')['tempNc'].shift(12)
        data['tempFi_lag12'] = data.groupby('permno')['tempFi'].shift(12)
        data['at_lag12'] = data.groupby('permno')['at'].shift(12)
        
        # Calculate TotalAccruals based on year
        data['TotalAccruals'] = np.nan
        
        # For years <= 1989 (equivalent to Stata's logic)
        pre_1990_condition = data['year'] <= 1989
        data.loc[pre_1990_condition, 'TotalAccruals'] = (
            (data['tempWc'] - data['tempWc_lag12']) + 
            (data['tempNc'] - data['tempNc_lag12']) + 
            (data['tempFi'] - data['tempFi_lag12'])
        )
        
        # For years > 1989 (equivalent to Stata's logic)
        post_1989_condition = data['year'] > 1989
        data.loc[post_1989_condition, 'TotalAccruals'] = (
            data['ni'] - (data['oancf'] + data['ivncf'] + data['fincf']) + 
            (data['sstk'] - data['prstkc'] - data['dv'])
        )
        
        # Scale by lagged total assets (equivalent to Stata's "replace TotalAccruals = TotalAccruals/l12.at")
        data['TotalAccruals'] = data['TotalAccruals'] / data['at_lag12']
        
        logger.info("Successfully calculated TotalAccruals signal")
        
        # SAVE RESULTS
        logger.info("Saving TotalAccruals predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'TotalAccruals']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['TotalAccruals'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "TotalAccruals.csv"
        csv_data = output_data[['permno', 'yyyymm', 'TotalAccruals']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved TotalAccruals predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed TotalAccruals predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct TotalAccruals predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    totalaccruals()
