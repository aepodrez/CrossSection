"""
Python equivalent of ChForecastAccrual.do
Generated from: ChForecastAccrual.do

Original Stata file: ChForecastAccrual.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def chforecastacrual():
    """
    Python equivalent of ChForecastAccrual.do
    
    Constructs the ChForecastAccrual predictor signal for change in forecast and accrual.
    """
    logger.info("Constructing predictor signal: ChForecastAccrual...")
    
    try:
        # Prep IBES data
        logger.info("Preparing IBES data...")
        
        ibes_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/IBES_EPS_Unadj.csv")
        
        if not ibes_path.exists():
            logger.error(f"IBES data not found: {ibes_path}")
            logger.error("Please run the IBES data download scripts first")
            return False
        
        # Load IBES data and filter for fpi == "1"
        ibes_data = pd.read_csv(ibes_path)
        ibes_data = ibes_data[ibes_data['fpi'] == "1"]
        
        # Save temporary file
        temp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Temp/temp.csv")
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        ibes_data.to_csv(temp_path, index=False)
        
        # DATA LOAD
        # Load Compustat annual data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading Compustat annual data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"Compustat data not found: {compustat_path}")
            logger.error("Please run the Compustat data download scripts first")
            return False
        
        # Load the required variables
        required_vars = ['permno', 'time_avail_m', 'act', 'che', 'lct', 'dlc', 'txp', 'at']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates: keep first observation per permno-time_avail_m
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        
        # Merge with SignalMasterTable to get tickerIBES
        logger.info("Merging with SignalMasterTable...")
        
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        master_data = pd.read_csv(master_path, usecols=['permno', 'time_avail_m', 'tickerIBES'])
        
        # Merge data
        merged_data = data.merge(
            master_data, 
            on=['permno', 'time_avail_m'], 
            how='left'  # equivalent to Stata's keep(master match)
        )
        
        # Merge with IBES data
        logger.info("Merging with IBES data...")
        
        ibes_temp = pd.read_csv(temp_path, usecols=['tickerIBES', 'time_avail_m', 'meanest'])
        
        merged_data = merged_data.merge(
            ibes_temp, 
            on=['tickerIBES', 'time_avail_m'], 
            how='inner'  # equivalent to Stata's keep(master match)
        )
        
        logger.info(f"Successfully merged data: {len(merged_data)} observations")
        
        # Sort by permno and time_avail_m for lag calculations
        merged_data = merged_data.sort_values(['permno', 'time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating ChForecastAccrual signal...")
        
        # Calculate 12-month lags for required variables
        lag_vars = ['act', 'che', 'lct', 'dlc', 'txp', 'at']
        for var in lag_vars:
            merged_data[f'{var}_lag12'] = merged_data.groupby('permno')[var].shift(12)
        
        # Calculate tempAccruals (equivalent to Stata's complex formula)
        merged_data['tempAccruals'] = (
            ((merged_data['act'] - merged_data['act_lag12']) - 
             (merged_data['che'] - merged_data['che_lag12']) - 
             ((merged_data['lct'] - merged_data['lct_lag12']) - 
              (merged_data['dlc'] - merged_data['dlc_lag12']) - 
              (merged_data['txp'] - merged_data['txp_lag12'])))
        ) / ((merged_data['at'] + merged_data['at_lag12']) / 2)
        
        # Calculate quintiles (equivalent to Stata's "egen tempsort = fastxtile(tempAccruals), by(time_avail_m) n(2)")
        merged_data['tempsort'] = merged_data.groupby('time_avail_m')['tempAccruals'].transform(
            lambda x: pd.qcut(x, q=2, labels=False, duplicates='drop') + 1
        )
        
        # Calculate lag of meanest
        merged_data['meanest_lag'] = merged_data.groupby('permno')['meanest'].shift(1)
        
        # Calculate ChForecastAccrual (equivalent to Stata's conditional logic)
        merged_data['ChForecastAccrual'] = np.nan
        
        # Set to 1 if forecast increased
        mask1 = (merged_data['meanest'] > merged_data['meanest_lag']) & \
                (merged_data['meanest'].notna()) & \
                (merged_data['meanest_lag'].notna())
        merged_data.loc[mask1, 'ChForecastAccrual'] = 1
        
        # Set to 0 if forecast decreased
        mask2 = (merged_data['meanest'] < merged_data['meanest_lag']) & \
                (merged_data['meanest'].notna()) & \
                (merged_data['meanest_lag'].notna())
        merged_data.loc[mask2, 'ChForecastAccrual'] = 0
        
        # Set to missing if in bottom quintile of accruals
        merged_data.loc[merged_data['tempsort'] == 1, 'ChForecastAccrual'] = np.nan
        
        logger.info("Successfully calculated ChForecastAccrual signal")
        
        # SAVE RESULTS
        logger.info("Saving ChForecastAccrual predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = merged_data[['permno', 'time_avail_m', 'ChForecastAccrual']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['ChForecastAccrual'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "ChForecastAccrual.csv"
        csv_data = output_data[['permno', 'yyyymm', 'ChForecastAccrual']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved ChForecastAccrual predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed ChForecastAccrual predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct ChForecastAccrual predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    chforecastacrual() 