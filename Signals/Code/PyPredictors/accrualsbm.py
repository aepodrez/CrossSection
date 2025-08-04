"""
Python equivalent of AccrualsBM.do
Generated from: AccrualsBM.do

Original Stata file: AccrualsBM.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def accrualsbm():
    """
    Python equivalent of AccrualsBM.do
    
    Constructs the AccrualsBM predictor signal combining accruals and book-to-market.
    """
    logger.info("Constructing predictor signal: AccrualsBM...")
    
    try:
        # DATA LOAD
        # Load Compustat monthly data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading Compustat monthly data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"Input file not found: {compustat_path}")
            logger.error("Please run the Compustat data download scripts first")
            return False
        
        # Load the required variables
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'ceq', 'act', 'che', 'lct', 'dlc', 'txp', 'at']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        initial_count = len(data)
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        logger.info(f"Removed {initial_count - len(data)} duplicate observations")
        
        # Load SignalMasterTable for market value
        signal_master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {signal_master_path}")
        
        if not signal_master_path.exists():
            logger.error(f"SignalMasterTable not found: {signal_master_path}")
            return False
        
        signal_master = pd.read_csv(signal_master_path, usecols=['permno', 'time_avail_m', 'mve_c'])
        logger.info(f"Successfully loaded SignalMasterTable with {len(signal_master)} records")
        
        # Merge with SignalMasterTable (equivalent to Stata's "merge 1:1 permno time_avail_m using SignalMasterTable")
        data = data.merge(signal_master, on=['permno', 'time_avail_m'], how='inner')
        logger.info(f"After merge: {len(data)} records")
        
        # Sort by permno and time_avail_m for lag calculations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Convert time_avail_m to datetime for proper lagging
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Constructing AccrualsBM signal...")
        
        # Calculate Book-to-Market ratio (equivalent to Stata's "gen BM = log(ceq/mve_c)")
        data['BM'] = np.log(data['ceq'] / data['mve_c'])
        
        # Calculate 12-month lags for accruals components
        lag_vars = ['act', 'che', 'lct', 'dlc', 'txp', 'at']
        
        for var in lag_vars:
            data[f'{var}_lag12'] = data.groupby('permno')[var].shift(12)
        
        # Calculate temporary accruals (equivalent to Stata's tempacc calculation)
        data['tempacc'] = (
            ((data['act'] - data['act_lag12']) - (data['che'] - data['che_lag12']) - 
             ((data['lct'] - data['lct_lag12']) - (data['dlc'] - data['dlc_lag12']) - 
              (data['txp'] - data['txp_lag12']))) / 
            ((data['at'] + data['at_lag12'])/2)
        )
        
        # Create quintiles for BM and accruals (equivalent to Stata's fastxtile)
        data['tempqBM'] = data.groupby('time_avail_m')['BM'].transform(
            lambda x: pd.qcut(x, q=5, labels=False, duplicates='drop') + 1
        )
        
        data['tempqAcc'] = data.groupby('time_avail_m')['tempacc'].transform(
            lambda x: pd.qcut(x, q=5, labels=False, duplicates='drop') + 1
        )
        
        # Construct AccrualsBM signal (equivalent to Stata's conditional assignment)
        data['AccrualsBM'] = np.nan
        
        # High BM (quintile 5) and low accruals (quintile 1) = 1
        data.loc[(data['tempqBM'] == 5) & (data['tempqAcc'] == 1), 'AccrualsBM'] = 1
        
        # Low BM (quintile 1) and high accruals (quintile 5) = 0
        data.loc[(data['tempqBM'] == 1) & (data['tempqAcc'] == 5), 'AccrualsBM'] = 0
        
        # Set to missing if ceq < 0 (equivalent to Stata's "replace AccrualsBM = . if ceq <0")
        data.loc[data['ceq'] < 0, 'AccrualsBM'] = np.nan
        
        logger.info("Successfully calculated AccrualsBM signal")
        
        # SAVE RESULTS
        logger.info("Saving AccrualsBM predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'AccrualsBM']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['AccrualsBM'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "AccrualsBM.csv"
        csv_data = output_data[['permno', 'yyyymm', 'AccrualsBM']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved AccrualsBM predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed AccrualsBM predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct AccrualsBM predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    accrualsbm()
