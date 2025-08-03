"""
Python equivalent of MomVol.do
Generated from: MomVol.do

Original Stata file: MomVol.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def momvol():
    """
    Python equivalent of MomVol.do
    
    Constructs the MomVol predictor signal for momentum among high volume stocks.
    """
    logger.info("Constructing predictor signal: MomVol...")
    
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
        
        # Load monthlyCRSP data for volume
        crsp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/monthlyCRSP.csv")
        
        logger.info(f"Loading monthlyCRSP from: {crsp_path}")
        
        if not crsp_path.exists():
            logger.error(f"monthlyCRSP not found: {crsp_path}")
            logger.error("Please run the monthlyCRSP creation script first")
            return False
        
        # Load volume data
        crsp_data = pd.read_csv(crsp_path, usecols=['permno', 'time_avail_m', 'vol'])
        logger.info(f"Successfully loaded {len(crsp_data)} CRSP records")
        
        # Merge with SignalMasterTable
        data = data.merge(crsp_data, on=['permno', 'time_avail_m'], how='inner')
        logger.info(f"After merge: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating MomVol signal...")
        
        # Replace negative volume with missing (equivalent to Stata's "replace vol = . if vol <0")
        data['vol'] = data['vol'].where(data['vol'] >= 0, np.nan)
        
        # Replace missing returns with 0 (equivalent to Stata's "replace ret = 0 if mi(ret)")
        data['ret'] = data['ret'].fillna(0)
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate 6-month momentum (equivalent to Stata's "gen Mom6m = ( (1+l.ret)*(1+l2.ret)*(1+l3.ret)*(1+l4.ret)*(1+l5.ret)) - 1")
        # Calculate lags of returns (1-5 months for Mom6m)
        for lag in range(1, 6):
            data[f'ret_lag{lag}'] = data.groupby('permno')['ret'].shift(lag)
        
        data['Mom6m'] = ((1 + data['ret_lag1']) * (1 + data['ret_lag2']) * 
                         (1 + data['ret_lag3']) * (1 + data['ret_lag4']) * 
                         (1 + data['ret_lag5'])) - 1
        
        # Create deciles for momentum (equivalent to Stata's "egen catMom = fastxtile(Mom6m), by(time_avail_m) n(10)")
        data['catMom'] = data.groupby('time_avail_m')['Mom6m'].transform(
            lambda x: pd.qcut(x, q=10, labels=False, duplicates='drop') + 1
        )
        
        # Calculate 6-month rolling mean of volume (equivalent to Stata's "asrol vol, gen(temp) window(time_avail_m 6) min(5) stat(mean)")
        def rolling_mean_vol(group):
            return group['vol'].rolling(window=6, min_periods=5).mean()
        
        data['temp'] = data.groupby('permno').apply(rolling_mean_vol).reset_index(level=0, drop=True)
        
        # Create terciles for volume (equivalent to Stata's "egen catVol = fastxtile(temp), by(time_avail_m) n(3)")
        data['catVol'] = data.groupby('time_avail_m')['temp'].transform(
            lambda x: pd.qcut(x, q=3, labels=False, duplicates='drop') + 1
        )
        
        # Create MomVol signal (equivalent to Stata's "gen MomVol = catMom if catVol == 3")
        # Only keep momentum for high volume stocks (catVol == 3)
        data['MomVol'] = np.nan
        data.loc[data['catVol'] == 3, 'MomVol'] = data.loc[data['catVol'] == 3, 'catMom']
        
        # Filter: remove first 24 observations for each permno (equivalent to Stata's "bys permno (time_avail_m): replace MomVol = . if _n < 24")
        data['obs_count'] = data.groupby('permno').cumcount() + 1
        data.loc[data['obs_count'] < 24, 'MomVol'] = np.nan
        
        logger.info("Successfully calculated MomVol signal")
        
        # SAVE RESULTS
        logger.info("Saving MomVol predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'MomVol']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['MomVol'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "MomVol.csv"
        csv_data = output_data[['permno', 'yyyymm', 'MomVol']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved MomVol predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed MomVol predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct MomVol predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    momvol()
