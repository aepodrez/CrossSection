"""
Python equivalent of ShareVol.do
Generated from: ShareVol.do

Original Stata file: ShareVol.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def sharevol():
    """
    Python equivalent of ShareVol.do
    
    Constructs the ShareVol predictor signal for share volume.
    """
    logger.info("Constructing predictor signal: ShareVol...")
    
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
        required_vars = ['permno', 'time_avail_m', 'sicCRSP', 'exchcd']
        
        data = pd.read_csv(master_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Load monthly CRSP data
        crsp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/monthlyCRSP.csv")
        
        logger.info(f"Loading monthly CRSP data from: {crsp_path}")
        
        if not crsp_path.exists():
            logger.error(f"monthlyCRSP not found: {crsp_path}")
            logger.error("Please run the monthly CRSP data creation script first")
            return False
        
        crsp_data = pd.read_csv(crsp_path, usecols=['permno', 'time_avail_m', 'shrout', 'vol'])
        
        # Merge with CRSP data
        data = data.merge(crsp_data, on=['permno', 'time_avail_m'], how='inner')
        logger.info(f"After merging with CRSP data: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating ShareVol signal...")
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate lags (equivalent to Stata's "l1." and "l2." variables)
        data['vol_lag1'] = data.groupby('permno')['vol'].shift(1)
        data['vol_lag2'] = data.groupby('permno')['vol'].shift(2)
        data['shrout_lag1'] = data.groupby('permno')['shrout'].shift(1)
        
        # Calculate tempShareVol (equivalent to Stata's "gen tempShareVol = (vol + l1.vol + l2.vol)/(3*shrout)*100")
        data['tempShareVol'] = (data['vol'] + data['vol_lag1'] + data['vol_lag2']) / (3 * data['shrout']) * 100
        
        # Drop if shrout changes in last 3 months
        # Calculate dshrout (equivalent to Stata's "gen dshrout = shrout != l1.shrout")
        data['dshrout'] = data['shrout'] != data['shrout_lag1']
        
        # Set to no change in first month (equivalent to Stata's "bys permno (time_avail_m): replace dshrout = 0 if _n == 1")
        data.loc[data.groupby('permno').cumcount() == 0, 'dshrout'] = False
        
        # Calculate lags of dshrout
        data['dshrout_lag1'] = data.groupby('permno')['dshrout'].shift(1)
        data['dshrout_lag2'] = data.groupby('permno')['dshrout'].shift(2)
        
        # Create dropObs indicator (equivalent to Stata's "gen dropObs = 1 if (dshrout + l1.dshrout + l2.dshrout) > 0")
        data['dropObs'] = (data['dshrout'] + data['dshrout_lag1'] + data['dshrout_lag2']) > 0
        
        # Don't drop if first two months (equivalent to Stata's "bys permno (time_avail_m): replace dropObs = . if _n == 1 | _n == 2")
        data.loc[data.groupby('permno').cumcount() < 2, 'dropObs'] = np.nan
        
        # Drop observations with dropObs == 1 (equivalent to Stata's "drop if dropObs == 1")
        data = data[data['dropObs'] != True]
        logger.info(f"After dropping observations with share changes: {len(data)} records")
        
        # Create ShareVol signal (equivalent to Stata's logic)
        data['ShareVol'] = np.nan
        data.loc[data['tempShareVol'] < 5, 'ShareVol'] = 0  # Low volume
        data.loc[data['tempShareVol'] > 10, 'ShareVol'] = 1  # High volume
        
        logger.info("Successfully calculated ShareVol signal")
        
        # SAVE RESULTS
        logger.info("Saving ShareVol predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'ShareVol']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['ShareVol'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "ShareVol.csv"
        csv_data = output_data[['permno', 'yyyymm', 'ShareVol']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved ShareVol predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed ShareVol predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct ShareVol predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    sharevol()
