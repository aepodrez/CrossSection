"""
Python equivalent of RDcap.do
Generated from: RDcap.do

Original Stata file: RDcap.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def rdcap():
    """
    Python equivalent of RDcap.do
    
    Constructs the RDcap predictor signal for R&D capital to assets ratio.
    """
    logger.info("Constructing predictor signal: RDcap...")
    
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
        required_vars = ['permno', 'time_avail_m', 'at', 'xrd']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # Load SignalMasterTable for market value data
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        master_data = pd.read_csv(master_path, usecols=['permno', 'time_avail_m', 'mve_c'])
        
        # Merge with SignalMasterTable
        data = data.merge(master_data, on=['permno', 'time_avail_m'], how='inner')
        logger.info(f"After merging with SignalMasterTable: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating RDcap signal...")
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Create year variable (equivalent to Stata's "gen year = yofd(dofm(time_avail_m))")
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        # Convert time_avail_m to datetime if needed for year extraction
        if not pd.api.types.is_datetime64_any_dtype(data['time_avail_m']):
            data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        
        data['year'] = data['time_avail_m'].dt.year
        
        # Create tempXRD and replace missing with 0 (equivalent to Stata's "gen tempXRD = xrd" and "replace tempXRD = 0 if mi(tempXRD)")
        data['tempXRD'] = data['xrd'].fillna(0)
        
        # Calculate lags (equivalent to Stata's "l12.", "l24.", "l36.", "l48.")
        data['tempXRD_lag12'] = data.groupby('permno')['tempXRD'].shift(12)
        data['tempXRD_lag24'] = data.groupby('permno')['tempXRD'].shift(24)
        data['tempXRD_lag36'] = data.groupby('permno')['tempXRD'].shift(36)
        data['tempXRD_lag48'] = data.groupby('permno')['tempXRD'].shift(48)
        
        # Calculate RDcap (equivalent to Stata's formula)
        # RDcap = (tempXRD + 0.8*l12.tempXRD + 0.6*l24.tempXRD + 0.4*l36.tempXRD + 0.2*l48.tempXRD)/at
        data['RDcap'] = (data['tempXRD'] + 
                         0.8 * data['tempXRD_lag12'] + 
                         0.6 * data['tempXRD_lag24'] + 
                         0.4 * data['tempXRD_lag36'] + 
                         0.2 * data['tempXRD_lag48']) / data['at']
        
        # Replace RDcap with missing if year < 1980 (equivalent to Stata's "replace RDcap = . if year < 1980")
        data.loc[data['year'] < 1980, 'RDcap'] = np.nan
        
        # Create size terciles (equivalent to Stata's "egen tempsizeq = fastxtile(mve_c), by(time_avail_m) n(3)")
        data['tempsizeq'] = data.groupby('time_avail_m')['mve_c'].transform(
            lambda x: pd.qcut(x, q=3, labels=False, duplicates='drop') + 1
        )
        
        # Keep only small firms (equivalent to Stata's "replace RDcap = . if tempsizeq >= 2")
        data.loc[data['tempsizeq'] >= 2, 'RDcap'] = np.nan
        
        logger.info("Successfully calculated RDcap signal")
        
        # SAVE RESULTS
        logger.info("Saving RDcap predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'RDcap']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['RDcap'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "RDcap.csv"
        csv_data = output_data[['permno', 'yyyymm', 'RDcap']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved RDcap predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed RDcap predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct RDcap predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    rdcap()
