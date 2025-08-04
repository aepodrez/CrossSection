"""
Python equivalent of PayoutYield.do
Generated from: PayoutYield.do

Original Stata file: PayoutYield.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def payoutyield():
    """
    Python equivalent of PayoutYield.do
    
    Constructs the PayoutYield predictor signal for total payout yield.
    """
    logger.info("Constructing predictor signal: PayoutYield...")
    
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
        required_vars = ['permno', 'time_avail_m', 'dvc', 'prstkc', 'pstkrv', 'sstk', 'sic', 'ceq', 'datadate']
        
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
        logger.info("Calculating PayoutYield signal...")
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate 6-month lag of market value (equivalent to Stata's "l6.mve_c")
        data['mve_c_lag6'] = data.groupby('permno')['mve_c'].shift(6)
        
        # Calculate PayoutYield (equivalent to Stata's "gen PayoutYield = (dvc + prstkc + pstkrv)/l6.mve_c")
        data['PayoutYield'] = (data['dvc'] + data['prstkc'] + data['pstkrv']) / data['mve_c_lag6']
        
        # Replace non-positive values with missing (equivalent to Stata's "replace PayoutYield = . if PayoutYield <= 0")
        data.loc[data['PayoutYield'] <= 0, 'PayoutYield'] = np.nan
        
        logger.info("Successfully calculated PayoutYield signal")
        
        # FILTER
        logger.info("Applying filters...")
        
        # Convert SIC to numeric (equivalent to Stata's "destring sic, replace")
        data['sic'] = pd.to_numeric(data['sic'], errors='coerce')
        
        # Exclude financial firms and keep only positive common equity (equivalent to Stata's "keep if (sic < 6000 | sic >= 7000) & ceq > 0")
        data = data[((data['sic'] < 6000) | (data['sic'] >= 7000)) & (data['ceq'] > 0)]
        logger.info(f"After applying SIC and common equity filters: {len(data)} records")
        
        # Sort by permno and time_avail_m
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Keep only observations with at least 24 months of history (equivalent to Stata's "bysort permno: keep if _n >= 24")
        data = data.groupby('permno').apply(lambda x: x.iloc[23:]).reset_index(drop=True)
        logger.info(f"After keeping firms with at least 24 months of history: {len(data)} records")
        
        # SAVE RESULTS
        logger.info("Saving PayoutYield predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'PayoutYield']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['PayoutYield'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "PayoutYield.csv"
        csv_data = output_data[['permno', 'yyyymm', 'PayoutYield']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved PayoutYield predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed PayoutYield predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct PayoutYield predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    payoutyield()
