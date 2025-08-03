"""
Python equivalent of NetPayoutYield.do
Generated from: NetPayoutYield.do

Original Stata file: NetPayoutYield.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def netpayoutyield():
    """
    Python equivalent of NetPayoutYield.do
    
    Constructs the NetPayoutYield predictor signal for net payout yield.
    """
    logger.info("Constructing predictor signal: NetPayoutYield...")
    
    try:
        # DATA LOAD
        # Load Compustat data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading Compustat data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"m_aCompustat not found: {compustat_path}")
            logger.error("Please run the Compustat data creation script first")
            return False
        
        # Load required variables
        required_vars = ['permno', 'time_avail_m', 'dvc', 'prstkc', 'sstk', 'sic', 'ceq']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'])
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # Load SignalMasterTable for market value
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
        logger.info("Calculating NetPayoutYield signal...")
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate 6-month lag of market value (equivalent to Stata's "l6.mve_c")
        data['mve_c_lag6'] = data.groupby('permno')['mve_c'].shift(6)
        
        # Calculate NetPayoutYield (equivalent to Stata's "gen NetPayoutYield = (dvc + prstkc - sstk)/l6.mve_c")
        data['NetPayoutYield'] = (data['dvc'] + data['prstkc'] - data['sstk']) / data['mve_c_lag6']
        
        # Drop observations where NetPayoutYield is 0 (equivalent to Stata's "drop if NetPayoutYield == 0")
        data = data[data['NetPayoutYield'] != 0]
        logger.info(f"After dropping zero NetPayoutYield: {len(data)} records")
        
        # Convert SIC to numeric (equivalent to Stata's "destring sic, replace")
        data['sic'] = pd.to_numeric(data['sic'], errors='coerce')
        
        # Keep non-financial firms and firms with positive book equity (equivalent to Stata's "keep if (sic < 6000 | sic >= 7000) & ceq > 0")
        data = data[((data['sic'] < 6000) | (data['sic'] >= 7000)) & (data['ceq'] > 0)]
        logger.info(f"After SIC and book equity filter: {len(data)} records")
        
        # Sort by permno and time_avail_m (equivalent to Stata's "sort permno time_avail_m")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Keep only observations with at least 24 months of history (equivalent to Stata's "bysort permno: keep if _n >= 24")
        data['obs_count'] = data.groupby('permno').cumcount() + 1
        data = data[data['obs_count'] >= 24]
        data = data.drop('obs_count', axis=1)
        logger.info(f"After minimum history filter: {len(data)} records")
        
        logger.info("Successfully calculated NetPayoutYield signal")
        
        # SAVE RESULTS
        logger.info("Saving NetPayoutYield predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'NetPayoutYield']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['NetPayoutYield'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "NetPayoutYield.csv"
        csv_data = output_data[['permno', 'yyyymm', 'NetPayoutYield']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved NetPayoutYield predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed NetPayoutYield predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct NetPayoutYield predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    netpayoutyield()
