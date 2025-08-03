"""
Python equivalent of IndMom.do
Generated from: IndMom.do

Original Stata file: IndMom.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def indmom():
    """
    Python equivalent of IndMom.do
    
    Constructs the IndMom predictor signal for industry momentum.
    """
    logger.info("Constructing predictor signal: IndMom...")
    
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
        required_vars = ['permno', 'time_avail_m', 'ret', 'sicCRSP', 'mve_c']
        
        data = pd.read_csv(master_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating IndMom signal...")
        
        # Convert SIC to string and create 2-digit SIC (equivalent to Stata's "tostring sicCRSP, replace" and "gen sic2D = substr(sicCRSP,1,2)")
        data['sicCRSP'] = data['sicCRSP'].astype(str)
        data['sic2D'] = data['sicCRSP'].str[:2]
        
        # Replace missing returns with 0 (equivalent to Stata's "replace ret = 0 if mi(ret)")
        data['ret'] = data['ret'].fillna(0)
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate lags of returns (1-5 months)
        for lag in range(1, 6):
            data[f'ret_lag{lag}'] = data.groupby('permno')['ret'].shift(lag)
        
        # Calculate 6-month momentum (equivalent to Stata's "gen Mom6m = ( (1+l.ret)*(1+l2.ret)*(1+l3.ret)*(1+l4.ret)*(1+l5.ret)) - 1")
        data['Mom6m'] = ((1 + data['ret_lag1']) * (1 + data['ret_lag2']) * 
                         (1 + data['ret_lag3']) * (1 + data['ret_lag4']) * 
                         (1 + data['ret_lag5'])) - 1
        
        # Calculate weighted mean by industry (equivalent to Stata's "egen IndMom = wtmean(Mom6m), by(sic2D time_avail_m) weight(mve_c)")
        # Group by industry and time, then calculate weighted mean
        data['IndMom'] = data.groupby(['sic2D', 'time_avail_m']).apply(
            lambda x: np.average(x['Mom6m'], weights=x['mve_c'])
        ).reset_index(level=[0, 1], drop=True)
        
        logger.info("Successfully calculated IndMom signal")
        
        # SAVE RESULTS
        logger.info("Saving IndMom predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'IndMom']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['IndMom'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "IndMom.csv"
        csv_data = output_data[['permno', 'yyyymm', 'IndMom']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved IndMom predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed IndMom predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct IndMom predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    indmom()
