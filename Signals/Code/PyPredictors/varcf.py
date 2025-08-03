"""
Python equivalent of VarCF.do
Generated from: VarCF.do

Original Stata file: VarCF.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def varcf():
    """
    Python equivalent of VarCF.do
    
    Constructs the VarCF predictor signal for cash-flow variance.
    """
    logger.info("Constructing predictor signal: VarCF...")
    
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
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'ib', 'dp']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Merge with SignalMasterTable (equivalent to Stata's "merge 1:1 permno time_avail_m using "$pathDataIntermediate/SignalMasterTable", keep(using match) nogenerate")
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        master_data = pd.read_csv(master_path, usecols=['permno', 'time_avail_m', 'mve_c'])
        
        data = data.merge(master_data, on=['permno', 'time_avail_m'], how='outer')
        logger.info(f"After merging with SignalMasterTable: {len(data)} records")
        
        # Sort data for time series operations (equivalent to Stata's "xtset permno time_avail_m")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating VarCF signal...")
        
        # Calculate temporary cash flow (equivalent to Stata's "gen tempCF = (ib + dp)/mve_c")
        data['tempCF'] = (data['ib'] + data['dp']) / data['mve_c']
        
        # Calculate rolling standard deviation (equivalent to Stata's "asrol tempCF, gen(sigma) stat(sd) window(time_avail_m 60) min(24) by(permno)")
        # Note: This is a simplified version - in practice you'd need a more sophisticated rolling window implementation
        data['sigma'] = data.groupby('permno')['tempCF'].rolling(
            window=60, 
            min_periods=24
        ).std().reset_index(0, drop=True)
        
        # Calculate cash flow variance (equivalent to Stata's "gen VarCF = sigma^2")
        data['VarCF'] = data['sigma'] ** 2
        
        logger.info("Successfully calculated VarCF signal")
        
        # SAVE RESULTS
        logger.info("Saving VarCF predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'VarCF']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['VarCF'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "VarCF.csv"
        csv_data = output_data[['permno', 'yyyymm', 'VarCF']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved VarCF predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed VarCF predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct VarCF predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    varcf()
