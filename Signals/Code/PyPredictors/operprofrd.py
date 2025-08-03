"""
Python equivalent of OperProfRD.do
Generated from: OperProfRD.do

Original Stata file: OperProfRD.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def operprofrd():
    """
    Python equivalent of OperProfRD.do
    
    Constructs the OperProfRD predictor signal for operating profits to assets (including R&D).
    """
    logger.info("Constructing predictor signal: OperProfRD...")
    
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
        required_vars = ['permno', 'gvkey', 'time_avail_m', 'exchcd', 'sicCRSP', 'mve_c', 'shrcd']
        
        data = pd.read_csv(master_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Load Compustat data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading Compustat data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"m_aCompustat not found: {compustat_path}")
            logger.error("Please run the Compustat data creation script first")
            return False
        
        compustat_data = pd.read_csv(compustat_path, usecols=['gvkey', 'permno', 'time_avail_m', 'xrd', 'revt', 'cogs', 'xsga', 'at', 'ceq'])
        
        # Merge with Compustat data
        data = data.merge(compustat_data, on=['gvkey', 'permno', 'time_avail_m'], how='inner')
        logger.info(f"After merging with Compustat data: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating OperProfRD signal...")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'])
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # Create temporary R&D variable (equivalent to Stata's "gen tempXRD = xrd" and "replace tempXRD = 0 if mi(tempXRD)")
        data['tempXRD'] = data['xrd'].fillna(0)
        
        # Calculate OperProfRD (equivalent to Stata's "gen OperProfRD = (revt - cogs - xsga + tempXRD)/at")
        data['OperProfRD'] = (data['revt'] - data['cogs'] - data['xsga'] + data['tempXRD']) / data['at']
        
        # Apply filters (equivalent to Stata's "drop if shrcd > 11 | mi(mve_c) | mi(ceq) | mi(at) | (sicCRSP >= 6000 & sicCRSP < 7000)")
        # Drop if share code > 11
        data = data[data['shrcd'] <= 11]
        
        # Drop if missing key variables
        missing_vars = ['mve_c', 'ceq', 'at']
        for var in missing_vars:
            data = data.dropna(subset=[var])
        
        # Drop financial firms (SIC 6000-6999)
        data['sicCRSP'] = pd.to_numeric(data['sicCRSP'], errors='coerce')
        data = data[~((data['sicCRSP'] >= 6000) & (data['sicCRSP'] < 7000))]
        
        logger.info(f"After applying filters: {len(data)} records")
        logger.info("Successfully calculated OperProfRD signal")
        
        # SAVE RESULTS
        logger.info("Saving OperProfRD predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'OperProfRD']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['OperProfRD'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "OperProfRD.csv"
        csv_data = output_data[['permno', 'yyyymm', 'OperProfRD']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved OperProfRD predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed OperProfRD predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct OperProfRD predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    operprofrd()
