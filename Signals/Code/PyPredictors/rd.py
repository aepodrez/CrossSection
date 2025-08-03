"""
Python equivalent of RD.do
Generated from: RD.do

Original Stata file: RD.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def rd():
    """
    Python equivalent of RD.do
    
    Constructs the RD predictor signal for R&D-to-market cap ratio.
    """
    logger.info("Constructing predictor signal: RD...")
    
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
        required_vars = ['permno', 'gvkey', 'time_avail_m', 'mve_c']
        
        data = pd.read_csv(master_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Drop if gvkey is missing (equivalent to Stata's "drop if mi(gvkey)")
        data = data.dropna(subset=['gvkey'])
        logger.info(f"After dropping missing gvkey: {len(data)} records")
        
        # Load annual Compustat data for R&D
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading annual Compustat data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"m_aCompustat not found: {compustat_path}")
            logger.error("Please run the annual Compustat data creation script first")
            return False
        
        compustat_data = pd.read_csv(compustat_path, usecols=['gvkey', 'time_avail_m', 'xrd'])
        
        # Merge with Compustat data
        data = data.merge(compustat_data, on=['gvkey', 'time_avail_m'], how='inner')
        logger.info(f"After merging with Compustat data: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating RD signal...")
        
        # Calculate RD (equivalent to Stata's "gen RD = xrd/mve_c")
        data['RD'] = data['xrd'] / data['mve_c']
        
        logger.info("Successfully calculated RD signal")
        
        # SAVE RESULTS
        logger.info("Saving RD predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'RD']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['RD'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "RD.csv"
        csv_data = output_data[['permno', 'yyyymm', 'RD']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved RD predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed RD predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct RD predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    rd()
