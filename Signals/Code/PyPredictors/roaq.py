"""
Python equivalent of roaq.do
Generated from: roaq.do

Original Stata file: roaq.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def roaq():
    """
    Python equivalent of roaq.do
    
    Constructs the roaq predictor signal for return on assets (quarterly).
    """
    logger.info("Constructing predictor signal: roaq...")
    
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
        
        # Keep only observations with non-missing gvkey (equivalent to Stata's "keep if !mi(gvkey)")
        data = data.dropna(subset=['gvkey'])
        logger.info(f"After dropping missing gvkey: {len(data)} records")
        
        # Load quarterly Compustat data
        qcompustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_QCompustat.csv")
        
        logger.info(f"Loading quarterly Compustat data from: {qcompustat_path}")
        
        if not qcompustat_path.exists():
            logger.error(f"m_QCompustat not found: {qcompustat_path}")
            logger.error("Please run the quarterly Compustat data creation script first")
            return False
        
        qcompustat_data = pd.read_csv(qcompustat_path, usecols=['gvkey', 'time_avail_m', 'atq', 'ibq'])
        
        # Merge with quarterly data
        data = data.merge(qcompustat_data, on=['gvkey', 'time_avail_m'], how='inner')
        logger.info(f"After merging with quarterly data: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating roaq signal...")
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate 3-month lag of assets (equivalent to Stata's "l3.atq")
        data['atq_lag3'] = data.groupby('permno')['atq'].shift(3)
        
        # Calculate roaq (equivalent to Stata's "gen roaq = ibq/l3.atq")
        data['roaq'] = data['ibq'] / data['atq_lag3']
        
        logger.info("Successfully calculated roaq signal")
        
        # SAVE RESULTS
        logger.info("Saving roaq predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'roaq']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['roaq'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "roaq.csv"
        csv_data = output_data[['permno', 'yyyymm', 'roaq']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved roaq predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed roaq predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct roaq predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    roaq()
