"""
Python equivalent of SP.do
Generated from: SP.do

Original Stata file: SP.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def sp():
    """
    Python equivalent of SP.do
    
    Constructs the SP predictor signal for sales-to-price ratio.
    """
    logger.info("Constructing predictor signal: SP...")
    
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
        required_vars = ['permno', 'time_avail_m', 'sale']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # Load SignalMasterTable for market value
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        master_data = pd.read_csv(master_path, usecols=['permno', 'time_avail_m', 'mve_c'])
        
        # Merge with SignalMasterTable (equivalent to Stata's "merge 1:1 permno time_avail_m using "$pathDataIntermediate/SignalMasterTable", keep(match) nogenerate keepusing(mve_c)")
        data = data.merge(master_data, on=['permno', 'time_avail_m'], how='inner')
        logger.info(f"After merging with SignalMasterTable: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating SP signal...")
        
        # Calculate SP (equivalent to Stata's "gen SP = sale/mve_c")
        data['SP'] = data['sale'] / data['mve_c']
        
        logger.info("Successfully calculated SP signal")
        
        # SAVE RESULTS
        logger.info("Saving SP predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'SP']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['SP'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "SP.csv"
        csv_data = output_data[['permno', 'yyyymm', 'SP']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved SP predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed SP predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct SP predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    sp()
