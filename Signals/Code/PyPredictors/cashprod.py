"""
Python equivalent of CashProd.do
Generated from: CashProd.do

Original Stata file: CashProd.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def cashprod():
    """
    Python equivalent of CashProd.do
    
    Constructs the CashProd predictor signal for cash productivity.
    """
    logger.info("Constructing predictor signal: CashProd...")
    
    try:
        # DATA LOAD
        # Load Compustat annual data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading Compustat annual data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"Input file not found: {compustat_path}")
            logger.error("Please run the Compustat data download scripts first")
            return False
        
        # Load the required variables
        required_vars = ['permno', 'time_avail_m', 'at', 'che']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates: keep first observation per permno-time_avail_m
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        
        # Merge with SignalMasterTable to get mve_c
        logger.info("Merging with SignalMasterTable...")
        
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        master_data = pd.read_csv(master_path, usecols=['permno', 'time_avail_m', 'mve_c'])
        
        # Merge data
        merged_data = data.merge(
            master_data, 
            on=['permno', 'time_avail_m'], 
            how='inner'
        )
        
        logger.info(f"Successfully merged data: {len(merged_data)} observations")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating CashProd signal...")
        
        # Calculate CashProd (equivalent to Stata's "gen CashProd = (mve_c - at)/che")
        merged_data['CashProd'] = (merged_data['mve_c'] - merged_data['at']) / merged_data['che']
        
        logger.info("Successfully calculated CashProd signal")
        
        # SAVE RESULTS
        logger.info("Saving CashProd predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = merged_data[['permno', 'time_avail_m', 'CashProd']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['CashProd'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "CashProd.csv"
        csv_data = output_data[['permno', 'yyyymm', 'CashProd']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved CashProd predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed CashProd predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct CashProd predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    cashprod()
