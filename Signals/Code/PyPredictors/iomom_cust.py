"""
Python equivalent of iomom_cust.do
Generated from: iomom_cust.do

Original Stata file: iomom_cust.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def iomom_cust():
    """
    Python equivalent of iomom_cust.do
    
    Constructs the iomom_cust predictor signal for input-output customer momentum.
    Note: This signal is constructed in R3_InputOutputMomentum.R
    """
    logger.info("Constructing predictor signal: iomom_cust...")
    
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
        required_vars = ['permno', 'gvkey', 'time_avail_m']
        
        data = pd.read_csv(master_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Drop observations with missing gvkey (equivalent to Stata's "drop if gvkey ==.")
        data = data.dropna(subset=['gvkey'])
        logger.info(f"After dropping missing gvkey: {len(data)} records")
        
        # Merge with Input-Output Momentum processed data
        iomom_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/InputOutputMomentumProcessed.csv")
        
        logger.info(f"Loading Input-Output Momentum processed data from: {iomom_path}")
        
        if not iomom_path.exists():
            logger.error(f"Input-Output Momentum processed file not found: {iomom_path}")
            logger.error("Please run the R3_InputOutputMomentum.R script first")
            return False
        
        iomom_data = pd.read_csv(iomom_path)
        
        # Merge data (equivalent to Stata's "merge 1:1 gvkey time_avail_m using "$pathDataIntermediate/InputOutputMomentumProcessed", keep(master match) nogenerate")
        data = data.merge(
            iomom_data,
            on=['gvkey', 'time_avail_m'],
            how='inner'  # keep(master match)
        )
        
        logger.info(f"After merging with Input-Output Momentum data: {len(data)} observations")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating iomom_cust signal...")
        
        # Assign customer momentum (equivalent to Stata's "gen iomom_cust = retmatchcustomer")
        data['iomom_cust'] = data['retmatchcustomer']
        
        # Keep only non-missing observations (equivalent to Stata's "keep if iomom_cust != .")
        data = data.dropna(subset=['iomom_cust'])
        logger.info(f"After dropping missing iomom_cust: {len(data)} observations")
        
        logger.info("Successfully calculated iomom_cust signal")
        
        # SAVE RESULTS
        logger.info("Saving iomom_cust predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'iomom_cust']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['iomom_cust'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "iomom_cust.csv"
        csv_data = output_data[['permno', 'yyyymm', 'iomom_cust']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved iomom_cust predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed iomom_cust predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct iomom_cust predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    iomom_cust()
