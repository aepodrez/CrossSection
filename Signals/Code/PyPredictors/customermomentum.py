"""
Python equivalent of CustomerMomentum.do
Generated from: CustomerMomentum.do

Original Stata file: CustomerMomentum.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def customermomentum():
    """
    Python equivalent of CustomerMomentum.do
    
    Constructs the CustomerMomentum predictor signal for customer momentum.
    Note: This signal is constructed in R4_CustomerMomentum.R
    """
    logger.info("Constructing predictor signal: CustomerMomentum...")
    
    try:
        # DATA LOAD
        # Load customer momentum data (constructed in R)
        customer_mom_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/customerMom.csv")
        
        logger.info(f"Loading customer momentum data from: {customer_mom_path}")
        
        if not customer_mom_path.exists():
            logger.error(f"Customer momentum data not found: {customer_mom_path}")
            logger.error("Please run R4_CustomerMomentum.R first to construct the customer momentum data")
            return False
        
        # Load the customer momentum data
        data = pd.read_csv(customer_mom_path)
        
        logger.info(f"Successfully loaded {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Processing CustomerMomentum signal...")
        
        # Rename custmom to CustomerMomentum (equivalent to Stata's "rename custmom CustomerMomentum")
        if 'CustMom' in data.columns:
            data = data.rename(columns={'CustMom': 'CustomerMomentum'})
        elif 'custmom' in data.columns:
            data = data.rename(columns={'custmom': 'CustomerMomentum'})
        elif 'CustomerMomentum' not in data.columns:
            logger.error("Neither 'CustMom', 'custmom', nor 'CustomerMomentum' column found in data")
            return False
        
        logger.info("Successfully processed CustomerMomentum signal")
        
        # SAVE RESULTS
        logger.info("Saving CustomerMomentum predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        # Assuming the data has permno, time_avail_m, and CustomerMomentum columns
        required_cols = ['permno', 'time_avail_m', 'CustomerMomentum']
        
        if not all(col in data.columns for col in required_cols):
            logger.error(f"Required columns not found. Available columns: {list(data.columns)}")
            return False
        
        output_data = data[required_cols].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['CustomerMomentum'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "CustomerMomentum.csv"
        csv_data = output_data[['permno', 'yyyymm', 'CustomerMomentum']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved CustomerMomentum predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed CustomerMomentum predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct CustomerMomentum predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    customermomentum()
