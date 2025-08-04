"""
Python equivalent of OrderBacklog.do
Generated from: OrderBacklog.do

Original Stata file: OrderBacklog.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def orderbacklog():
    """
    Python equivalent of OrderBacklog.do
    
    Constructs the OrderBacklog predictor signal for order backlog.
    """
    logger.info("Constructing predictor signal: OrderBacklog...")
    
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
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'ob', 'at']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating OrderBacklog signal...")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'])
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate 12-month lag of total assets (equivalent to Stata's "l12.at")
        data['at_lag12'] = data.groupby('permno')['at'].shift(12)
        
        # Calculate OrderBacklog (equivalent to Stata's "gen OrderBacklog = ob/(.5*(at + l12.at))")
        data['OrderBacklog'] = data['ob'] / (0.5 * (data['at'] + data['at_lag12']))
        
        # Set OrderBacklog to missing if ob is 0 (equivalent to Stata's "replace OrderBacklog = . if ob == 0")
        data.loc[data['ob'] == 0, 'OrderBacklog'] = np.nan
        
        logger.info("Successfully calculated OrderBacklog signal")
        
        # SAVE RESULTS
        logger.info("Saving OrderBacklog predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'OrderBacklog']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['OrderBacklog'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "OrderBacklog.csv"
        csv_data = output_data[['permno', 'yyyymm', 'OrderBacklog']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved OrderBacklog predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed OrderBacklog predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct OrderBacklog predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    orderbacklog()
