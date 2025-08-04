"""
Python equivalent of ShareIss5Y.do
Generated from: ShareIss5Y.do

Original Stata file: ShareIss5Y.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def shareiss5y():
    """
    Python equivalent of ShareIss5Y.do
    
    Constructs the ShareIss5Y predictor signal for share issuance (5 year).
    """
    logger.info("Constructing predictor signal: ShareIss5Y...")
    
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
        required_vars = ['permno', 'time_avail_m']
        
        data = pd.read_csv(master_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Load monthly CRSP data
        crsp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/monthlyCRSP.csv")
        
        logger.info(f"Loading monthly CRSP data from: {crsp_path}")
        
        if not crsp_path.exists():
            logger.error(f"monthlyCRSP not found: {crsp_path}")
            logger.error("Please run the monthly CRSP data creation script first")
            return False
        
        crsp_data = pd.read_csv(crsp_path, usecols=['permno', 'time_avail_m', 'shrout', 'cfacshr'])
        
        # Merge with CRSP data
        data = data.merge(crsp_data, on=['permno', 'time_avail_m'], how='inner')
        logger.info(f"After merging with CRSP data: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating ShareIss5Y signal...")
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate adjusted shares (equivalent to Stata's "gen temp = shrout*cfacshr")
        data['temp'] = data['shrout'] * data['cfacshr']
        
        # Calculate 5-month and 65-month lags (equivalent to Stata's "l5.temp" and "l65.temp")
        data['temp_lag5'] = data.groupby('permno')['temp'].shift(5)
        data['temp_lag65'] = data.groupby('permno')['temp'].shift(65)
        
        # Calculate ShareIss5Y (equivalent to Stata's "gen ShareIss5Y = (l5.temp - l65.temp)/l65.temp")
        data['ShareIss5Y'] = (data['temp_lag5'] - data['temp_lag65']) / data['temp_lag65']
        
        logger.info("Successfully calculated ShareIss5Y signal")
        
        # SAVE RESULTS
        logger.info("Saving ShareIss5Y predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'ShareIss5Y']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['ShareIss5Y'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "ShareIss5Y.csv"
        csv_data = output_data[['permno', 'yyyymm', 'ShareIss5Y']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved ShareIss5Y predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed ShareIss5Y predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct ShareIss5Y predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    shareiss5y()
