"""
Python equivalent of InvestPPEInv.do
Generated from: InvestPPEInv.do

Original Stata file: InvestPPEInv.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def investppeinv():
    """
    Python equivalent of InvestPPEInv.do
    
    Constructs the InvestPPEInv predictor signal for PPE and inventory changes to assets.
    """
    logger.info("Constructing predictor signal: InvestPPEInv...")
    
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
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'ppegt', 'invt', 'at']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating InvestPPEInv signal...")
        
        # Sort data for time series operations (equivalent to Stata's "xtset permno time_avail_m")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate 12-month lags (equivalent to Stata's "l12." prefix)
        data['ppegt_lag12'] = data.groupby('permno')['ppegt'].shift(12)
        data['invt_lag12'] = data.groupby('permno')['invt'].shift(12)
        data['at_lag12'] = data.groupby('permno')['at'].shift(12)
        
        # Calculate changes in PPE and inventory (equivalent to Stata's "gen tempPPE = ppegt - l12.ppegt" and "gen tempInv = invt - l12.invt")
        data['tempPPE'] = data['ppegt'] - data['ppegt_lag12']
        data['tempInv'] = data['invt'] - data['invt_lag12']
        
        # Calculate InvestPPEInv (equivalent to Stata's "gen InvestPPEInv = (tempPPE + tempInv)/l12.at")
        data['InvestPPEInv'] = (data['tempPPE'] + data['tempInv']) / data['at_lag12']
        
        logger.info("Successfully calculated InvestPPEInv signal")
        
        # SAVE RESULTS
        logger.info("Saving InvestPPEInv predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'InvestPPEInv']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['InvestPPEInv'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "InvestPPEInv.csv"
        csv_data = output_data[['permno', 'yyyymm', 'InvestPPEInv']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved InvestPPEInv predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed InvestPPEInv predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct InvestPPEInv predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    investppeinv()
