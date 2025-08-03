"""
Python equivalent of dNoa.do
Generated from: dNoa.do

Original Stata file: dNoa.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def dnoa():
    """
    Python equivalent of dNoa.do
    
    Constructs the dNoa predictor signal for change in net operating assets.
    """
    logger.info("Constructing predictor signal: dNoa...")
    
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
        required_vars = ['permno', 'time_avail_m', 'at', 'che', 'dltt', 'dlc', 'mib', 'pstk', 'ceq']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # Sort for lag calculations (equivalent to Stata's "xtset permno time_avail_m")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating dNoa signal...")
        
        # Calculate operating assets (equivalent to Stata's "gen tempOA = at - che")
        data['tempOA'] = data['at'] - data['che']
        
        # Create temporary variables for liabilities and set missing to 0 (equivalent to Stata's foreach loop)
        for var in ['dltt', 'dlc', 'mib', 'pstk']:
            data[f'temp{var}'] = data[var].fillna(0)
        
        # Calculate operating liabilities (equivalent to Stata's "gen tempOL = at - tempdltt - tempmib - tempdlc - temppstk - ceq")
        data['tempOL'] = data['at'] - data['tempdltt'] - data['tempmib'] - data['tempdlc'] - data['temppstk'] - data['ceq']
        
        # Calculate net operating assets (equivalent to Stata's "gen tempNOA = tempOA - tempOL")
        data['tempNOA'] = data['tempOA'] - data['tempOL']
        
        # Calculate 12-month lags
        data['tempNOA_lag12'] = data.groupby('permno')['tempNOA'].shift(12)
        data['at_lag12'] = data.groupby('permno')['at'].shift(12)
        
        # Calculate change in net operating assets (equivalent to Stata's "gen dNoa = (tempNOA - l12.tempNOA)/l12.at")
        data['dNoa'] = (data['tempNOA'] - data['tempNOA_lag12']) / data['at_lag12']
        
        logger.info("Successfully calculated dNoa signal")
        
        # SAVE RESULTS
        logger.info("Saving dNoa predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'dNoa']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['dNoa'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "dNoa.csv"
        csv_data = output_data[['permno', 'yyyymm', 'dNoa']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved dNoa predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed dNoa predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct dNoa predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    dnoa()
