"""
Python equivalent of DelNetFin.do
Generated from: DelNetFin.do

Original Stata file: DelNetFin.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def delnetfin():
    """
    Python equivalent of DelNetFin.do
    
    Constructs the DelNetFin predictor signal for change in net financial assets.
    """
    logger.info("Constructing predictor signal: DelNetFin...")
    
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
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'at', 'pstk', 'dltt', 'dlc', 'ivst', 'ivao']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # Sort for lag calculations (equivalent to Stata's "xtset permno time_avail_m")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating DelNetFin signal...")
        
        # Calculate 12-month lags
        data['at_lag12'] = data.groupby('permno')['at'].shift(12)
        data['ivst_lag12'] = data.groupby('permno')['ivst'].shift(12)
        data['ivao_lag12'] = data.groupby('permno')['ivao'].shift(12)
        data['dltt_lag12'] = data.groupby('permno')['dltt'].shift(12)
        data['dlc_lag12'] = data.groupby('permno')['dlc'].shift(12)
        data['pstk_lag12'] = data.groupby('permno')['pstk'].shift(12)
        
        # Calculate average total assets (equivalent to Stata's "gen tempAvAT = .5*(at + l12.at)")
        data['tempAvAT'] = 0.5 * (data['at'] + data['at_lag12'])
        
        # Create tempPSTK (equivalent to Stata's "gen tempPSTK = pstk" and "replace tempPSTK = 0 if mi(pstk)")
        data['tempPSTK'] = data['pstk'].fillna(0)
        data['tempPSTK_lag12'] = data['pstk_lag12'].fillna(0)
        
        # Calculate net financial assets (equivalent to Stata's "gen temp = (ivst + ivao) - (dltt + dlc + tempPSTK)")
        data['temp'] = (data['ivst'] + data['ivao']) - (data['dltt'] + data['dlc'] + data['tempPSTK'])
        data['temp_lag12'] = (data['ivst_lag12'] + data['ivao_lag12']) - (data['dltt_lag12'] + data['dlc_lag12'] + data['tempPSTK_lag12'])
        
        # Calculate change in net financial assets (equivalent to Stata's "gen DelNetFin = temp - l12.temp")
        data['DelNetFin'] = data['temp'] - data['temp_lag12']
        
        # Scale by average total assets (equivalent to Stata's "replace DelNetFin = DelNetFin/tempAvAT")
        data['DelNetFin'] = data['DelNetFin'] / data['tempAvAT']
        
        logger.info("Successfully calculated DelNetFin signal")
        
        # SAVE RESULTS
        logger.info("Saving DelNetFin predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'DelNetFin']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['DelNetFin'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "DelNetFin.csv"
        csv_data = output_data[['permno', 'yyyymm', 'DelNetFin']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved DelNetFin predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed DelNetFin predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct DelNetFin predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    delnetfin()
