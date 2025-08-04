"""
Python equivalent of GrLTNOA.do
Generated from: GrLTNOA.do

Original Stata file: GrLTNOA.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def grltnoa():
    """
    Python equivalent of GrLTNOA.do
    
    Constructs the GrLTNOA predictor signal for growth in long term net operating assets.
    """
    logger.info("Constructing predictor signal: GrLTNOA...")
    
    try:
        # DATA LOAD
        # Load Compustat annual data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading Compustat annual data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"Compustat annual file not found: {compustat_path}")
            logger.error("Please run the Compustat data download scripts first")
            return False
        
        # Load the required variables
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'rect', 'invt', 'ppent', 'aco', 'intan', 'ao', 'ap', 'lco', 'lo', 'at', 'dp']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating GrLTNOA signal...")
        
        # Sort data for time series operations (equivalent to Stata's "xtset permno time_avail_m")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate 12-month lags for all variables
        lag_vars = ['rect', 'invt', 'ppent', 'aco', 'intan', 'ao', 'ap', 'lco', 'lo', 'at']
        for var in lag_vars:
            data[f'{var}_lag12'] = data.groupby('permno')[var].shift(12)
        
        # Calculate GrLTNOA using the complex formula from Stata
        # First part: (rect + invt + ppent + aco + intan + ao - ap - lco - lo)/at
        numerator1 = (data['rect'] + data['invt'] + data['ppent'] + data['aco'] + 
                     data['intan'] + data['ao'] - data['ap'] - data['lco'] - data['lo'])
        denominator1 = data['at']
        
        # Second part: (l12.rect + l12.invt + l12.ppent + l12.aco + l12.intan + l12.ao - l12.ap - l12.lco - l12.lo)/l12.at
        numerator2 = (data['rect_lag12'] + data['invt_lag12'] + data['ppent_lag12'] + 
                     data['aco_lag12'] + data['intan_lag12'] + data['ao_lag12'] - 
                     data['ap_lag12'] - data['lco_lag12'] - data['lo_lag12'])
        denominator2 = data['at_lag12']
        
        # Third part: (rect - l12.rect + invt - l12.invt + aco - l12.aco - (ap - l12.ap + lco - l12.lco) - dp)/((at + l12.at)/2)
        numerator3 = (data['rect'] - data['rect_lag12'] + data['invt'] - data['invt_lag12'] + 
                     data['aco'] - data['aco_lag12'] - 
                     (data['ap'] - data['ap_lag12'] + data['lco'] - data['lco_lag12']) - data['dp'])
        denominator3 = (data['at'] + data['at_lag12']) / 2
        
        # Combine all parts
        data['GrLTNOA'] = (numerator1 / denominator1) - (numerator2 / denominator2) - (numerator3 / denominator3)
        
        logger.info("Successfully calculated GrLTNOA signal")
        
        # SAVE RESULTS
        logger.info("Saving GrLTNOA predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'GrLTNOA']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['GrLTNOA'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "GrLTNOA.csv"
        csv_data = output_data[['permno', 'yyyymm', 'GrLTNOA']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved GrLTNOA predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed GrLTNOA predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct GrLTNOA predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    grltnoa()
