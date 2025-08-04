"""
Python equivalent of EarningsConsistency.do
Generated from: EarningsConsistency.do

Original Stata file: EarningsConsistency.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def earningsconsistency():
    """
    Python equivalent of EarningsConsistency.do
    
    Constructs the EarningsConsistency predictor signal for earnings consistency.
    """
    logger.info("Constructing predictor signal: EarningsConsistency...")
    
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
        required_vars = ['permno', 'time_avail_m', 'epspx']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Sort for lag calculations (equivalent to Stata's "xtset permno time_avail_m")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating EarningsConsistency signal...")
        
        # Calculate 12-month and 24-month lags
        data['epspx_lag12'] = data.groupby('permno')['epspx'].shift(12)
        data['epspx_lag24'] = data.groupby('permno')['epspx'].shift(24)
        
        # Calculate temp (equivalent to Stata's "gen temp = (epspx - l12.epspx)/(.5*(abs(l12.epspx) + abs(l24.epspx)))")
        data['temp'] = (data['epspx'] - data['epspx_lag12']) / (0.5 * (data['epspx_lag12'].abs() + data['epspx_lag24'].abs()))
        
        # Calculate lags of temp for different periods (equivalent to Stata's foreach loop)
        for n in [12, 24, 36, 48]:
            data[f'temp{n}'] = data.groupby('permno')['temp'].shift(n)
        
        # Calculate row mean of temp variables (equivalent to Stata's "egen EarningsConsistency = rowmean(temp*)")
        temp_cols = ['temp12', 'temp24', 'temp36', 'temp48']
        data['EarningsConsistency'] = data[temp_cols].mean(axis=1)
        
        # Apply filters (equivalent to Stata's replace conditions)
        # Filter 1: abs(epspx/l12.epspx) > 6
        filter1 = (data['epspx'] / data['epspx_lag12']).abs() > 6
        
        # Filter 2: (temp > 0 & l12.temp < 0 & !mi(temp))
        data['temp_lag12'] = data.groupby('permno')['temp'].shift(12)
        filter2 = (data['temp'] > 0) & (data['temp_lag12'] < 0) & (data['temp'].notna())
        
        # Filter 3: (temp < 0 & l12.temp > 0 & !mi(temp))
        filter3 = (data['temp'] < 0) & (data['temp_lag12'] > 0) & (data['temp'].notna())
        
        # Apply all filters
        data.loc[filter1 | filter2 | filter3, 'EarningsConsistency'] = np.nan
        
        logger.info("Successfully calculated EarningsConsistency signal")
        
        # SAVE RESULTS
        logger.info("Saving EarningsConsistency predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'EarningsConsistency']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['EarningsConsistency'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "EarningsConsistency.csv"
        csv_data = output_data[['permno', 'yyyymm', 'EarningsConsistency']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved EarningsConsistency predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed EarningsConsistency predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct EarningsConsistency predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    earningsconsistency()
