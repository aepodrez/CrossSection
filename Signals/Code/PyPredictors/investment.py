"""
Python equivalent of Investment.do
Generated from: Investment.do

Original Stata file: Investment.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def investment():
    """
    Python equivalent of Investment.do
    
    Constructs the Investment predictor signal for investment rate.
    """
    logger.info("Constructing predictor signal: Investment...")
    
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
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'capx', 'revt']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating Investment signal...")
        
        # Sort data for time series operations (equivalent to Stata's "xtset permno time_avail_m")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate Investment (equivalent to Stata's "gen Investment = capx/revt")
        data['Investment'] = data['capx'] / data['revt']
        
        # Calculate rolling mean over 36 months with minimum 24 observations (equivalent to Stata's "asrol Investment, gen(tempMean) window(time_avail_m 36) min(24) stat(mean)")
        data['tempMean'] = data.groupby('permno')['Investment'].rolling(
            window=36, min_periods=24
        ).mean().reset_index(0, drop=True)
        
        # Normalize Investment by its rolling mean (equivalent to Stata's "replace Investment = Investment/tempMean")
        data['Investment'] = data['Investment'] / data['tempMean']
        
        # Set Investment to missing if revenue < 10 million (equivalent to Stata's "replace Investment = . if revt<10")
        data.loc[data['revt'] < 10, 'Investment'] = np.nan
        
        # Drop temporary variables (equivalent to Stata's "drop temp*")
        data = data.drop('tempMean', axis=1)
        
        logger.info("Successfully calculated Investment signal")
        
        # SAVE RESULTS
        logger.info("Saving Investment predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'Investment']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['Investment'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "Investment.csv"
        csv_data = output_data[['permno', 'yyyymm', 'Investment']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved Investment predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed Investment predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct Investment predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    investment()
