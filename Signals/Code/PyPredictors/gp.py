"""
Python equivalent of GP.do
Generated from: GP.do

Original Stata file: GP.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def gp():
    """
    Python equivalent of GP.do
    
    Constructs the GP predictor signal for gross profitability.
    """
    logger.info("Constructing predictor signal: GP...")
    
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
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'revt', 'cogs', 'at', 'sic', 'datadate']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Convert SIC to numeric (equivalent to Stata's "destring sic, replace")
        data['sic'] = pd.to_numeric(data['sic'], errors='coerce')
        
        # Filter out financial firms (equivalent to Stata's "keep if (sic < 6000 | sic >= 7000)")
        data = data[(data['sic'] < 6000) | (data['sic'] >= 7000)]
        logger.info(f"After filtering out financial firms: {len(data)} observations")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating GP signal...")
        
        # Sort data for time series operations (equivalent to Stata's "xtset permno time_avail_m")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate GP (equivalent to Stata's "gen GP = (revt-cogs)/at")
        data['GP'] = (data['revt'] - data['cogs']) / data['at']
        
        logger.info("Successfully calculated GP signal")
        
        # SAVE RESULTS
        logger.info("Saving GP predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'GP']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['GP'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "GP.csv"
        csv_data = output_data[['permno', 'yyyymm', 'GP']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved GP predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed GP predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct GP predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    gp()
