"""
Python equivalent of ChTax.do
Generated from: ChTax.do

Original Stata file: ChTax.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def chtax():
    """
    Python equivalent of ChTax.do
    
    Constructs the ChTax predictor signal for change in taxes.
    """
    logger.info("Constructing predictor signal: ChTax...")
    
    try:
        # DATA LOAD
        # Load Compustat annual data
        compustat_annual_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading Compustat annual data from: {compustat_annual_path}")
        
        if not compustat_annual_path.exists():
            logger.error(f"Compustat annual data not found: {compustat_annual_path}")
            logger.error("Please run the Compustat data download scripts first")
            return False
        
        # Load the required variables from annual data
        annual_vars = ['permno', 'gvkey', 'time_avail_m', 'at']
        
        annual_data = pd.read_csv(compustat_annual_path, usecols=annual_vars)
        logger.info(f"Successfully loaded {len(annual_data)} annual records")
        
        # Load Compustat quarterly data
        compustat_quarterly_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_QCompustat.csv")
        
        if not compustat_quarterly_path.exists():
            logger.error(f"Compustat quarterly data not found: {compustat_quarterly_path}")
            logger.error("Please run the Compustat data download scripts first")
            return False
        
        # Load the required variables from quarterly data
        quarterly_vars = ['gvkey', 'time_avail_m', 'txtq']
        
        quarterly_data = pd.read_csv(compustat_quarterly_path, usecols=quarterly_vars)
        logger.info(f"Successfully loaded {len(quarterly_data)} quarterly records")
        
        # Merge annual and quarterly data
        logger.info("Merging annual and quarterly data...")
        
        merged_data = annual_data.merge(
            quarterly_data, 
            on=['gvkey', 'time_avail_m'], 
            how='inner'  # equivalent to Stata's keep(match)
        )
        
        logger.info(f"Successfully merged data: {len(merged_data)} observations")
        
        # Sort by gvkey and time_avail_m for lag calculations
        merged_data = merged_data.sort_values(['gvkey', 'time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating ChTax signal...")
        
        # Calculate 12-month lag of txtq
        merged_data['txtq_lag12'] = merged_data.groupby('gvkey')['txtq'].shift(12)
        
        # Calculate 12-month lag of at
        merged_data['at_lag12'] = merged_data.groupby('gvkey')['at'].shift(12)
        
        # Calculate ChTax (equivalent to Stata's "gen ChTax = (txtq - l12.txtq)/l12.at")
        merged_data['ChTax'] = (merged_data['txtq'] - merged_data['txtq_lag12']) / merged_data['at_lag12']
        
        logger.info("Successfully calculated ChTax signal")
        
        # SAVE RESULTS
        logger.info("Saving ChTax predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = merged_data[['permno', 'time_avail_m', 'ChTax']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['ChTax'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "ChTax.csv"
        csv_data = output_data[['permno', 'yyyymm', 'ChTax']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved ChTax predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed ChTax predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct ChTax predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    chtax()
