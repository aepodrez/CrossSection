"""
Python equivalent of Tax.do
Generated from: Tax.do

Original Stata file: Tax.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def tax():
    """
    Python equivalent of Tax.do
    
    Constructs the Tax predictor signal for taxable income to income ratio.
    """
    logger.info("Constructing predictor signal: Tax...")
    
    try:
        # DATA LOAD
        # Load annual Compustat data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading annual Compustat data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"m_aCompustat not found: {compustat_path}")
            logger.error("Please run the annual Compustat data creation script first")
            return False
        
        # Load required variables
        required_vars = ['permno', 'time_avail_m', 'txfo', 'txfed', 'ib', 'txt', 'txdi']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating Tax signal...")
        
        # Convert time_avail_m to year (equivalent to Stata's "gen year = yofd(dofm(time_avail_m))")
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        data['year'] = data['time_avail_m'].dt.year
        
        # Define highest tax rate by year (equivalent to Stata's logic)
        data['tr'] = 0.48  # Default rate
        
        # Replace rates based on year ranges
        data.loc[(data['year'] >= 1979) & (data['year'] <= 1986), 'tr'] = 0.46
        data.loc[data['year'] == 1987, 'tr'] = 0.40
        data.loc[(data['year'] >= 1988) & (data['year'] <= 1992), 'tr'] = 0.34
        data.loc[data['year'] >= 1993, 'tr'] = 0.35
        
        # Calculate Tax (equivalent to Stata's logic)
        data['Tax'] = np.nan
        
        # Primary calculation: ((txfo+txfed)/tr)/ib
        primary_condition = (data['txfo'].notna()) & (data['txfed'].notna())
        data.loc[primary_condition, 'Tax'] = ((data['txfo'] + data['txfed']) / data['tr']) / data['ib']
        
        # Alternative calculation: ((txt-txdi)/tr)/ib if txfo or txfed is missing
        alt_condition = (data['txfo'].isna()) | (data['txfed'].isna())
        data.loc[alt_condition, 'Tax'] = ((data['txt'] - data['txdi']) / data['tr']) / data['ib']
        
        # Set to 1 if positive taxes but negative income
        special_condition = (
            ((data['txfo'] + data['txfed'] > 0) | (data['txt'] > data['txdi'])) & 
            (data['ib'] <= 0)
        )
        data.loc[special_condition, 'Tax'] = 1
        
        logger.info("Successfully calculated Tax signal")
        
        # SAVE RESULTS
        logger.info("Saving Tax predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'Tax']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['Tax'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "Tax.csv"
        csv_data = output_data[['permno', 'yyyymm', 'Tax']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved Tax predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed Tax predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct Tax predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    tax()
