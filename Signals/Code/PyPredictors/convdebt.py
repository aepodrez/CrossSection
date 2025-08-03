"""
Python equivalent of ConvDebt.do
Generated from: ConvDebt.do

Original Stata file: ConvDebt.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def convdebt():
    """
    Python equivalent of ConvDebt.do
    
    Constructs the ConvDebt predictor signal for convertible debt indicator.
    """
    logger.info("Constructing predictor signal: ConvDebt...")
    
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
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'dc', 'cshrc']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates: keep first observation per permno-time_avail_m
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        
        # Sort by permno and time_avail_m
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating ConvDebt signal...")
        
        # Initialize ConvDebt to 0 (equivalent to Stata's "gen ConvDebt = 0")
        data['ConvDebt'] = 0
        
        # Set to 1 if convertible debt or convertible shares exist (equivalent to Stata's replace logic)
        mask = ((data['dc'].notna()) & (data['dc'] != 0)) | ((data['cshrc'].notna()) & (data['cshrc'] != 0))
        data.loc[mask, 'ConvDebt'] = 1
        
        logger.info("Successfully calculated ConvDebt signal")
        
        # SAVE RESULTS
        logger.info("Saving ConvDebt predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'ConvDebt']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['ConvDebt'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "ConvDebt.csv"
        csv_data = output_data[['permno', 'yyyymm', 'ConvDebt']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved ConvDebt predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed ConvDebt predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct ConvDebt predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    convdebt()
