"""
Python equivalent of ChEQ.do
Generated from: ChEQ.do

Original Stata file: ChEQ.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def cheq():
    """
    Python equivalent of ChEQ.do
    
    Constructs the ChEQ predictor signal for sustainable growth (change in common equity).
    """
    logger.info("Constructing predictor signal: ChEQ...")
    
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
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'ceq']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates: keep first observation per permno-time_avail_m
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        
        # Sort by permno and time_avail_m for lag calculations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating ChEQ signal...")
        
        # Calculate 12-month lag of ceq
        data['ceq_lag12'] = data.groupby('permno')['ceq'].shift(12)
        
        # Calculate ChEQ (equivalent to Stata's "gen ChEQ = ceq/l12.ceq if ceq >0 & l12.ceq >0")
        data['ChEQ'] = np.nan  # Initialize as missing
        mask = (data['ceq'] > 0) & (data['ceq_lag12'] > 0)
        data.loc[mask, 'ChEQ'] = data.loc[mask, 'ceq'] / data.loc[mask, 'ceq_lag12']
        
        logger.info("Successfully calculated ChEQ signal")
        
        # SAVE RESULTS
        logger.info("Saving ChEQ predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'ChEQ']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['ChEQ'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "ChEQ.csv"
        csv_data = output_data[['permno', 'yyyymm', 'ChEQ']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved ChEQ predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed ChEQ predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct ChEQ predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    cheq()
