"""
Python equivalent of CompositeDebtIssuance.do
Generated from: CompositeDebtIssuance.do

Original Stata file: CompositeDebtIssuance.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def compositedebtissuance():
    """
    Python equivalent of CompositeDebtIssuance.do
    
    Constructs the CompositeDebtIssuance predictor signal for composite debt issuance.
    """
    logger.info("Constructing predictor signal: CompositeDebtIssuance...")
    
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
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'dltt', 'dlc']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates: keep first observation per permno-time_avail_m
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        
        # Sort by permno and time_avail_m for lag calculations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating CompositeDebtIssuance signal...")
        
        # Calculate tempBD (equivalent to Stata's "gen tempBD = dltt + dlc")
        data['tempBD'] = data['dltt'] + data['dlc']
        
        # Calculate 60-month lag of tempBD
        data['tempBD_lag60'] = data.groupby('permno')['tempBD'].shift(60)
        
        # Calculate CompositeDebtIssuance (equivalent to Stata's "gen CompositeDebtIssuance = log(tempBD/l60.tempBD)")
        data['CompositeDebtIssuance'] = np.log(data['tempBD'] / data['tempBD_lag60'])
        
        logger.info("Successfully calculated CompositeDebtIssuance signal")
        
        # SAVE RESULTS
        logger.info("Saving CompositeDebtIssuance predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'CompositeDebtIssuance']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['CompositeDebtIssuance'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "CompositeDebtIssuance.csv"
        csv_data = output_data[['permno', 'yyyymm', 'CompositeDebtIssuance']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved CompositeDebtIssuance predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed CompositeDebtIssuance predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct CompositeDebtIssuance predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    compositedebtissuance()
