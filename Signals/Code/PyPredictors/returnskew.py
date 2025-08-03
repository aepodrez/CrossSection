"""
Python equivalent of ReturnSkew.do
Generated from: ReturnSkew.do

Original Stata file: ReturnSkew.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def returnskew():
    """
    Python equivalent of ReturnSkew.do
    
    Constructs the ReturnSkew predictor signal for return skewness.
    """
    logger.info("Constructing predictor signal: ReturnSkew...")
    
    try:
        # DATA LOAD
        # Load daily CRSP data
        crsp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/dailyCRSP.csv")
        
        logger.info(f"Loading daily CRSP data from: {crsp_path}")
        
        if not crsp_path.exists():
            logger.error(f"dailyCRSP not found: {crsp_path}")
            logger.error("Please run the daily CRSP data creation script first")
            return False
        
        data = pd.read_csv(crsp_path, usecols=['permno', 'time_d', 'ret'])
        logger.info(f"Successfully loaded {len(data)} daily records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating ReturnSkew signal...")
        
        # Create time_avail_m (equivalent to Stata's "gen time_avail_m = mofd(time_d)")
        data['time_d'] = pd.to_datetime(data['time_d'])
        data['time_avail_m'] = data['time_d'].dt.to_period('M').dt.to_timestamp()
        
        # Create days variable (equivalent to Stata's "gen days = 0")
        data['days'] = 0
        
        # Collapse to monthly level (equivalent to Stata's "gcollapse (count) ndays = days (skewness) ReturnSkew = ret, by(permno time_avail_m)")
        monthly_data = data.groupby(['permno', 'time_avail_m']).agg({
            'days': 'count',  # Count of days
            'ret': 'skew'     # Skewness of returns
        }).reset_index()
        
        monthly_data = monthly_data.rename(columns={'days': 'ndays', 'ret': 'ReturnSkew'})
        
        # Replace ReturnSkew with missing if ndays < 15 (equivalent to Stata's "replace ReturnSkew = . if ndays < 15")
        monthly_data.loc[monthly_data['ndays'] < 15, 'ReturnSkew'] = np.nan
        
        logger.info("Successfully calculated ReturnSkew signal")
        
        # SAVE RESULTS
        logger.info("Saving ReturnSkew predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = monthly_data[['permno', 'time_avail_m', 'ReturnSkew']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['ReturnSkew'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "ReturnSkew.csv"
        csv_data = output_data[['permno', 'yyyymm', 'ReturnSkew']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved ReturnSkew predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed ReturnSkew predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct ReturnSkew predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    returnskew()
