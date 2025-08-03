"""
Python equivalent of High52.do
Generated from: High52.do

Original Stata file: High52.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def high52():
    """
    Python equivalent of High52.do
    
    Constructs the High52 predictor signal for 52-week high.
    """
    logger.info("Constructing predictor signal: High52...")
    
    try:
        # DATA LOAD
        # Load daily CRSP data
        crsp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/dailyCRSP.csv")
        
        logger.info(f"Loading daily CRSP data from: {crsp_path}")
        
        if not crsp_path.exists():
            logger.error(f"Daily CRSP file not found: {crsp_path}")
            logger.error("Please run the CRSP data download scripts first")
            return False
        
        # Load the required variables
        required_vars = ['permno', 'time_d', 'prc']
        
        data = pd.read_csv(crsp_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating High52 signal...")
        
        # Create time_avail_m (equivalent to Stata's "gen time_avail_m = mofd(time_d)")
        data['time_d'] = pd.to_datetime(data['time_d'])
        data['time_avail_m'] = data['time_d'].dt.to_period('M').dt.to_timestamp()
        
        # Create adjusted price (equivalent to Stata's "gen prcadj = abs(prc)")
        data['prcadj'] = data['prc'].abs()
        
        # Collapse to monthly data (equivalent to Stata's "gcollapse (max) maxpr = prcadj (lastnm) prcadj, by(permno time_avail_m)")
        # Get maximum price per month
        max_prices = data.groupby(['permno', 'time_avail_m'])['prcadj'].max().reset_index()
        max_prices = max_prices.rename(columns={'prcadj': 'maxpr'})
        
        # Get last price per month
        last_prices = data.groupby(['permno', 'time_avail_m'])['prcadj'].last().reset_index()
        last_prices = last_prices.rename(columns={'prcadj': 'prcadj'})
        
        # Merge max and last prices
        data = max_prices.merge(last_prices, on=['permno', 'time_avail_m'])
        
        logger.info(f"After collapsing to monthly: {len(data)} observations")
        
        # Sort data for time series operations (equivalent to Stata's "xtset permno time_avail_m")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate 12-month rolling maximum (equivalent to Stata's "gen temp = max(l1.maxpr, l2.maxpr, ..., l12.maxpr)")
        data['temp'] = data.groupby('permno')['maxpr'].rolling(
            window=12, min_periods=1
        ).max().reset_index(0, drop=True)
        
        # Calculate High52 (equivalent to Stata's "gen High52 = prcadj / temp")
        data['High52'] = data['prcadj'] / data['temp']
        
        # Drop temporary variables (equivalent to Stata's "drop temp*")
        data = data.drop(['temp', 'maxpr'], axis=1)
        
        logger.info("Successfully calculated High52 signal")
        
        # SAVE RESULTS
        logger.info("Saving High52 predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'High52']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['High52'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "High52.csv"
        csv_data = output_data[['permno', 'yyyymm', 'High52']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved High52 predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed High52 predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct High52 predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    high52()
