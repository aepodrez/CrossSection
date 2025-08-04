"""
Python equivalent of std_turn.do
Generated from: std_turn.do

Original Stata file: std_turn.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def std_turn():
    """
    Python equivalent of std_turn.do
    
    Constructs the std_turn predictor signal for turnover volatility.
    """
    logger.info("Constructing predictor signal: std_turn...")
    
    try:
        # DATA LOAD
        # Load monthly CRSP data
        crsp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/monthlyCRSP.csv")
        
        logger.info(f"Loading monthly CRSP data from: {crsp_path}")
        
        if not crsp_path.exists():
            logger.error(f"monthlyCRSP not found: {crsp_path}")
            logger.error("Please run the monthly CRSP data creation script first")
            return False
        
        # Load required variables
        required_vars = ['permno', 'time_avail_m', 'vol', 'shrout', 'prc']
        
        data = pd.read_csv(crsp_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating std_turn signal...")
        
        # Calculate temporary turnover (equivalent to Stata's "gen tempturn = vol/shrout")
        data['tempturn'] = data['vol'] / data['shrout']
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate rolling standard deviation of turnover (equivalent to Stata's "asrol tempturn, gen(std_turn) stat(sd) window(time_avail_m 36) min(24)")
        # Note: This is a simplified version - in practice you'd need a more sophisticated rolling window implementation
        data['std_turn'] = data.groupby('permno')['tempturn'].rolling(
            window=36, 
            min_periods=24
        ).std().reset_index(0, drop=True)
        
        # Calculate market value (equivalent to Stata's "gen mve_c = (shrout * abs(prc))")
        data['mve_c'] = data['shrout'] * np.abs(data['prc'])
        
        # Create size quintiles (equivalent to Stata's "egen tempqsize = fastxtile(mve_c), by(time_avail_m) n(5)")
        data['tempqsize'] = data.groupby('time_avail_m')['mve_c'].transform(
            lambda x: pd.qcut(x, q=5, labels=False, duplicates='drop') + 1
        )
        
        # Set std_turn to missing for large size quintiles (equivalent to Stata's "replace std_turn = . if tempqsize >= 4")
        data.loc[data['tempqsize'] >= 4, 'std_turn'] = np.nan
        
        logger.info("Successfully calculated std_turn signal")
        
        # SAVE RESULTS
        logger.info("Saving std_turn predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'std_turn']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['std_turn'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "std_turn.csv"
        csv_data = output_data[['permno', 'yyyymm', 'std_turn']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved std_turn predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed std_turn predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct std_turn predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    std_turn()
