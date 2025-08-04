"""
Python equivalent of Mom12mOffSeason.do
Generated from: Mom12mOffSeason.do

Original Stata file: Mom12mOffSeason.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def mom12moffseason():
    """
    Python equivalent of Mom12mOffSeason.do
    
    Constructs the Mom12mOffSeason predictor signal for momentum without seasonal part.
    """
    logger.info("Constructing predictor signal: Mom12mOffSeason...")
    
    try:
        # DATA LOAD
        # Load SignalMasterTable data
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load the required variables
        required_vars = ['permno', 'time_avail_m', 'ret']
        
        data = pd.read_csv(master_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating Mom12mOffSeason signal...")
        
        # Replace missing returns with 0 (equivalent to Stata's "replace ret = 0 if mi(ret)")
        data['ret'] = data['ret'].fillna(0)
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate rolling mean excluding the most recent return (equivalent to Stata's asrol with xf(focal))
        # This creates a 10-month rolling window with minimum 6 observations, excluding the focal (most recent) return
        def rolling_mean_exclude_focal(group):
            # Create a rolling window of 10 months, minimum 6 observations
            # Exclude the most recent return (focal) from the calculation
            result = []
            for i in range(len(group)):
                if i >= 5:  # Need at least 6 observations (including focal)
                    # Get the 10-month window ending at current position
                    start_idx = max(0, i - 9)  # 10-month window
                    window_data = group.iloc[start_idx:i]  # Exclude focal (current position)
                    
                    if len(window_data) >= 6:  # Minimum 6 observations
                        result.append(window_data['ret'].mean())
                    else:
                        result.append(np.nan)
                else:
                    result.append(np.nan)
            return result
        
        # Apply the rolling mean calculation by permno
        data['Mom12mOffSeason'] = data.groupby('permno').apply(
            lambda x: pd.Series(rolling_mean_exclude_focal(x), index=x.index)
        ).reset_index(level=0, drop=True)
        
        logger.info("Successfully calculated Mom12mOffSeason signal")
        
        # SAVE RESULTS
        logger.info("Saving Mom12mOffSeason predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'Mom12mOffSeason']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['Mom12mOffSeason'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "Mom12mOffSeason.csv"
        csv_data = output_data[['permno', 'yyyymm', 'Mom12mOffSeason']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved Mom12mOffSeason predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed Mom12mOffSeason predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct Mom12mOffSeason predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    mom12moffseason()
