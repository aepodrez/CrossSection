"""
Python equivalent of MomOffSeason.do
Generated from: MomOffSeason.do

Original Stata file: MomOffSeason.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def momoffseason():
    """
    Python equivalent of MomOffSeason.do
    
    Constructs the MomOffSeason predictor signal for off-season long-term reversal.
    """
    logger.info("Constructing predictor signal: MomOffSeason...")
    
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
        logger.info("Calculating MomOffSeason signal...")
        
        # Replace missing returns with 0 (equivalent to Stata's "replace ret = 0 if mi(ret)")
        data['ret'] = data['ret'].fillna(0)
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate lags for seasonal returns (23, 35, 47, 59 months)
        seasonal_lags = [23, 35, 47, 59]
        for lag in seasonal_lags:
            data[f'temp{lag}'] = data.groupby('permno')['ret'].shift(lag)
        
        # Calculate sum and count of seasonal returns (equivalent to Stata's egen rowtotal and rownonmiss)
        seasonal_cols = [f'temp{lag}' for lag in seasonal_lags]
        data['retTemp1'] = data[seasonal_cols].sum(axis=1)  # Sum of seasonal returns
        data['retTemp2'] = data[seasonal_cols].count(axis=1)  # Count of non-missing seasonal returns
        
        # Calculate 12-month lag of returns (equivalent to Stata's "gen retLagTemp = l12.ret")
        data['retLagTemp'] = data.groupby('permno')['ret'].shift(12)
        
        # Calculate rolling sum and count over 48-month window (equivalent to Stata's asrol)
        def rolling_sum_48(group):
            result = []
            for i in range(len(group)):
                if i >= 47:  # Need at least 48 observations
                    window_data = group.iloc[i-47:i+1]  # 48-month window
                    result.append(window_data['retLagTemp'].sum())
                else:
                    result.append(np.nan)
            return result
        
        def rolling_count_48(group):
            result = []
            for i in range(len(group)):
                if i >= 47:  # Need at least 48 observations
                    window_data = group.iloc[i-47:i+1]  # 48-month window
                    result.append(window_data['retLagTemp'].count())
                else:
                    result.append(np.nan)
            return result
        
        # Apply rolling calculations by permno
        data['retLagTemp_sum48'] = data.groupby('permno').apply(
            lambda x: pd.Series(rolling_sum_48(x), index=x.index)
        ).reset_index(level=0, drop=True)
        
        data['retLagTemp_count48'] = data.groupby('permno').apply(
            lambda x: pd.Series(rolling_count_48(x), index=x.index)
        ).reset_index(level=0, drop=True)
        
        # Calculate MomOffSeason (equivalent to Stata's formula)
        # MomOffSeason = (retLagTemp_sum48 - retTemp1)/(retLagTemp_count48 - retTemp2)
        data['MomOffSeason'] = (data['retLagTemp_sum48'] - data['retTemp1']) / (data['retLagTemp_count48'] - data['retTemp2'])
        
        logger.info("Successfully calculated MomOffSeason signal")
        
        # SAVE RESULTS
        logger.info("Saving MomOffSeason predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'MomOffSeason']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['MomOffSeason'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "MomOffSeason.csv"
        csv_data = output_data[['permno', 'yyyymm', 'MomOffSeason']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved MomOffSeason predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed MomOffSeason predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct MomOffSeason predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    momoffseason()
