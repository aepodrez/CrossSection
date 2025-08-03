"""
Python equivalent of MomOffSeason11YrPlus.do
Generated from: MomOffSeason11YrPlus.do

Original Stata file: MomOffSeason11YrPlus.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def momoffseason11yrplus():
    """
    Python equivalent of MomOffSeason11YrPlus.do
    
    Constructs the MomOffSeason11YrPlus predictor signal for off-season reversal years 11 to 15.
    """
    logger.info("Constructing predictor signal: MomOffSeason11YrPlus...")
    
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
        logger.info("Calculating MomOffSeason11YrPlus signal...")
        
        # Replace missing returns with 0 (equivalent to Stata's "replace ret = 0 if mi(ret)")
        data['ret'] = data['ret'].fillna(0)
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate lags for seasonal returns (131, 143, 155, 167, 179 months)
        seasonal_lags = [131, 143, 155, 167, 179]
        for lag in seasonal_lags:
            data[f'temp{lag}'] = data.groupby('permno')['ret'].shift(lag)
        
        # Calculate sum and count of seasonal returns (equivalent to Stata's egen rowtotal and rownonmiss)
        seasonal_cols = [f'temp{lag}' for lag in seasonal_lags]
        data['retTemp1'] = data[seasonal_cols].sum(axis=1)  # Sum of seasonal returns
        data['retTemp2'] = data[seasonal_cols].count(axis=1)  # Count of non-missing seasonal returns
        
        # Calculate 120-month lag of returns (equivalent to Stata's "gen retLagTemp = l120.ret")
        data['retLagTemp'] = data.groupby('permno')['ret'].shift(120)
        
        # Calculate rolling sum and count over 60-month window (equivalent to Stata's asrol)
        def rolling_sum_60(group):
            result = []
            for i in range(len(group)):
                if i >= 59:  # Need at least 60 observations
                    window_data = group.iloc[i-59:i+1]  # 60-month window
                    result.append(window_data['retLagTemp'].sum())
                else:
                    result.append(np.nan)
            return result
        
        def rolling_count_60(group):
            result = []
            for i in range(len(group)):
                if i >= 59:  # Need at least 60 observations
                    window_data = group.iloc[i-59:i+1]  # 60-month window
                    result.append(window_data['retLagTemp'].count())
                else:
                    result.append(np.nan)
            return result
        
        # Apply rolling calculations by permno
        data['retLagTemp_sum60'] = data.groupby('permno').apply(
            lambda x: pd.Series(rolling_sum_60(x), index=x.index)
        ).reset_index(level=0, drop=True)
        
        data['retLagTemp_count60'] = data.groupby('permno').apply(
            lambda x: pd.Series(rolling_count_60(x), index=x.index)
        ).reset_index(level=0, drop=True)
        
        # Calculate MomOffSeason11YrPlus (equivalent to Stata's formula)
        # MomOffSeason11YrPlus = (retLagTemp_sum60 - retTemp1)/(retLagTemp_count60 - retTemp2)
        data['MomOffSeason11YrPlus'] = (data['retLagTemp_sum60'] - data['retTemp1']) / (data['retLagTemp_count60'] - data['retTemp2'])
        
        logger.info("Successfully calculated MomOffSeason11YrPlus signal")
        
        # SAVE RESULTS
        logger.info("Saving MomOffSeason11YrPlus predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'MomOffSeason11YrPlus']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['MomOffSeason11YrPlus'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "MomOffSeason11YrPlus.csv"
        csv_data = output_data[['permno', 'yyyymm', 'MomOffSeason11YrPlus']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved MomOffSeason11YrPlus predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed MomOffSeason11YrPlus predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct MomOffSeason11YrPlus predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    momoffseason11yrplus()
