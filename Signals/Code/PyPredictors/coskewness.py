"""
Python equivalent of Coskewness.do
Generated from: Coskewness.do

Original Stata file: Coskewness.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def coskewness():
    """
    Python equivalent of Coskewness.do
    
    Constructs the Coskewness predictor signal for coskewness of stock return wrt market return.
    """
    logger.info("Constructing predictor signal: Coskewness...")
    
    try:
        # DATA LOAD
        # Load monthly CRSP data
        crsp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/monthlyCRSP.csv")
        
        logger.info(f"Loading monthly CRSP data from: {crsp_path}")
        
        if not crsp_path.exists():
            logger.error(f"Monthly CRSP data not found: {crsp_path}")
            logger.error("Please run the CRSP data download scripts first")
            return False
        
        # Load required variables from monthly CRSP
        crsp_vars = ['permno', 'time_avail_m', 'ret']
        data = pd.read_csv(crsp_path, usecols=crsp_vars)
        
        # Load monthly Fama-French data
        ff_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/monthlyFF.csv")
        
        if not ff_path.exists():
            logger.error(f"Monthly Fama-French data not found: {ff_path}")
            logger.error("Please run the Fama-French data download scripts first")
            return False
        
        # Load required variables from monthly FF
        ff_vars = ['time_avail_m', 'mktrf', 'rf']
        ff_data = pd.read_csv(ff_path, usecols=ff_vars)
        
        # Merge with Fama-French data
        data = data.merge(ff_data, on='time_avail_m', how='inner')
        
        # Convert to excess returns
        data['mkt'] = data['mktrf']
        data['ret'] = data['ret'] - data['rf']
        data = data.drop('rf', axis=1)
        
        logger.info(f"Successfully loaded and merged data: {len(data)} records")
        
        # Sort by permno and reverse time for processing
        data = data.sort_values(['permno', 'time_avail_m'], ascending=[True, False])
        
        # Create time variables
        data['time_m'] = data['time_avail_m']
        
        # Convert time_avail_m to datetime if it's not already
        if not pd.api.types.is_datetime64_any_dtype(data['time_avail_m']):
            data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        
        data['m60'] = data['time_avail_m'].dt.month - 1  # month in 60-month calendar (0-59)
        
        logger.info("Processing coskewness by 60-month window...")
        
        # Process each 60-month window (0-59)
        all_results = []
        
        for m60 in range(60):
            logger.info(f"Processing window {m60}...")
            
            # Filter for current m60 window
            window_data = data[data['m60'] == m60].copy()
            
            if len(window_data) == 0:
                continue
            
            # Forward fill time_avail_m within each permno
            window_data = window_data.sort_values(['permno', 'time_avail_m'])
            window_data['time_avail_m'] = window_data.groupby('permno')['time_avail_m'].ffill()
            
            # Drop observations without time_avail_m
            window_data = window_data.dropna(subset=['time_avail_m'])
            
            if len(window_data) == 0:
                continue
            
            # Simple demeaning following ACX
            # Demean returns
            expected_ret = window_data.groupby(['permno', 'time_avail_m'])['ret'].mean().reset_index()
            expected_ret = expected_ret.rename(columns={'ret': 'E_ret'})
            
            window_data = window_data.merge(expected_ret, on=['permno', 'time_avail_m'], how='left')
            window_data['ret'] = window_data['ret'] - window_data['E_ret']
            window_data = window_data.drop('E_ret', axis=1)
            
            # Demean market returns
            expected_mkt = window_data.groupby(['permno', 'time_avail_m'])['mkt'].mean().reset_index()
            expected_mkt = expected_mkt.rename(columns={'mkt': 'E_mkt'})
            
            window_data = window_data.merge(expected_mkt, on=['permno', 'time_avail_m'], how='left')
            window_data['mkt'] = window_data['mkt'] - window_data['E_mkt']
            window_data = window_data.drop('E_mkt', axis=1)
            
            # Calculate squared terms and cross terms
            window_data['ret2'] = window_data['ret'] ** 2
            window_data['mkt2'] = window_data['mkt'] ** 2
            window_data['ret_mkt2'] = window_data['ret'] * window_data['mkt2']
            
            # Calculate sample moments
            moments = window_data.groupby(['permno', 'time_avail_m']).agg({
                'ret_mkt2': 'mean',
                'ret2': 'mean',
                'mkt2': 'mean',
                'ret': 'count'
            }).reset_index()
            
            moments = moments.rename(columns={
                'ret_mkt2': 'E_ret_mkt2',
                'ret2': 'E_ret2',
                'mkt2': 'E_mkt2',
                'ret': 'nobs'
            })
            
            # Calculate Coskewness (equation B-9)
            moments['Coskewness'] = moments['E_ret_mkt2'] / (np.sqrt(moments['E_ret2']) * moments['E_mkt2'])
            
            # Filter for minimum 12 observations
            moments = moments[moments['nobs'] >= 12]
            
            # Keep only required columns
            window_results = moments[['permno', 'time_avail_m', 'Coskewness']].copy()
            all_results.append(window_results)
        
        # Combine all window results
        if all_results:
            final_data = pd.concat(all_results, ignore_index=True)
            final_data = final_data.sort_values(['permno', 'time_avail_m'])
        else:
            logger.error("No data processed successfully")
            return False
        
        logger.info(f"Successfully calculated Coskewness signal: {len(final_data)} observations")
        
        # SAVE RESULTS
        logger.info("Saving Coskewness predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = final_data[['permno', 'time_avail_m', 'Coskewness']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['Coskewness'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "Coskewness.csv"
        csv_data = output_data[['permno', 'yyyymm', 'Coskewness']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved Coskewness predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed Coskewness predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct Coskewness predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    coskewness()
