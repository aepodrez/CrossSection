"""
Python equivalent of CoskewACX.do
Generated from: CoskewACX.do

Original Stata file: CoskewACX.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def coskewacx():
    """
    Python equivalent of CoskewACX.do
    
    Constructs the CoskewACX predictor signal for coskewness following Ang, Chen, Xing 2006.
    """
    logger.info("Constructing predictor signal: CoskewACX...")
    
    try:
        # DATA LOAD
        # Load daily CRSP data
        crsp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/dailyCRSP.csv")
        
        logger.info(f"Loading daily CRSP data from: {crsp_path}")
        
        if not crsp_path.exists():
            logger.error(f"Daily CRSP data not found: {crsp_path}")
            logger.error("Please run the CRSP data download scripts first")
            return False
        
        # Load required variables from daily CRSP
        crsp_vars = ['permno', 'time_d', 'ret']
        data = pd.read_csv(crsp_path, usecols=crsp_vars)
        
        # Filter for dates after July 2, 1962
        data['time_d'] = pd.to_datetime(data['time_d'])
        data = data[data['time_d'] >= pd.to_datetime('1962-07-02')]
        
        logger.info(f"Successfully loaded {len(data)} daily records")
        
        # Load daily Fama-French data
        ff_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/dailyFF.csv")
        
        if not ff_path.exists():
            logger.error(f"Daily Fama-French data not found: {ff_path}")
            logger.error("Please run the Fama-French data download scripts first")
            return False
        
        # Load required variables from daily FF
        ff_vars = ['time_d', 'mktrf', 'rf']
        ff_data = pd.read_csv(ff_path, usecols=ff_vars)
        ff_data['time_d'] = pd.to_datetime(ff_data['time_d'])
        
        # Merge with Fama-French data
        data = data.merge(ff_data, on='time_d', how='inner')
        
        # Calculate market return
        data['mkt'] = data['mktrf'] + data['rf']
        
        # Convert to continuously compounded excess returns
        data['mkt'] = np.log(1 + data['mkt']) - np.log(1 + data['rf'])
        data['ret'] = np.log(1 + data['ret']) - np.log(1 + data['rf'])
        data = data.drop('rf', axis=1)
        
        # Sort by permno and reverse time for processing
        data = data.sort_values(['permno', 'time_d'], ascending=[True, False])
        
        logger.info("Processing coskewness by month...")
        
        # Process each month (1-12)
        all_results = []
        
        for month in range(1, 13):
            logger.info(f"Processing month {month}...")
            
            # Filter for current month
            month_data = data[data['time_d'].dt.month == month].copy()
            
            if len(month_data) == 0:
                continue
            
            # Create time_avail_m (end of month)
            # Convert time_d to datetime if needed for period conversion
            if not pd.api.types.is_datetime64_any_dtype(month_data['time_d']):
                month_data['time_d'] = pd.to_datetime(month_data['time_d'])
            
            month_data['time_avail_m'] = month_data['time_d'].dt.to_period('M')
            
            # Forward fill time_avail_m within each permno
            month_data = month_data.sort_values(['permno', 'time_d'])
            month_data['time_avail_m'] = month_data.groupby('permno')['time_avail_m'].ffill()
            
            # Drop observations without time_avail_m
            month_data = month_data.dropna(subset=['time_avail_m'])
            
            if len(month_data) == 0:
                continue
            
            # Calculate expected returns within each 12-month period
            expected_returns = month_data.groupby(['permno', 'time_avail_m'])[['ret', 'mkt']].mean().reset_index()
            expected_returns = expected_returns.rename(columns={'ret': 'E_ret', 'mkt': 'E_mkt'})
            
            # Merge back and demean returns
            month_data = month_data.merge(expected_returns, on=['permno', 'time_avail_m'], how='left')
            month_data['ret'] = month_data['ret'] - month_data['E_ret']
            month_data['mkt'] = month_data['mkt'] - month_data['E_mkt']
            
            # Calculate squared terms and cross terms
            month_data['ret2'] = month_data['ret'] ** 2
            month_data['mkt2'] = month_data['mkt'] ** 2
            month_data['ret_mkt2'] = month_data['ret'] * month_data['mkt2']
            
            # Calculate sample moments
            moments = month_data.groupby(['permno', 'time_avail_m']).agg({
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
            
            # Calculate CoskewACX (equation B-9)
            moments['CoskewACX'] = moments['E_ret_mkt2'] / (np.sqrt(moments['E_ret2']) * moments['E_mkt2'])
            
            # Filter out observations with more than 5 missing observations
            max_nobs = moments.groupby('time_avail_m')['nobs'].max().reset_index()
            max_nobs = max_nobs.rename(columns={'nobs': 'max_nobs'})
            
            moments = moments.merge(max_nobs, on='time_avail_m', how='left')
            moments = moments[moments['max_nobs'] - moments['nobs'] <= 5]
            
            # Keep only required columns
            month_results = moments[['permno', 'time_avail_m', 'CoskewACX']].copy()
            all_results.append(month_results)
        
        # Combine all monthly results
        if all_results:
            final_data = pd.concat(all_results, ignore_index=True)
            final_data = final_data.sort_values(['permno', 'time_avail_m'])
        else:
            logger.error("No data processed successfully")
            return False
        
        logger.info(f"Successfully calculated CoskewACX signal: {len(final_data)} observations")
        
        # SAVE RESULTS
        logger.info("Saving CoskewACX predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = final_data[['permno', 'time_avail_m', 'CoskewACX']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['CoskewACX'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        # Handle PeriodDtype by converting to timestamp first
        if hasattr(output_data['time_avail_m'], 'dt'):
            output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        else:
            # For PeriodDtype, convert to timestamp first
            output_data['yyyymm'] = output_data['time_avail_m'].dt.to_timestamp().dt.year * 100 + output_data['time_avail_m'].dt.to_timestamp().dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "CoskewACX.csv"
        csv_data = output_data[['permno', 'yyyymm', 'CoskewACX']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved CoskewACX predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed CoskewACX predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct CoskewACX predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    coskewacx()
