"""
ZZ1_zerotrade_zerotradeAlt1_zerotradeAlt12 Predictor Implementation

This script implements three zero trade predictors based on Liu (2006):
- zerotrade1M: Days with zero trades (1 month version)
- zerotrade6M: Days with zero trades (6 month version)
- zerotrade12M: Days with zero trades (12 month version)

The script calculates measures of trading illiquidity by counting days with zero volume
and adjusting for turnover, providing different time horizons for analysis.
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def zz1_zerotrade_zerotradealt1_zerotradealt12():
    """Main function to calculate zerotrade1M, zerotrade6M, and zerotrade12M predictors."""
    
    # Define file paths
    base_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data")
    daily_crsp_path = base_path / "Intermediate" / "dailyCRSP.csv"
    output_path = base_path / "Predictors"
    
    # Ensure output directory exists
    output_path.mkdir(parents=True, exist_ok=True)
    
    logger.info("Starting zerotrade1M, zerotrade6M, zerotrade12M predictor calculation")
    
    try:
        # DATA LOAD
        logger.info("Loading daily CRSP data")
        data = pd.read_csv(daily_crsp_path)
        
        # Convert time_d to datetime
        data['time_d'] = pd.to_datetime(data['time_d'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating monthly aggregates")
        
        # Create monthly time period
        data['time_avail_m'] = data['time_d'].dt.to_period('M').dt.to_timestamp()
        
        # Calculate zero trade indicators and turnover
        data['countzero'] = np.where(data['vol'] == 0, 1, 0)
        data['turn'] = data['vol'] / data['shrout']  # daily turnover
        
        # Create help variable for collapse
        data['days'] = 0
        
        # Aggregate to monthly level
        logger.info("Aggregating to monthly level")
        monthly_data = data.groupby(['permno', 'time_avail_m']).agg({
            'countzero': 'sum',
            'turn': 'sum',
            'days': 'count'
        }).reset_index()
        
        # Rename days count to ndays
        monthly_data = monthly_data.rename(columns={'days': 'ndays'})
        
        # Sort by permno and time_avail_m
        monthly_data = monthly_data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate zero trade measures
        logger.info("Calculating zero trade measures")
        
        # 1-month version
        monthly_data['temp_zerotrade'] = (monthly_data['countzero'] + ((1 / monthly_data['turn']) / 480000)) * (21 / monthly_data['ndays'])
        monthly_data['zerotrade1M'] = monthly_data.groupby('permno')['temp_zerotrade'].shift(1)
        
        # 6-month version
        logger.info("Calculating 6-month zero trade measure")
        for i in range(1, 6):
            monthly_data[f'turn_lag{i}'] = monthly_data.groupby('permno')['turn'].shift(i)
            monthly_data[f'countzero_lag{i}'] = monthly_data.groupby('permno')['countzero'].shift(i)
            monthly_data[f'ndays_lag{i}'] = monthly_data.groupby('permno')['ndays'].shift(i)
        
        monthly_data['Turn6'] = monthly_data['turn'] + monthly_data['turn_lag1'] + monthly_data['turn_lag2'] + monthly_data['turn_lag3'] + monthly_data['turn_lag4'] + monthly_data['turn_lag5']
        monthly_data['countzero6'] = monthly_data['countzero'] + monthly_data['countzero_lag1'] + monthly_data['countzero_lag2'] + monthly_data['countzero_lag3'] + monthly_data['countzero_lag4'] + monthly_data['countzero_lag5']
        monthly_data['ndays6'] = monthly_data['ndays'] + monthly_data['ndays_lag1'] + monthly_data['ndays_lag2'] + monthly_data['ndays_lag3'] + monthly_data['ndays_lag4'] + monthly_data['ndays_lag5']
        
        monthly_data['temp_zerotrade6'] = (monthly_data['countzero6'] + ((1 / monthly_data['Turn6']) / 11000)) * (21 * 6 / monthly_data['ndays6'])
        monthly_data['zerotrade6M'] = monthly_data.groupby('permno')['temp_zerotrade6'].shift(1)
        
        # 12-month version
        logger.info("Calculating 12-month zero trade measure")
        for i in range(6, 12):
            monthly_data[f'turn_lag{i}'] = monthly_data.groupby('permno')['turn'].shift(i)
            monthly_data[f'countzero_lag{i}'] = monthly_data.groupby('permno')['countzero'].shift(i)
            monthly_data[f'ndays_lag{i}'] = monthly_data.groupby('permno')['ndays'].shift(i)
        
        monthly_data['Turn12'] = monthly_data['Turn6'] + monthly_data['turn_lag6'] + monthly_data['turn_lag7'] + monthly_data['turn_lag8'] + monthly_data['turn_lag9'] + monthly_data['turn_lag10'] + monthly_data['turn_lag11']
        monthly_data['countzero12'] = monthly_data['countzero6'] + monthly_data['countzero_lag6'] + monthly_data['countzero_lag7'] + monthly_data['countzero_lag8'] + monthly_data['countzero_lag9'] + monthly_data['countzero_lag10'] + monthly_data['countzero_lag11']
        monthly_data['ndays12'] = monthly_data['ndays6'] + monthly_data['ndays_lag6'] + monthly_data['ndays_lag7'] + monthly_data['ndays_lag8'] + monthly_data['ndays_lag9'] + monthly_data['ndays_lag10'] + monthly_data['ndays_lag11']
        
        monthly_data['temp_zerotrade12'] = (monthly_data['countzero12'] + ((1 / monthly_data['Turn12']) / 11000)) * (21 * 12 / monthly_data['ndays12'])
        monthly_data['zerotrade12M'] = monthly_data.groupby('permno')['temp_zerotrade12'].shift(1)
        
        # Prepare output data
        logger.info("Preparing output data")
        
        # For zerotrade1M
        zerotrade1m_output = monthly_data[['permno', 'time_avail_m', 'zerotrade1M']].copy()
        zerotrade1m_output = zerotrade1m_output.dropna(subset=['zerotrade1M'])
        zerotrade1m_output['yyyymm'] = zerotrade1m_output['time_avail_m'].dt.strftime('%Y%m').astype(int)
        zerotrade1m_output = zerotrade1m_output[['permno', 'yyyymm', 'zerotrade1M']]
        
        # For zerotrade6M
        zerotrade6m_output = monthly_data[['permno', 'time_avail_m', 'zerotrade6M']].copy()
        zerotrade6m_output = zerotrade6m_output.dropna(subset=['zerotrade6M'])
        zerotrade6m_output['yyyymm'] = zerotrade6m_output['time_avail_m'].dt.strftime('%Y%m').astype(int)
        zerotrade6m_output = zerotrade6m_output[['permno', 'yyyymm', 'zerotrade6M']]
        
        # For zerotrade12M
        zerotrade12m_output = monthly_data[['permno', 'time_avail_m', 'zerotrade12M']].copy()
        zerotrade12m_output = zerotrade12m_output.dropna(subset=['zerotrade12M'])
        zerotrade12m_output['yyyymm'] = zerotrade12m_output['time_avail_m'].dt.strftime('%Y%m').astype(int)
        zerotrade12m_output = zerotrade12m_output[['permno', 'yyyymm', 'zerotrade12M']]
        
        # Save results
        logger.info("Saving results")
        
        # Save zerotrade1M
        zerotrade1m_file = output_path / "zerotrade1M.csv"
        zerotrade1m_output.to_csv(zerotrade1m_file, index=False)
        logger.info(f"Saved zerotrade1M predictor to {zerotrade1m_file}")
        logger.info(f"zerotrade1M: {len(zerotrade1m_output)} observations")
        
        # Save zerotrade6M
        zerotrade6m_file = output_path / "zerotrade6M.csv"
        zerotrade6m_output.to_csv(zerotrade6m_file, index=False)
        logger.info(f"Saved zerotrade6M predictor to {zerotrade6m_file}")
        logger.info(f"zerotrade6M: {len(zerotrade6m_output)} observations")
        
        # Save zerotrade12M
        zerotrade12m_file = output_path / "zerotrade12M.csv"
        zerotrade12m_output.to_csv(zerotrade12m_file, index=False)
        logger.info(f"Saved zerotrade12M predictor to {zerotrade12m_file}")
        logger.info(f"zerotrade12M: {len(zerotrade12m_output)} observations")
        
        logger.info("Successfully completed zerotrade1M, zerotrade6M, zerotrade12M predictor calculation")
        
    except Exception as e:
        logger.error(f"Error in zerotrade1M, zerotrade6M, zerotrade12M calculation: {str(e)}")
        raise

if __name__ == "__main__":
    main()
