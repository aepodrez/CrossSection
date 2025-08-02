"""
Python equivalent of S_QFactorModel.do
Generated from: S_QFactorModel.do

Original Stata file: S_QFactorModel.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime
import requests
import tempfile
import os

logger = logging.getLogger(__name__)

def s_qfactormodel():
    """
    Python equivalent of S_QFactorModel.do
    
    Downloads and processes Q-factor model data from global-q.org
    """
    logger.info("Downloading Q-factor model data...")
    
    try:
        # URL from original Stata file
        url = "http://global-q.org/uploads/1/2/2/6/122679606/q5_factors_daily_2019.csv"
        
        logger.info(f"Downloading Q-factor data from: {url}")
        
        # Download the CSV file
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()  # Raise an exception for bad status codes
            
            # Create a temporary file to store the downloaded data
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
                temp_file.write(response.text)
                temp_file_path = temp_file.name
            
            # Read the CSV file
            data = pd.read_csv(temp_file_path)
            
            # Clean up temporary file
            os.unlink(temp_file_path)
            
            logger.info(f"Successfully downloaded {len(data)} Q-factor records")
            
        except requests.RequestException as e:
            logger.error(f"Failed to download Q-factor data: {e}")
            return False
        
        # Drop r_eg column (equivalent to Stata's "drop r_eg")
        if 'r_eg' in data.columns:
            data = data.drop(columns=['r_eg'])
            logger.info("Dropped r_eg column")
        
        # Rename r_* columns to r_*_qfac (equivalent to Stata's "rename r_* r_*_qfac")
        rename_dict = {}
        for col in data.columns:
            if col.startswith('r_'):
                rename_dict[col] = f"{col}_qfac"
        
        data = data.rename(columns=rename_dict)
        logger.info(f"Renamed {len(rename_dict)} columns with _qfac suffix")
        
        # Convert date column to time_d (equivalent to Stata's date conversion)
        if 'date' in data.columns:
            # Convert date string to datetime
            data['time_d'] = pd.to_datetime(data['date'], format='%Y%m%d')
            data = data.drop(columns=['date'])
            logger.info("Converted date column to time_d")
        
        # Convert percentage returns to decimal (divide by 100)
        # Equivalent to Stata's "foreach v of varlist r_* { replace `v' = `v'/100 }"
        r_columns = [col for col in data.columns if col.startswith('r_')]
        for col in r_columns:
            data[col] = data[col] / 100
        
        logger.info(f"Converted {len(r_columns)} return columns from percentage to decimal")
        
        # Save to intermediate file
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/d_qfactor.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved Q-factor data to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/d_qfactor.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log some summary statistics
        logger.info("Q-factor model summary:")
        logger.info(f"  Total records: {len(data)}")
        logger.info(f"  Time range: {data['time_d'].min()} to {data['time_d'].max()}")
        
        # Check data availability for each factor
        q_factors = [col for col in data.columns if col.startswith('r_') and col.endswith('_qfac')]
        factor_names = {
            'r_mkt_qfac': 'Market Factor',
            'r_me_qfac': 'Size Factor (ME)',
            'r_ia_qfac': 'Investment Factor (IA)',
            'r_roe_qfac': 'Profitability Factor (ROE)'
        }
        
        logger.info("  Q-factor availability:")
        for factor in q_factors:
            if factor in data.columns:
                non_missing = data[factor].notna().sum()
                missing = data[factor].isna().sum()
                factor_desc = factor_names.get(factor, factor)
                logger.info(f"    {factor} ({factor_desc}): {non_missing} non-missing, {missing} missing")
        
        # Summary statistics for each factor
        logger.info("  Q-factor summary statistics:")
        for factor in q_factors:
            if factor in data.columns:
                factor_data = data[factor].dropna()
                if len(factor_data) > 0:
                    mean_val = factor_data.mean()
                    std_val = factor_data.std()
                    min_val = factor_data.min()
                    max_val = factor_data.max()
                    factor_desc = factor_names.get(factor, factor)
                    logger.info(f"    {factor} ({factor_desc}): mean={mean_val:.6f}, std={std_val:.6f}, range=[{min_val:.6f}, {max_val:.6f}]")
        
        # Daily frequency analysis
        logger.info("  Daily frequency analysis:")
        logger.info(f"    Total days: {len(data)}")
        logger.info(f"    Years covered: {(data['time_d'].max() - data['time_d'].min()).days / 365.25:.1f}")
        
        # Check for any gaps in daily data
        expected_days = pd.date_range(
            start=data['time_d'].min(),
            end=data['time_d'].max(),
            freq='D'
        )
        actual_days = data['time_d'].sort_values()
        missing_days = set(expected_days) - set(actual_days)
        
        if missing_days:
            logger.warning(f"    Missing trading days: {len(missing_days)}")
            logger.warning(f"    First few missing days: {sorted(list(missing_days))[:5]}")
        else:
            logger.info("    No missing trading days detected")
        
        # Q-factor model interpretation
        logger.info("  Q-factor model interpretation:")
        logger.info("    Q-factor model (Hou, Xue, and Zhang, 2015):")
        logger.info("    - Market factor: Market excess return")
        logger.info("    - Size factor (ME): Small-minus-big based on market equity")
        logger.info("    - Investment factor (IA): Conservative-minus-aggressive based on investment")
        logger.info("    - Profitability factor (ROE): Robust-minus-weak based on profitability")
        logger.info("    - Alternative to Fama-French factors with investment and profitability")
        
        logger.info("Successfully downloaded and processed Q-factor model data")
        logger.info("Note: Daily Q-factor data for alternative factor model construction")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download Q-factor model data: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the download function
    s_qfactormodel()
