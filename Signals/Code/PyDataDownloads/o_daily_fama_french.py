"""
Python equivalent of O_Daily_Fama-French.do
Generated from: O_Daily_Fama-French.do

Original Stata file: O_Daily_Fama-French.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def o_daily_fama_french(wrds_conn=None):
    """
    Python equivalent of O_Daily_Fama-French.do
    
    Downloads and processes daily Fama-French factors from WRDS
    """
    logger.info("Downloading daily Fama-French factors...")
    
    try:
        # Check if WRDS connection is provided
        if wrds_conn is None:
            logger.error("WRDS connection not available. Please run master.py")
            return False
        conn = wrds_conn
        
        # SQL query from original Stata file
        query = """
        SELECT 
            date, mktrf, smb, hml, rf, umd
        FROM ff.factors_daily
        """
        
        # Execute query
        data = conn.raw_sql(query, date_cols=['date'])
        logger.info(f"Downloaded {len(data)} daily Fama-French factor records")
        
        # Rename date to time_d
        data = data.rename(columns={'date': 'time_d'})
        
        # Save to intermediate file
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/dailyFF.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved daily Fama-French factors to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/dailyFF.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log some summary statistics
        logger.info("Daily Fama-French factors summary:")
        logger.info(f"  Total records: {len(data)}")
        logger.info(f"  Time range: {data['time_d'].min()} to {data['time_d'].max()}")
        
        # Check data availability for each factor
        factors = ['mktrf', 'smb', 'hml', 'rf', 'umd']
        factor_names = {
            'mktrf': 'Market Risk Premium',
            'smb': 'Small-Minus-Big',
            'hml': 'High-Minus-Low',
            'rf': 'Risk-Free Rate',
            'umd': 'Up-Minus-Down (Momentum)'
        }
        
        logger.info("  Factor availability:")
        for factor in factors:
            if factor in data.columns:
                non_missing = data[factor].notna().sum()
                missing = data[factor].isna().sum()
                logger.info(f"    {factor} ({factor_names[factor]}): {non_missing} non-missing, {missing} missing")
        
        # Summary statistics for each factor
        logger.info("  Factor summary statistics:")
        for factor in factors:
            if factor in data.columns:
                factor_data = data[factor].dropna()
                if len(factor_data) > 0:
                    mean_val = factor_data.mean()
                    std_val = factor_data.std()
                    min_val = factor_data.min()
                    max_val = factor_data.max()
                    logger.info(f"    {factor}: mean={mean_val:.6f}, std={std_val:.6f}, range=[{min_val:.6f}, {max_val:.6f}]")
        
        logger.info("Successfully downloaded and processed daily Fama-French factors")
        logger.info("Note: Daily frequency factors for risk model construction and factor analysis")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download daily Fama-French factors: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the download function
    o_daily_fama_french()
