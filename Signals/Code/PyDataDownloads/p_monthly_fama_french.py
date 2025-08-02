"""
Python equivalent of P_Monthly_Fama-French.do
Generated from: P_Monthly_Fama-French.do

Original Stata file: P_Monthly_Fama-French.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def p_monthly_fama_french():
    """
    Python equivalent of P_Monthly_Fama-French.do
    
    Downloads and processes monthly Fama-French factors from WRDS
    """
    logger.info("Downloading monthly Fama-French factors...")
    
    try:
        # Use global WRDS connection from master.py
        from master import wrds_conn
        if wrds_conn is None:
            logger.error("WRDS connection not available. Please run master.py")
            return False
        conn = wrds_conn
        
        # SQL query from original Stata file
        query = """
        SELECT 
            date, mktrf, smb, hml, rf, umd
        FROM ff.factors_monthly
        """
        
        # Execute query
        data = conn.raw_sql(query, date_cols=['date'])
        logger.info(f"Downloaded {len(data)} monthly Fama-French factor records")
        
        # Create time_avail_m (month of date) - equivalent to Stata's mofd(date)
        data['time_avail_m'] = data['date'].dt.to_period('M')
        
        # Drop the original date column
        data = data.drop(columns=['date'])
        
        # Save to intermediate file
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/monthlyFF.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved monthly Fama-French factors to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/monthlyFF.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log some summary statistics
        logger.info("Monthly Fama-French factors summary:")
        logger.info(f"  Total records: {len(data)}")
        logger.info(f"  Time range: {data['time_avail_m'].min()} to {data['time_avail_m'].max()}")
        
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
        
        # Monthly frequency analysis
        logger.info("  Monthly frequency analysis:")
        logger.info(f"    Total months: {len(data)}")
        logger.info(f"    Years covered: {data['time_avail_m'].dt.year.max() - data['time_avail_m'].dt.year.min() + 1}")
        
        # Check for any gaps in monthly data
        expected_months = pd.date_range(
            start=data['time_avail_m'].min().to_timestamp(),
            end=data['time_avail_m'].max().to_timestamp(),
            freq='M'
        )
        actual_months = data['time_avail_m'].dt.to_timestamp().sort_values()
        missing_months = set(expected_months) - set(actual_months)
        
        if missing_months:
            logger.warning(f"    Missing months: {len(missing_months)}")
            logger.warning(f"    First few missing months: {sorted(list(missing_months))[:5]}")
        else:
            logger.info("    No missing months detected")
        
        logger.info("Successfully downloaded and processed monthly Fama-French factors")
        logger.info("Note: Monthly frequency factors for risk model construction and factor analysis")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download monthly Fama-French factors: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the download function
    p_monthly_fama_french()
