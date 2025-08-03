"""
Python equivalent of Q_MarketReturns.do
Generated from: Q_MarketReturns.do

Original Stata file: Q_MarketReturns.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def q_marketreturns(wrds_conn=None):
    """
    Python equivalent of Q_MarketReturns.do
    
    Downloads and processes monthly equal- and value-weighted market returns from CRSP
    """
    logger.info("Downloading monthly market returns...")
    
    try:
        # Check if WRDS connection is provided
        if wrds_conn is None:
            logger.error("WRDS connection not available. Please run master.py")
            return False
        conn = wrds_conn
        
        # SQL query from original Stata file
        query = """
        SELECT 
            date, vwretd, ewretd, usdval
        FROM crsp.msi
        """
        
        # Execute query
        data = conn.raw_sql(query, date_cols=['date'])
        logger.info(f"Downloaded {len(data)} monthly market return records")
        
        # Create time_avail_m (month of date) - equivalent to Stata's mofd(date)
        data['time_avail_m'] = data['date'].dt.to_period('M')
        
        # Drop the original date column
        data = data.drop(columns=['date'])
        
        # Save to intermediate file
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/monthlyMarket.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved monthly market returns to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/monthlyMarket.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log some summary statistics
        logger.info("Monthly market returns summary:")
        logger.info(f"  Total records: {len(data)}")
        logger.info(f"  Time range: {data['time_avail_m'].min()} to {data['time_avail_m'].max()}")
        
        # Check data availability for each variable
        variables = ['vwretd', 'ewretd', 'usdval']
        variable_names = {
            'vwretd': 'Value-Weighted Market Return',
            'ewretd': 'Equal-Weighted Market Return',
            'usdval': 'Market Value (USD)'
        }
        
        logger.info("  Variable availability:")
        for var in variables:
            if var in data.columns:
                non_missing = data[var].notna().sum()
                missing = data[var].isna().sum()
                logger.info(f"    {var} ({variable_names[var]}): {non_missing} non-missing, {missing} missing")
        
        # Summary statistics for each variable
        logger.info("  Variable summary statistics:")
        for var in variables:
            if var in data.columns:
                var_data = data[var].dropna()
                if len(var_data) > 0:
                    mean_val = var_data.mean()
                    std_val = var_data.std()
                    min_val = var_data.min()
                    max_val = var_data.max()
                    logger.info(f"    {var}: mean={mean_val:.6f}, std={std_val:.6f}, range=[{min_val:.6f}, {max_val:.6f}]")
        
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
        
        # Market return analysis
        if 'vwretd' in data.columns and 'ewretd' in data.columns:
            logger.info("  Market return analysis:")
            
            # Calculate correlation between value-weighted and equal-weighted returns
            vw_ew_corr = data['vwretd'].corr(data['ewretd'])
            logger.info(f"    VW-EW correlation: {vw_ew_corr:.4f}")
            
            # Calculate average difference between EW and VW returns
            ew_vw_diff = (data['ewretd'] - data['vwretd']).mean()
            logger.info(f"    Average EW-VW difference: {ew_vw_diff:.6f}")
            
            # Calculate volatility comparison
            vw_vol = data['vwretd'].std()
            ew_vol = data['ewretd'].std()
            logger.info(f"    VW volatility: {vw_vol:.6f}")
            logger.info(f"    EW volatility: {ew_vol:.6f}")
            logger.info(f"    EW/VW volatility ratio: {ew_vol/vw_vol:.4f}")
        
        # Market value analysis
        if 'usdval' in data.columns:
            logger.info("  Market value analysis:")
            usdval_data = data['usdval'].dropna()
            if len(usdval_data) > 0:
                logger.info(f"    Average market value: ${usdval_data.mean():,.0f}")
                logger.info(f"    Market value range: ${usdval_data.min():,.0f} to ${usdval_data.max():,.0f}")
                
                # Calculate market value growth
                usdval_growth = (usdval_data.iloc[-1] / usdval_data.iloc[0]) ** (12 / len(usdval_data)) - 1
                logger.info(f"    Annualized market value growth: {usdval_growth:.4f}")
        
        logger.info("Successfully downloaded and processed monthly market returns")
        logger.info("Note: Monthly market returns for market analysis and benchmark construction")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download monthly market returns: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the download function
    q_marketreturns()
