"""
Python equivalent of R_MonthlyLiquidityFactor.do
Generated from: R_MonthlyLiquidityFactor.do

Original Stata file: R_MonthlyLiquidityFactor.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def r_monthlyliquidityfactor(wrds_conn=None):
    """
    Python equivalent of R_MonthlyLiquidityFactor.do
    
    Downloads and processes monthly liquidity factor from Fama-French database
    """
    logger.info("Downloading monthly liquidity factor...")
    
    try:
        # Check if WRDS connection is provided
        if wrds_conn is None:
            logger.error("WRDS connection not available. Please run master.py")
            return False
        conn = wrds_conn
        
        # SQL query from original Stata file
        query = """
        SELECT 
            date, ps_innov
        FROM ff.liq_ps
        """
        
        # Execute query
        data = conn.raw_sql(query, date_cols=['date'])
        logger.info(f"Downloaded {len(data)} monthly liquidity factor records")
        
        # Create time_avail_m (month of date) - equivalent to Stata's mofd(date)
        data['time_avail_m'] = data['date'].dt.to_period('M')
        
        # Drop the original date column
        data = data.drop(columns=['date'])
        
        # Save to intermediate file
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/monthlyLiquidity.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved monthly liquidity factor to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/monthlyLiquidity.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log some summary statistics
        logger.info("Monthly liquidity factor summary:")
        logger.info(f"  Total records: {len(data)}")
        logger.info(f"  Time range: {data['time_avail_m'].min()} to {data['time_avail_m'].max()}")
        
        # Check data availability for ps_innov
        if 'ps_innov' in data.columns:
            non_missing = data['ps_innov'].notna().sum()
            missing = data['ps_innov'].isna().sum()
            logger.info(f"  ps_innov (Pastor-Stambaugh Liquidity Innovation): {non_missing} non-missing, {missing} missing")
            
            # Summary statistics for liquidity factor
            liq_data = data['ps_innov'].dropna()
            if len(liq_data) > 0:
                mean_val = liq_data.mean()
                std_val = liq_data.std()
                min_val = liq_data.min()
                max_val = liq_data.max()
                logger.info(f"  ps_innov statistics: mean={mean_val:.6f}, std={std_val:.6f}, range=[{min_val:.6f}, {max_val:.6f}]")
                
                # Additional liquidity factor analysis
                logger.info("  Liquidity factor analysis:")
                
                # Calculate percentiles
                p25 = liq_data.quantile(0.25)
                p50 = liq_data.quantile(0.50)
                p75 = liq_data.quantile(0.75)
                logger.info(f"    Percentiles: 25th={p25:.6f}, 50th={p50:.6f}, 75th={p75:.6f}")
                
                # Calculate skewness and kurtosis
                skewness = liq_data.skew()
                kurtosis = liq_data.kurtosis()
                logger.info(f"    Skewness: {skewness:.4f}")
                logger.info(f"    Kurtosis: {kurtosis:.4f}")
                
                # Calculate positive vs negative innovations
                positive_innov = (liq_data > 0).sum()
                negative_innov = (liq_data < 0).sum()
                zero_innov = (liq_data == 0).sum()
                logger.info(f"    Positive innovations: {positive_innov} ({positive_innov/len(liq_data)*100:.1f}%)")
                logger.info(f"    Negative innovations: {negative_innov} ({negative_innov/len(liq_data)*100:.1f}%)")
                logger.info(f"    Zero innovations: {zero_innov} ({zero_innov/len(liq_data)*100:.1f}%)")
        
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
        
        # Liquidity factor interpretation
        logger.info("  Liquidity factor interpretation:")
        logger.info("    ps_innov: Pastor-Stambaugh liquidity innovation factor")
        logger.info("    - Positive values: Improving market liquidity")
        logger.info("    - Negative values: Deteriorating market liquidity")
        logger.info("    - Used in liquidity-augmented factor models")
        logger.info("    - Captures systematic liquidity risk")
        
        logger.info("Successfully downloaded and processed monthly liquidity factor")
        logger.info("Note: Monthly liquidity factor for liquidity-augmented factor models")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download monthly liquidity factor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the download function
    r_monthlyliquidityfactor()
