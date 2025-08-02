"""
Python equivalent of V_TBill3M.do
Generated from: V_TBill3M.do

Original Stata file: V_TBill3M.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime
import fredapi

logger = logging.getLogger(__name__)

def v_tbill3m():
    """
    Python equivalent of V_TBill3M.do
    
    Downloads and processes 3-month T-Bill rate data from FRED
    """
    logger.info("Downloading 3-month T-Bill rate data from FRED...")
    
    try:
        # Use global FRED connection from master.py
        from master import fred
        if fred is None:
            logger.error("FRED connection not available. Please run master.py")
            return False
        
        # FRED series ID from original Stata file
        series_id = 'TB3MS'  # 3-Month Treasury Bill: Secondary Market Rate
        
        logger.info(f"Downloading 3-month T-Bill rate data for series: {series_id}")
        
        # Download data from FRED
        try:
            data = fred.get_series(series_id)
            data = data.reset_index()
            data.columns = ['daten', 'TB3MS']
            logger.info(f"Downloaded {len(data)} 3-month T-Bill rate records")
        except Exception as e:
            logger.error(f"Failed to download 3-month T-Bill rate data: {e}")
            return False
        
        # Aggregate to quarterly (average) - equivalent to Stata's "aggregate(q, avg)"
        data['year'] = data['daten'].dt.year
        data['quarter'] = data['daten'].dt.quarter
        
        # Group by year and quarter and calculate average
        quarterly_data = data.groupby(['year', 'quarter'])['TB3MS'].mean().reset_index()
        quarterly_data.columns = ['year', 'qtr', 'TB3MS']
        
        logger.info(f"Aggregated to {len(quarterly_data)} quarterly observations")
        
        # Convert TB3MS to decimal - equivalent to Stata's "gen TbillRate3M = TB3MS/100"
        quarterly_data['TbillRate3M'] = quarterly_data['TB3MS'] / 100
        
        # Keep only year, qtr, and TbillRate3M - equivalent to Stata's "keep year qtr TbillRate3M"
        quarterly_data = quarterly_data[['year', 'qtr', 'TbillRate3M']]
        
        # Save to intermediate file
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/TBill3M.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        quarterly_data.to_csv(output_path, index=False)
        logger.info(f"Saved 3-month T-Bill rate data to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/TBill3M.csv")
        quarterly_data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log some summary statistics
        logger.info("3-month T-Bill rate summary:")
        logger.info(f"  Total records: {len(quarterly_data)}")
        logger.info(f"  Time range: {quarterly_data['year'].min()} Q{quarterly_data['qtr'].min()} to {quarterly_data['year'].max()} Q{quarterly_data['qtr'].max()}")
        
        # Check data availability
        if 'TbillRate3M' in quarterly_data.columns:
            non_missing = quarterly_data['TbillRate3M'].notna().sum()
            missing = quarterly_data['TbillRate3M'].isna().sum()
            logger.info(f"  T-Bill rate availability: {non_missing} non-missing, {missing} missing")
            
            # T-Bill rate summary statistics
            tbill_data = quarterly_data['TbillRate3M'].dropna()
            if len(tbill_data) > 0:
                mean_val = tbill_data.mean()
                std_val = tbill_data.std()
                min_val = tbill_data.min()
                max_val = tbill_data.max()
                logger.info(f"  T-Bill rate statistics: mean={mean_val:.4f}, std={std_val:.4f}, range=[{min_val:.4f}, {max_val:.4f}]")
                
                # Interest rate analysis
                logger.info("  Interest rate analysis:")
                logger.info(f"    Average 3-month T-Bill rate: {mean_val:.4f}")
                logger.info(f"    T-Bill rate volatility: {std_val:.4f}")
                logger.info(f"    Historical range: {min_val:.4f} to {max_val:.4f}")
                
                # Interest rate regime analysis
                very_low = (tbill_data < 0.01).sum()  # <1%
                low = ((tbill_data >= 0.01) & (tbill_data < 0.03)).sum()  # 1-3%
                moderate = ((tbill_data >= 0.03) & (tbill_data < 0.06)).sum()  # 3-6%
                high = ((tbill_data >= 0.06) & (tbill_data < 0.10)).sum()  # 6-10%
                very_high = (tbill_data >= 0.10).sum()  # ≥10%
                
                total_obs = len(tbill_data)
                logger.info(f"    Very low rates (<1%): {very_low} ({very_low/total_obs*100:.1f}%)")
                logger.info(f"    Low rates (1-3%): {low} ({low/total_obs*100:.1f}%)")
                logger.info(f"    Moderate rates (3-6%): {moderate} ({moderate/total_obs*100:.1f}%)")
                logger.info(f"    High rates (6-10%): {high} ({high/total_obs*100:.1f}%)")
                logger.info(f"    Very high rates (≥10%): {very_high} ({very_high/total_obs*100:.1f}%)")
                
                # Calculate rate changes (quarter-over-quarter)
                quarterly_data_sorted = quarterly_data.sort_values(['year', 'qtr'])
                quarterly_data_sorted['rate_change'] = quarterly_data_sorted['TbillRate3M'].diff()
                
                rate_change_data = quarterly_data_sorted['rate_change'].dropna()
                if len(rate_change_data) > 0:
                    avg_change = rate_change_data.mean()
                    change_vol = rate_change_data.std()
                    positive_changes = (rate_change_data > 0).sum()
                    negative_changes = (rate_change_data < 0).sum()
                    zero_changes = (rate_change_data == 0).sum()
                    
                    logger.info(f"    Average quarterly rate change: {avg_change:.4f}")
                    logger.info(f"    Rate change volatility: {change_vol:.4f}")
                    logger.info(f"    Positive changes: {positive_changes} ({positive_changes/len(rate_change_data)*100:.1f}%)")
                    logger.info(f"    Negative changes: {negative_changes} ({negative_changes/len(rate_change_data)*100:.1f}%)")
                    logger.info(f"    Zero changes: {zero_changes} ({zero_changes/len(rate_change_data)*100:.1f}%)")
        
        # Quarterly frequency analysis
        logger.info("  Quarterly frequency analysis:")
        logger.info(f"    Total quarters: {len(quarterly_data)}")
        logger.info(f"    Years covered: {quarterly_data['year'].max() - quarterly_data['year'].min() + 1}")
        
        # Check for any gaps in quarterly data
        expected_quarters = []
        for year in range(quarterly_data['year'].min(), quarterly_data['year'].max() + 1):
            for quarter in range(1, 5):
                expected_quarters.append((year, quarter))
        
        actual_quarters = list(zip(quarterly_data['year'], quarterly_data['qtr']))
        missing_quarters = set(expected_quarters) - set(actual_quarters)
        
        if missing_quarters:
            logger.warning(f"    Missing quarters: {len(missing_quarters)}")
            logger.warning(f"    First few missing quarters: {sorted(list(missing_quarters))[:5]}")
        else:
            logger.info("    No missing quarters detected")
        
        # Data processing explanation
        logger.info("  Data processing explanation:")
        logger.info("    - Original data: Monthly 3-month T-Bill rates from FRED")
        logger.info("    - Aggregation: Monthly data averaged to quarterly frequency")
        logger.info("    - Conversion: Percentage rates divided by 100 (percentage to decimal)")
        logger.info("    - Usage: Risk-free rate for asset pricing and return calculations")
        
        # T-Bill rate interpretation
        logger.info("  T-Bill rate interpretation:")
        logger.info("    TB3MS: 3-Month Treasury Bill Secondary Market Rate")
        logger.info("    - Risk-free interest rate benchmark")
        logger.info("    - Used as risk-free rate in asset pricing models")
        logger.info("    - Reflects monetary policy and market conditions")
        logger.info("    - Higher rates indicate tighter monetary policy")
        logger.info("    - Lower rates indicate accommodative monetary policy")
        
        logger.info("Successfully downloaded and processed 3-month T-Bill rate data")
        logger.info("Note: Quarterly 3-month T-Bill rate data for risk-free rate calculations")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download 3-month T-Bill rate data: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the download function
    v_tbill3m()
