"""
Python equivalent of U_GNPDeflator.do
Generated from: U_GNPDeflator.do

Original Stata file: U_GNPDeflator.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime
import fredapi

logger = logging.getLogger(__name__)

def u_gnpdeflator():
    """
    Python equivalent of U_GNPDeflator.do
    
    Downloads and processes GNP Deflator data from FRED
    """
    logger.info("Downloading GNP Deflator data from FRED...")
    
    try:
        # Use global FRED connection from master.py
        from master import fred
        if fred is None:
            logger.error("FRED connection not available. Please run master.py")
            return False
        
        # FRED series ID from original Stata file
        series_id = 'GNPCTPI'  # Gross National Product: Implicit Price Deflator
        
        logger.info(f"Downloading GNP Deflator data for series: {series_id}")
        
        # Download data from FRED
        try:
            data = fred.get_series(series_id)
            data = data.reset_index()
            data.columns = ['daten', 'GNPCTPI']
            logger.info(f"Downloaded {len(data)} GNP Deflator records")
        except Exception as e:
            logger.error(f"Failed to download GNP Deflator data: {e}")
            return False
        
        # Create temp_time_m (month of date) - equivalent to Stata's "gen temp_time_m = mofd(daten)"
        data['temp_time_m'] = data['daten'].dt.to_period('M')
        
        # Expand to monthly (3 months per quarter) - equivalent to Stata's "expand 3"
        expanded_data = []
        for _, row in data.iterrows():
            quarter_start = row['temp_time_m']
            # Create 3 monthly observations for each quarter
            for i in range(3):
                month_period = quarter_start + i
                expanded_data.append({
                    'temp_time_m': month_period,
                    'GNPCTPI': row['GNPCTPI']
                })
        
        data = pd.DataFrame(expanded_data)
        logger.info(f"Expanded to {len(data)} monthly observations")
        
        # Create time_avail_m - equivalent to Stata's "bys temp: gen time_avail_m = temp + _n - 1"
        data = data.sort_values('temp_time_m')
        data['time_avail_m'] = data['temp_time_m']
        
        # Add 3-month lag - equivalent to Stata's "replace time_avail_m = time_avail_m + 3"
        data['time_avail_m'] = data['time_avail_m'] + 3
        
        # Convert GNPCTPI to decimal - equivalent to Stata's "gen gnpdefl = GNPCTPI/100"
        data['gnpdefl'] = data['GNPCTPI'] / 100
        
        # Keep only time_avail_m and gnpdefl - equivalent to Stata's "keep time gnpdefl"
        data = data[['time_avail_m', 'gnpdefl']]
        
        # Save to intermediate file
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/GNPdefl.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved GNP Deflator data to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/GNPdefl.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log some summary statistics
        logger.info("GNP Deflator summary:")
        logger.info(f"  Total records: {len(data)}")
        logger.info(f"  Time range: {data['time_avail_m'].min()} to {data['time_avail_m'].max()}")
        
        # Check data availability
        if 'gnpdefl' in data.columns:
            non_missing = data['gnpdefl'].notna().sum()
            missing = data['gnpdefl'].isna().sum()
            logger.info(f"  GNP Deflator availability: {non_missing} non-missing, {missing} missing")
            
            # GNP Deflator summary statistics
            gnp_data = data['gnpdefl'].dropna()
            if len(gnp_data) > 0:
                mean_val = gnp_data.mean()
                std_val = gnp_data.std()
                min_val = gnp_data.min()
                max_val = gnp_data.max()
                logger.info(f"  GNP Deflator statistics: mean={mean_val:.4f}, std={std_val:.4f}, range=[{min_val:.4f}, {max_val:.4f}]")
                
                # Inflation analysis
                logger.info("  Inflation analysis:")
                logger.info(f"    Average GNP Deflator: {mean_val:.4f}")
                logger.info(f"    GNP Deflator volatility: {std_val:.4f}")
                logger.info(f"    Historical range: {min_val:.4f} to {max_val:.4f}")
                
                # Calculate inflation rates (period-over-period changes)
                data_sorted = data.sort_values('time_avail_m')
                data_sorted['inflation_rate'] = data_sorted['gnpdefl'].pct_change()
                
                inflation_data = data_sorted['inflation_rate'].dropna()
                if len(inflation_data) > 0:
                    avg_inflation = inflation_data.mean() * 12  # Annualized
                    inflation_vol = inflation_data.std() * np.sqrt(12)  # Annualized volatility
                    logger.info(f"    Average annualized inflation rate: {avg_inflation:.4f}")
                    logger.info(f"    Inflation volatility: {inflation_vol:.4f}")
                    
                    # Inflation regime analysis
                    deflation = (inflation_data < 0).sum()
                    low_inflation = ((inflation_data >= 0) & (inflation_data < 0.02/12)).sum()  # <2% annual
                    moderate_inflation = ((inflation_data >= 0.02/12) & (inflation_data < 0.05/12)).sum()  # 2-5% annual
                    high_inflation = (inflation_data >= 0.05/12).sum()  # >5% annual
                    
                    total_obs = len(inflation_data)
                    logger.info(f"    Deflation periods: {deflation} ({deflation/total_obs*100:.1f}%)")
                    logger.info(f"    Low inflation (<2% annual): {low_inflation} ({low_inflation/total_obs*100:.1f}%)")
                    logger.info(f"    Moderate inflation (2-5% annual): {moderate_inflation} ({moderate_inflation/total_obs*100:.1f}%)")
                    logger.info(f"    High inflation (>5% annual): {high_inflation} ({high_inflation/total_obs*100:.1f}%)")
        
        # Monthly frequency analysis
        logger.info("  Monthly frequency analysis:")
        logger.info(f"    Total months: {len(data)}")
        logger.info(f"    Years covered: {data['time_avail_m'].dt.year.max() - data['time_avail_m'].dt.year.min() + 1}")
        
        # Check for any gaps in monthly data
        expected_months = pd.date_range(
            start=data['time_avail_m'].min().to_timestamp(),
            end=data['time_avail_m'].max().to_timestamp(),
            freq='ME'
        )
        actual_months = data['time_avail_m'].dt.to_timestamp().sort_values()
        missing_months = set(expected_months) - set(actual_months)
        
        if missing_months:
            logger.warning(f"    Missing months: {len(missing_months)}")
            logger.warning(f"    First few missing months: {sorted(list(missing_months))[:5]}")
        else:
            logger.info("    No missing months detected")
        
        # Data processing explanation
        logger.info("  Data processing explanation:")
        logger.info("    - Original data: Quarterly GNP Deflator from FRED")
        logger.info("    - Expansion: Each quarter expanded to 3 monthly observations")
        logger.info("    - Lag: 3-month availability lag applied")
        logger.info("    - Conversion: Index values divided by 100 (percentage to decimal)")
        logger.info("    - Usage: Inflation adjustment and real return calculations")
        
        # GNP Deflator interpretation
        logger.info("  GNP Deflator interpretation:")
        logger.info("    GNP Deflator: Gross National Product Implicit Price Deflator")
        logger.info("    - Measures price level changes in the economy")
        logger.info("    - Base year typically set to 100")
        logger.info("    - Used for inflation adjustment and real return calculations")
        logger.info("    - Higher values indicate higher price levels (inflation)")
        logger.info("    - Lower values indicate lower price levels (deflation)")
        
        logger.info("Successfully downloaded and processed GNP Deflator data")
        logger.info("Note: Monthly GNP Deflator data for inflation adjustment and real return calculations")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download GNP Deflator data: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the download function
    u_gnpdeflator()
