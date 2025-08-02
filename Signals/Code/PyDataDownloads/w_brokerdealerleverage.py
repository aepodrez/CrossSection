"""
Python equivalent of W_BrokerDealerLeverage.do
Generated from: W_BrokerDealerLeverage.do

Original Stata file: W_BrokerDealerLeverage.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime
import fredapi

logger = logging.getLogger(__name__)

def w_brokerdealerleverage():
    """
    Python equivalent of W_BrokerDealerLeverage.do
    
    Downloads and processes broker-dealer leverage data from FRED
    """
    logger.info("Downloading broker-dealer leverage data from FRED...")
    
    try:
        # Use global FRED connection from master.py
        from master import fred
        if fred is None:
            logger.error("FRED connection not available. Please run master.py")
            return False
        
        # FRED series IDs from original Stata file
        series_ids = {
            'BOGZ1FL664090005Q': 'assets',    # Financial assets
            'BOGZ1FL664190005Q': 'liab',      # Financial liabilities  
            'BOGZ1FL665080003Q': 'equity'     # Financial equity
        }
        
        logger.info(f"Downloading broker-dealer data for series: {list(series_ids.keys())}")
        
        # Download and merge all series
        merged_data = None
        
        for series_id, col_name in series_ids.items():
            try:
                # Download data from FRED
                data = fred.get_series(series_id)
                data = data.reset_index()
                data.columns = ['daten', col_name]
                
                logger.info(f"Downloaded {len(data)} records for {series_id} ({col_name})")
                
                # Merge with existing data
                if merged_data is None:
                    merged_data = data
                else:
                    merged_data = merged_data.merge(data, on='daten', how='outer')
                    
            except Exception as e:
                logger.error(f"Failed to download {series_id}: {e}")
                return False
        
        logger.info(f"Combined data has {len(merged_data)} records")
        
        # Create year and quarter columns
        merged_data['year'] = merged_data['daten'].dt.year
        merged_data['qtr'] = merged_data['daten'].dt.quarter
        
        # Calculate leverage ratio
        merged_data['lev'] = merged_data['assets'] / merged_data['equity']
        
        # Drop data before 1968
        merged_data = merged_data[merged_data['year'] >= 1968].copy()
        logger.info(f"Filtered to {len(merged_data)} records from 1968 onwards")
        
        # Calculate log leverage change (non-seasonally adjusted)
        merged_data = merged_data.sort_values(['year', 'qtr'])
        merged_data['levfacnsa'] = np.log(merged_data['lev']).diff()
        
        # Compute seasonal adjustment (rolling mean of quarter values)
        # This replicates the complex Stata logic for seasonal adjustment
        
        # Initialize seasonal adjustment columns
        merged_data['tempMean'] = np.nan
        merged_data['levfac'] = np.nan
        
        # Process each quarter separately
        for qtr in [1, 2, 3, 4]:
            qtr_data = merged_data[merged_data['qtr'] == qtr].copy()
            qtr_data = qtr_data.sort_values('year')
            
            # Calculate rolling mean for seasonal adjustment
            qtr_data['tempMean'] = qtr_data['levfacnsa'].expanding().mean()
            
            # Handle the special case for first quarter (as in Stata)
            if qtr == 1:
                # For Q1, use the mean excluding current observation
                qtr_data['tempMean'] = qtr_data['levfacnsa'].expanding().mean().shift(1)
                # Set first observation to 0
                qtr_data.loc[qtr_data.index[0], 'tempMean'] = 0
            
            # Calculate seasonally adjusted leverage factor
            qtr_data['levfac'] = qtr_data['levfacnsa'] - qtr_data['tempMean'].shift(1)
            
            # Handle first observation in each quarter
            qtr_data.loc[qtr_data.index[0], 'levfac'] = qtr_data.loc[qtr_data.index[0], 'levfacnsa']
            
            # Special case for Q1 1969
            if qtr == 1 and 1969 in qtr_data['year'].values:
                qtr_data.loc[qtr_data['year'] == 1969, 'levfac'] = qtr_data.loc[qtr_data['year'] == 1969, 'levfacnsa']
            
            # Update the main dataframe
            merged_data.loc[qtr_data.index, 'tempMean'] = qtr_data['tempMean']
            merged_data.loc[qtr_data.index, 'levfac'] = qtr_data['levfac']
        
        # Keep only year, qtr, and levfac
        final_data = merged_data[['year', 'qtr', 'levfac']].copy()
        final_data = final_data.sort_values(['year', 'qtr'])
        
        # Save to intermediate file
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/brokerLev.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        final_data.to_csv(output_path, index=False)
        logger.info(f"Saved broker-dealer leverage data to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/brokerLev.csv")
        final_data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log some summary statistics
        logger.info("Broker-dealer leverage summary:")
        logger.info(f"  Total records: {len(final_data)}")
        logger.info(f"  Time range: {final_data['year'].min()} Q{final_data['qtr'].min()} to {final_data['year'].max()} Q{final_data['qtr'].max()}")
        
        # Check data availability
        if 'levfac' in final_data.columns:
            non_missing = final_data['levfac'].notna().sum()
            missing = final_data['levfac'].isna().sum()
            logger.info(f"  Leverage factor availability: {non_missing} non-missing, {missing} missing")
            
            # Leverage factor summary statistics
            levfac_data = final_data['levfac'].dropna()
            if len(levfac_data) > 0:
                mean_val = levfac_data.mean()
                std_val = levfac_data.std()
                min_val = levfac_data.min()
                max_val = levfac_data.max()
                logger.info(f"  Leverage factor statistics: mean={mean_val:.6f}, std={std_val:.6f}, range=[{min_val:.6f}, {max_val:.6f}]")
                
                # Leverage analysis
                logger.info("  Leverage factor analysis:")
                logger.info(f"    Average leverage factor: {mean_val:.6f}")
                logger.info(f"    Leverage factor volatility: {std_val:.6f}")
                logger.info(f"    Historical range: {min_val:.6f} to {max_val:.6f}")
                
                # Leverage regime analysis
                negative_leverage = (levfac_data < 0).sum()
                low_leverage = ((levfac_data >= 0) & (levfac_data < 0.01)).sum()
                moderate_leverage = ((levfac_data >= 0.01) & (levfac_data < 0.05)).sum()
                high_leverage = (levfac_data >= 0.05).sum()
                
                total_obs = len(levfac_data)
                logger.info(f"    Negative leverage changes: {negative_leverage} ({negative_leverage/total_obs*100:.1f}%)")
                logger.info(f"    Low leverage changes (0-1%): {low_leverage} ({low_leverage/total_obs*100:.1f}%)")
                logger.info(f"    Moderate leverage changes (1-5%): {moderate_leverage} ({moderate_leverage/total_obs*100:.1f}%)")
                logger.info(f"    High leverage changes (>5%): {high_leverage} ({high_leverage/total_obs*100:.1f}%)")
                
                # Calculate leverage changes (quarter-over-quarter)
                final_data_sorted = final_data.sort_values(['year', 'qtr'])
                final_data_sorted['leverage_change'] = final_data_sorted['levfac'].diff()
                
                leverage_change_data = final_data_sorted['leverage_change'].dropna()
                if len(leverage_change_data) > 0:
                    avg_change = leverage_change_data.mean()
                    change_vol = leverage_change_data.std()
                    positive_changes = (leverage_change_data > 0).sum()
                    negative_changes = (leverage_change_data < 0).sum()
                    zero_changes = (leverage_change_data == 0).sum()
                    
                    logger.info(f"    Average quarterly leverage change: {avg_change:.6f}")
                    logger.info(f"    Leverage change volatility: {change_vol:.6f}")
                    logger.info(f"    Positive changes: {positive_changes} ({positive_changes/len(leverage_change_data)*100:.1f}%)")
                    logger.info(f"    Negative changes: {negative_changes} ({negative_changes/len(leverage_change_data)*100:.1f}%)")
                    logger.info(f"    Zero changes: {zero_changes} ({zero_changes/len(leverage_change_data)*100:.1f}%)")
        
        # Quarterly frequency analysis
        logger.info("  Quarterly frequency analysis:")
        logger.info(f"    Total quarters: {len(final_data)}")
        logger.info(f"    Years covered: {final_data['year'].max() - final_data['year'].min() + 1}")
        
        # Check for any gaps in quarterly data
        expected_quarters = []
        for year in range(final_data['year'].min(), final_data['year'].max() + 1):
            for quarter in range(1, 5):
                expected_quarters.append((year, quarter))
        
        actual_quarters = list(zip(final_data['year'], final_data['qtr']))
        missing_quarters = set(expected_quarters) - set(actual_quarters)
        
        if missing_quarters:
            logger.warning(f"    Missing quarters: {len(missing_quarters)}")
            logger.warning(f"    First few missing quarters: {sorted(list(missing_quarters))[:5]}")
        else:
            logger.info("    No missing quarters detected")
        
        # Data processing explanation
        logger.info("  Data processing explanation:")
        logger.info("    - Original data: Quarterly broker-dealer financial data from FRED")
        logger.info("    - Components: Assets, liabilities, and equity")
        logger.info("    - Leverage calculation: Assets / Equity")
        logger.info("    - Seasonal adjustment: Quarter-specific rolling means")
        logger.info("    - Usage: Financial intermediation and market liquidity analysis")
        
        # Broker-dealer leverage interpretation
        logger.info("  Broker-dealer leverage interpretation:")
        logger.info("    Broker-Dealer Leverage Factor:")
        logger.info("    - Measures financial intermediation capacity")
        logger.info("    - Higher leverage = More financial intermediation")
        logger.info("    - Lower leverage = Less financial intermediation")
        logger.info("    - Seasonal adjustment removes quarter-specific patterns")
        logger.info("    - Used in financial intermediation and liquidity analysis")
        
        logger.info("Successfully downloaded and processed broker-dealer leverage data")
        logger.info("Note: Quarterly broker-dealer leverage data for financial intermediation analysis")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download broker-dealer leverage data: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the download function
    w_brokerdealerleverage()
