"""
Python equivalent of dCPVolSpread.do
Generated from: dCPVolSpread.do

Original Stata file: dCPVolSpread.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def dcpvolspread():
    """
    Python equivalent of dCPVolSpread.do
    
    Constructs the dCPVolSpread predictor signal for change in call-put volatility spread.
    """
    logger.info("Constructing predictor signal: dCPVolSpread...")
    
    try:
        # Clean OptionMetrics data
        logger.info("Loading and cleaning OptionMetrics volatility surface data...")
        
        optionmetrics_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/OptionMetricsVolSurf.csv")
        
        if not optionmetrics_path.exists():
            logger.error(f"OptionMetrics volatility surface data not found: {optionmetrics_path}")
            logger.error("Please run the OptionMetrics data download scripts first")
            return False
        
        # Load OptionMetrics volatility surface data
        option_data = pd.read_csv(optionmetrics_path)
        
        # Screen: keep if days == 30 & abs(delta) == 50
        option_data = option_data[
            (option_data['days'] == 30) & 
            (np.abs(option_data['delta']) == 50)
        ]
        
        logger.info(f"After screening: {len(option_data)} records")
        
        # Keep required variables
        option_data = option_data[['secid', 'time_avail_m', 'cp_flag', 'impl_vol']]
        
        # Reshape to wide format (equivalent to Stata's reshape wide)
        option_wide = option_data.pivot_table(
            index=['secid', 'time_avail_m'], 
            columns='cp_flag', 
            values='impl_vol', 
            aggfunc='first'
        ).reset_index()
        
        # Rename columns to match Stata output
        option_wide.columns.name = None
        option_wide = option_wide.rename(columns={'C': 'impl_volC', 'P': 'impl_volP'})
        
        # Sort by secid and time_avail_m for lag calculations
        option_wide = option_wide.sort_values(['secid', 'time_avail_m'])
        
        # Calculate lags of implied volatilities
        option_wide['impl_volC_lag1'] = option_wide.groupby('secid')['impl_volC'].shift(1)
        option_wide['impl_volP_lag1'] = option_wide.groupby('secid')['impl_volP'].shift(1)
        
        # Calculate changes in volatility
        option_wide['dVolCall'] = option_wide['impl_volC'] - option_wide['impl_volC_lag1']
        option_wide['dVolPut'] = option_wide['impl_volP'] - option_wide['impl_volP_lag1']
        
        # Calculate change in call-put volatility spread
        option_wide['dCPVolSpread'] = option_wide['dVolPut'] - option_wide['dVolCall']
        
        # Keep only required variables
        option_wide = option_wide[['secid', 'time_avail_m', 'dCPVolSpread']]
        
        # Save temporary file
        temp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Temp/temp.csv")
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        option_wide.to_csv(temp_path, index=False)
        
        logger.info(f"Processed option data: {len(option_wide)} records")
        
        # Merge onto master table
        logger.info("Loading SignalMasterTable...")
        
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load required variables from SignalMasterTable
        master_vars = ['permno', 'time_avail_m', 'secid']
        master_data = pd.read_csv(master_path, usecols=master_vars)
        
        # Merge with option data
        logger.info("Merging with option data...")
        
        merged_data = master_data.merge(
            option_wide, 
            on=['secid', 'time_avail_m'], 
            how='left'  # equivalent to Stata's keep(master match)
        )
        
        logger.info(f"Successfully merged data: {len(merged_data)} observations")
        
        # Keep only non-missing dCPVolSpread values
        merged_data = merged_data.dropna(subset=['dCPVolSpread'])
        
        logger.info("Successfully calculated dCPVolSpread signal")
        
        # SAVE RESULTS
        logger.info("Saving dCPVolSpread predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = merged_data[['permno', 'time_avail_m', 'dCPVolSpread']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['dCPVolSpread'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "dCPVolSpread.csv"
        csv_data = output_data[['permno', 'yyyymm', 'dCPVolSpread']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved dCPVolSpread predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed dCPVolSpread predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct dCPVolSpread predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    dcpvolspread()
