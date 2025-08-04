"""
Python equivalent of CPVolSpread.do
Generated from: CPVolSpread.do

Original Stata file: CPVolSpread.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def cpvolspread():
    """
    Python equivalent of CPVolSpread.do
    
    Constructs the CPVolSpread predictor signal for call-put volatility spread.
    """
    logger.info("Constructing predictor signal: CPVolSpread...")
    
    try:
        # Clean OptionMetrics data
        logger.info("Loading and cleaning OptionMetrics data...")
        
        optionmetrics_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/OptionMetricsBH.csv")
        
        if not optionmetrics_path.exists():
            logger.error(f"OptionMetrics data not found: {optionmetrics_path}")
            logger.error("Please run the OptionMetrics data download scripts first")
            return False
        
        # Load OptionMetrics data
        option_data = pd.read_csv(optionmetrics_path)
        
        # Drop if cp_flag == "BOTH"
        option_data = option_data[option_data['cp_flag'] != "BOTH"]
        
        # Keep if mean_day >= 0 (filter out stale data)
        option_data = option_data[option_data['mean_day'] >= 0]
        
        logger.info(f"Successfully loaded {len(option_data)} option records")
        
        # Make wide format (equivalent to Stata's reshape wide)
        option_data = option_data.drop(['mean_day', 'nobs', 'ticker'], axis=1, errors='ignore')
        
        # Pivot to wide format
        option_wide = option_data.pivot_table(
            index=['secid', 'time_avail_m'], 
            columns='cp_flag', 
            values='mean_imp_vol', 
            aggfunc='first'
        ).reset_index()
        
        # Rename columns to match Stata output
        option_wide.columns.name = None
        option_wide = option_wide.rename(columns={'C': 'mean_imp_volC', 'P': 'mean_imp_volP'})
        
        # Compute vol spread
        option_wide['CPVolSpread'] = option_wide['mean_imp_volC'] - option_wide['mean_imp_volP']
        
        # Save temporary file
        temp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Temp/temp.csv")
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        option_wide.to_csv(temp_path, index=False)
        
        # DATA LOAD
        # Load SignalMasterTable
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load required variables from SignalMasterTable
        master_vars = ['permno', 'time_avail_m', 'secid', 'sicCRSP']
        master_data = pd.read_csv(master_path, usecols=master_vars)
        
        # Drop if secid is missing
        master_data = master_data[master_data['secid'].notna()]
        
        logger.info(f"Successfully loaded {len(master_data)} master records")
        
        # Merge with option data
        logger.info("Merging with option data...")
        
        merged_data = master_data.merge(
            option_wide, 
            on=['secid', 'time_avail_m'], 
            how='left'  # equivalent to Stata's keep(master match)
        )
        
        logger.info(f"Successfully merged data: {len(merged_data)} observations")
        
        # Filter out closed-end funds and REITs
        # Drop closed-end funds (6720-6730) and REITs (6798)
        merged_data = merged_data[
            (merged_data['sicCRSP'] < 6720) | (merged_data['sicCRSP'] > 6730)
        ]
        merged_data = merged_data[merged_data['sicCRSP'] != 6798]
        
        logger.info(f"After filtering funds and REITs: {len(merged_data)} observations")
        
        # SIGNAL CONSTRUCTION
        # Drop if CPVolSpread is missing
        merged_data = merged_data.dropna(subset=['CPVolSpread'])
        
        logger.info("Successfully calculated CPVolSpread signal")
        
        # SAVE RESULTS
        logger.info("Saving CPVolSpread predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = merged_data[['permno', 'time_avail_m', 'CPVolSpread']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['CPVolSpread'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "CPVolSpread.csv"
        csv_data = output_data[['permno', 'yyyymm', 'CPVolSpread']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved CPVolSpread predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed CPVolSpread predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct CPVolSpread predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    cpvolspread()
