"""
Python equivalent of dVolPut.do
Generated from: dVolPut.do

Original Stata file: dVolPut.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def dvolput():
    """
    Python equivalent of dVolPut.do
    
    Constructs the dVolPut predictor signal for change in put implied volatility.
    """
    logger.info("Constructing predictor signal: dVolPut...")
    
    try:
        # Clean OptionMetrics data
        # Load OptionMetrics volatility surface data
        optionmetrics_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/OptionMetricsVolSurf.csv")
        
        logger.info(f"Loading OptionMetrics volatility surface data from: {optionmetrics_path}")
        
        if not optionmetrics_path.exists():
            logger.error(f"OptionMetrics volatility surface file not found: {optionmetrics_path}")
            logger.error("Please run the OptionMetrics data download script first")
            return False
        
        # Load OptionMetrics data
        data = pd.read_csv(optionmetrics_path)
        logger.info(f"Successfully loaded {len(data)} option records")
        
        # Screen data (equivalent to Stata's "keep if days == 30 & abs(delta) == 50")
        data = data[(data['days'] == 30) & (data['delta'].abs() == 50)]
        logger.info(f"After screening for days=30 and delta=50: {len(data)} records")
        
        # Keep only put options (equivalent to Stata's "keep if cp_flag == 'P'")
        data = data[data['cp_flag'] == 'P']
        logger.info(f"After filtering for put options: {len(data)} records")
        
        # Sort for lag calculations (equivalent to Stata's "xtset secid time_avail_m")
        data = data.sort_values(['secid', 'time_avail_m'])
        
        # Calculate 1-month lag of implied volatility
        data['impl_vol_lag1'] = data.groupby('secid')['impl_vol'].shift(1)
        
        # Calculate dVolPut (equivalent to Stata's "gen dVolPut = impl_vol - l1.impl_vol")
        data['dVolPut'] = data['impl_vol'] - data['impl_vol_lag1']
        
        # Keep only required variables (equivalent to Stata's "keep secid time_avail_m dVolPut")
        data = data[['secid', 'time_avail_m', 'dVolPut']]
        
        # Save temporary file (equivalent to Stata's "save "$pathtemp/temp", replace")
        temp_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Temp")
        temp_dir.mkdir(parents=True, exist_ok=True)
        data.to_csv(temp_dir / "temp.csv", index=False)
        
        logger.info("Successfully calculated dVolPut signal")
        
        # Merge onto master table
        logger.info("Merging with SignalMasterTable...")
        
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load required variables from SignalMasterTable
        master_vars = ['permno', 'time_avail_m', 'secid']
        master_data = pd.read_csv(master_path, usecols=master_vars)
        
        # Merge with option data (equivalent to Stata's "merge m:1 secid time_avail_m using "$pathtemp/temp", keep(master match)")
        merged_data = master_data.merge(
            data,
            on=['secid', 'time_avail_m'],
            how='inner'  # keep(master match)
        )
        
        logger.info(f"Successfully merged data: {len(merged_data)} observations")
        
        # Keep only non-missing dVolPut (equivalent to Stata's "keep if dVolPut != .")
        merged_data = merged_data[merged_data['dVolPut'].notna()]
        logger.info(f"After removing missing dVolPut: {len(merged_data)} observations")
        
        # SAVE RESULTS
        logger.info("Saving dVolPut predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = merged_data[['permno', 'time_avail_m', 'dVolPut']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['dVolPut'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "dVolPut.csv"
        csv_data = output_data[['permno', 'yyyymm', 'dVolPut']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved dVolPut predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed dVolPut predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct dVolPut predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    dvolput()
