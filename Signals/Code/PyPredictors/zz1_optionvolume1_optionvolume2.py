"""
ZZ1_OptionVolume1_OptionVolume2 Predictor Implementation

This script implements two option volume predictors based on Johnson and So 2012 JFE:
- OptionVolume1: Option Volume (ratio of option volume to stock volume)
- OptionVolume2: Option Volume (abnormal) (current ratio divided by 6-month average)

The script loads SignalMasterTable, monthly CRSP, and OptionMetrics data to calculate
option volume measures relative to stock volume.
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def zz1_optionvolume1_optionvolume2():
    """Main function to calculate OptionVolume1 and OptionVolume2 predictors."""
    
    # Define file paths
    base_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data")
    master_path = base_path / "Intermediate" / "SignalMasterTable.csv"
    crsp_path = base_path / "Intermediate" / "monthlyCRSP.csv"
    optionmetrics_path = base_path / "Intermediate" / "OptionMetricsVolume.csv"
    temp_path = base_path / "Temp" / "temp_optionvolume.csv"
    output_path = base_path / "Predictors"
    
    # Ensure directories exist
    temp_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.mkdir(parents=True, exist_ok=True)
    
    logger.info("Starting OptionVolume1 and OptionVolume2 predictor calculation")
    
    try:
        # DATA LOAD
        logger.info("Loading SignalMasterTable data")
        required_vars = ['permno', 'time_avail_m', 'prc', 'shrcd', 'secid']
        data = pd.read_csv(master_path, usecols=required_vars)
        
        # Check if secid column exists in SignalMasterTable
        if 'secid' not in data.columns:
            logger.warning("secid column not found in SignalMasterTable")
            logger.warning("This requires the CRSP-OptionMetrics linking table which depends on oclink.csv from WRDS")
            logger.warning("Creating placeholder files for OptionVolume1 and OptionVolume2")
            
            # Create placeholder files
            placeholder_data = pd.DataFrame({
                'permno': [],
                'yyyymm': [],
                'OptionVolume1': []
            })
            placeholder_data.to_csv(output_path / "OptionVolume1.csv", index=False)
            
            placeholder_data2 = pd.DataFrame({
                'permno': [],
                'yyyymm': [],
                'OptionVolume2': []
            })
            placeholder_data2.to_csv(output_path / "OptionVolume2.csv", index=False)
            
            logger.info("Created placeholder files for OptionVolume1 and OptionVolume2")
            return True
        
        # Add stock volume
        logger.info("Merging with monthly CRSP for stock volume")
        crsp_data = pd.read_csv(crsp_path, usecols=['permno', 'time_avail_m', 'vol'])
        data = data.merge(crsp_data, on=['permno', 'time_avail_m'], how='left')
        
        # Handle missing secid observations
        logger.info("Handling missing secid observations")
        missing_secid_data = data[data['secid'].isna()].copy()
        missing_secid_data.to_csv(temp_path, index=False)
        
        # Keep only observations with secid
        data = data.dropna(subset=['secid'])
        
        # Add option volume
        logger.info("Merging with OptionMetrics for option volume")
        option_data = pd.read_csv(optionmetrics_path, usecols=['secid', 'time_avail_m', 'optVolume'])
        data = data.merge(option_data, on=['secid', 'time_avail_m'], how='inner')
        
        # Append back the missing secid observations
        logger.info("Appending missing secid observations")
        if temp_path.exists():
            missing_data = pd.read_csv(temp_path)
            data = pd.concat([data, missing_data], ignore_index=True)
        
        # Convert time_avail_m to datetime
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        
        # Sort by permno and time_avail_m
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating option volume measures")
        
        # Calculate OptionVolume1 (option volume / stock volume)
        data['OptionVolume1'] = data['optVolume'] / data['vol']
        
        # Set OptionVolume1 to missing if lagged values are missing
        data['optvolume_lag1'] = data.groupby('permno')['optVolume'].shift(1)
        data['vol_lag1'] = data.groupby('permno')['vol'].shift(1)
        data.loc[(data['optvolume_lag1'].isna()) | (data['vol_lag1'].isna()), 'OptionVolume1'] = np.nan
        
        # Calculate 6-month average for OptionVolume2
        logger.info("Calculating 6-month average for abnormal option volume")
        for n in range(1, 7):
            data[f'tempVol{n}'] = data.groupby('permno')['OptionVolume1'].shift(n)
        
        # Calculate row mean of the 6 lagged values
        temp_vol_cols = [f'tempVol{n}' for n in range(1, 7)]
        data['tempMean'] = data[temp_vol_cols].mean(axis=1)
        
        # Calculate OptionVolume2 (abnormal option volume)
        data['OptionVolume2'] = data['OptionVolume1'] / data['tempMean']
        
        # Prepare output data
        logger.info("Preparing output data")
        
        # For OptionVolume1
        optionvolume1_output = data[['permno', 'time_avail_m', 'OptionVolume1']].copy()
        optionvolume1_output = optionvolume1_output.dropna(subset=['OptionVolume1'])
        # Convert time_avail_m to datetime if needed for strftime
        if not pd.api.types.is_datetime64_any_dtype(optionvolume1_output['time_avail_m']):
            optionvolume1_output['time_avail_m'] = pd.to_datetime(optionvolume1_output['time_avail_m'])
        
        optionvolume1_output['yyyymm'] = optionvolume1_output['time_avail_m'].dt.strftime('%Y%m').astype(int)
        optionvolume1_output = optionvolume1_output[['permno', 'yyyymm', 'OptionVolume1']]
        
        # For OptionVolume2
        optionvolume2_output = data[['permno', 'time_avail_m', 'OptionVolume2']].copy()
        optionvolume2_output = optionvolume2_output.dropna(subset=['OptionVolume2'])
        # Convert time_avail_m to datetime if needed for strftime
        if not pd.api.types.is_datetime64_any_dtype(optionvolume2_output['time_avail_m']):
            optionvolume2_output['time_avail_m'] = pd.to_datetime(optionvolume2_output['time_avail_m'])
        
        optionvolume2_output['yyyymm'] = optionvolume2_output['time_avail_m'].dt.strftime('%Y%m').astype(int)
        optionvolume2_output = optionvolume2_output[['permno', 'yyyymm', 'OptionVolume2']]
        
        # Save results
        logger.info("Saving results")
        
        # Save OptionVolume1
        optionvolume1_file = output_path / "optionvolume1.csv"
        optionvolume1_output.to_csv(optionvolume1_file, index=False)
        logger.info(f"Saved OptionVolume1 predictor to {optionvolume1_file}")
        logger.info(f"OptionVolume1: {len(optionvolume1_output)} observations")
        
        # Save OptionVolume2
        optionvolume2_file = output_path / "optionvolume2.csv"
        optionvolume2_output.to_csv(optionvolume2_file, index=False)
        logger.info(f"Saved OptionVolume2 predictor to {optionvolume2_file}")
        logger.info(f"OptionVolume2: {len(optionvolume2_output)} observations")
        
        # Clean up temporary file
        if temp_path.exists():
            temp_path.unlink()
        
        logger.info("Successfully completed OptionVolume1 and OptionVolume2 predictor calculation")
        
    except Exception as e:
        logger.error(f"Error in OptionVolume1 and OptionVolume2 calculation: {str(e)}")
        raise

if __name__ == "__main__":
    zz1_optionvolume1_optionvolume2()
