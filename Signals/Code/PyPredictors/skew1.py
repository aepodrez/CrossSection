"""
Python equivalent of skew1.do
Generated from: skew1.do

Original Stata file: skew1.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def skew1():
    """
    Python equivalent of skew1.do
    
    Constructs the skew1 predictor signal for smirk skewness.
    """
    logger.info("Constructing predictor signal: skew1...")
    
    try:
        # DATA LOAD
        # Load SignalMasterTable data
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load required variables
        required_vars = ['permno', 'time_avail_m', 'secid']
        
        data = pd.read_csv(master_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Preserve observations with missing secid (equivalent to Stata's preserve/restore logic)
        missing_secid_data = data[data['secid'].isna()].copy()
        logger.info(f"Preserved {len(missing_secid_data)} records with missing secid")
        
        # Drop observations with missing secid for merging
        data = data.dropna(subset=['secid'])
        logger.info(f"After dropping missing secid: {len(data)} records")
        
        # Load OptionMetricsVolSurf data (contains Skew1)
        optionmetrics_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/OptionMetricsVolSurf.csv")
        
        logger.info(f"Loading OptionMetricsVolSurf data from: {optionmetrics_path}")
        
        if not optionmetrics_path.exists():
            logger.error(f"OptionMetricsVolSurf not found: {optionmetrics_path}")
            logger.error("Please run the OptionMetrics data creation script first")
            return False
        
        optionmetrics_data = pd.read_csv(optionmetrics_path, usecols=['secid', 'time_avail_m', 'Skew1'])
        
        # Merge with OptionMetricsVolSurf data (equivalent to Stata's "merge m:1 secid time_avail_m using "$pathDataIntermediate/OptionMetricsXZZ", keep(master match) nogenerate")
        data = data.merge(optionmetrics_data, on=['secid', 'time_avail_m'], how='inner')
        logger.info(f"After merging with OptionMetricsVolSurf data: {len(data)} records")
        
        # Append back the missing secid data (equivalent to Stata's "append using "$pathtemp/temp"")
        missing_secid_data['Skew1'] = np.nan  # Add Skew1 column with NaN values
        data = pd.concat([data, missing_secid_data], ignore_index=True)
        logger.info(f"After appending missing secid data: {len(data)} records")
        
        # Rename Skew1 to skew1 to match expected column name
        data = data.rename(columns={'Skew1': 'skew1'})
        
        logger.info("Successfully prepared data for skew1 calculation")
        
        # SAVE RESULTS
        logger.info("Saving skew1 predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'skew1']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['skew1'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "skew1.csv"
        csv_data = output_data[['permno', 'yyyymm', 'skew1']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved skew1 predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed skew1 predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct skew1 predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    skew1()
