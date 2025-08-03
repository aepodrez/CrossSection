"""
Python equivalent of DelBreadth.do
Generated from: DelBreadth.do

Original Stata file: DelBreadth.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def delbreadth():
    """
    Python equivalent of DelBreadth.do
    
    Constructs the DelBreadth predictor signal for institutional ownership breadth.
    """
    logger.info("Constructing predictor signal: DelBreadth...")
    
    try:
        # DATA LOAD
        # Load SignalMasterTable
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load required variables from SignalMasterTable
        master_vars = ['permno', 'time_avail_m', 'exchcd', 'mve_c']
        data = pd.read_csv(master_path, usecols=master_vars)
        logger.info(f"Successfully loaded {len(data)} records from SignalMasterTable")
        
        # Merge with TR_13F data
        logger.info("Merging with TR_13F data...")
        
        tr13f_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/TR_13F.csv")
        
        if not tr13f_path.exists():
            logger.error(f"TR_13F data not found: {tr13f_path}")
            logger.error("Please run the 13F data download script first")
            return False
        
        # Load required variables from TR_13F
        tr13f_vars = ['permno', 'time_avail_m', 'dbreadth']
        tr13f_data = pd.read_csv(tr13f_path, usecols=tr13f_vars)
        
        # Merge data (equivalent to Stata's keep(master match))
        merged_data = data.merge(
            tr13f_data,
            on=['permno', 'time_avail_m'],
            how='left'  # keep master and match
        )
        
        logger.info(f"Successfully merged data: {len(merged_data)} observations")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating DelBreadth signal...")
        
        # Set DelBreadth = dbreadth (equivalent to Stata's "gen DelBreadth = dbreadth")
        merged_data['DelBreadth'] = merged_data['dbreadth']
        
        # Calculate 20th percentile of market value for NYSE stocks (equivalent to Stata's preserve/restore logic)
        logger.info("Calculating NYSE market value threshold...")
        
        # Filter for NYSE stocks (exchcd == 1)
        nyse_data = merged_data[merged_data['exchcd'] == 1].copy()
        
        # Calculate 20th percentile of mve_c by time_avail_m
        nyse_threshold = nyse_data.groupby('time_avail_m')['mve_c'].quantile(0.20).reset_index()
        nyse_threshold = nyse_threshold.rename(columns={'mve_c': 'temp'})
        
        # Remove duplicates (equivalent to Stata's "duplicates drop")
        nyse_threshold = nyse_threshold.drop_duplicates()
        
        # Merge back with main data
        merged_data = merged_data.merge(
            nyse_threshold,
            on='time_avail_m',
            how='left'
        )
        
        # Set DelBreadth to missing if mve_c < temp (equivalent to Stata's "replace DelBreadth = . if mve_c < temp")
        merged_data.loc[merged_data['mve_c'] < merged_data['temp'], 'DelBreadth'] = np.nan
        
        # Drop temp column
        merged_data = merged_data.drop(columns=['temp'])
        
        logger.info("Successfully calculated DelBreadth signal")
        
        # SAVE RESULTS
        logger.info("Saving DelBreadth predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = merged_data[['permno', 'time_avail_m', 'DelBreadth']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['DelBreadth'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "DelBreadth.csv"
        csv_data = output_data[['permno', 'yyyymm', 'DelBreadth']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved DelBreadth predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed DelBreadth predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct DelBreadth predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    delbreadth()
