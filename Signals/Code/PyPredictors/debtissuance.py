"""
Python equivalent of DebtIssuance.do
Generated from: DebtIssuance.do

Original Stata file: DebtIssuance.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def debtissuance():
    """
    Python equivalent of DebtIssuance.do
    
    Constructs the DebtIssuance predictor signal for debt issuance.
    """
    logger.info("Constructing predictor signal: DebtIssuance...")
    
    try:
        # DATA LOAD
        # Load Compustat annual data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading Compustat annual data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"Input file not found: {compustat_path}")
            logger.error("Please run the Compustat data download scripts first")
            return False
        
        # Load the required variables
        required_vars = ['permno', 'time_avail_m', 'ceq', 'dltis']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Merge with SignalMasterTable
        logger.info("Merging with SignalMasterTable...")
        
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load required variables from SignalMasterTable
        master_vars = ['permno', 'time_avail_m', 'mve_c', 'shrcd']
        master_data = pd.read_csv(master_path, usecols=master_vars)
        
        # Merge data
        merged_data = data.merge(
            master_data, 
            on=['permno', 'time_avail_m'], 
            how='inner'  # equivalent to Stata's keep(match)
        )
        
        logger.info(f"Successfully merged data: {len(merged_data)} observations")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating DebtIssuance signal...")
        
        # Calculate BM (equivalent to Stata's "gen BM = log(ceq/mve_c)")
        merged_data['BM'] = np.log(merged_data['ceq'] / merged_data['mve_c'])
        
        # Calculate DebtIssuance (equivalent to Stata's "gen DebtIssuance = (dltis > 0 & dltis !=.)")
        merged_data['DebtIssuance'] = np.where(
            (merged_data['dltis'] > 0) & (merged_data['dltis'].notna()),
            1,
            np.nan
        )
        
        # Set to missing if shrcd > 11 or BM is missing (equivalent to Stata's replace conditions)
        merged_data.loc[
            (merged_data['shrcd'] > 11) | (merged_data['BM'].isna()),
            'DebtIssuance'
        ] = np.nan
        
        logger.info("Successfully calculated DebtIssuance signal")
        
        # SAVE RESULTS
        logger.info("Saving DebtIssuance predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = merged_data[['permno', 'time_avail_m', 'DebtIssuance']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['DebtIssuance'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "DebtIssuance.csv"
        csv_data = output_data[['permno', 'yyyymm', 'DebtIssuance']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved DebtIssuance predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed DebtIssuance predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct DebtIssuance predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    debtissuance()
