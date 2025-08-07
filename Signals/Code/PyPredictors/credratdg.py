"""
Python equivalent of CredRatDG.do
Generated from: CredRatDG.do

Original Stata file: CredRatDG.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def credratdg():
    """
    Python equivalent of CredRatDG.do
    
    Constructs the CredRatDG predictor signal for credit rating downgrade.
    """
    logger.info("Constructing predictor signal: CredRatDG...")
    
    try:
        # Define signal for Compustat SP ratings data
        logger.info("Processing Compustat credit ratings...")
        
        compustat_ratings_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_SP_creditratings.csv")
        
        if not compustat_ratings_path.exists():
            logger.error(f"Compustat credit ratings not found: {compustat_ratings_path}")
            logger.error("Please run the credit ratings data download scripts first")
            return False
        
        # Load Compustat credit ratings data
        compustat_data = pd.read_csv(compustat_ratings_path, usecols=['gvkey', 'time_avail_m', 'credrat'])
        
        # Sort by gvkey and time_avail_m for lag calculations
        compustat_data = compustat_data.sort_values(['gvkey', 'time_avail_m'])
        
        # Calculate lag of credrat
        compustat_data['credrat_lag'] = compustat_data.groupby('gvkey')['credrat'].shift(1)
        
        # Define downgrade indicator
        compustat_data['credrat_dwn'] = np.where(
            compustat_data['credrat'] - compustat_data['credrat_lag'] < 0, 
            1, 
            np.nan
        )
        
        # Set first observation per gvkey to missing
        compustat_data['credrat_dwn'] = compustat_data.groupby('gvkey')['credrat_dwn'].bfill()
        
        # Save temporary file
        temp_comp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Temp/temp_comp_rat.csv")
        temp_comp_path.parent.mkdir(parents=True, exist_ok=True)
        compustat_data[['gvkey', 'time_avail_m', 'credrat_dwn']].to_csv(temp_comp_path, index=False)
        
        logger.info(f"Processed Compustat ratings: {len(compustat_data)} records")
        
        # Define signal for CIQ SP ratings data
        logger.info("Processing CIQ credit ratings...")
        
        ciq_ratings_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_CIQ_creditratings.csv")
        
        if not ciq_ratings_path.exists():
            logger.error(f"CIQ credit ratings not found: {ciq_ratings_path}")
            logger.error("Please run the credit ratings data download scripts first")
            return False
        
        # Load CIQ credit ratings data
        ciq_data = pd.read_csv(ciq_ratings_path)
        
        # Keep downgrades only
        ciq_data = ciq_data[
            (ciq_data['gvkey'].notna()) & 
            (ciq_data['ratingactionword'] == "Downgrade")
        ]
        
        # Create downgrade indicator
        ciq_data['ciq_dg'] = 1
        
        # Handle duplicates within month by taking max (any downgrade)
        ciq_data = ciq_data.groupby(['gvkey', 'time_avail_m'])['ciq_dg'].max().reset_index()
        
        # Save temporary file
        temp_ciq_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Temp/temp_ciq_rat.csv")
        temp_ciq_path.parent.mkdir(parents=True, exist_ok=True)
        ciq_data.to_csv(temp_ciq_path, index=False)
        
        logger.info(f"Processed CIQ ratings: {len(ciq_data)} records")
        
        # Merge on to permnos
        logger.info("Loading SignalMasterTable...")
        
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load required variables from SignalMasterTable
        master_vars = ['gvkey', 'permno', 'time_avail_m']
        master_data = pd.read_csv(master_path, usecols=master_vars)
        
        # Drop if gvkey is missing
        master_data = master_data[master_data['gvkey'].notna()]
        
        # Merge with Compustat ratings
        merged_data = master_data.merge(
            compustat_data[['gvkey', 'time_avail_m', 'credrat_dwn']], 
            on=['gvkey', 'time_avail_m'], 
            how='left'
        )
        
        # Merge with CIQ ratings
        merged_data = merged_data.merge(
            ciq_data[['gvkey', 'time_avail_m', 'ciq_dg']], 
            on=['gvkey', 'time_avail_m'], 
            how='left'
        )
        
        # Use CIQ if Compustat data is missing
        merged_data['credrat_dwn'] = merged_data['credrat_dwn'].fillna(merged_data['ciq_dg'])
        
        logger.info(f"Successfully merged data: {len(merged_data)} observations")
        
        # Sort by permno and time_avail_m for lag calculations
        merged_data = merged_data.sort_values(['permno', 'time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating CredRatDG signal...")
        
        # Calculate lags of credrat_dwn (up to 5 lags)
        for lag in range(1, 6):
            merged_data[f'credrat_dwn_lag{lag}'] = merged_data.groupby('permno')['credrat_dwn'].shift(lag)
        
        # Initialize CredRatDG to 0
        merged_data['CredRatDG'] = 0
        
        # Set to 1 if any downgrade in current or previous 5 periods
        downgrade_mask = (
            (merged_data['credrat_dwn'] == 1) |
            (merged_data['credrat_dwn_lag1'] == 1) |
            (merged_data['credrat_dwn_lag2'] == 1) |
            (merged_data['credrat_dwn_lag3'] == 1) |
            (merged_data['credrat_dwn_lag4'] == 1) |
            (merged_data['credrat_dwn_lag5'] == 1)
        )
        merged_data.loc[downgrade_mask, 'CredRatDG'] = 1
        
        # Set to missing if year < 1979 (no data before that)
        # Convert time_avail_m to datetime if needed for year extraction
        if not pd.api.types.is_datetime64_any_dtype(merged_data['time_avail_m']):
            merged_data['time_avail_m'] = pd.to_datetime(merged_data['time_avail_m'])
        
        merged_data['year'] = merged_data['time_avail_m'].dt.year
        merged_data.loc[merged_data['year'] < 1979, 'CredRatDG'] = np.nan
        
        logger.info("Successfully calculated CredRatDG signal")
        
        # SAVE RESULTS
        logger.info("Saving CredRatDG predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = merged_data[['permno', 'time_avail_m', 'CredRatDG']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['CredRatDG'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "CredRatDG.csv"
        csv_data = output_data[['permno', 'yyyymm', 'CredRatDG']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved CredRatDG predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed CredRatDG predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct CredRatDG predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    credratdg()
