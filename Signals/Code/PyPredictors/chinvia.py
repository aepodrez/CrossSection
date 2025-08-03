"""
Python equivalent of ChInvIA.do
Generated from: ChInvIA.do

Original Stata file: ChInvIA.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def chinvia():
    """
    Python equivalent of ChInvIA.do
    
    Constructs the ChInvIA predictor signal for change in capital investment (industry adjusted).
    """
    logger.info("Constructing predictor signal: ChInvIA...")
    
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
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'capx', 'ppent', 'at']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Merge with SignalMasterTable to get sicCRSP
        logger.info("Merging with SignalMasterTable...")
        
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        master_data = pd.read_csv(master_path, usecols=['permno', 'time_avail_m', 'sicCRSP'])
        
        # Merge data
        merged_data = data.merge(
            master_data, 
            on=['permno', 'time_avail_m'], 
            how='right'  # equivalent to Stata's keep(using match)
        )
        
        logger.info(f"Successfully merged data: {len(merged_data)} observations")
        
        # Sort by permno and time_avail_m for lag calculations
        merged_data = merged_data.sort_values(['permno', 'time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating ChInvIA signal...")
        
        # Convert sicCRSP to string and create 2-digit SIC code
        merged_data['sicCRSP'] = merged_data['sicCRSP'].astype(str)
        merged_data['sic2D'] = merged_data['sicCRSP'].str[:2]
        
        # Calculate 12-month and 24-month lags for required variables
        merged_data['ppent_lag12'] = merged_data.groupby('permno')['ppent'].shift(12)
        merged_data['capx_lag12'] = merged_data.groupby('permno')['capx'].shift(12)
        merged_data['capx_lag24'] = merged_data.groupby('permno')['capx'].shift(24)
        
        # Replace missing capx with change in ppent (equivalent to Stata's "replace capx = ppent - l12.ppent if capx ==.")
        merged_data.loc[merged_data['capx'].isna(), 'capx'] = (
            merged_data.loc[merged_data['capx'].isna(), 'ppent'] - 
            merged_data.loc[merged_data['capx'].isna(), 'ppent_lag12']
        )
        
        # Calculate pchcapx (equivalent to Stata's complex formula)
        merged_data['pchcapx'] = (
            (merged_data['capx'] - 0.5 * (merged_data['capx_lag12'] + merged_data['capx_lag24'])) /
            (0.5 * (merged_data['capx_lag12'] + merged_data['capx_lag24']))
        )
        
        # Replace missing pchcapx with simple change (equivalent to Stata's fallback)
        mask = merged_data['pchcapx'].isna()
        merged_data.loc[mask, 'pchcapx'] = (
            (merged_data.loc[mask, 'capx'] - merged_data.loc[mask, 'capx_lag12']) /
            merged_data.loc[mask, 'capx_lag12']
        )
        
        # Calculate industry mean (equivalent to Stata's "egen temp = mean(pchcapx), by(sic2D time_avail_m)")
        industry_mean = merged_data.groupby(['sic2D', 'time_avail_m'])['pchcapx'].transform('mean')
        
        # Calculate ChInvIA (equivalent to Stata's "gen ChInvIA = pchcapx - temp")
        merged_data['ChInvIA'] = merged_data['pchcapx'] - industry_mean
        
        logger.info("Successfully calculated ChInvIA signal")
        
        # SAVE RESULTS
        logger.info("Saving ChInvIA predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = merged_data[['permno', 'time_avail_m', 'ChInvIA']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['ChInvIA'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "ChInvIA.csv"
        csv_data = output_data[['permno', 'yyyymm', 'ChInvIA']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved ChInvIA predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed ChInvIA predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct ChInvIA predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    chinvia()
