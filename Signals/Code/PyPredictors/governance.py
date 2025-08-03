"""
Python equivalent of Governance.do
Generated from: Governance.do

Original Stata file: Governance.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def governance():
    """
    Python equivalent of Governance.do
    
    Constructs the Governance predictor signal for governance index.
    """
    logger.info("Constructing predictor signal: Governance...")
    
    try:
        # DATA LOAD
        # Load SignalMasterTable data
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load the required variables
        required_vars = ['permno', 'time_avail_m', 'ticker', 'exchcd', 'mve_c']
        
        data = pd.read_csv(master_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Save observations with missing ticker (equivalent to Stata's preserve/restore logic)
        missing_ticker_data = data[data['ticker'].isna()].copy()
        logger.info(f"Saved {len(missing_ticker_data)} observations with missing ticker")
        
        # Keep only observations with non-missing ticker
        data = data.dropna(subset=['ticker'])
        logger.info(f"After dropping missing ticker: {len(data)} observations")
        
        # Merge with governance index data
        gov_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/GovIndex.csv")
        
        logger.info(f"Loading governance index data from: {gov_path}")
        
        if not gov_path.exists():
            logger.error(f"Governance index file not found: {gov_path}")
            logger.error("Please run the governance data download scripts first")
            return False
        
        # Load governance index data
        gov_data = pd.read_csv(gov_path)
        
        # Merge data (equivalent to Stata's "merge m:1 ticker time_avail_m using "$pathDataIntermediate/GovIndex", keep(master match) nogenerate")
        data = data.merge(
            gov_data,
            on=['ticker', 'time_avail_m'],
            how='inner'  # keep(master match)
        )
        
        logger.info(f"After merging with governance data: {len(data)} observations")
        
        # Append back the missing ticker observations (equivalent to Stata's "append using "$pathtemp/temp"")
        data = pd.concat([data, missing_ticker_data], ignore_index=True)
        logger.info(f"After appending missing ticker observations: {len(data)} observations")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating Governance signal...")
        
        # Calculate Governance (equivalent to Stata's gen statements)
        data['Governance'] = data['G']
        
        # Apply caps and floors (equivalent to Stata's replace statements)
        data.loc[data['Governance'] <= 5, 'Governance'] = 5
        data.loc[(data['Governance'] >= 14) & (data['Governance'].notna()), 'Governance'] = 14
        
        logger.info("Successfully calculated Governance signal")
        
        # SAVE RESULTS
        logger.info("Saving Governance predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'Governance']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['Governance'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "Governance.csv"
        csv_data = output_data[['permno', 'yyyymm', 'Governance']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved Governance predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed Governance predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct Governance predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    governance()
