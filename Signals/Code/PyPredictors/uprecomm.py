"""
Python equivalent of UpRecomm.do
Generated from: UpRecomm.do

Original Stata file: UpRecomm.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def uprecomm():
    """
    Python equivalent of UpRecomm.do
    
    Constructs the UpRecomm predictor signal for recommendation upgrades.
    """
    logger.info("Constructing predictor signal: UpRecomm...")
    
    try:
        # PREP IBES DATA
        # Load IBES Recommendations data
        ibes_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/IBES_Recommendations.csv")
        
        logger.info(f"Loading IBES Recommendations data from: {ibes_path}")
        
        if not ibes_path.exists():
            logger.error(f"IBES_Recommendations not found: {ibes_path}")
            logger.error("Please run the IBES Recommendations data creation script first")
            return False
        
        # Load required variables
        required_vars = ['tickerIBES', 'amaskcd', 'anndats', 'time_avail_m', 'ireccd']
        
        data = pd.read_csv(ibes_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Collapse down to firm-month (equivalent to Stata's "gcollapse (lastnm) ireccd, by(tickerIBES amaskcd time_avail_m)")
        data = data.groupby(['tickerIBES', 'amaskcd', 'time_avail_m'])['ireccd'].last().reset_index()
        logger.info(f"After first collapse: {len(data)} records")
        
        # Collapse to firm-month level (equivalent to Stata's "gcollapse (mean) ireccd, by(tickerIBES time_avail_m)")
        data = data.groupby(['tickerIBES', 'time_avail_m'])['ireccd'].mean().reset_index()
        logger.info(f"After second collapse: {len(data)} records")
        
        # Sort data for time series operations (equivalent to Stata's "bys tickerIBES (time_avail_m)")
        data = data.sort_values(['tickerIBES', 'time_avail_m'])
        
        # Generate UpRecomm indicator (equivalent to Stata's "gen UpRecomm = ireccd < ireccd[_n-1] & ireccd[_n-1] != .")
        data['ireccd_lag1'] = data.groupby('tickerIBES')['ireccd'].shift(1)
        data['UpRecomm'] = (data['ireccd'] < data['ireccd_lag1']) & (data['ireccd_lag1'].notna())
        
        logger.info("Successfully calculated UpRecomm signal")
        
        # Add permno by merging with SignalMasterTable
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        master_data = pd.read_csv(master_path, usecols=['tickerIBES', 'time_avail_m', 'permno'])
        
        # Merge with SignalMasterTable (equivalent to Stata's "merge 1:m tickerIBES time_avail_m using "$pathDataIntermediate/SignalMasterTable", keep(match) nogenerate keepusing(permno)")
        data = data.merge(master_data, on=['tickerIBES', 'time_avail_m'], how='inner')
        logger.info(f"After merging with SignalMasterTable: {len(data)} records")
        
        # SAVE RESULTS
        logger.info("Saving UpRecomm predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'UpRecomm']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['UpRecomm'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "UpRecomm.csv"
        csv_data = output_data[['permno', 'yyyymm', 'UpRecomm']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved UpRecomm predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed UpRecomm predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct UpRecomm predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    upreccomm() 