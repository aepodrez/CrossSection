"""
Python equivalent of DownRecomm.do
Generated from: DownRecomm.do

Original Stata file: DownRecomm.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def downrecomm():
    """
    Python equivalent of DownRecomm.do
    
    Constructs the DownRecomm predictor signal for recommendation downgrade.
    """
    logger.info("Constructing predictor signal: DownRecomm...")
    
    try:
        # PREP IBES DATA
        # Load IBES recommendations data
        ibes_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/IBES_Recommendations.csv")
        
        logger.info(f"Loading IBES recommendations data from: {ibes_path}")
        
        if not ibes_path.exists():
            logger.error(f"IBES recommendations file not found: {ibes_path}")
            logger.error("Please run the IBES data download script first")
            return False
        
        # Load required variables from IBES
        ibes_vars = ['tickerIBES', 'amaskcd', 'anndats', 'time_avail_m', 'ireccd']
        data = pd.read_csv(ibes_path, usecols=ibes_vars)
        logger.info(f"Successfully loaded {len(data)} recommendation records")
        
        # Collapse to firm-month (equivalent to Stata's "gcollapse (lastnm) ireccd, by(tickerIBES amaskcd time_avail_m)")
        # First collapse by tickerIBES, amaskcd, time_avail_m keeping last non-missing
        data = data.sort_values(['tickerIBES', 'amaskcd', 'time_avail_m', 'anndats'])
        data = data.drop_duplicates(subset=['tickerIBES', 'amaskcd', 'time_avail_m'], keep='last')
        logger.info(f"After first collapse: {len(data)} records")
        
        # Then collapse by tickerIBES, time_avail_m taking mean (equivalent to Stata's "gcollapse (mean) ireccd, by(tickerIBES time_avail_m)")
        data = data.groupby(['tickerIBES', 'time_avail_m'])['ireccd'].mean().reset_index()
        logger.info(f"After second collapse: {len(data)} records")
        
        # Sort for lag calculations (equivalent to Stata's "bys tickerIBES (time_avail_m)")
        data = data.sort_values(['tickerIBES', 'time_avail_m'])
        
        # Calculate lagged ireccd
        data['ireccd_lag'] = data.groupby('tickerIBES')['ireccd'].shift(1)
        
        # Create DownRecomm (equivalent to Stata's "gen DownRecomm = ireccd > ireccd[_n-1] & ireccd[_n-1] != .")
        data['DownRecomm'] = ((data['ireccd'] > data['ireccd_lag']) & 
                              (data['ireccd_lag'].notna())).astype(int)
        
        logger.info("Successfully calculated DownRecomm signal")
        
        # Add permno by merging with SignalMasterTable
        logger.info("Merging with SignalMasterTable to add permno...")
        
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load required variables from SignalMasterTable
        master_vars = ['ticker', 'time_avail_m', 'permno']
        master_data = pd.read_csv(master_path, usecols=master_vars)
        
        # Merge with SignalMasterTable (equivalent to Stata's "merge 1:m tickerIBES time_avail_m using "$pathDataIntermediate/SignalMasterTable", keep(match)")
        merged_data = data.merge(
            master_data,
            left_on=['tickerIBES', 'time_avail_m'],
            right_on=['ticker', 'time_avail_m'],
            how='inner'  # keep(match)
        )
        
        logger.info(f"Successfully merged data: {len(merged_data)} observations")
        
        # SAVE RESULTS
        logger.info("Saving DownRecomm predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = merged_data[['permno', 'time_avail_m', 'DownRecomm']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['DownRecomm'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "DownRecomm.csv"
        csv_data = output_data[['permno', 'yyyymm', 'DownRecomm']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved DownRecomm predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed DownRecomm predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct DownRecomm predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    downrecomm()
