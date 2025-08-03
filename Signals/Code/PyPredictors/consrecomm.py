"""
Python equivalent of ConsRecomm.do
Generated from: ConsRecomm.do

Original Stata file: ConsRecomm.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def consrecomm():
    """
    Python equivalent of ConsRecomm.do
    
    Constructs the ConsRecomm predictor signal for consensus recommendation.
    """
    logger.info("Constructing predictor signal: ConsRecomm...")
    
    try:
        # PREP IBES DATA
        # Load IBES recommendations data
        ibes_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/IBES_Recommendations.csv")
        
        logger.info(f"Loading IBES recommendations data from: {ibes_path}")
        
        if not ibes_path.exists():
            logger.error(f"Input file not found: {ibes_path}")
            logger.error("Please run the IBES data download scripts first")
            return False
        
        # Load the required variables
        required_vars = ['tickerIBES', 'amaskcd', 'anndats', 'time_avail_m', 'ireccd']
        
        data = pd.read_csv(ibes_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Collapse down to firm-month: first by tickerIBES, amaskcd, time_avail_m (lastnm)
        logger.info("Collapsing to firm-month level...")
        
        # First collapse: keep last non-missing value per tickerIBES-amaskcd-time_avail_m
        data = data.sort_values(['tickerIBES', 'amaskcd', 'time_avail_m', 'anndats'])
        data = data.drop_duplicates(subset=['tickerIBES', 'amaskcd', 'time_avail_m'], keep='last')
        
        # Second collapse: take mean per tickerIBES-time_avail_m
        data = data.groupby(['tickerIBES', 'time_avail_m'])['ireccd'].mean().reset_index()
        
        logger.info(f"After collapsing: {len(data)} firm-month observations")
        
        # Define signal (equivalent to Stata's conditional logic)
        data['ConsRecomm'] = np.nan
        
        # Set to 1 if ireccd > 3 (equivalent to Stata's "gen ConsRecomm = 1 if ireccd > 3 & ireccd < .")
        data.loc[(data['ireccd'] > 3) & (data['ireccd'].notna()), 'ConsRecomm'] = 1
        
        # Set to 0 if ireccd <= 1.5 (equivalent to Stata's "replace ConsRecomm = 0 if ireccd <= 1.5")
        data.loc[data['ireccd'] <= 1.5, 'ConsRecomm'] = 0
        
        logger.info(f"After signal definition: {len(data)} observations")
        
        # Add permno by merging with SignalMasterTable
        logger.info("Merging with SignalMasterTable...")
        
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        master_data = pd.read_csv(master_path, usecols=['tickerIBES', 'time_avail_m', 'permno'])
        
        # Merge data
        merged_data = data.merge(
            master_data, 
            on=['tickerIBES', 'time_avail_m'], 
            how='inner'  # equivalent to Stata's keep(match)
        )
        
        logger.info(f"Successfully merged data: {len(merged_data)} observations")
        
        # SAVE RESULTS
        logger.info("Saving ConsRecomm predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = merged_data[['permno', 'time_avail_m', 'ConsRecomm']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['ConsRecomm'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "ConsRecomm.csv"
        csv_data = output_data[['permno', 'yyyymm', 'ConsRecomm']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved ConsRecomm predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed ConsRecomm predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct ConsRecomm predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    consrecomm()
