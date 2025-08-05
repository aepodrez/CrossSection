"""
Python equivalent of ChangeInRecommendation.do
Generated from: ChangeInRecommendation.do

Original Stata file: ChangeInRecommendation.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def changeinrecommendation():
    """
    Python equivalent of ChangeInRecommendation.do
    
    Constructs the ChangeInRecommendation predictor signal for analyst recommendation changes.
    """
    logger.info("Constructing predictor signal: ChangeInRecommendation...")
    
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
        
        # Reverse score following OP (equivalent to Stata's "gen opscore = 6 - ireccd")
        data['opscore'] = 6 - data['ireccd']
        
        # Calculate change in recommendation (equivalent to Stata's lag calculation)
        data = data.sort_values(['tickerIBES', 'time_avail_m'])
        data['opscore_lag'] = data.groupby('tickerIBES')['opscore'].shift(1)
        data['ChangeInRecommendation'] = data['opscore'] - data['opscore_lag']
        
        # Keep only observations where lag is not missing
        data = data[data['opscore_lag'].notna()]
        
        logger.info(f"After calculating changes: {len(data)} observations")
        
        # Add permno by merging with SignalMasterTable
        logger.info("Merging with SignalMasterTable...")
        
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        master_data = pd.read_csv(master_path, usecols=['ticker', 'time_avail_m', 'permno'])
        
        # Merge data
        merged_data = data.merge(
            master_data, 
            left_on=['tickerIBES', 'time_avail_m'],
            right_on=['ticker', 'time_avail_m'], 
            how='inner'  # equivalent to Stata's keep(match)
        )
        
        logger.info(f"Successfully merged data: {len(merged_data)} observations")
        
        # SAVE RESULTS
        logger.info("Saving ChangeInRecommendation predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = merged_data[['permno', 'time_avail_m', 'ChangeInRecommendation']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['ChangeInRecommendation'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "ChangeInRecommendation.csv"
        csv_data = output_data[['permno', 'yyyymm', 'ChangeInRecommendation']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved ChangeInRecommendation predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed ChangeInRecommendation predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct ChangeInRecommendation predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    changeinrecommendation()
