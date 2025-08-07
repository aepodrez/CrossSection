"""
Python equivalent of BidAskSpread.do
Generated from: BidAskSpread.do

Original Stata file: BidAskSpread.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def bidaskspread():
    """
    Python equivalent of BidAskSpread.do
    
    Constructs the BidAskSpread predictor signal from Corwin-Schultz bid-ask spread data.
    """
    logger.info("Constructing predictor signal: BidAskSpread...")
    
    try:
        # DATA LOAD
        # Load BAspreadsCorwin data (equivalent to Stata's "use "$pathDataIntermediate/BAspreadsCorwin.dta", clear")
        baspreads_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/BAspreadsCorwin.csv")
        
        logger.info(f"Loading BAspreadsCorwin data from: {baspreads_path}")
        
        if not baspreads_path.exists():
            logger.error(f"BAspreadsCorwin.csv not found: {baspreads_path}")
            logger.error("Please run the Corwin-Schultz preparation script first")
            return False
        
        # Load the data
        data = pd.read_csv(baspreads_path)
        logger.info(f"Successfully loaded {len(data)} bid-ask spread records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Processing bid-ask spread data...")
        
        # The construction is already done in the SAS code (Corwin_Schultz_Edit.sas)
        # We just need to prepare the data for saving as a predictor
        
        # Convert time_avail_m to datetime for processing
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        
        # Keep only the required columns (equivalent to Stata's savepredictor)
        output_data = data[['permno', 'time_avail_m', 'BidAskSpread']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['BidAskSpread'])
        logger.info(f"After removing missing values: {len(output_data)} records")
        
        # SAVE RESULTS
        logger.info("Saving BidAskSpread predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "bidaskspread.csv"
        csv_data = output_data[['permno', 'yyyymm', 'BidAskSpread']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved BidAskSpread predictor to: {csv_output_path}")
        
        # Log summary statistics
        logger.info(f"Final dataset: {len(output_data)} records")
        logger.info(f"Unique companies: {output_data['permno'].nunique()}")
        logger.info(f"Time range: {output_data['time_avail_m'].min()} to {output_data['time_avail_m'].max()}")
        logger.info(f"BidAskSpread range: {output_data['BidAskSpread'].min():.4f} to {output_data['BidAskSpread'].max():.4f}")
        
        logger.info("Successfully constructed BidAskSpread predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct BidAskSpread predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    bidaskspread()
