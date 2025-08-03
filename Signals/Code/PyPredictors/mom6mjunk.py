"""
Python equivalent of Mom6mJunk.do
Generated from: Mom6mJunk.do

Original Stata file: Mom6mJunk.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def mom6mjunk():
    """
    Python equivalent of Mom6mJunk.do
    
    Constructs the Mom6mJunk predictor signal for junk stock momentum.
    """
    logger.info("Constructing predictor signal: Mom6mJunk...")
    
    try:
        # Clean CIQ ratings first
        logger.info("Processing CIQ credit ratings...")
        
        # Load CIQ credit ratings
        ciq_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_CIQ_creditratings.csv")
        
        if not ciq_path.exists():
            logger.error(f"CIQ credit ratings file not found: {ciq_path}")
            logger.error("Please run the CIQ credit ratings download scripts first")
            return False
        
        ciq_data = pd.read_csv(ciq_path)
        
        # Remove suffixes (equivalent to Stata's subinstr commands)
        ciq_data['currentratingsymbol'] = ciq_data['currentratingsymbol'].str.replace('pi', '', regex=False)
        ciq_data['currentratingsymbol'] = ciq_data['currentratingsymbol'].str.replace('q', '', regex=False)
        ciq_data['currentratingsymbol'] = ciq_data['currentratingsymbol'].str.replace(' prelim', '', regex=False)
        
        # Create numerical rating mapping
        rating_mapping = {
            'D': 1, 'C': 2, 'CC': 3, 'CCC-': 4, 'CCC': 5, 'CCC+': 6,
            'B-': 7, 'B': 8, 'B+': 9, 'BB-': 10, 'BB': 11, 'BB+': 12,
            'BBB-': 13, 'BBB': 14, 'BBB+': 15, 'A-': 16, 'A': 17, 'A+': 18,
            'AA-': 19, 'AA': 20, 'AA+': 21, 'AAA': 22
        }
        
        # Create numerical rating
        ciq_data['credratciq'] = ciq_data['currentratingsymbol'].map(rating_mapping).fillna(0)
        
        # Keep required variables
        ciq_data = ciq_data[['gvkey', 'time_avail_m', 'credratciq']]
        
        logger.info(f"Processed CIQ ratings: {len(ciq_data)} records")
        
        # DATA LOAD
        # Load SignalMasterTable data
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load the required variables
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'ret']
        
        data = pd.read_csv(master_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Drop observations with missing gvkey (equivalent to Stata's "drop if gvkey ==.")
        data = data.dropna(subset=['gvkey'])
        logger.info(f"After dropping missing gvkey: {len(data)} records")
        
        # Merge with SP credit ratings
        sp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_SP_creditratings.csv")
        
        logger.info(f"Loading SP credit ratings from: {sp_path}")
        
        if not sp_path.exists():
            logger.error(f"SP credit ratings file not found: {sp_path}")
            logger.error("Please run the SP credit ratings download scripts first")
            return False
        
        sp_data = pd.read_csv(sp_path)
        
        # Merge data (equivalent to Stata's "merge 1:1 gvkey time_avail_m using "$pathDataIntermediate/m_SP_creditratings", keep(master match) nogenerate")
        data = data.merge(
            sp_data,
            on=['gvkey', 'time_avail_m'],
            how='left'  # keep(master match)
        )
        
        logger.info(f"After merging with SP ratings: {len(data)} observations")
        
        # Merge with CIQ credit ratings
        data = data.merge(
            ciq_data,
            on=['gvkey', 'time_avail_m'],
            how='left'  # keep(master match)
        )
        
        logger.info(f"After merging with CIQ ratings: {len(data)} observations")
        
        # Fill missing credratciq with most recent (equivalent to Stata's tsfill and forward fill)
        data = data.sort_values(['permno', 'time_avail_m'])
        data['credratciq'] = data.groupby('permno')['credratciq'].fillna(method='ffill')
        
        # Coalesce credit ratings (equivalent to Stata's "replace credrat = credratciq if credrat == .")
        data['credrat'] = data['credrat'].fillna(data['credratciq'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating Mom6mJunk signal...")
        
        # Replace missing returns with 0 (equivalent to Stata's "replace ret = 0 if mi(ret)")
        data['ret'] = data['ret'].fillna(0)
        
        # Sort data for time series operations (equivalent to Stata's "xtset permno time_avail_m")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate lags of returns (1-5 months)
        for lag in range(1, 6):
            data[f'ret_lag{lag}'] = data.groupby('permno')['ret'].shift(lag)
        
        # Calculate 6-month momentum (equivalent to Stata's "gen Mom6m = ( (1+l.ret)*(1+l2.ret)*(1+l3.ret)*(1+l4.ret)*(1+l5.ret)) - 1")
        data['Mom6m'] = ((1 + data['ret_lag1']) * (1 + data['ret_lag2']) * 
                         (1 + data['ret_lag3']) * (1 + data['ret_lag4']) * 
                         (1 + data['ret_lag5'])) - 1
        
        # Create Mom6mJunk for junk stocks (equivalent to Stata's "gen Mom6mJunk = Mom6m if ( credrat <= 14 & credrat > 0 )")
        data['Mom6mJunk'] = np.where((data['credrat'] <= 14) & (data['credrat'] > 0), data['Mom6m'], np.nan)
        
        logger.info("Successfully calculated Mom6mJunk signal")
        
        # SAVE RESULTS
        logger.info("Saving Mom6mJunk predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'Mom6mJunk']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['Mom6mJunk'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "Mom6mJunk.csv"
        csv_data = output_data[['permno', 'yyyymm', 'Mom6mJunk']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved Mom6mJunk predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed Mom6mJunk predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct Mom6mJunk predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    mom6mjunk()
