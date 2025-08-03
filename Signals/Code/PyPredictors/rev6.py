"""
Python equivalent of REV6.do
Generated from: REV6.do

Original Stata file: REV6.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def rev6():
    """
    Python equivalent of REV6.do
    
    Constructs the REV6 predictor signal for earnings forecast revision.
    """
    logger.info("Constructing predictor signal: REV6...")
    
    try:
        # Prep IBES data
        logger.info("Preparing IBES data...")
        
        # Load IBES EPS data
        ibes_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/IBES_EPS_Unadj.csv")
        
        logger.info(f"Loading IBES EPS data from: {ibes_path}")
        
        if not ibes_path.exists():
            logger.error(f"IBES_EPS_Unadj not found: {ibes_path}")
            logger.error("Please run the IBES EPS data creation script first")
            return False
        
        ibes_data = pd.read_csv(ibes_path)
        logger.info(f"Successfully loaded {len(ibes_data)} IBES records")
        
        # Keep only fpi == "1" (equivalent to Stata's "keep if fpi == '1'")
        ibes_data = ibes_data[ibes_data['fpi'] == '1']
        logger.info(f"After keeping fpi == '1': {len(ibes_data)} records")
        
        # Set to last non-missing forecast in period that trade happens
        # Create tmp indicator (equivalent to Stata's "gen tmp = 1 if fpedats != . & fpedats > statpers + 30")
        ibes_data['tmp'] = np.where((ibes_data['fpedats'].notna()) & 
                                   (ibes_data['fpedats'] > ibes_data['statpers'] + 30), 1, np.nan)
        
        # Sort by tickerIBES and fpedats
        ibes_data = ibes_data.sort_values(['tickerIBES', 'fpedats'])
        
        # Forward fill meanest for missing tmp (equivalent to Stata's "bys tickerIBES: replace meanest = meanest[_n-1] if mi(tmp) & fpedats == fpedats[_n-1]")
        ibes_data['meanest'] = ibes_data.groupby('tickerIBES')['meanest'].fillna(method='ffill')
        
        # Drop tmp column
        ibes_data = ibes_data.drop('tmp', axis=1)
        
        # Save temporary file (equivalent to Stata's "save '$pathtemp/temp', replace")
        temp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/temp_ibes.csv")
        ibes_data.to_csv(temp_path, index=False)
        logger.info(f"Saved temporary IBES data to: {temp_path}")
        
        # DATA LOAD
        logger.info("Loading main data sources...")
        
        # Load SignalMasterTable
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        master_data = pd.read_csv(master_path, usecols=['permno', 'tickerIBES', 'time_avail_m', 'prc'])
        logger.info(f"Successfully loaded {len(master_data)} master records")
        
        # Merge with temporary IBES data
        data = master_data.merge(ibes_data[['tickerIBES', 'time_avail_m', 'meanest']], 
                                on=['tickerIBES', 'time_avail_m'], how='left')
        logger.info(f"After merging with IBES data: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating REV6 signal...")
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate lags (equivalent to Stata's "l." variables)
        data['meanest_lag1'] = data.groupby('permno')['meanest'].shift(1)
        data['prc_lag1'] = data.groupby('permno')['prc'].shift(1)
        
        # Calculate tempRev (equivalent to Stata's "gen tempRev = (meanest - l.meanest)/abs(l.prc)")
        data['tempRev'] = (data['meanest'] - data['meanest_lag1']) / np.abs(data['prc_lag1'])
        
        # Calculate lags of tempRev for REV6 (equivalent to Stata's lags 1-6)
        data['tempRev_lag1'] = data.groupby('permno')['tempRev'].shift(1)
        data['tempRev_lag2'] = data.groupby('permno')['tempRev'].shift(2)
        data['tempRev_lag3'] = data.groupby('permno')['tempRev'].shift(3)
        data['tempRev_lag4'] = data.groupby('permno')['tempRev'].shift(4)
        data['tempRev_lag5'] = data.groupby('permno')['tempRev'].shift(5)
        data['tempRev_lag6'] = data.groupby('permno')['tempRev'].shift(6)
        
        # Calculate REV6 (equivalent to Stata's "gen REV6 = tempRev + l.tempRev + l2.tempRev + l3.tempRev + l4.tempRev + l5.tempRev + l6.tempRev")
        data['REV6'] = (data['tempRev'] + 
                        data['tempRev_lag1'] + 
                        data['tempRev_lag2'] + 
                        data['tempRev_lag3'] + 
                        data['tempRev_lag4'] + 
                        data['tempRev_lag5'] + 
                        data['tempRev_lag6'])
        
        logger.info("Successfully calculated REV6 signal")
        
        # Clean up temporary file
        if temp_path.exists():
            temp_path.unlink()
            logger.info("Cleaned up temporary IBES file")
        
        # SAVE RESULTS
        logger.info("Saving REV6 predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'REV6']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['REV6'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "REV6.csv"
        csv_data = output_data[['permno', 'yyyymm', 'REV6']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved REV6 predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed REV6 predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct REV6 predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    rev6()
