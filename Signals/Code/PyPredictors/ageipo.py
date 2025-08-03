"""
Python equivalent of AgeIPO.do
Generated from: AgeIPO.do

Original Stata file: AgeIPO.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def ageipo():
    """
    Python equivalent of AgeIPO.do
    
    Constructs the AgeIPO predictor signal for IPO age analysis.
    """
    logger.info("Constructing predictor signal: AgeIPO...")
    
    try:
        # DATA LOAD
        # Load SignalMasterTable
        signal_master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {signal_master_path}")
        
        if not signal_master_path.exists():
            logger.error(f"SignalMasterTable not found: {signal_master_path}")
            return False
        
        data = pd.read_csv(signal_master_path, usecols=['permno', 'time_avail_m'])
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Load IPODates
        ipo_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/IPODates.csv")
        
        logger.info(f"Loading IPODates from: {ipo_path}")
        
        if not ipo_path.exists():
            logger.error(f"IPODates not found: {ipo_path}")
            return False
        
        ipo_data = pd.read_csv(ipo_path)
        logger.info(f"Successfully loaded IPODates with {len(ipo_data)} records")
        
        # Merge with IPODates (equivalent to Stata's "merge m:1 permno using IPODates")
        data = data.merge(ipo_data, on='permno', how='left')
        logger.info(f"After merge: {len(data)} records")
        
        # Convert time_avail_m to datetime for calculations
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Constructing AgeIPO signal...")
        
        # Create tempipo flag (equivalent to Stata's "gen tempipo = (time_avail_m - IPOdate <= 36) & (time_avail_m - IPOdate >= 3)")
        # Calculate months since IPO
        data['months_since_ipo'] = ((data['time_avail_m'].dt.year - data['IPOdate'].dt.year) * 12 + 
                                   (data['time_avail_m'].dt.month - data['IPOdate'].dt.month))
        
        data['tempipo'] = ((data['months_since_ipo'] <= 36) & (data['months_since_ipo'] >= 3))
        
        # Set to missing if IPOdate is missing (equivalent to Stata's "replace tempipo = . if IPOdate == .")
        data.loc[data['IPOdate'].isna(), 'tempipo'] = np.nan
        
        # Calculate AgeIPO (equivalent to Stata's "gen AgeIPO = year(dofm(time_avail_m)) - FoundingYear")
        data['AgeIPO'] = data['time_avail_m'].dt.year - data['FoundingYear']
        
        # Set to missing if not recent IPO (equivalent to Stata's "replace AgeIPO = . if tempipo == 0")
        data.loc[data['tempipo'] == False, 'AgeIPO'] = np.nan
        
        # Calculate total recent IPOs per month (equivalent to Stata's "egen tempTotal = total(tempipo), by(time_avail_m)")
        data['tempTotal'] = data.groupby('time_avail_m')['tempipo'].transform('sum')
        
        # Set to missing if insufficient IPOs (equivalent to Stata's "replace AgeIPO = . if tempTotal < 20*5")
        data.loc[data['tempTotal'] < 100, 'AgeIPO'] = np.nan
        
        logger.info("Successfully calculated AgeIPO signal")
        
        # SAVE RESULTS
        logger.info("Saving AgeIPO predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'AgeIPO']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['AgeIPO'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "AgeIPO.csv"
        csv_data = output_data[['permno', 'yyyymm', 'AgeIPO']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved AgeIPO predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed AgeIPO predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct AgeIPO predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    ageipo()
