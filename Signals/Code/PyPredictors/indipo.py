"""
Python equivalent of IndIPO.do
Generated from: IndIPO.do

Original Stata file: IndIPO.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def indipo():
    """
    Python equivalent of IndIPO.do
    
    Constructs the IndIPO predictor signal for IPO indicator (3 months to 3 years after IPO).
    """
    logger.info("Constructing predictor signal: IndIPO...")
    
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
        required_vars = ['permno', 'time_avail_m']
        
        data = pd.read_csv(master_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Merge with IPO dates data
        ipo_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/IPODates.csv")
        
        logger.info(f"Loading IPO dates data from: {ipo_path}")
        
        if not ipo_path.exists():
            logger.error(f"IPO dates file not found: {ipo_path}")
            logger.error("Please run the IPO data download scripts first")
            return False
        
        # Load IPO dates data
        ipo_data = pd.read_csv(ipo_path)
        
        # Merge data (equivalent to Stata's "merge m:1 permno using "$pathDataIntermediate/IPODates", keep(master match) nogenerate")
        data = data.merge(
            ipo_data,
            on='permno',
            how='inner'  # keep(master match)
        )
        
        logger.info(f"After merging with IPO dates: {len(data)} observations")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating IndIPO signal...")
        
        # Convert dates to datetime
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        data['IPOdate'] = pd.to_datetime(data['IPOdate'])
        
        # Calculate months since IPO (equivalent to Stata's "time_avail_m - IPOdate")
        data['months_since_ipo'] = ((data['time_avail_m'].dt.year - data['IPOdate'].dt.year) * 12 + 
                                   (data['time_avail_m'].dt.month - data['IPOdate'].dt.month))
        
        # Create IndIPO indicator (equivalent to Stata's "gen IndIPO = (time_avail_m - IPOdate <= 36) & (time_avail_m - IPOdate >= 3)")
        data['IndIPO'] = ((data['months_since_ipo'] <= 36) & (data['months_since_ipo'] >= 3)).astype(int)
        
        # Set to 0 if IPO date is missing (equivalent to Stata's "replace IndIPO = 0 if IPOdate == .")
        data.loc[data['IPOdate'].isna(), 'IndIPO'] = 0
        
        logger.info("Successfully calculated IndIPO signal")
        
        # SAVE RESULTS
        logger.info("Saving IndIPO predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'IndIPO']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['IndIPO'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "IndIPO.csv"
        csv_data = output_data[['permno', 'yyyymm', 'IndIPO']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved IndIPO predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed IndIPO predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct IndIPO predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    indipo()
