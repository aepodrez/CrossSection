"""
Python equivalent of ChNAnalyst.do
Generated from: ChNAnalyst.do

Original Stata file: ChNAnalyst.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def chnanalyst():
    """
    Python equivalent of ChNAnalyst.do
    
    Constructs the ChNAnalyst predictor signal for decline in analyst coverage.
    """
    logger.info("Constructing predictor signal: ChNAnalyst...")
    
    try:
        # Prep IBES data
        logger.info("Preparing IBES data...")
        
        ibes_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/IBES_EPS_Unadj.csv")
        
        if not ibes_path.exists():
            logger.error(f"IBES data not found: {ibes_path}")
            logger.error("Please run the IBES data download scripts first")
            return False
        
        # Load IBES data and filter for fpi == "1"
        ibes_data = pd.read_csv(ibes_path)
        ibes_data = ibes_data[ibes_data['fpi'] == "1"]
        
        # Set to last non-missing forecast in period that trade happens
        ibes_data = ibes_data.sort_values(['tickerIBES', 'fpedats'])
        
        # Create tmp flag for valid forecasts
        ibes_data['tmp'] = np.where(
            (ibes_data['fpedats'].notna()) & (ibes_data['fpedats'] > ibes_data['statpers'] + 30), 
            1, 
            np.nan
        )
        
        # Forward fill meanest for missing tmp values
        ibes_data[''meanest''] = ibes_data.groupby('tickerIBES')[''meanest''].ffill()
        
        # Keep required variables
        ibes_data = ibes_data[['tickerIBES', 'time_avail_m', 'numest', 'statpers', 'fpedats']]
        
        # Save temporary file
        temp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Temp/temp.csv")
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        ibes_data.to_csv(temp_path, index=False)
        
        # DATA LOAD
        # Load SignalMasterTable
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load required variables from SignalMasterTable
        master_vars = ['permno', 'time_avail_m', 'tickerIBES', 'mve_c']
        master_data = pd.read_csv(master_path, usecols=master_vars)
        
        # Merge with IBES data
        logger.info("Merging with IBES data...")
        
        ibes_temp = pd.read_csv(temp_path)
        
        merged_data = master_data.merge(
            ibes_temp, 
            on=['tickerIBES', 'time_avail_m'], 
            how='left'  # equivalent to Stata's keep(master match)
        )
        
        logger.info(f"Successfully merged data: {len(merged_data)} observations")
        
        # Sort by permno and time_avail_m for lag calculations
        merged_data = merged_data.sort_values(['permno', 'time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating ChNAnalyst signal...")
        
        # Calculate 3-month lag of numest
        merged_data['numest_lag3'] = merged_data.groupby('permno')['numest'].shift(3)
        
        # Calculate ChNAnalyst (equivalent to Stata's conditional logic)
        merged_data['ChNAnalyst'] = np.nan
        
        # Set to 1 if number of analysts decreased
        mask1 = (merged_data['numest'] < merged_data['numest_lag3']) & \
                (merged_data['numest_lag3'].notna())
        merged_data.loc[mask1, 'ChNAnalyst'] = 1
        
        # Set to 0 if number of analysts increased or stayed same
        mask2 = (merged_data['numest'] >= merged_data['numest_lag3']) & \
                (merged_data['numest'].notna())
        merged_data.loc[mask2, 'ChNAnalyst'] = 0
        
        # Set to missing for specific time period (equivalent to Stata's date filter)
        # Convert time_avail_m to datetime for comparison
        merged_data['time_avail_m'] = pd.to_datetime(merged_data['time_avail_m'])
        mask3 = (merged_data['time_avail_m'] >= pd.to_datetime('1987-07-01')) & \
                (merged_data['time_avail_m'] <= pd.to_datetime('1987-09-30'))
        merged_data.loc[mask3, 'ChNAnalyst'] = np.nan
        
        # Filter for small firms only (equivalent to Stata's quintile filter)
        merged_data['temp'] = pd.qcut(merged_data['mve_c'], q=5, labels=False, duplicates='drop') + 1
        merged_data = merged_data[merged_data['temp'] <= 2]
        
        logger.info("Successfully calculated ChNAnalyst signal")
        
        # SAVE RESULTS
        logger.info("Saving ChNAnalyst predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = merged_data[['permno', 'time_avail_m', 'ChNAnalyst']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['ChNAnalyst'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "ChNAnalyst.csv"
        csv_data = output_data[['permno', 'yyyymm', 'ChNAnalyst']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved ChNAnalyst predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed ChNAnalyst predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct ChNAnalyst predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    chnanalyst()
