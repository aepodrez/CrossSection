"""
Python equivalent of EarningsForecastDisparity.do
Generated from: EarningsForecastDisparity.do

Original Stata file: EarningsForecastDisparity.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def earningsforecastdisparity():
    """
    Python equivalent of EarningsForecastDisparity.do
    
    Constructs the EarningsForecastDisparity predictor signal for long vs short-term earnings expectations.
    """
    logger.info("Constructing predictor signal: EarningsForecastDisparity...")
    
    try:
        # Prep IBES data
        # Load IBES EPS unadjusted data
        ibes_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/IBES_EPS_Unadj.csv")
        
        logger.info(f"Loading IBES EPS unadjusted data from: {ibes_path}")
        
        if not ibes_path.exists():
            logger.error(f"IBES EPS unadjusted file not found: {ibes_path}")
            logger.error("Please run the IBES data download script first")
            return False
        
        # Load IBES data
        ibes_data = pd.read_csv(ibes_path)
        logger.info(f"Successfully loaded {len(ibes_data)} IBES records")
        
        # Create temporary directory
        temp_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Temp")
        temp_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare short-term forecasts (equivalent to Stata's first use block)
        logger.info("Preparing short-term forecasts...")
        
        # Keep fpi == "1" (equivalent to Stata's "keep if fpi == '1'")
        ibes_short = ibes_data[ibes_data['fpi'] == "1"].copy()
        
        # Keep if fpedats is not missing and > statpers + 30 (equivalent to Stata's "keep if fpedats != . & fpedats > statpers + 30")
        ibes_short = ibes_short[
            (ibes_short['fpedats'].notna()) & 
            (ibes_short['fpedats'] > ibes_short['statpers'] + 30)
        ]
        
        # Save temporary short-term file (equivalent to Stata's "save "$pathtemp/tempIBESshort", replace")
        ibes_short.to_csv(temp_dir / "tempIBESshort.csv", index=False)
        logger.info(f"Saved short-term forecasts: {len(ibes_short)} records")
        
        # Prepare long-term forecasts (equivalent to Stata's second use block)
        logger.info("Preparing long-term forecasts...")
        
        # Keep fpi == "0" (equivalent to Stata's "keep if fpi == '0'")
        ibes_long = ibes_data[ibes_data['fpi'] == "0"].copy()
        
        # Rename meanest to fgr5yr (equivalent to Stata's "rename meanest fgr5yr")
        ibes_long = ibes_long.rename(columns={'meanest': 'fgr5yr'})
        
        # Save temporary long-term file (equivalent to Stata's "save "$pathtemp/tempIBESlong", replace")
        ibes_long.to_csv(temp_dir / "tempIBESlong.csv", index=False)
        logger.info(f"Saved long-term forecasts: {len(ibes_long)} records")
        
        # DATA LOAD
        # Load SignalMasterTable
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load required variables from SignalMasterTable
        master_vars = ['permno', 'time_avail_m', 'tickerIBES']
        data = pd.read_csv(master_path, usecols=master_vars)
        logger.info(f"Successfully loaded {len(data)} records from SignalMasterTable")
        
        # Merge with short-term forecasts (equivalent to Stata's first merge)
        data = data.merge(
            ibes_short[['tickerIBES', 'time_avail_m', 'meanest']],
            on=['tickerIBES', 'time_avail_m'],
            how='left'
        )
        
        # Merge with long-term forecasts (equivalent to Stata's second merge)
        data = data.merge(
            ibes_long[['tickerIBES', 'time_avail_m', 'fgr5yr']],
            on=['tickerIBES', 'time_avail_m'],
            how='left'
        )
        
        # Merge with IBES unadjusted actuals (equivalent to Stata's third merge)
        actuals_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/IBES_UnadjustedActuals.csv")
        
        if not actuals_path.exists():
            logger.error(f"IBES unadjusted actuals file not found: {actuals_path}")
            logger.error("Please run the IBES data download script first")
            return False
        
        actuals_data = pd.read_csv(actuals_path, usecols=['tickerIBES', 'time_avail_m', 'fy0a'])
        
        data = data.merge(
            actuals_data,
            on=['tickerIBES', 'time_avail_m'],
            how='left'
        )
        
        logger.info(f"Successfully merged data: {len(data)} observations")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating EarningsForecastDisparity signal...")
        
        # Calculate tempShort (equivalent to Stata's "gen tempShort = 100* (meanest - fy0a)/abs(fy0a)")
        data['tempShort'] = 100 * (data['meanest'] - data['fy0a']) / data['fy0a'].abs()
        
        # Calculate EarningsForecastDisparity (equivalent to Stata's "gen EarningsForecastDisparity = fgr5yr - tempShort")
        data['EarningsForecastDisparity'] = data['fgr5yr'] - data['tempShort']
        
        logger.info("Successfully calculated EarningsForecastDisparity signal")
        
        # SAVE RESULTS
        logger.info("Saving EarningsForecastDisparity predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'EarningsForecastDisparity']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['EarningsForecastDisparity'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "EarningsForecastDisparity.csv"
        csv_data = output_data[['permno', 'yyyymm', 'EarningsForecastDisparity']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved EarningsForecastDisparity predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed EarningsForecastDisparity predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct EarningsForecastDisparity predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    earningsforecastdisparity()
