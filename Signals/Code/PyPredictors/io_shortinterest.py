"""
Python equivalent of IO_ShortInterest.do
Generated from: IO_ShortInterest.do

Original Stata file: IO_ShortInterest.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def io_shortinterest():
    """
    Python equivalent of IO_ShortInterest.do
    
    Constructs the IO_ShortInterest predictor signal for institutional ownership for high short interest.
    """
    logger.info("Constructing predictor signal: IO_ShortInterest...")
    
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
        required_vars = ['permno', 'gvkey', 'time_avail_m']
        
        data = pd.read_csv(master_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Merge with 13F institutional ownership data
        tr13f_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/TR_13F.csv")
        
        logger.info(f"Loading 13F institutional ownership data from: {tr13f_path}")
        
        if not tr13f_path.exists():
            logger.error(f"13F institutional ownership file not found: {tr13f_path}")
            logger.error("Please run the 13F data download scripts first")
            return False
        
        tr13f_data = pd.read_csv(tr13f_path, usecols=['permno', 'time_avail_m', 'instown_perc'])
        
        # Merge data (equivalent to Stata's "merge 1:1 permno time_avail_m using "$pathDataIntermediate/TR_13F", keep(master match) nogenerate keepusing(instown_perc)")
        data = data.merge(
            tr13f_data,
            on=['permno', 'time_avail_m'],
            how='left'  # keep(master match)
        )
        
        logger.info(f"After merging with 13F data: {len(data)} observations")
        
        # Merge with monthly CRSP data for shares outstanding
        crsp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/monthlyCRSP.csv")
        
        logger.info(f"Loading monthly CRSP data from: {crsp_path}")
        
        if not crsp_path.exists():
            logger.error(f"Monthly CRSP file not found: {crsp_path}")
            logger.error("Please run the CRSP data download scripts first")
            return False
        
        crsp_data = pd.read_csv(crsp_path, usecols=['permno', 'time_avail_m', 'shrout'])
        
        # Merge data (equivalent to Stata's "merge 1:1 permno time_avail_m using "$pathDataIntermediate/monthlyCRSP", keep(master match) nogenerate keepusing(shrout)")
        data = data.merge(
            crsp_data,
            on=['permno', 'time_avail_m'],
            how='left'  # keep(master match)
        )
        
        logger.info(f"After merging with CRSP data: {len(data)} observations")
        
        # Preserve observations with missing gvkey (equivalent to Stata's preserve/restore logic)
        missing_gvkey = data[data['gvkey'].isna()].copy()
        data_with_gvkey = data[data['gvkey'].notna()].copy()
        
        # Merge with short interest data for observations with gvkey
        shortint_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/monthlyShortInterest.csv")
        
        logger.info(f"Loading short interest data from: {shortint_path}")
        
        if not shortint_path.exists():
            logger.error(f"Short interest file not found: {shortint_path}")
            logger.error("Please run the short interest data download scripts first")
            return False
        
        shortint_data = pd.read_csv(shortint_path, usecols=['gvkey', 'time_avail_m', 'shortint'])
        
        # Merge data (equivalent to Stata's "merge 1:1 gvkey time_avail_m using "$pathDataIntermediate/monthlyShortInterest", keep(master match) nogenerate keepusing(shortint)")
        data_with_gvkey = data_with_gvkey.merge(
            shortint_data,
            on=['gvkey', 'time_avail_m'],
            how='left'  # keep(master match)
        )
        
        # Append observations without gvkey back (equivalent to Stata's "append using "$pathtemp/temp"")
        data = pd.concat([data_with_gvkey, missing_gvkey], ignore_index=True)
        
        logger.info(f"After merging with short interest data: {len(data)} observations")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating IO_ShortInterest signal...")
        
        # Calculate short ratio (equivalent to Stata's "gen tempshortratio = shortint/shrout")
        data['tempshortratio'] = data['shortint'] / data['shrout']
        
        # Replace missing short ratio with 0 (equivalent to Stata's "replace tempshortratio = 0 if tempshortratio == .")
        data['tempshortratio'] = data['tempshortratio'].fillna(0)
        
        # Calculate 99th percentile of short ratio by month (equivalent to Stata's "by time_avail_m: egen temps99 = pctile(shortint/shrout), p(99)")
        data['temps99'] = data.groupby('time_avail_m')['tempshortratio'].transform(lambda x: x.quantile(0.99))
        
        # Initialize temp with institutional ownership percentage (equivalent to Stata's "gen temp = instown_perc")
        data['temp'] = data['instown_perc']
        
        # Replace missing temp with 0 (equivalent to Stata's "replace temp = 0 if mi(temp)")
        data['temp'] = data['temp'].fillna(0)
        
        # Set temp to missing if short ratio < 99th percentile (equivalent to Stata's "replace temp = . if tempshortratio < temps99")
        data.loc[data['tempshortratio'] < data['temps99'], 'temp'] = np.nan
        
        # Assign to IO_ShortInterest (equivalent to Stata's "gen IO_ShortInterest = temp")
        data['IO_ShortInterest'] = data['temp']
        
        logger.info("Successfully calculated IO_ShortInterest signal")
        
        # SAVE RESULTS
        logger.info("Saving IO_ShortInterest predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'IO_ShortInterest']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['IO_ShortInterest'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "IO_ShortInterest.csv"
        csv_data = output_data[['permno', 'yyyymm', 'IO_ShortInterest']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved IO_ShortInterest predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed IO_ShortInterest predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct IO_ShortInterest predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    io_shortinterest()
