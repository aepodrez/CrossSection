"""
Python equivalent of Recomm_ShortInterest.do
Generated from: Recomm_ShortInterest.do

Original Stata file: Recomm_ShortInterest.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def recomm_shortinterest():
    """
    Python equivalent of Recomm_ShortInterest.do
    
    Constructs the Recomm_ShortInterest predictor signal for recommendation and short interest.
    """
    logger.info("Constructing predictor signal: Recomm_ShortInterest...")
    
    try:
        # Prepare consensus recommendation
        logger.info("Preparing consensus recommendation data...")
        
        # Load IBES recommendations data
        ibes_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/IBES_Recommendations.csv")
        
        logger.info(f"Loading IBES recommendations from: {ibes_path}")
        
        if not ibes_path.exists():
            logger.error(f"IBES_Recommendations not found: {ibes_path}")
            logger.error("Please run the IBES recommendations data creation script first")
            return False
        
        ibes_data = pd.read_csv(ibes_path, usecols=['tickerIBES', 'amaskcd', 'anndats', 'time_avail_m', 'ireccd'])
        logger.info(f"Successfully loaded {len(ibes_data)} IBES records")
        
        # Drop duplicates - keep latest recommendation per month (equivalent to Stata's "bys tickerIBES amaskcd time_avail_m (anndats): keep if _n==_N")
        ibes_data = ibes_data.sort_values(['tickerIBES', 'amaskcd', 'time_avail_m', 'anndats'])
        ibes_data = ibes_data.groupby(['tickerIBES', 'amaskcd', 'time_avail_m']).last().reset_index()
        logger.info(f"After keeping latest recommendation per month: {len(ibes_data)} records")
        
        # Create tempID for grouping (equivalent to Stata's "egen tempID = group(tickerIBES amaskcd)")
        ibes_data['tempID'] = ibes_data.groupby(['tickerIBES', 'amaskcd']).ngroup()
        
        # Sort and fill missing time periods (equivalent to Stata's "xtset tempID time" and "tsfill")
        ibes_data = ibes_data.sort_values(['tempID', 'time_avail_m'])
        
        # Fill missing tickerIBES (equivalent to Stata's "bys tempID (time_avail_m): replace tickerIBES = tickerIBES[_n-1] if mi(tickerIBES) & _n >1")
        ibes_data['tickerIBES'] = ibes_data.groupby('tempID')['tickerIBES'].fillna(method='ffill')
        
        # Calculate 12-month rolling first value (equivalent to Stata's "asrol ireccd, gen(ireccd12) by(tempID) stat(first) window(time_avail_m 12) min(1)")
        ibes_data['ireccd12'] = ibes_data.groupby('tempID').rolling(
            window=12, min_periods=1
        )['ireccd'].first().reset_index(0, drop=True)
        
        # Collapse to firm-month (equivalent to Stata's "gcollapse (mean) ireccd12, by(tickerIBES time_avail_m)")
        ibes_data = ibes_data.groupby(['tickerIBES', 'time_avail_m'])['ireccd12'].mean().reset_index()
        logger.info(f"After collapsing to firm-month: {len(ibes_data)} records")
        
        # Save temporary file (equivalent to Stata's "save tempRec, replace")
        temp_rec_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/tempRec.csv")
        ibes_data.to_csv(temp_rec_path, index=False)
        logger.info(f"Saved temporary recommendations to: {temp_rec_path}")
        
        # DATA LOAD
        logger.info("Loading main data sources...")
        
        # Load SignalMasterTable
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        master_data = pd.read_csv(master_path, usecols=['permno', 'gvkey', 'tickerIBES', 'time_avail_m', 'bh1m'])
        logger.info(f"Successfully loaded {len(master_data)} master records")
        
        # Drop if gvkey or tickerIBES is missing (equivalent to Stata's "drop if mi(gvkey) | mi(tickerIBES)")
        master_data = master_data.dropna(subset=['gvkey', 'tickerIBES'])
        logger.info(f"After dropping missing gvkey or tickerIBES: {len(master_data)} records")
        
        # Load monthly CRSP data
        crsp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/monthlyCRSP.csv")
        
        logger.info(f"Loading monthly CRSP data from: {crsp_path}")
        
        if not crsp_path.exists():
            logger.error(f"monthlyCRSP not found: {crsp_path}")
            logger.error("Please run the monthly CRSP data creation script first")
            return False
        
        crsp_data = pd.read_csv(crsp_path, usecols=['permno', 'time_avail_m', 'shrout'])
        
        # Merge with CRSP data
        data = master_data.merge(crsp_data, on=['permno', 'time_avail_m'], how='inner')
        logger.info(f"After merging with CRSP data: {len(data)} records")
        
        # Load monthly short interest data
        shortint_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/monthlyShortInterest.csv")
        
        logger.info(f"Loading monthly short interest data from: {shortint_path}")
        
        if not shortint_path.exists():
            logger.error(f"monthlyShortInterest not found: {shortint_path}")
            logger.error("Please run the monthly short interest data creation script first")
            return False
        
        shortint_data = pd.read_csv(shortint_path, usecols=['gvkey', 'time_avail_m', 'shortint'])
        
        # Merge with short interest data
        data = data.merge(shortint_data, on=['gvkey', 'time_avail_m'], how='inner')
        logger.info(f"After merging with short interest data: {len(data)} records")
        
        # Merge with temporary recommendations data
        data = data.merge(ibes_data, on=['tickerIBES', 'time_avail_m'], how='inner')
        logger.info(f"After merging with recommendations data: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating Recomm_ShortInterest signal...")
        
        # Calculate short interest ratio (equivalent to Stata's "gen ShortInterest = shortint/shrout")
        data['ShortInterest'] = data['shortint'] / data['shrout']
        
        # Calculate consensus recommendation (equivalent to Stata's "gen ConsRecomm = 6 - ireccd12")
        data['ConsRecomm'] = 6 - data['ireccd12']
        
        # Create quintiles for short interest (equivalent to Stata's "egen QuintShortInterest = xtile(ShortInterest), n(5) by(time_avail_m)")
        data['QuintShortInterest'] = data.groupby('time_avail_m')['ShortInterest'].transform(
            lambda x: pd.qcut(x, q=5, labels=False, duplicates='drop') + 1
        )
        
        # Create quintiles for consensus recommendation (equivalent to Stata's "egen QuintConsRecomm = xtile(ConsRecomm), n(5) by(time_avail_m)")
        data['QuintConsRecomm'] = data.groupby('time_avail_m')['ConsRecomm'].transform(
            lambda x: pd.qcut(x, q=5, labels=False, duplicates='drop') + 1
        )
        
        # Create Recomm_ShortInterest signal (equivalent to Stata's logic)
        data['Recomm_ShortInterest'] = np.nan
        
        # Set to 1 if low short interest and high recommendation (equivalent to Stata's "replace Recomm_ShortInterest = 1 if QuintShortInterest == 1 & QuintConsRecomm ==1")
        data.loc[(data['QuintShortInterest'] == 1) & (data['QuintConsRecomm'] == 1), 'Recomm_ShortInterest'] = 1
        
        # Set to 0 if high short interest and low recommendation (equivalent to Stata's "replace Recomm_ShortInterest = 0 if QuintShortInterest == 5 & QuintConsRecomm ==5")
        data.loc[(data['QuintShortInterest'] == 5) & (data['QuintConsRecomm'] == 5), 'Recomm_ShortInterest'] = 0
        
        # Keep only observations with non-missing Recomm_ShortInterest (equivalent to Stata's "keep if !mi(Recomm_ShortInterest)")
        data = data.dropna(subset=['Recomm_ShortInterest'])
        logger.info(f"After keeping non-missing Recomm_ShortInterest: {len(data)} records")
        
        logger.info("Successfully calculated Recomm_ShortInterest signal")
        
        # Clean up temporary file
        if temp_rec_path.exists():
            temp_rec_path.unlink()
            logger.info("Cleaned up temporary recommendations file")
        
        # SAVE RESULTS
        logger.info("Saving Recomm_ShortInterest predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'Recomm_ShortInterest']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['Recomm_ShortInterest'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "Recomm_ShortInterest.csv"
        csv_data = output_data[['permno', 'yyyymm', 'Recomm_ShortInterest']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved Recomm_ShortInterest predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed Recomm_ShortInterest predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct Recomm_ShortInterest predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    recomm_shortinterest()
