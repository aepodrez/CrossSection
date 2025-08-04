"""
Python equivalent of BMdec.do
Generated from: BMdec.do

Original Stata file: BMdec.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def bmdec():
    """
    Python equivalent of BMdec.do
    
    Constructs the BMdec predictor signal for book-to-market ratio using December market equity.
    """
    logger.info("Constructing predictor signal: BMdec...")
    
    try:
        # DATA LOAD
        # Load Compustat monthly data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading Compustat monthly data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"Input file not found: {compustat_path}")
            logger.error("Please run the Compustat data download scripts first")
            return False
        
        # Load the required variables
        required_vars = ['permno', 'time_avail_m', 'txditc', 'seq', 'ceq', 'at', 'lt', 'pstk', 'pstkrv', 'pstkl']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        initial_count = len(data)
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        logger.info(f"Removed {initial_count - len(data)} duplicate observations")
        
        # Load monthly CRSP data
        crsp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/monthlyCRSP.csv")
        
        logger.info(f"Loading monthly CRSP data from: {crsp_path}")
        
        if not crsp_path.exists():
            logger.error(f"Monthly CRSP data not found: {crsp_path}")
            return False
        
        crsp_data = pd.read_csv(crsp_path, usecols=['permno', 'time_avail_m', 'prc', 'shrout'])
        logger.info(f"Successfully loaded CRSP data with {len(crsp_data)} records")
        
        # Merge with CRSP data
        data = data.merge(crsp_data, on=['permno', 'time_avail_m'], how='inner')
        logger.info(f"After merge: {len(data)} records")
        
        # Sort by permno and time_avail_m for lag calculations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Convert time_avail_m to datetime
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Constructing BMdec signal...")
        
        # Calculate market equity for December (equivalent to Stata's "gen tempME = abs(prc)*shrout if month(dofm(time_avail_m)) == 12")
        data['tempME'] = np.where(data['time_avail_m'].dt.month == 12, 
                                 np.abs(data['prc']) * data['shrout'], np.nan)
        
        # Create year variable (equivalent to Stata's "gen tempYear = yofd(dofm(time_avail_m))")
        data['tempYear'] = data['time_avail_m'].dt.year
        
        # Calculate minimum market equity by permno and year (equivalent to Stata's "egen tempDecME = min(tempME), by(permno tempYear)")
        data['tempDecME'] = data.groupby(['permno', 'tempYear'])['tempME'].transform('min')
        
        # Forward fill December market equity within each permno
        data['tempDecME'] = data.groupby('permno')['tempDecME'].ffill()
        
        # Compute book equity
        # Set txditc to 0 if missing (equivalent to Stata's "replace txditc = 0 if mi(txditc)")
        data['txditc'] = data['txditc'].fillna(0)
        
        # Calculate preferred stock (equivalent to Stata's preferred stock logic)
        data['tempPS'] = data['pstk']
        data.loc[data['tempPS'].isna(), 'tempPS'] = data.loc[data['tempPS'].isna(), 'pstkrv']
        data.loc[data['tempPS'].isna(), 'tempPS'] = data.loc[data['tempPS'].isna(), 'pstkl']
        
        # Calculate stockholders' equity (equivalent to Stata's stockholders' equity logic)
        data['tempSE'] = data['seq']
        data.loc[data['tempSE'].isna(), 'tempSE'] = data.loc[data['tempSE'].isna(), 'ceq'] + data.loc[data['tempSE'].isna(), 'tempPS']
        data.loc[data['tempSE'].isna(), 'tempSE'] = data.loc[data['tempSE'].isna(), 'at'] - data.loc[data['tempSE'].isna(), 'lt']
        
        # Calculate book equity (equivalent to Stata's "gen tempBE = tempSE + txditc - tempPS")
        data['tempBE'] = data['tempSE'] + data['txditc'] - data['tempPS']
        
        # Calculate BMdec with different lags based on month (equivalent to Stata's conditional logic)
        data['BMdec'] = np.nan
        
        # For months >= 6, use 12-month lag (equivalent to Stata's "gen BMdec = tempBE/l12.tempDecME if month(dofm(time_avail_m)) >= 6")
        data.loc[data['time_avail_m'].dt.month >= 6, 'BMdec'] = (
            data.loc[data['time_avail_m'].dt.month >= 6, 'tempBE'] / 
            data.loc[data['time_avail_m'].dt.month >= 6].groupby('permno')['tempDecME'].shift(12)
        )
        
        # For months < 6, use 17-month lag (equivalent to Stata's "replace BMdec = tempBE/l17.tempDecME if month(dofm(time_avail_m)) < 6")
        data.loc[data['time_avail_m'].dt.month < 6, 'BMdec'] = (
            data.loc[data['time_avail_m'].dt.month < 6, 'tempBE'] / 
            data.loc[data['time_avail_m'].dt.month < 6].groupby('permno')['tempDecME'].shift(17)
        )
        
        logger.info("Successfully calculated BMdec signal")
        
        # SAVE RESULTS
        logger.info("Saving BMdec predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'BMdec']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['BMdec'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "BMdec.csv"
        csv_data = output_data[['permno', 'yyyymm', 'BMdec']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved BMdec predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed BMdec predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct BMdec predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    bmdec()
