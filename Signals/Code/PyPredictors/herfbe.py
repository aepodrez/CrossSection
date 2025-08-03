"""
Python equivalent of HerfBE.do
Generated from: HerfBE.do

Original Stata file: HerfBE.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def herfbe():
    """
    Python equivalent of HerfBE.do
    
    Constructs the HerfBE predictor signal for industry concentration (book equity-based Herfindahl index).
    """
    logger.info("Constructing predictor signal: HerfBE...")
    
    try:
        # DATA LOAD
        # Load Compustat annual data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading Compustat annual data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"Compustat annual file not found: {compustat_path}")
            logger.error("Please run the Compustat data download scripts first")
            return False
        
        # Load the required variables
        required_vars = ['permno', 'time_avail_m', 'txditc', 'pstk', 'pstkrv', 'pstkl', 'seq', 'ceq', 'at', 'lt']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # Merge with SignalMasterTable to get sicCRSP and shrcd
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load required variables from SignalMasterTable
        master_vars = ['permno', 'time_avail_m', 'sicCRSP', 'shrcd']
        master_data = pd.read_csv(master_path, usecols=master_vars)
        
        # Merge data (equivalent to Stata's "merge 1:1 permno time_avail_m using "$pathDataIntermediate/SignalMasterTable", keep(match) nogenerate keepusing(sicCRSP shrcd)")
        data = data.merge(
            master_data,
            on=['permno', 'time_avail_m'],
            how='inner'  # keep(match)
        )
        
        logger.info(f"After merging with SignalMasterTable: {len(data)} observations")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating HerfBE signal...")
        
        # Convert SIC to string and create 4-digit SIC (equivalent to Stata's "tostring sicCRSP, gen(tempSIC)" and "gen sic3D = substr(tempSIC,1, 4)")
        data['tempSIC'] = data['sicCRSP'].astype(str)
        data['sic3D'] = data['tempSIC'].str[:4]
        
        # Compute book equity (equivalent to Stata's book equity calculation)
        # Replace missing txditc with 0
        data['txditc'] = data['txditc'].fillna(0)
        
        # Create tempPS (preferred stock) with fallback logic
        data['tempPS'] = data['pstk']
        data.loc[data['tempPS'].isna(), 'tempPS'] = data.loc[data['tempPS'].isna(), 'pstkrv']
        data.loc[data['tempPS'].isna(), 'tempPS'] = data.loc[data['tempPS'].isna(), 'pstkl']
        
        # Create tempSE (stockholders' equity) with fallback logic
        data['tempSE'] = data['seq']
        data.loc[data['tempSE'].isna(), 'tempSE'] = data.loc[data['tempSE'].isna(), 'ceq'] + data.loc[data['tempSE'].isna(), 'tempPS']
        data.loc[data['tempSE'].isna(), 'tempSE'] = data.loc[data['tempSE'].isna(), 'at'] - data.loc[data['tempSE'].isna(), 'lt']
        
        # Calculate book equity
        data['tempBE'] = data['tempSE'] + data['txditc'] - data['tempPS']
        
        # Calculate industry total book equity (equivalent to Stata's "egen indequity = total(tempBE), by(sic3D time_avail_m)")
        data['indequity'] = data.groupby(['sic3D', 'time_avail_m'])['tempBE'].transform('sum')
        
        # Calculate squared market share based on book equity (equivalent to Stata's "gen temp = (tempBE/indequity)^2")
        data['temp'] = (data['tempBE'] / data['indequity']) ** 2
        
        # Calculate Herfindahl index (equivalent to Stata's "egen tempHerf = total(temp), by(sic3D time_avail_m)")
        data['tempHerf'] = data.groupby(['sic3D', 'time_avail_m'])['temp'].transform('sum')
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate 36-month moving average with minimum 12 observations (equivalent to Stata's "asrol tempHerf, gen(HerfBE) stat(mean) window(time_avail_m 36) min(12)")
        data['HerfBE'] = data.groupby('permno')['tempHerf'].rolling(
            window=36, min_periods=12
        ).mean().reset_index(0, drop=True)
        
        # Apply filters
        # Filter out non-common stocks (equivalent to Stata's "replace HerfBE = . if shrcd > 11")
        data.loc[data['shrcd'] > 11, 'HerfBE'] = np.nan
        
        # Create year variable (equivalent to Stata's "gen year = yofd(dofm(time_avail_m))")
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        data['year'] = data['time_avail_m'].dt.year
        
        # Apply regulated industry filters (equivalent to Stata's replace statements)
        # Airlines, trucking, railroads before 1980
        data.loc[(data['tempSIC'].isin(['4011', '4210', '4213'])) & (data['year'] <= 1980), 'HerfBE'] = np.nan
        
        # Airlines before 1978
        data.loc[(data['tempSIC'] == '4512') & (data['year'] <= 1978), 'HerfBE'] = np.nan
        
        # Telecommunications before 1982
        data.loc[(data['tempSIC'].isin(['4812', '4813'])) & (data['year'] <= 1982), 'HerfBE'] = np.nan
        
        # Utilities (SIC starting with 49)
        data.loc[data['tempSIC'].str.startswith('49'), 'HerfBE'] = np.nan
        
        logger.info("Successfully calculated HerfBE signal")
        
        # SAVE RESULTS
        logger.info("Saving HerfBE predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'HerfBE']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['HerfBE'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "HerfBE.csv"
        csv_data = output_data[['permno', 'yyyymm', 'HerfBE']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved HerfBE predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed HerfBE predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct HerfBE predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    herfbe()
