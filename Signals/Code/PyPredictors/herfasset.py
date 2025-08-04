"""
Python equivalent of HerfAsset.do
Generated from: HerfAsset.do

Original Stata file: HerfAsset.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def herfasset():
    """
    Python equivalent of HerfAsset.do
    
    Constructs the HerfAsset predictor signal for industry concentration (asset-based Herfindahl index).
    """
    logger.info("Constructing predictor signal: HerfAsset...")
    
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
        required_vars = ['permno', 'time_avail_m', 'at']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
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
        logger.info("Calculating HerfAsset signal...")
        
        # Sort data for time series operations (equivalent to Stata's "xtset permno time_avail_m")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Convert SIC to string and create 4-digit SIC (equivalent to Stata's "tostring sicCRSP, gen(tempSIC)" and "gen sic3D = substr(tempSIC,1, 4)")
        data['tempSIC'] = data['sicCRSP'].astype(str)
        data['sic3D'] = data['tempSIC'].str[:4]
        
        # Calculate industry total assets (equivalent to Stata's "egen indasset = total(at), by(sic3D time_avail_m)")
        data['indasset'] = data.groupby(['sic3D', 'time_avail_m'])['at'].transform('sum')
        
        # Calculate squared market share based on assets (equivalent to Stata's "gen temp = (at/indasset)^2")
        data['temp'] = (data['at'] / data['indasset']) ** 2
        
        # Calculate Herfindahl index (equivalent to Stata's "egen tempHerf = total(temp), by(sic3D time_avail_m)")
        data['tempHerf'] = data.groupby(['sic3D', 'time_avail_m'])['temp'].transform('sum')
        
        # Calculate 36-month moving average with minimum 12 observations (equivalent to Stata's "asrol tempHerf, gen(HerfAsset) stat(mean) window(time_avail_m 36) min(12)")
        data['HerfAsset'] = data.groupby('permno')['tempHerf'].rolling(
            window=36, min_periods=12
        ).mean().reset_index(0, drop=True)
        
        # Apply filters
        # Filter out non-common stocks (equivalent to Stata's "replace HerfAsset = . if shrcd > 11")
        data.loc[data['shrcd'] > 11, 'HerfAsset'] = np.nan
        
        # Create year variable (equivalent to Stata's "gen year = year(dofm(time_avail_m))")
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        # Convert time_avail_m to datetime if needed for year extraction
        if not pd.api.types.is_datetime64_any_dtype(data['time_avail_m']):
            data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        
        data['year'] = data['time_avail_m'].dt.year
        
        # Apply regulated industry filters (equivalent to Stata's replace statements)
        # Airlines, trucking, railroads before 1980
        data.loc[(data['tempSIC'].isin(['4011', '4210', '4213'])) & (data['year'] <= 1980), 'HerfAsset'] = np.nan
        
        # Airlines before 1978
        data.loc[(data['tempSIC'] == '4512') & (data['year'] <= 1978), 'HerfAsset'] = np.nan
        
        # Telecommunications before 1982
        data.loc[(data['tempSIC'].isin(['4812', '4813'])) & (data['year'] <= 1982), 'HerfAsset'] = np.nan
        
        # Utilities (SIC starting with 49)
        data.loc[data['tempSIC'].str.startswith('49'), 'HerfAsset'] = np.nan
        
        logger.info("Successfully calculated HerfAsset signal")
        
        # SAVE RESULTS
        logger.info("Saving HerfAsset predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'HerfAsset']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['HerfAsset'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "HerfAsset.csv"
        csv_data = output_data[['permno', 'yyyymm', 'HerfAsset']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved HerfAsset predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed HerfAsset predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct HerfAsset predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    herfasset()
