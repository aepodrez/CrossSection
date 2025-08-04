"""
Python equivalent of OScore.do
Generated from: OScore.do

Original Stata file: OScore.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def oscore():
    """
    Python equivalent of OScore.do
    
    Constructs the OScore predictor signal for Ohlson O-Score.
    """
    logger.info("Constructing predictor signal: OScore...")
    
    try:
        # DATA LOAD
        # Load Compustat data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading Compustat data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"m_aCompustat not found: {compustat_path}")
            logger.error("Please run the Compustat data creation script first")
            return False
        
        # Load required variables
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'fopt', 'at', 'lt', 'act', 'lct', 'ib', 'oancf', 'sic']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'])
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # Load SignalMasterTable for price
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        master_data = pd.read_csv(master_path, usecols=['permno', 'time_avail_m', 'prc'])
        
        # Merge with SignalMasterTable
        data = data.merge(master_data, on=['permno', 'time_avail_m'], how='inner')
        logger.info(f"After merging with SignalMasterTable: {len(data)} records")
        
        # Load GNP deflator data
        gnp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/GNPdefl.csv")
        
        logger.info(f"Loading GNP deflator from: {gnp_path}")
        
        if not gnp_path.exists():
            logger.error(f"GNPdefl not found: {gnp_path}")
            logger.error("Please run the GNP deflator creation script first")
            return False
        
        gnp_data = pd.read_csv(gnp_path, usecols=['time_avail_m', 'gnpdefl'])
        
        # Merge with GNP deflator
        data = data.merge(gnp_data, on='time_avail_m', how='inner')
        logger.info(f"After merging with GNP deflator: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating OScore signal...")
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Replace missing fopt with oancf (equivalent to Stata's "replace fopt = oancf if fopt == .")
        data['fopt'] = data['fopt'].fillna(data['oancf'])
        
        # Calculate 12-month lag of ib (equivalent to Stata's "l12.ib")
        data['ib_lag12'] = data.groupby('permno')['ib'].shift(12)
        
        # Calculate OScore (equivalent to Stata's complex formula)
        data['OScore'] = (-1.32 
                         - 0.407 * np.log(data['at'] / data['gnpdefl'])
                         + 6.03 * (data['lt'] / data['at'])
                         - 1.43 * ((data['act'] - data['lct']) / data['at'])
                         + 0.076 * (data['lct'] / data['act'])
                         - 1.72 * (data['lt'] > data['at']).astype(int)
                         - 2.37 * (data['ib'] / data['at'])
                         - 1.83 * (data['fopt'] / data['lt'])
                         + 0.285 * ((data['ib'] + data['ib_lag12']) < 0).astype(int)
                         - 0.521 * ((data['ib'] - data['ib_lag12']) / (abs(data['ib']) + abs(data['ib_lag12']))))
        
        # Convert SIC to numeric (equivalent to Stata's "destring sic, replace")
        data['sic'] = pd.to_numeric(data['sic'], errors='coerce')
        
        # Exclude certain industries (equivalent to Stata's "replace OScore = . if (sic > 3999 & sic < 5000) | sic > 5999")
        data.loc[((data['sic'] > 3999) & (data['sic'] < 5000)) | (data['sic'] > 5999), 'OScore'] = np.nan
        
        # Create deciles (equivalent to Stata's "egen tempsort = fastxtile(OScore), by(time_avail_m) n(10)")
        data['tempsort'] = data.groupby('time_avail_m')['OScore'].transform(
            lambda x: pd.qcut(x, q=10, labels=False, duplicates='drop') + 1
        )
        
        # Create binary signal (equivalent to Stata's logic)
        # Replace OScore with missing
        data['OScore_original'] = data['OScore']
        data['OScore'] = np.nan
        
        # Set OScore = 1 if tempsort == 10 (top decile)
        data.loc[data['tempsort'] == 10, 'OScore'] = 1
        
        # Set OScore = 0 if tempsort <= 7 (bottom 7 deciles)
        data.loc[(data['tempsort'] >= 1) & (data['tempsort'] <= 7), 'OScore'] = 0
        
        logger.info("Successfully calculated OScore signal")
        
        # SAVE RESULTS
        logger.info("Saving OScore predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'OScore']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['OScore'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "OScore.csv"
        csv_data = output_data[['permno', 'yyyymm', 'OScore']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved OScore predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed OScore predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct OScore predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    oscore()
