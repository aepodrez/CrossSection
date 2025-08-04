"""
Python equivalent of BM.do
Generated from: BM.do

Original Stata file: BM.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def bm():
    """
    Python equivalent of BM.do
    
    Constructs the BM predictor signal for book-to-market ratio based on Stattman (1980).
    """
    logger.info("Constructing predictor signal: BM...")
    
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
        required_vars = ['permno', 'time_avail_m', 'datadate', 'ceqt']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Load SignalMasterTable for market value
        signal_master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {signal_master_path}")
        
        if not signal_master_path.exists():
            logger.error(f"SignalMasterTable not found: {signal_master_path}")
            return False
        
        signal_master = pd.read_csv(signal_master_path, usecols=['permno', 'time_avail_m', 'mve_c'])
        logger.info(f"Successfully loaded SignalMasterTable with {len(signal_master)} records")
        
        # Merge with SignalMasterTable
        data = data.merge(signal_master, on=['permno', 'time_avail_m'], how='inner')
        logger.info(f"After merge: {len(data)} records")
        
        # Sort by permno and time_avail_m for lag calculations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Convert time_avail_m and datadate to datetime
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        data['datadate'] = pd.to_datetime(data['datadate'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Constructing BM signal...")
        
        # Calculate 6-month lag of market equity (equivalent to Stata's "gen me_datadate = l6.mve_c")
        data['me_datadate'] = data.groupby('permno')['mve_c'].shift(6)
        
        # Convert datadate to month for comparison
        data['datadate_month'] = data['datadate'].dt.to_period('M')
        data['time_avail_m_month'] = data['time_avail_m'].dt.to_period('M')
        
        # Set to missing if 6-month lag doesn't match datadate (equivalent to Stata's "replace me_datadate = . if l6.time_avail_m != mofd(datadate)")
        data.loc[data['time_avail_m_month'] != data['datadate_month'], 'me_datadate'] = np.nan
        
        # Forward fill missing values within each permno (equivalent to Stata's "bys permno (time_avail_m): replace me_datadate = me_datadate[_n-1] if me_datadate == .")
        data[''me_datadate''] = data.groupby('permno')[''me_datadate''].ffill()
        
        # Calculate BM (equivalent to Stata's "gen BM = log(ceqt/me_datadate)")
        data['BM'] = np.log(data['ceqt'] / data['me_datadate'])
        
        logger.info("Successfully calculated BM signal")
        
        # SAVE RESULTS
        logger.info("Saving BM predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'BM']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['BM'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "BM.csv"
        csv_data = output_data[['permno', 'yyyymm', 'BM']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved BM predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed BM predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct BM predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    bm()
