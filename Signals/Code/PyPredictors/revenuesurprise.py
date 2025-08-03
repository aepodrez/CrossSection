"""
Python equivalent of RevenueSurprise.do
Generated from: RevenueSurprise.do

Original Stata file: RevenueSurprise.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def revenuesurprise():
    """
    Python equivalent of RevenueSurprise.do
    
    Constructs the RevenueSurprise predictor signal for revenue surprise.
    """
    logger.info("Constructing predictor signal: RevenueSurprise...")
    
    try:
        # DATA LOAD
        # Load SignalMasterTable data
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load required variables
        required_vars = ['permno', 'gvkey', 'time_avail_m', 'mve_c']
        
        data = pd.read_csv(master_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Keep only observations with non-missing gvkey (equivalent to Stata's "keep if !mi(gvkey)")
        data = data.dropna(subset=['gvkey'])
        logger.info(f"After dropping missing gvkey: {len(data)} records")
        
        # Load quarterly Compustat data
        qcompustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_QCompustat.csv")
        
        logger.info(f"Loading quarterly Compustat data from: {qcompustat_path}")
        
        if not qcompustat_path.exists():
            logger.error(f"m_QCompustat not found: {qcompustat_path}")
            logger.error("Please run the quarterly Compustat data creation script first")
            return False
        
        qcompustat_data = pd.read_csv(qcompustat_path, usecols=['gvkey', 'time_avail_m', 'revtq', 'cshprq'])
        
        # Merge with quarterly data
        data = data.merge(qcompustat_data, on=['gvkey', 'time_avail_m'], how='inner')
        logger.info(f"After merging with quarterly data: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating RevenueSurprise signal...")
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate revenue per share (equivalent to Stata's "gen revps = revtq/cshprq")
        data['revps'] = data['revtq'] / data['cshprq']
        
        # Calculate 12-month lag of revenue per share (equivalent to Stata's "l12.revps")
        data['revps_lag12'] = data.groupby('permno')['revps'].shift(12)
        
        # Calculate growth (equivalent to Stata's "gen GrTemp = (revps - l12.revps)")
        data['GrTemp'] = data['revps'] - data['revps_lag12']
        
        # Calculate lags of GrTemp for drift calculation (equivalent to Stata's foreach loop)
        temp_columns = []
        for n in range(3, 25, 3):  # 3, 6, 9, 12, 15, 18, 21, 24
            data[f'temp{n}'] = data.groupby('permno')['GrTemp'].shift(n)
            temp_columns.append(f'temp{n}')
        
        # Calculate drift as row mean (equivalent to Stata's "egen Drift = rowmean(temp*)")
        data['Drift'] = data[temp_columns].mean(axis=1)
        
        # Calculate RevenueSurprise (equivalent to Stata's "gen RevenueSurprise = revps - l12.revps - Drift")
        data['RevenueSurprise'] = data['revps'] - data['revps_lag12'] - data['Drift']
        
        # Calculate lags of RevenueSurprise for standard deviation (equivalent to Stata's second foreach loop)
        temp_sd_columns = []
        for n in range(3, 25, 3):  # 3, 6, 9, 12, 15, 18, 21, 24
            data[f'temp{n}'] = data.groupby('permno')['RevenueSurprise'].shift(n)
            temp_sd_columns.append(f'temp{n}')
        
        # Calculate standard deviation (equivalent to Stata's "egen SD = rowsd(temp*)")
        data['SD'] = data[temp_sd_columns].std(axis=1)
        
        # Standardize RevenueSurprise (equivalent to Stata's "replace RevenueSurprise = RevenueSurprise/SD")
        data['RevenueSurprise'] = data['RevenueSurprise'] / data['SD']
        
        logger.info("Successfully calculated RevenueSurprise signal")
        
        # SAVE RESULTS
        logger.info("Saving RevenueSurprise predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'RevenueSurprise']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['RevenueSurprise'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "RevenueSurprise.csv"
        csv_data = output_data[['permno', 'yyyymm', 'RevenueSurprise']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved RevenueSurprise predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed RevenueSurprise predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct RevenueSurprise predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    revenuesurprise()
