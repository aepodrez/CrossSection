"""
Python equivalent of OScore_q.do
Generated from: OScore_q.do

Original Stata file: OScore_q.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def oscore_q():
    """
    Python equivalent of OScore_q.do
    
    Constructs the OScore_q predictor signal for Ohlson O-Score (quarterly).
    """
    logger.info("Constructing predictor signal: OScore_q...")
    
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
        required_vars = ['permno', 'gvkey', 'time_avail_m', 'sicCRSP', 'prc']
        
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
        
        qcompustat_data = pd.read_csv(qcompustat_path, usecols=['gvkey', 'time_avail_m', 'foptyq', 'atq', 'ltq', 'actq', 'lctq', 'ibq', 'oancfyq'])
        
        # Merge with quarterly data
        data = data.merge(qcompustat_data, on=['gvkey', 'time_avail_m'], how='inner')
        logger.info(f"After merging with quarterly data: {len(data)} records")
        
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
        logger.info("Calculating OScore_q signal...")
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Replace missing foptyq with oancfyq (equivalent to Stata's "replace foptyq = oancfyq if foptyq == .")
        data['foptyq'] = data['foptyq'].fillna(data['oancfyq'])
        
        # Calculate 12-month lag of ibq (equivalent to Stata's "l12.ibq")
        data['ibq_lag12'] = data.groupby('permno')['ibq'].shift(12)
        
        # Calculate OScore_q (equivalent to Stata's complex formula)
        data['OScore_q'] = (-1.32 
                           - 0.407 * np.log(data['atq'] / data['gnpdefl'])
                           + 6.03 * (data['ltq'] / data['atq'])
                           - 1.43 * ((data['actq'] - data['lctq']) / data['atq'])
                           + 0.076 * (data['lctq'] / data['actq'])
                           - 1.72 * (data['ltq'] > data['atq']).astype(int)
                           - 2.37 * (data['ibq'] / data['atq'])
                           - 1.83 * (data['foptyq'] / data['ltq'])
                           + 0.285 * ((data['ibq'] + data['ibq_lag12']) < 0).astype(int)
                           - 0.521 * ((data['ibq'] - data['ibq_lag12']) / (abs(data['ibq']) + abs(data['ibq_lag12']))))
        
        # Convert SIC to numeric (equivalent to Stata's "destring sic, replace")
        data['sicCRSP'] = pd.to_numeric(data['sicCRSP'], errors='coerce')
        
        # Exclude certain industries (equivalent to Stata's "replace OScore_q = . if (sicCRSP > 3999 & sicCRSP < 5000) | sicCRSP > 5999")
        data.loc[((data['sicCRSP'] > 3999) & (data['sicCRSP'] < 5000)) | (data['sicCRSP'] > 5999), 'OScore_q'] = np.nan
        
        # Create deciles (equivalent to Stata's "egen tempsort = fastxtile(OScore), by(time_avail_m) n(10)")
        data['tempsort'] = data.groupby('time_avail_m')['OScore_q'].transform(
            lambda x: pd.qcut(x, q=10, labels=False, duplicates='drop') + 1
        )
        
        # Create binary signal (equivalent to Stata's logic)
        # Replace OScore_q with missing
        data['OScore_q_original'] = data['OScore_q']
        data['OScore_q'] = np.nan
        
        # Set OScore_q = 1 if tempsort == 10 (top decile)
        data.loc[data['tempsort'] == 10, 'OScore_q'] = 1
        
        # Set OScore_q = 0 if tempsort <= 7 (bottom 7 deciles)
        data.loc[(data['tempsort'] >= 1) & (data['tempsort'] <= 7), 'OScore_q'] = 0
        
        logger.info("Successfully calculated OScore_q signal")
        
        # SAVE RESULTS
        logger.info("Saving OScore_q predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'OScore_q']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['OScore_q'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "OScore_q.csv"
        csv_data = output_data[['permno', 'yyyymm', 'OScore_q']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved OScore_q predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed OScore_q predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct OScore_q predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    oscore_q()
