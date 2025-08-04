"""
Python equivalent of ZZ1_EBM_BPEBM.do
Generated from: ZZ1_EBM_BPEBM.do

Original Stata file: ZZ1_EBM_BPEBM.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def zz1_ebm_bpebm():
    """
    Python equivalent of ZZ1_EBM_BPEBM.do
    
    Constructs the EBM and BPEBM predictor signals for enterprise book-to-market and leverage components.
    """
    logger.info("Constructing predictor signals: EBM, BPEBM...")
    
    try:
        # DATA LOAD
        # Load annual Compustat data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading annual Compustat data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"m_aCompustat not found: {compustat_path}")
            logger.error("Please run the annual Compustat data creation script first")
            return False
        
        # Load required variables
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'che', 'dltt', 'dlc', 'dc', 'dvpa', 'tstkp', 'ceq']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # Merge with SignalMasterTable
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        master_data = pd.read_csv(master_path, usecols=['permno', 'time_avail_m', 'mve_c'])
        
        # Merge with SignalMasterTable (equivalent to Stata's "merge 1:1 permno time_avail_m using "$pathDataIntermediate/SignalMasterTable", keep(using match) nogenerate keepusing(mve_c)")
        data = data.merge(master_data, on=['permno', 'time_avail_m'], how='inner')
        logger.info(f"After merging with SignalMasterTable: {len(data)} records")
        
        # Sort data for time series operations (equivalent to Stata's "xtset permno time_avail_m")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating EBM and BPEBM signals...")
        
        # Calculate temporary variable (equivalent to Stata's "gen temp = che - dltt - dlc - dc - dvpa + tstkp")
        data['temp'] = data['che'] - data['dltt'] - data['dlc'] - data['dc'] - data['dvpa'] + data['tstkp']
        
        # Calculate EBM (equivalent to Stata's "gen EBM = (ceq + temp)/(mve_c + temp)")
        data['EBM'] = (data['ceq'] + data['temp']) / (data['mve_c'] + data['temp'])
        
        # Calculate BP (equivalent to Stata's "gen BP = (ceq + tstkp - dvpa)/mve_c")
        data['BP'] = (data['ceq'] + data['tstkp'] - data['dvpa']) / data['mve_c']
        
        # Calculate BPEBM (equivalent to Stata's "gen BPEBM = BP - EBM")
        data['BPEBM'] = data['BP'] - data['EBM']
        
        logger.info("Successfully calculated EBM and BPEBM signals")
        
        # SAVE RESULTS
        logger.info("Saving EBM and BPEBM predictor signals...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Save EBM
        ebm_data = data[['permno', 'time_avail_m', 'EBM']].copy()
        ebm_data = ebm_data.dropna(subset=['EBM'])
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(ebm_data['time_avail_m']):
            ebm_data['time_avail_m'] = pd.to_datetime(ebm_data['time_avail_m'])
        
        ebm_data['yyyymm'] = ebm_data['time_avail_m'].dt.year * 100 + ebm_data['time_avail_m'].dt.month
        csv_output_path = predictors_dir / "EBM.csv"
        ebm_data[['permno', 'yyyymm', 'EBM']].to_csv(csv_output_path, index=False)
        logger.info(f"Saved EBM predictor to: {csv_output_path}")
        
        # Save BPEBM
        bpebm_data = data[['permno', 'time_avail_m', 'BPEBM']].copy()
        bpebm_data = bpebm_data.dropna(subset=['BPEBM'])
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(bpebm_data['time_avail_m']):
            bpebm_data['time_avail_m'] = pd.to_datetime(bpebm_data['time_avail_m'])
        
        bpebm_data['yyyymm'] = bpebm_data['time_avail_m'].dt.year * 100 + bpebm_data['time_avail_m'].dt.month
        csv_output_path = predictors_dir / "BPEBM.csv"
        bpebm_data[['permno', 'yyyymm', 'BPEBM']].to_csv(csv_output_path, index=False)
        logger.info(f"Saved BPEBM predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed EBM and BPEBM predictor signals")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct EBM and BPEBM predictors: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    zz1_ebm_bpebm()
