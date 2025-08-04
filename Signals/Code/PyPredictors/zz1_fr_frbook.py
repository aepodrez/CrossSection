"""
Python equivalent of ZZ1_FR_FRbook.do
Generated from: ZZ1_FR_FRbook.do

Original Stata file: ZZ1_FR_FRbook.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def zz1_fr_frbook():
    """
    Python equivalent of ZZ1_FR_FRbook.do
    
    Constructs the FR and FRbook predictor signals for pension funding status.
    """
    logger.info("Constructing predictor signals: FR, FRbook...")
    
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
        required_vars = ['permno', 'gvkey', 'time_avail_m', 'shrcd', 'mve_c']
        
        data = pd.read_csv(master_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Drop if gvkey is missing (equivalent to Stata's "drop if mi(gvkey)")
        data = data.dropna(subset=['gvkey'])
        logger.info(f"After dropping missing gvkey: {len(data)} records")
        
        # Generate year (equivalent to Stata's "gen year = yofd(dofm(time_avail_m))")
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        # Convert time_avail_m to datetime if needed for year extraction
        if not pd.api.types.is_datetime64_any_dtype(data['time_avail_m']):
            data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        
        data['year'] = data['time_avail_m'].dt.year
        
        # Merge with CompustatPensions data
        pensions_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/CompustatPensions.csv")
        
        logger.info(f"Loading CompustatPensions data from: {pensions_path}")
        
        if not pensions_path.exists():
            logger.error(f"CompustatPensions not found: {pensions_path}")
            logger.error("Please run the CompustatPensions data creation script first")
            return False
        
        pensions_data = pd.read_csv(pensions_path)
        
        # Merge with CompustatPensions data (equivalent to Stata's "merge m:1 gvkey year using "$pathDataIntermediate/CompustatPensions", keep(match) nogenerate")
        data = data.merge(pensions_data, on=['gvkey', 'year'], how='inner')
        logger.info(f"After merging with CompustatPensions data: {len(data)} records")
        
        # Merge with annual Compustat data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading annual Compustat data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"m_aCompustat not found: {compustat_path}")
            logger.error("Please run the annual Compustat data creation script first")
            return False
        
        compustat_data = pd.read_csv(compustat_path, usecols=['gvkey', 'time_avail_m', 'at'])
        
        # Merge with annual Compustat data (equivalent to Stata's "merge 1:1 gvkey time_avail_m using "$pathDataIntermediate/m_aCompustat", keep(master match) nogenerate keepusing(at)")
        data = data.merge(compustat_data, on=['gvkey', 'time_avail_m'], how='inner')
        logger.info(f"After merging with annual Compustat data: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating FR and FRbook signals...")
        
        # Calculate FVPA (Fair Value of Plan Assets) based on year ranges
        data['FVPA'] = np.nan
        
        # 1980-1986: FVPA = pbnaa
        data.loc[(data['year'] >= 1980) & (data['year'] <= 1986), 'FVPA'] = data.loc[(data['year'] >= 1980) & (data['year'] <= 1986), 'pbnaa']
        
        # 1987-1997: FVPA = pplao + pplau
        data.loc[(data['year'] >= 1987) & (data['year'] <= 1997), 'FVPA'] = (
            data.loc[(data['year'] >= 1987) & (data['year'] <= 1997), 'pplao'] + 
            data.loc[(data['year'] >= 1987) & (data['year'] <= 1997), 'pplau']
        )
        
        # 1998+: FVPA = pplao
        data.loc[data['year'] >= 1998, 'FVPA'] = data.loc[data['year'] >= 1998, 'pplao']
        
        # Calculate PBO (Projected Benefit Obligation) based on year ranges
        data['PBO'] = np.nan
        
        # 1980-1986: PBO = pbnvv
        data.loc[(data['year'] >= 1980) & (data['year'] <= 1986), 'PBO'] = data.loc[(data['year'] >= 1980) & (data['year'] <= 1986), 'pbnvv']
        
        # 1987-1997: PBO = pbpro + pbpru
        data.loc[(data['year'] >= 1987) & (data['year'] <= 1997), 'PBO'] = (
            data.loc[(data['year'] >= 1987) & (data['year'] <= 1997), 'pbpro'] + 
            data.loc[(data['year'] >= 1987) & (data['year'] <= 1997), 'pbpru']
        )
        
        # 1998+: PBO = pbpro
        data.loc[data['year'] >= 1998, 'PBO'] = data.loc[data['year'] >= 1998, 'pbpro']
        
        # Calculate FR (equivalent to Stata's "gen FR = (FVPA - PBO)/mve_c")
        data['FR'] = (data['FVPA'] - data['PBO']) / data['mve_c']
        
        # Set FR to missing for non-common stocks (equivalent to Stata's "replace FR = . if shrcd > 11")
        data.loc[data['shrcd'] > 11, 'FR'] = np.nan
        
        # Calculate FRbook (equivalent to Stata's "gen FRbook = (FVPA - PBO)/at")
        data['FRbook'] = (data['FVPA'] - data['PBO']) / data['at']
        
        # Set FRbook to missing for non-common stocks (equivalent to Stata's "replace FRbook = . if shrcd > 11")
        data.loc[data['shrcd'] > 11, 'FRbook'] = np.nan
        
        logger.info("Successfully calculated FR and FRbook signals")
        
        # SAVE RESULTS
        logger.info("Saving FR and FRbook predictor signals...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Save FR
        fr_data = data[['permno', 'time_avail_m', 'FR']].copy()
        fr_data = fr_data.dropna(subset=['FR'])
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(fr_data['time_avail_m']):
            fr_data['time_avail_m'] = pd.to_datetime(fr_data['time_avail_m'])
        
        fr_data['yyyymm'] = fr_data['time_avail_m'].dt.year * 100 + fr_data['time_avail_m'].dt.month
        csv_output_path = predictors_dir / "FR.csv"
        fr_data[['permno', 'yyyymm', 'FR']].to_csv(csv_output_path, index=False)
        logger.info(f"Saved FR predictor to: {csv_output_path}")
        
        # Save FRbook
        frbook_data = data[['permno', 'time_avail_m', 'FRbook']].copy()
        frbook_data = frbook_data.dropna(subset=['FRbook'])
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(frbook_data['time_avail_m']):
            frbook_data['time_avail_m'] = pd.to_datetime(frbook_data['time_avail_m'])
        
        frbook_data['yyyymm'] = frbook_data['time_avail_m'].dt.year * 100 + frbook_data['time_avail_m'].dt.month
        csv_output_path = predictors_dir / "FRbook.csv"
        frbook_data[['permno', 'yyyymm', 'FRbook']].to_csv(csv_output_path, index=False)
        logger.info(f"Saved FRbook predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed FR and FRbook predictor signals")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct FR and FRbook predictors: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    zz1_fr_frbook()
