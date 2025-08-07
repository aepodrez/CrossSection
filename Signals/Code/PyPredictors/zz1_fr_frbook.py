"""
Python equivalent of ZZ1_FR_FRbook.do
Generated from: ZZ1_FR_FRbook.do

Original Stata file: ZZ1_FR_FRbook.do
"""

import pandas as pd
import numpy as np
from pathlib import Path

def zz1_fr_frbook():
    """
    Python equivalent of ZZ1_FR_FRbook.do
    
    Constructs the FR and FRbook predictor signals for pension funding status.
    """
    # DATA LOAD
    # Load SignalMasterTable data
    master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
    
    # Load required variables (equivalent to Stata's "use permno gvkey time_avail_m shrcd mve_c using")
    data = pd.read_csv(master_path, usecols=['permno', 'gvkey', 'time_avail_m', 'shrcd', 'mve_c'])
    
    # Drop if gvkey is missing (equivalent to Stata's "drop if mi(gvkey)")
    data = data.dropna(subset=['gvkey'])
    
    # Generate year (equivalent to Stata's "gen year = yofd(dofm(time_avail_m))")
    data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
    data['year'] = data['time_avail_m'].dt.year
    
    # Merge with CompustatPensions data (equivalent to Stata's "merge m:1 gvkey year using")
    pensions_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/CompustatPensions.csv")
    pensions_data = pd.read_csv(pensions_path)
    data = data.merge(pensions_data, on=['gvkey', 'year'], how='inner')
    
    # Merge with annual Compustat data (equivalent to Stata's "merge 1:1 gvkey time_avail_m using")
    compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
    compustat_data = pd.read_csv(compustat_path, usecols=['gvkey', 'time_avail_m', 'at'])
    compustat_data['time_avail_m'] = pd.to_datetime(compustat_data['time_avail_m'])
    data = data.merge(compustat_data, on=['gvkey', 'time_avail_m'], how='inner')
    
    # SIGNAL CONSTRUCTION
    # Calculate FVPA (equivalent to Stata's gen/replace logic)
    data['FVPA'] = np.nan
    data.loc[(data['year'] >= 1980) & (data['year'] <= 1986), 'FVPA'] = data.loc[(data['year'] >= 1980) & (data['year'] <= 1986), 'pbnaa']
    data.loc[(data['year'] >= 1987) & (data['year'] <= 1997), 'FVPA'] = (
        data.loc[(data['year'] >= 1987) & (data['year'] <= 1997), 'pplao'] + 
        data.loc[(data['year'] >= 1987) & (data['year'] <= 1997), 'pplau']
    )
    data.loc[data['year'] >= 1998, 'FVPA'] = data.loc[data['year'] >= 1998, 'pplao']
    
    # Calculate PBO (equivalent to Stata's gen/replace logic)
    data['PBO'] = np.nan
    data.loc[(data['year'] >= 1980) & (data['year'] <= 1986), 'PBO'] = data.loc[(data['year'] >= 1980) & (data['year'] <= 1986), 'pbnvv']
    data.loc[(data['year'] >= 1987) & (data['year'] <= 1997), 'PBO'] = (
        data.loc[(data['year'] >= 1987) & (data['year'] <= 1997), 'pbpro'] + 
        data.loc[(data['year'] >= 1987) & (data['year'] <= 1997), 'pbpru']
    )
    data.loc[data['year'] >= 1998, 'PBO'] = data.loc[data['year'] >= 1998, 'pbpro']
    
    # Calculate FR (equivalent to Stata's "gen FR = (FVPA - PBO)/mve_c")
    data['FR'] = (data['FVPA'] - data['PBO']) / data['mve_c']
    
    # Set FR to missing for non-common stocks (equivalent to Stata's "replace FR = . if shrcd > 11")
    data.loc[data['shrcd'] > 11, 'FR'] = np.nan
    
    # Calculate FRbook (equivalent to Stata's "gen FRbook = (FVPA - PBO)/at")
    data['FRbook'] = (data['FVPA'] - data['PBO']) / data['at']
    
    # Set FRbook to missing for non-common stocks (equivalent to Stata's "replace FRbook = . if shrcd > 11")
    data.loc[data['shrcd'] > 11, 'FRbook'] = np.nan
    
    # SAVE RESULTS
    # Create output directories
    predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
    placebos_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Placebos")
    predictors_dir.mkdir(parents=True, exist_ok=True)
    placebos_dir.mkdir(parents=True, exist_ok=True)
    
    # Save FR (equivalent to "do savepredictor FR")
    fr_data = data[['permno', 'time_avail_m', 'FR']].copy()
    fr_data = fr_data.dropna(subset=['FR'])  # equivalent to "drop if FR == ."
    fr_data['yyyymm'] = fr_data['time_avail_m'].dt.year * 100 + fr_data['time_avail_m'].dt.month
    fr_data[['permno', 'yyyymm', 'FR']].to_csv(predictors_dir / "fr.csv", index=False)
    
    # Save FRbook (equivalent to "do saveplacebo FRbook")
    frbook_data = data[['permno', 'time_avail_m', 'FRbook']].copy()
    frbook_data = frbook_data.dropna(subset=['FRbook'])  # equivalent to "drop if FRbook == ."
    frbook_data['yyyymm'] = frbook_data['time_avail_m'].dt.year * 100 + frbook_data['time_avail_m'].dt.month
    frbook_data[['permno', 'yyyymm', 'FRbook']].to_csv(placebos_dir / "FRbook.csv", index=False)

if __name__ == "__main__":
    zz1_fr_frbook()
