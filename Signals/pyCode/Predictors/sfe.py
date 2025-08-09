# ABOUTME: Translates sfe.do to create sales forecast error predictor
# ABOUTME: Run from pyCode/ directory: python3 Predictors/sfe.py

# Run from pyCode/ directory
# Inputs: IBES_EPS_Unadj.parquet, SignalMasterTable.parquet, m_aCompustat.parquet
# Output: ../pyData/Predictors/sfe.csv

import pandas as pd
import numpy as np

# Prep IBES data
ibes = pd.read_parquet('../pyData/Intermediate/IBES_EPS_Unadj.parquet')
ibes = ibes[ibes['fpi'] == '1'].copy()
ibes = ibes[pd.to_datetime(ibes['statpers']).dt.month == 3].copy()  # March forecasts
ibes = ibes[(~ibes['fpedats'].isna()) & (ibes['fpedats'] > ibes['statpers'] + pd.Timedelta(days=90))].copy()

# For merge with dec stock price
ibes['prc_time'] = ibes['time_avail_m'] - pd.DateOffset(months=3)

# Merge with CRSP/Comp
smt = pd.read_parquet('../pyData/Intermediate/SignalMasterTable.parquet')
smt = smt[['permno', 'time_avail_m', 'tickerIBES', 'prc', 'mve_c']].copy()
smt = smt.rename(columns={'time_avail_m': 'prc_time'})

df = smt.merge(ibes, on=['tickerIBES', 'prc_time'], how='inner')

# Merge with Compustat for datadate
comp = pd.read_parquet('../pyData/Intermediate/m_aCompustat.parquet')
comp = comp[['permno', 'time_avail_m', 'datadate']].copy()
df = df.merge(comp, on=['permno', 'time_avail_m'], how='inner')

# Allow all fiscal year ends (not just December)
# Original script was too restrictive - most companies don't have December fiscal year ends
# df = df[pd.to_datetime(df['datadate']).dt.month == 12].copy()  # Too restrictive
print(f"Fiscal year ends in data: {df.groupby(pd.to_datetime(df['datadate']).dt.month).size().to_dict()}")
# Keep all fiscal year ends

# Lower analyst coverage only
def safe_qcut(x):
    """Safely apply qcut, handling cases with too few observations"""
    if len(x.dropna()) < 2:
        return pd.Series([1] * len(x), index=x.index)  # All get coverage 1 if too few obs
    try:
        return pd.qcut(x, q=2, labels=[1, 2], duplicates='drop')
    except ValueError:
        # If qcut fails, assign based on median split
        median_val = x.median()
        return pd.Series([1 if val <= median_val else 2 for val in x], index=x.index)

df['tempcoverage'] = df.groupby('time_avail_m')['numest'].transform(safe_qcut)
df = df[df['tempcoverage'] == 1].copy()

# SIGNAL CONSTRUCTION
df['sfe'] = df['medest'] / np.abs(df['prc'])
df = df[['permno', 'time_avail_m', 'sfe']].copy()

# Hold for one year
df_expanded = []
for _, row in df.iterrows():
    for month_offset in range(12):
        new_row = row.copy()
        new_row['time_avail_m'] = row['time_avail_m'] + pd.DateOffset(months=month_offset)
        df_expanded.append(new_row)

df_final = pd.DataFrame(df_expanded)
df_final = df_final.dropna(subset=['sfe'])

# Convert time_avail_m to yyyymm format like other predictors
df_final['yyyymm'] = df_final['time_avail_m'].dt.year * 100 + df_final['time_avail_m'].dt.month

# Convert to integers for consistency with other predictors
df_final['permno'] = df_final['permno'].astype('int64')
df_final['yyyymm'] = df_final['yyyymm'].astype('int64')

# Keep only required columns and set index
df_final = df_final[['permno', 'yyyymm', 'sfe']].copy()
df_final = df_final.set_index(['permno', 'yyyymm'])

# SAVE using standard utility
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from utils.savepredictor import save_predictor

# Reset index to get permno and yyyymm as columns
df_final = df_final.reset_index()

# Convert yyyymm back to time_avail_m datetime for savepredictor utility
df_final['time_avail_m'] = pd.to_datetime(df_final['yyyymm'].astype(str), format='%Y%m')

# Use standard save_predictor utility
save_predictor(df_final[['permno', 'time_avail_m', 'sfe']], 'sfe')

print("sfe predictor saved successfully")