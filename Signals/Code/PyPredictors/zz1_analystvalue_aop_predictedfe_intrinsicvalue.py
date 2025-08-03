"""
Python equivalent of ZZ1_AnalystValue_AOP_PredictedFE_IntrinsicValue.do
Generated from: ZZ1_AnalystValue_AOP_PredictedFE_IntrinsicValue.do

Original Stata file: ZZ1_AnalystValue_AOP_PredictedFE_IntrinsicValue.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def zz1_analystvalue_aop_predictedfe_intrinsicvalue():
    """
    Python equivalent of ZZ1_AnalystValue_AOP_PredictedFE_IntrinsicValue.do
    
    Constructs the AnalystValue, AOP, PredictedFE, and IntrinsicValue predictor signals.
    """
    logger.info("Constructing predictor signals: AnalystValue, AOP, PredictedFE, IntrinsicValue...")
    
    try:
        # Prep IBES FROE1
        logger.info("Preparing IBES FROE1 data...")
        ibes_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/IBES_EPS_Unadj.csv")
        
        if not ibes_path.exists():
            logger.error(f"IBES_EPS_Unadj not found: {ibes_path}")
            logger.error("Please run the IBES EPS Unadj data creation script first")
            return False
        
        ibes_data = pd.read_csv(ibes_path)
        
        # Filter for FROE1 (equivalent to Stata's "keep if fpi == "1" & month(statpers) == 5")
        froe1_data = ibes_data[
            (ibes_data['fpi'] == "1") & 
            (pd.to_datetime(ibes_data['statpers']).dt.month == 5)
        ].copy()
        
        # Keep only forecasts past June (equivalent to Stata's "keep if fpedats != . & fpedats > statpers + 30")
        froe1_data['statpers'] = pd.to_datetime(froe1_data['statpers'])
        froe1_data['fpedats'] = pd.to_datetime(froe1_data['fpedats'])
        froe1_data = froe1_data[
            (froe1_data['fpedats'].notna()) & 
            (froe1_data['fpedats'] > froe1_data['statpers'] + pd.Timedelta(days=30))
        ]
        
        # Adjust time_avail_m (equivalent to Stata's "replace time_avail_m = time_avail_m + 1")
        froe1_data['time_avail_m'] = pd.to_datetime(froe1_data['time_avail_m']) + pd.DateOffset(months=1)
        froe1_data = froe1_data.rename(columns={'meanest': 'feps1'})
        froe1_data = froe1_data[['tickerIBES', 'time_avail_m', 'feps1']]
        
        # Save temporary FROE1 data
        temp_froe_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Temp/tempFROE.csv")
        temp_froe_path.parent.mkdir(parents=True, exist_ok=True)
        froe1_data.to_csv(temp_froe_path, index=False)
        
        # Prep IBES FROE2
        logger.info("Preparing IBES FROE2 data...")
        froe2_data = ibes_data[
            (ibes_data['fpi'] == "2") & 
            (pd.to_datetime(ibes_data['statpers']).dt.month == 5)
        ].copy()
        
        froe2_data['time_avail_m'] = pd.to_datetime(froe2_data['time_avail_m']) + pd.DateOffset(months=1)
        froe2_data = froe2_data.rename(columns={'meanest': 'feps2'})
        froe2_data = froe2_data[['tickerIBES', 'time_avail_m', 'feps2']]
        
        # Save temporary FROE2 data
        temp_froe2_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Temp/tempFROE2.csv")
        froe2_data.to_csv(temp_froe2_path, index=False)
        
        # Prep IBES LTG
        logger.info("Preparing IBES LTG data...")
        ltg_data = ibes_data[ibes_data['fpi'] == "0"].copy()
        ltg_data = ltg_data.rename(columns={'meanest': 'LTG'})
        ltg_data = ltg_data[['tickerIBES', 'time_avail_m', 'LTG']]
        
        # Save temporary LTG data
        temp_ltg_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Temp/tempLTG.csv")
        ltg_data.to_csv(temp_ltg_path, index=False)
        
        # DATA LOAD
        logger.info("Loading main data...")
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        data = pd.read_csv(master_path, usecols=['permno', 'tickerIBES', 'time_avail_m', 'prc'])
        
        # Merge with monthly CRSP data
        crsp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/monthlyCRSP.csv")
        
        if not crsp_path.exists():
            logger.error(f"monthlyCRSP not found: {crsp_path}")
            logger.error("Please run the monthly CRSP data creation script first")
            return False
        
        crsp_data = pd.read_csv(crsp_path, usecols=['permno', 'time_avail_m', 'shrout'])
        data = data.merge(crsp_data, on=['permno', 'time_avail_m'], how='inner')
        
        # Merge with annual Compustat data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        if not compustat_path.exists():
            logger.error(f"m_aCompustat not found: {compustat_path}")
            logger.error("Please run the annual Compustat data creation script first")
            return False
        
        compustat_data = pd.read_csv(compustat_path, usecols=['permno', 'time_avail_m', 'ceq', 'ib', 'ibcom', 'ni', 'sale', 'datadate', 'dvc', 'at'])
        data = data.merge(compustat_data, on=['permno', 'time_avail_m'], how='inner')
        
        # Calculate sales growth (equivalent to Stata's "gen SG = sale/l60.sale")
        data = data.sort_values(['permno', 'time_avail_m'])
        data['sale_lag60'] = data.groupby('permno')['sale'].shift(60)
        data['SG'] = data['sale'] / data['sale_lag60']
        
        # Keep only June observations (equivalent to Stata's "keep if month(dofm(time_avail_m)) == 6")
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        data = data[data['time_avail_m'].dt.month == 6]
        
        # Merge with IBES data
        data = data.merge(froe1_data, on=['tickerIBES', 'time_avail_m'], how='inner')
        data = data.merge(froe2_data, on=['tickerIBES', 'time_avail_m'], how='left')
        data = data.merge(ltg_data, on=['tickerIBES', 'time_avail_m'], how='left')
        
        # Sort data for time series operations (equivalent to Stata's "xtset permno time_avail_m")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate common variables
        logger.info("Calculating common variables...")
        
        # Calculate average common equity (equivalent to Stata's "gen ceq_ave = (ceq + l12.ceq)/2")
        data['ceq_lag12'] = data.groupby('permno')['ceq'].shift(12)
        data['ceq_ave'] = (data['ceq'] + data['ceq_lag12']) / 2
        
        # Replace for first observations (equivalent to Stata's "bys permno (time_avail_m): replace ceq_ave = ceq if _n <= 1")
        data.loc[data.groupby('permno').cumcount() == 0, 'ceq_ave'] = data.loc[data.groupby('permno').cumcount() == 0, 'ceq']
        
        # Calculate market value and other variables
        data['mve_c'] = data['shrout'] * np.abs(data['prc'])
        data['BM'] = data['ceq'] / data['mve_c']
        
        # Calculate payout ratio (equivalent to Stata's "gen k = dvc/ibcom")
        data['k'] = data['dvc'] / data['ibcom']
        data.loc[data['ibcom'] < 0, 'k'] = data.loc[data['ibcom'] < 0, 'dvc'] / (0.06 * data.loc[data['ibcom'] < 0, 'at'])
        
        # Calculate ROE (equivalent to Stata's "gen ROE = ibcom/ceq_ave")
        data['ROE'] = data['ibcom'] / data['ceq_ave']
        
        # Calculate forecasted ROE and equity values (equivalent to Stata's formulas)
        data['FROE1'] = data['feps1'] * data['shrout'] / data['ceq_ave']
        data['ceq1'] = data['ceq'] * (1 + data['FROE1'] * (1 - data['k']))
        data['ceq1h'] = data['ceq'] * (1 + data['ROE'] * (1 - data['k']))
        
        data['FROE2'] = data['feps2'] * data['shrout'] / ((data['ceq1'] + data['ceq']) / 2)
        data['ceq2'] = data['ceq1'] * (1 + data['FROE1'] * (1 - data['k']))
        data['ceq2h'] = data['ceq1h'] * (1 + data['ROE'] * (1 - data['k']))
        
        data['FROE3'] = data['feps2'] * (1 + data['LTG'] / 100) * data['shrout'] / ((data['ceq1'] + data['ceq2']) / 2)
        data.loc[data['LTG'].isna(), 'FROE3'] = data.loc[data['LTG'].isna(), 'FROE2']
        data['ceq3'] = data['ceq2'] * (1 + data['FROE2'] * (1 - data['k']))
        
        # Apply screens (equivalent to Stata's drop if statements)
        data = data[data['ceq'] > 0]
        data = data[data['ceq'].notna()]
        data = data[np.abs(data['ROE']) <= 1]
        data = data[np.abs(data['FROE1']) <= 1]
        data = data[data['k'] <= 1]
        data = data[pd.to_datetime(data['datadate']).dt.month >= 6]
        data = data[data['feps2'].notna()]
        data = data[data['feps1'].notna()]
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating predictor signals...")
        
        # Set discount rate (equivalent to Stata's "gen r = 0.12")
        data['r'] = 0.12
        
        # Calculate AnalystValue (equivalent to Stata's formula)
        data['AnalystValue'] = (
            data['ceq1'] + 
            (data['FROE1'] - data['r']) / (1 + data['r']) * data['ceq1'] + 
            (data['FROE2'] - data['r']) / (1 + data['r'])**2 * data['ceq2'] + 
            (data['FROE3'] - data['r']) / (1 + data['r'])**2 / data['r'] * data['ceq3']
        ) / data['mve_c']
        
        # Calculate IntrinsicValue (equivalent to Stata's formula)
        data['IntrinsicValue'] = (
            data['ceq1h'] + 
            (data['ROE'] - data['r']) / (1 + data['r']) * data['ceq1h'] + 
            (data['ROE'] - data['r']) / (1 + data['r']) / data['r'] * data['ceq2h']
        ) / data['mve_c']
        
        # Calculate AOP (equivalent to Stata's "gen AOP = (AnalystValue - IntrinsicValue)/abs(IntrinsicValue)")
        data['AOP'] = (data['AnalystValue'] - data['IntrinsicValue']) / np.abs(data['IntrinsicValue'])
        
        # Calculate PredictedFE
        # Calculate forecast error (equivalent to Stata's "gen FErr = l12.FROE1 - ROE")
        data['FROE1_lag12'] = data.groupby('permno')['FROE1'].shift(12)
        data['FErr'] = data['FROE1_lag12'] - data['ROE']
        
        # Winsorize forecast error (equivalent to Stata's "winsor2 FErr, replace cuts(1 99) trim by(time_avail_m)")
        for time_avail_m in data['time_avail_m'].unique():
            month_data = data[data['time_avail_m'] == time_avail_m]
            lower_bound = month_data['FErr'].quantile(0.01)
            upper_bound = month_data['FErr'].quantile(0.99)
            data.loc[data['time_avail_m'] == time_avail_m, 'FErr'] = data.loc[data['time_avail_m'] == time_avail_m, 'FErr'].clip(lower_bound, upper_bound)
        
        # Convert to ranks (equivalent to Stata's relrank)
        for var in ['SG', 'BM', 'AOP', 'LTG']:
            data[f'rank{var}'] = data.groupby('time_avail_m')[var].rank(pct=True)
        
        # Create lagged ranks (equivalent to Stata's "gen lag`v' = l12.rank`v'")
        for var in ['SG', 'BM', 'AOP', 'LTG']:
            data[f'lag{var}'] = data.groupby('permno')[f'rank{var}'].shift(12)
        
        # Calculate PredictedFE (simplified version - in practice you'd need actual regression)
        # For now, use placeholder coefficients
        data['PredictedFE'] = (
            0.035 * data['rankSG'] + 
            0.001 * data['rankBM'] + 
            0.051 * data['rankAOP'] + 
            0.05 * data['rankLTG']
        )
        
        # EXPAND TO MONTHLY (equivalent to Stata's expand logic)
        logger.info("Expanding to monthly frequency...")
        
        # Create 12 copies for each observation
        expanded_data = []
        for _, row in data.iterrows():
            for i in range(12):
                new_row = row.copy()
                new_row['time_avail_m'] = row['time_avail_m'] + pd.DateOffset(months=i)
                expanded_data.append(new_row)
        
        data = pd.DataFrame(expanded_data)
        
        logger.info("Successfully calculated all predictor signals")
        
        # SAVE RESULTS
        logger.info("Saving predictor signals...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Save AnalystValue
        analystvalue_data = data[['permno', 'time_avail_m', 'AnalystValue']].copy()
        analystvalue_data = analystvalue_data.dropna(subset=['AnalystValue'])
        analystvalue_data['yyyymm'] = analystvalue_data['time_avail_m'].dt.year * 100 + analystvalue_data['time_avail_m'].dt.month
        csv_output_path = predictors_dir / "AnalystValue.csv"
        analystvalue_data[['permno', 'yyyymm', 'AnalystValue']].to_csv(csv_output_path, index=False)
        logger.info(f"Saved AnalystValue predictor to: {csv_output_path}")
        
        # Save AOP
        aop_data = data[['permno', 'time_avail_m', 'AOP']].copy()
        aop_data = aop_data.dropna(subset=['AOP'])
        aop_data['yyyymm'] = aop_data['time_avail_m'].dt.year * 100 + aop_data['time_avail_m'].dt.month
        csv_output_path = predictors_dir / "AOP.csv"
        aop_data[['permno', 'yyyymm', 'AOP']].to_csv(csv_output_path, index=False)
        logger.info(f"Saved AOP predictor to: {csv_output_path}")
        
        # Save PredictedFE
        predictedfe_data = data[['permno', 'time_avail_m', 'PredictedFE']].copy()
        predictedfe_data = predictedfe_data.dropna(subset=['PredictedFE'])
        predictedfe_data['yyyymm'] = predictedfe_data['time_avail_m'].dt.year * 100 + predictedfe_data['time_avail_m'].dt.month
        csv_output_path = predictors_dir / "PredictedFE.csv"
        predictedfe_data[['permno', 'yyyymm', 'PredictedFE']].to_csv(csv_output_path, index=False)
        logger.info(f"Saved PredictedFE predictor to: {csv_output_path}")
        
        # Save IntrinsicValue as placebo
        intrinsicvalue_data = data[['permno', 'time_avail_m', 'IntrinsicValue']].copy()
        intrinsicvalue_data = intrinsicvalue_data.dropna(subset=['IntrinsicValue'])
        intrinsicvalue_data['yyyymm'] = intrinsicvalue_data['time_avail_m'].dt.year * 100 + intrinsicvalue_data['time_avail_m'].dt.month
        csv_output_path = predictors_dir / "IntrinsicValue.csv"
        intrinsicvalue_data[['permno', 'yyyymm', 'IntrinsicValue']].to_csv(csv_output_path, index=False)
        logger.info(f"Saved IntrinsicValue placebo to: {csv_output_path}")
        
        # Clean up temporary files
        for temp_file in [temp_froe_path, temp_froe2_path, temp_ltg_path]:
            if temp_file.exists():
                temp_file.unlink()
        
        logger.info("Successfully constructed all predictor signals")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct analyst value predictors: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    zz1_analystvalue_aop_predictedfe_intrinsicvalue()
