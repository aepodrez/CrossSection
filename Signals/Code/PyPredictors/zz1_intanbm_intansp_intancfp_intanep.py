"""
ZZ1_IntanBM_IntanSP_IntanCFP_IntanEP Predictor Implementation

This script implements four intangible return predictors:
- IntanBM: Intangible return (Book-to-Market)
- IntanSP: Intangible return (Sales-to-Price)
- IntanCFP: Intangible return (Cash Flow-to-Price)
- IntanEP: Intangible return (Earnings-to-Price)

The script calculates these measures by decomposing returns into tangible and intangible components
using cross-sectional regressions on fundamental ratios.
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path
from scipy import stats

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def zz1_intanbm_intansp_intancfp_intanep():
    """Main function to calculate IntanBM, IntanSP, IntanCFP, and IntanEP predictors."""
    
    # Define file paths
    base_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data")
    compustat_path = base_path / "Intermediate" / "m_aCompustat.csv"
    master_path = base_path / "Intermediate" / "SignalMasterTable.csv"
    output_path = base_path / "Predictors"
    
    # Ensure output directory exists
    output_path.mkdir(parents=True, exist_ok=True)
    
    logger.info("Starting IntanBM, IntanSP, IntanCFP, IntanEP predictor calculation")
    
    try:
        # DATA LOAD
        logger.info("Loading Compustat data")
        required_vars = ['permno', 'gvkey', 'time_avail_m', 'sale', 'ib', 'dp', 'ni', 'ceq']
        data = pd.read_csv(compustat_path, usecols=required_vars)
        
        # Remove duplicates
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        
        # Merge with SignalMasterTable for ret and mve_c
        logger.info("Merging with SignalMasterTable")
        master_data = pd.read_csv(master_path, usecols=['permno', 'time_avail_m', 'ret', 'mve_c'])
        data = data.merge(master_data, on=['permno', 'time_avail_m'], how='inner')
        
        # Convert time_avail_m to datetime
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        
        # Sort by permno and time_avail_m
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating fundamental ratios")
        
        # Calculate fundamental ratios
        data['tempAccBM'] = np.log(data['ceq'] / data['mve_c'])
        data['tempAccSP'] = data['sale'] / data['mve_c']
        data['tempAccCFP'] = (data['ib'] + data['dp']) / data['mve_c']
        data['tempAccEP'] = data['ni'] / data['mve_c']
        
        # Handle missing returns
        data['ret'] = data['ret'].fillna(0)
        
        # Calculate cumulative returns
        logger.info("Calculating cumulative returns")
        data['log_ret'] = np.log(1 + data['ret'])
        data['cumsum_log_ret'] = data.groupby('permno')['log_ret'].cumsum()
        data['tempCumRet'] = np.exp(data['cumsum_log_ret'])
        
        # Calculate 60-month return
        data['tempCumRet_lag60'] = data.groupby('permno')['tempCumRet'].shift(60)
        data['tempRet60'] = (data['tempCumRet'] - data['tempCumRet_lag60']) / data['tempCumRet_lag60']
        
        # Winsorize tempRet60
        lower_bound = data['tempRet60'].quantile(0.01)
        upper_bound = data['tempRet60'].quantile(0.99)
        data.loc[data['tempRet60'] < lower_bound, 'tempRet60'] = lower_bound
        data.loc[data['tempRet60'] > upper_bound, 'tempRet60'] = upper_bound
        
        # Calculate lagged fundamental ratios
        logger.info("Calculating lagged fundamental ratios")
        data['tempAccBM_lag60'] = data.groupby('permno')['tempAccBM'].shift(60)
        data['tempAccSP_lag60'] = data.groupby('permno')['tempAccSP'].shift(60)
        data['tempAccCFP_lag60'] = data.groupby('permno')['tempAccCFP'].shift(60)
        data['tempAccEP_lag60'] = data.groupby('permno')['tempAccEP'].shift(60)
        
        # Calculate return-adjusted fundamental ratios
        data['tempAccBMRet'] = data['tempAccBM'] - data['tempAccBM_lag60'] + data['tempRet60']
        data['tempAccSPRet'] = data['tempAccSP'] - data['tempAccSP_lag60'] + data['tempRet60']
        data['tempAccCFPRet'] = data['tempAccCFP'] - data['tempAccCFP_lag60'] + data['tempRet60']
        data['tempAccEPRet'] = data['tempAccEP'] - data['tempAccEP_lag60'] + data['tempRet60']
        
        # Initialize intangible return measures
        data['IntanBM'] = np.nan
        data['IntanSP'] = np.nan
        data['IntanCFP'] = np.nan
        data['IntanEP'] = np.nan
        
        # Run cross-sectional regressions for each time period
        logger.info("Running cross-sectional regressions")
        for time_period in data['time_avail_m'].unique():
            period_data = data[data['time_avail_m'] == time_period].copy()
            
            # Filter out missing values for regression
            valid_data = period_data.dropna(subset=['tempRet60', 'tempAccBM_lag60', 'tempAccBMRet'])
            if len(valid_data) > 10:  # Need sufficient observations
                try:
                    # IntanBM regression
                    X = np.column_stack([np.ones(len(valid_data)), 
                                       valid_data['tempAccBM_lag60'].values,
                                       valid_data['tempAccBMRet'].values])
                    y = valid_data['tempRet60'].values
                    beta = np.linalg.lstsq(X, y, rcond=None)[0]
                    residuals = y - X @ beta
                    data.loc[valid_data.index, 'IntanBM'] = residuals
                except:
                    pass
            
            # IntanSP regression
            valid_data = period_data.dropna(subset=['tempRet60', 'tempAccSP_lag60', 'tempAccSPRet'])
            if len(valid_data) > 10:
                try:
                    X = np.column_stack([np.ones(len(valid_data)), 
                                       valid_data['tempAccSP_lag60'].values,
                                       valid_data['tempAccSPRet'].values])
                    y = valid_data['tempRet60'].values
                    beta = np.linalg.lstsq(X, y, rcond=None)[0]
                    residuals = y - X @ beta
                    data.loc[valid_data.index, 'IntanSP'] = residuals
                except:
                    pass
            
            # IntanCFP regression
            valid_data = period_data.dropna(subset=['tempRet60', 'tempAccCFP_lag60', 'tempAccCFPRet'])
            if len(valid_data) > 10:
                try:
                    X = np.column_stack([np.ones(len(valid_data)), 
                                       valid_data['tempAccCFP_lag60'].values,
                                       valid_data['tempAccCFPRet'].values])
                    y = valid_data['tempRet60'].values
                    beta = np.linalg.lstsq(X, y, rcond=None)[0]
                    residuals = y - X @ beta
                    data.loc[valid_data.index, 'IntanCFP'] = residuals
                except:
                    pass
            
            # IntanEP regression
            valid_data = period_data.dropna(subset=['tempRet60', 'tempAccEP_lag60', 'tempAccEPRet'])
            if len(valid_data) > 10:
                try:
                    X = np.column_stack([np.ones(len(valid_data)), 
                                       valid_data['tempAccEP_lag60'].values,
                                       valid_data['tempAccEPRet'].values])
                    y = valid_data['tempRet60'].values
                    beta = np.linalg.lstsq(X, y, rcond=None)[0]
                    residuals = y - X @ beta
                    data.loc[valid_data.index, 'IntanEP'] = residuals
                except:
                    pass
        
        # Prepare output data
        logger.info("Preparing output data")
        
        # For IntanBM
        intanbm_output = data[['permno', 'time_avail_m', 'IntanBM']].copy()
        intanbm_output = intanbm_output.dropna(subset=['IntanBM'])
        # Convert time_avail_m to datetime if needed for strftime
        if not pd.api.types.is_datetime64_any_dtype(intanbm_output['time_avail_m']):
            intanbm_output['time_avail_m'] = pd.to_datetime(intanbm_output['time_avail_m'])
        
        intanbm_output['yyyymm'] = intanbm_output['time_avail_m'].dt.strftime('%Y%m').astype(int)
        intanbm_output = intanbm_output[['permno', 'yyyymm', 'IntanBM']]
        
        # For IntanSP
        intansp_output = data[['permno', 'time_avail_m', 'IntanSP']].copy()
        intansp_output = intansp_output.dropna(subset=['IntanSP'])
        # Convert time_avail_m to datetime if needed for strftime
        if not pd.api.types.is_datetime64_any_dtype(intansp_output['time_avail_m']):
            intansp_output['time_avail_m'] = pd.to_datetime(intansp_output['time_avail_m'])
        
        intansp_output['yyyymm'] = intansp_output['time_avail_m'].dt.strftime('%Y%m').astype(int)
        intansp_output = intansp_output[['permno', 'yyyymm', 'IntanSP']]
        
        # For IntanCFP
        intancfp_output = data[['permno', 'time_avail_m', 'IntanCFP']].copy()
        intancfp_output = intancfp_output.dropna(subset=['IntanCFP'])
        # Convert time_avail_m to datetime if needed for strftime
        if not pd.api.types.is_datetime64_any_dtype(intancfp_output['time_avail_m']):
            intancfp_output['time_avail_m'] = pd.to_datetime(intancfp_output['time_avail_m'])
        
        intancfp_output['yyyymm'] = intancfp_output['time_avail_m'].dt.strftime('%Y%m').astype(int)
        intancfp_output = intancfp_output[['permno', 'yyyymm', 'IntanCFP']]
        
        # For IntanEP
        intanep_output = data[['permno', 'time_avail_m', 'IntanEP']].copy()
        intanep_output = intanep_output.dropna(subset=['IntanEP'])
        # Convert time_avail_m to datetime if needed for strftime
        if not pd.api.types.is_datetime64_any_dtype(intanep_output['time_avail_m']):
            intanep_output['time_avail_m'] = pd.to_datetime(intanep_output['time_avail_m'])
        
        intanep_output['yyyymm'] = intanep_output['time_avail_m'].dt.strftime('%Y%m').astype(int)
        intanep_output = intanep_output[['permno', 'yyyymm', 'IntanEP']]
        
        # Save results
        logger.info("Saving results")
        
        # Save IntanBM
        intanbm_file = output_path / "IntanBM.csv"
        intanbm_output.to_csv(intanbm_file, index=False)
        logger.info(f"Saved IntanBM predictor to {intanbm_file}")
        logger.info(f"IntanBM: {len(intanbm_output)} observations")
        
        # Save IntanSP
        intansp_file = output_path / "IntanSP.csv"
        intansp_output.to_csv(intansp_file, index=False)
        logger.info(f"Saved IntanSP predictor to {intansp_file}")
        logger.info(f"IntanSP: {len(intansp_output)} observations")
        
        # Save IntanCFP
        intancfp_file = output_path / "IntanCFP.csv"
        intancfp_output.to_csv(intancfp_file, index=False)
        logger.info(f"Saved IntanCFP predictor to {intancfp_file}")
        logger.info(f"IntanCFP: {len(intancfp_output)} observations")
        
        # Save IntanEP
        intanep_file = output_path / "IntanEP.csv"
        intanep_output.to_csv(intanep_file, index=False)
        logger.info(f"Saved IntanEP predictor to {intanep_file}")
        logger.info(f"IntanEP: {len(intanep_output)} observations")
        
        logger.info("Successfully completed IntanBM, IntanSP, IntanCFP, IntanEP predictor calculation")
        
    except Exception as e:
        logger.error(f"Error in IntanBM, IntanSP, IntanCFP, IntanEP calculation: {str(e)}")
        raise

if __name__ == "__main__":
    main()
