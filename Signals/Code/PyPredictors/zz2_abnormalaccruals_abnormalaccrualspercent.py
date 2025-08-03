"""
ZZ2_AbnormalAccruals_AbnormalAccrualsPercent Predictor Implementation

This script implements two abnormal accruals predictors based on Xie (2001):
- AbnormalAccruals: Abnormal accruals measure
- AbnormalAccrualsPercent: Abnormal accruals as a percentage of net income

The script calculates abnormal accruals using the modified Jones model with industry-year
regressions and provides both raw and percentage-based measures.
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def zz2_abnormalaccruals_abnormalaccrualspercent():
    """Main function to calculate AbnormalAccruals and AbnormalAccrualsPercent predictors."""
    
    # Define file paths
    base_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data")
    compustat_path = base_path / "Intermediate" / "a_aCompustat.csv"
    master_path = base_path / "Intermediate" / "SignalMasterTable.csv"
    output_path = base_path / "Predictors"
    
    # Ensure output directory exists
    output_path.mkdir(parents=True, exist_ok=True)
    
    logger.info("Starting AbnormalAccruals and AbnormalAccrualsPercent predictor calculation")
    
    try:
        # DATA LOAD
        logger.info("Loading Compustat data")
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'fyear', 'datadate', 'at', 'oancf', 'fopt', 'act', 'che', 'lct', 'dlc', 'ib', 'sale', 'ppegt', 'ni', 'sic']
        data = pd.read_csv(compustat_path, usecols=required_vars)
        
        # Merge with SignalMasterTable for exchcd
        logger.info("Merging with SignalMasterTable")
        master_data = pd.read_csv(master_path, usecols=['permno', 'time_avail_m', 'exchcd'])
        data = data.merge(master_data, on=['permno', 'time_avail_m'], how='inner')
        
        # Convert datadate to datetime
        data['datadate'] = pd.to_datetime(data['datadate'])
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        
        # Convert sic to numeric
        data['sic'] = pd.to_numeric(data['sic'], errors='coerce')
        
        # Sort by gvkey and fyear
        data = data.sort_values(['gvkey', 'fyear'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating abnormal accruals components")
        
        # Calculate lagged values
        data['at_lag1'] = data.groupby('gvkey')['at'].shift(1)
        data['act_lag1'] = data.groupby('gvkey')['act'].shift(1)
        data['che_lag1'] = data.groupby('gvkey')['che'].shift(1)
        data['lct_lag1'] = data.groupby('gvkey')['lct'].shift(1)
        data['dlc_lag1'] = data.groupby('gvkey')['dlc'].shift(1)
        data['sale_lag1'] = data.groupby('gvkey')['sale'].shift(1)
        
        # Compute abnormal accruals for Xie (2001)
        data['tempCFO'] = data['oancf']
        
        # Alternative CFO calculation when oancf is missing
        missing_cfo_condition = data['tempCFO'].isna()
        data.loc[missing_cfo_condition, 'tempCFO'] = (
            data.loc[missing_cfo_condition, 'fopt'] - 
            (data.loc[missing_cfo_condition, 'act'] - data.loc[missing_cfo_condition, 'act_lag1']) + 
            (data.loc[missing_cfo_condition, 'che'] - data.loc[missing_cfo_condition, 'che_lag1']) + 
            (data.loc[missing_cfo_condition, 'lct'] - data.loc[missing_cfo_condition, 'lct_lag1']) - 
            (data.loc[missing_cfo_condition, 'dlc'] - data.loc[missing_cfo_condition, 'dlc_lag1'])
        )
        
        # Calculate accruals components
        data['tempAccruals'] = (data['ib'] - data['tempCFO']) / data['at_lag1']
        data['tempInvTA'] = 1 / data['at_lag1']
        data['tempDelRev'] = (data['sale'] - data['sale_lag1']) / data['at_lag1']
        data['tempPPE'] = data['ppegt'] / data['at_lag1']
        
        # Winsorize variables by fiscal year
        logger.info("Winsorizing variables")
        for var in ['tempAccruals', 'tempInvTA', 'tempDelRev', 'tempPPE']:
            data[var] = data.groupby('fyear')[var].transform(
                lambda x: x.clip(lower=x.quantile(0.001), upper=x.quantile(0.999))
            )
        
        # Create SIC2 industry classification
        data['sic2'] = np.floor(data['sic'] / 100)
        
        # Run regressions for each year and industry
        logger.info("Running industry-year regressions")
        data['AbnormalAccruals'] = np.nan
        data['_Nobs'] = 0
        
        for fyear in data['fyear'].unique():
            for sic2 in data[data['fyear'] == fyear]['sic2'].unique():
                year_industry_data = data[(data['fyear'] == fyear) & (data['sic2'] == sic2)].copy()
                
                if len(year_industry_data) >= 6:  # Need at least 6 observations
                    try:
                        # Prepare regression variables
                        valid_data = year_industry_data.dropna(subset=['tempAccruals', 'tempInvTA', 'tempDelRev', 'tempPPE'])
                        
                        if len(valid_data) >= 6:
                            X = np.column_stack([
                                np.ones(len(valid_data)),
                                valid_data['tempInvTA'].values,
                                valid_data['tempDelRev'].values,
                                valid_data['tempPPE'].values
                            ])
                            y = valid_data['tempAccruals'].values
                            
                            # Run regression
                            beta = np.linalg.lstsq(X, y, rcond=None)[0]
                            fitted_values = X @ beta
                            residuals = y - fitted_values
                            
                            # Store residuals and observation count
                            data.loc[valid_data.index, 'AbnormalAccruals'] = residuals
                            data.loc[valid_data.index, '_Nobs'] = len(valid_data)
                    except:
                        continue
        
        # Drop observations with insufficient observations
        data = data[data['_Nobs'] >= 6]
        
        # Drop NASDAQ observations before 1982
        data = data[~((data['exchcd'] == 3) & (data['fyear'] < 1982))]
        
        # Drop temporary variables
        temp_cols = [col for col in data.columns if col.startswith('temp') or col.startswith('_')]
        data = data.drop(temp_cols, axis=1)
        
        # Drop duplicates
        data = data.drop_duplicates(subset=['permno', 'fyear'], keep='first')
        
        # Calculate Abnormal Accruals Percent
        logger.info("Calculating abnormal accruals percentage")
        data['AbnormalAccrualsPercent'] = data['AbnormalAccruals'] * data['at'] / np.abs(data['ni'])
        
        # Expand to monthly
        logger.info("Expanding to monthly frequency")
        data_expanded = pd.DataFrame()
        
        for _, row in data.iterrows():
            start_date = row['time_avail_m']
            for i in range(12):
                new_row = row.copy()
                new_row['time_avail_m'] = start_date + pd.DateOffset(months=i)
                data_expanded = pd.concat([data_expanded, pd.DataFrame([new_row])], ignore_index=True)
        
        data = data_expanded
        
        # Keep only the most recent observation for each gvkey-time_avail_m combination
        data = data.sort_values(['gvkey', 'time_avail_m', 'datadate'])
        data = data.groupby(['gvkey', 'time_avail_m']).last().reset_index()
        
        # Keep only the most recent observation for each permno-time_avail_m combination
        data = data.sort_values(['permno', 'time_avail_m', 'datadate'])
        data = data.groupby(['permno', 'time_avail_m']).last().reset_index()
        
        # Prepare output data
        logger.info("Preparing output data")
        
        # For AbnormalAccruals (predictor)
        abnormalaccruals_output = data[['permno', 'time_avail_m', 'AbnormalAccruals']].copy()
        abnormalaccruals_output = abnormalaccruals_output.dropna(subset=['AbnormalAccruals'])
        abnormalaccruals_output['yyyymm'] = abnormalaccruals_output['time_avail_m'].dt.strftime('%Y%m').astype(int)
        abnormalaccruals_output = abnormalaccruals_output[['permno', 'yyyymm', 'AbnormalAccruals']]
        
        # For AbnormalAccrualsPercent (placebo)
        abnormalaccrualspercent_output = data[['permno', 'time_avail_m', 'AbnormalAccrualsPercent']].copy()
        abnormalaccrualspercent_output = abnormalaccrualspercent_output.dropna(subset=['AbnormalAccrualsPercent'])
        abnormalaccrualspercent_output['yyyymm'] = abnormalaccrualspercent_output['time_avail_m'].dt.strftime('%Y%m').astype(int)
        abnormalaccrualspercent_output = abnormalaccrualspercent_output[['permno', 'yyyymm', 'AbnormalAccrualsPercent']]
        
        # Save results
        logger.info("Saving results")
        
        # Save AbnormalAccruals (predictor)
        abnormalaccruals_file = output_path / "AbnormalAccruals.csv"
        abnormalaccruals_output.to_csv(abnormalaccruals_file, index=False)
        logger.info(f"Saved AbnormalAccruals predictor to {abnormalaccruals_file}")
        logger.info(f"AbnormalAccruals: {len(abnormalaccruals_output)} observations")
        
        # Save AbnormalAccrualsPercent (placebo)
        abnormalaccrualspercent_file = output_path / "AbnormalAccrualsPercent.csv"
        abnormalaccrualspercent_output.to_csv(abnormalaccrualspercent_file, index=False)
        logger.info(f"Saved AbnormalAccrualsPercent placebo to {abnormalaccrualspercent_file}")
        logger.info(f"AbnormalAccrualsPercent: {len(abnormalaccrualspercent_output)} observations")
        
        logger.info("Successfully completed AbnormalAccruals and AbnormalAccrualsPercent predictor calculation")
        
    except Exception as e:
        logger.error(f"Error in AbnormalAccruals and AbnormalAccrualsPercent calculation: {str(e)}")
        raise

if __name__ == "__main__":
    main()
