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
from sklearn.linear_model import LinearRegression

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def zz2_abnormalaccruals_abnormalaccrualspercent():
    """
    Python equivalent of ZZ2_AbnormalAccruals_AbnormalAccrualsPercent.do
    
    Constructs AbnormalAccruals and AbnormalAccrualsPercent predictor signals.
    """
    logger.info("Constructing predictor signal: zz2_abnormalaccruals_abnormalaccrualspercent...")
    
    try:
        # DATA LOAD
        logger.info("Loading Compustat data")
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/a_aCompustat.csv")
        
        # Load required variables from Compustat
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'fyear', 'datadate', 'at', 'oancf', 
                        'fopt', 'act', 'che', 'lct', 'dlc', 'ib', 'sale', 'ppegt', 'ni', 'sic']
        data = pd.read_csv(compustat_path, usecols=required_vars)
        
        # Merge with SignalMasterTable (keep=master match means left join with only matches)
        logger.info("Merging with SignalMasterTable")
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        master_data = pd.read_csv(master_path, usecols=['permno', 'time_avail_m', 'exchcd'])
        
        # Stata: merge 1:1 permno time_avail_m using SignalMasterTable, keep(master match)
        # This means keep all from master (data) and only matches from using (master_data)
        data = data.merge(master_data, on=['permno', 'time_avail_m'], how='left')
        # Keep only observations that have exchcd (i.e., matched with SignalMasterTable)
        data = data.dropna(subset=['exchcd'])
        
        logger.info(f"Data after merge: {len(data)} observations")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating abnormal accruals components")
        
        # Sort by gvkey and fyear for lagged calculations
        data = data.sort_values(['gvkey', 'fyear'])
        
        # Calculate lagged values (Stata: l.variable)
        data['at_lag1'] = data.groupby('gvkey')['at'].shift(1)
        data['act_lag1'] = data.groupby('gvkey')['act'].shift(1)
        data['che_lag1'] = data.groupby('gvkey')['che'].shift(1)
        data['lct_lag1'] = data.groupby('gvkey')['lct'].shift(1)
        data['dlc_lag1'] = data.groupby('gvkey')['dlc'].shift(1)
        data['sale_lag1'] = data.groupby('gvkey')['sale'].shift(1)
        
        # Compute abnormal accruals for Xie (2001)
        data['tempCFO'] = data['oancf']
        
        # Alternative CFO calculation when oancf is missing
        # replace tempCFO = fopt - (act - l.act) + (che - l.che) + (lct - l.lct) - (dlc - l.dlc) if mi(tempCFO)
        missing_cfo = data['tempCFO'].isna()
        data.loc[missing_cfo, 'tempCFO'] = (
            data.loc[missing_cfo, 'fopt'] - 
            (data.loc[missing_cfo, 'act'] - data.loc[missing_cfo, 'act_lag1']) + 
            (data.loc[missing_cfo, 'che'] - data.loc[missing_cfo, 'che_lag1']) + 
            (data.loc[missing_cfo, 'lct'] - data.loc[missing_cfo, 'lct_lag1']) - 
            (data.loc[missing_cfo, 'dlc'] - data.loc[missing_cfo, 'dlc_lag1'])
        )
        
        # Calculate accruals components
        data['tempAccruals'] = (data['ib'] - data['tempCFO']) / data['at_lag1']
        data['tempInvTA'] = 1 / data['at_lag1']
        data['tempDelRev'] = (data['sale'] - data['sale_lag1']) / data['at_lag1']
        data['tempPPE'] = data['ppegt'] / data['at_lag1']
        
        # Winsorize variables by fiscal year (winsor2 temp*, replace cuts(0.1 99.9) trim by(fyear))
        logger.info("Winsorizing variables by fiscal year")
        temp_vars = ['tempAccruals', 'tempInvTA', 'tempDelRev', 'tempPPE']
        for var in temp_vars:
            data[var] = data.groupby('fyear')[var].transform(
                lambda x: x.clip(lower=x.quantile(0.001), upper=x.quantile(0.999))
            )
        
        # Create SIC2 industry classification
        data['sic'] = pd.to_numeric(data['sic'], errors='coerce')
        data['sic2'] = np.floor(data['sic'] / 100)
        
        # Run regressions for each year and industry
        logger.info("Running industry-year regressions")
        data['AbnormalAccruals'] = np.nan
        data['_Nobs'] = 0
        
        # Process each fyear-sic2 combination
        for (fyear, sic2), group in data.groupby(['fyear', 'sic2']):
            if pd.isna(fyear) or pd.isna(sic2):
                continue
                
            # Get valid observations for regression
            reg_data = group.dropna(subset=['tempAccruals', 'tempInvTA', 'tempDelRev', 'tempPPE'])
            
            if len(reg_data) >= 6:  # Need at least 6 observations
                try:
                    # Prepare regression variables (asreg tempAccruals tempInvTA tempDelRev tempPPE, fitted)
                    X = reg_data[['tempInvTA', 'tempDelRev', 'tempPPE']].values
                    y = reg_data['tempAccruals'].values
                    
                    # Run regression
                    model = LinearRegression()
                    model.fit(X, y)
                    
                    # Calculate residuals
                    fitted_values = model.predict(X)
                    residuals = y - fitted_values
                    
                    # Store residuals and observation count
                    data.loc[reg_data.index, 'AbnormalAccruals'] = residuals
                    data.loc[reg_data.index, '_Nobs'] = len(reg_data)
                    
                except Exception as e:
                    logger.warning(f"Regression failed for fyear={fyear}, sic2={sic2}: {e}")
                    continue
        
        # Drop observations with insufficient observations (drop if _Nobs < 6)
        data = data[data['_Nobs'] >= 6]
        logger.info(f"After dropping insufficient observations: {len(data)} observations")
        
        # Drop NASDAQ observations before 1982 (drop if exchcd == 3 & fyear < 1982)
        data = data[~((data['exchcd'] == 3) & (data['fyear'] < 1982))]
        logger.info(f"After dropping NASDAQ pre-1982: {len(data)} observations")
        
        # Drop temporary variables (drop _* temp*)
        temp_cols = [col for col in data.columns if col.startswith('temp') or col.startswith('_')]
        data = data.drop(temp_cols, axis=1)
        
        # Drop duplicates (by permno fyear: keep if _n == 1)
        data = data.sort_values(['permno', 'fyear'])
        data = data.drop_duplicates(subset=['permno', 'fyear'], keep='first')
        logger.info(f"After dropping duplicates: {len(data)} observations")
        
        # Calculate Abnormal Accruals Percent
        logger.info("Calculating abnormal accruals percentage")
        # gen AbnormalAccrualsPercent = AbnormalAccruals*l.at/abs(ni)
        data['AbnormalAccrualsPercent'] = data['AbnormalAccruals'] * data['at_lag1'] / np.abs(data['ni'])
        
        # Expand to monthly
        logger.info("Expanding to monthly frequency")
        
        # Stata logic:
        # gen temp = 12
        # expand temp
        # gen tempTime = time_avail_m
        # bysort gvkey tempTime: replace time_avail_m = time_avail_m + _n - 1
        
        expanded_data = []
        for _, row in data.iterrows():
            base_time = pd.to_datetime(row['time_avail_m'])
            for i in range(12):  # expand 12 times
                new_row = row.copy()
                new_row['time_avail_m'] = base_time + pd.DateOffset(months=i)
                expanded_data.append(new_row)
        
        data_expanded = pd.DataFrame(expanded_data)
        logger.info(f"After monthly expansion: {len(data_expanded)} observations")
        
        # Keep only the most recent observation for each gvkey-time_avail_m combination
        # bysort gvkey time_avail_m (datadate): keep if _n == _N
        data_expanded['datadate'] = pd.to_datetime(data_expanded['datadate'])
        data_expanded = data_expanded.sort_values(['gvkey', 'time_avail_m', 'datadate'])
        data_expanded = data_expanded.groupby(['gvkey', 'time_avail_m']).last().reset_index()
        
        # Keep only the most recent observation for each permno-time_avail_m combination
        # bysort permno time_avail_m (datadate): keep if _n == _N
        data_expanded = data_expanded.sort_values(['permno', 'time_avail_m', 'datadate'])
        data_expanded = data_expanded.groupby(['permno', 'time_avail_m']).last().reset_index()
        
        logger.info(f"Final data after deduplication: {len(data_expanded)} observations")
        
        # Prepare output data
        logger.info("Preparing output data")
        
        # For AbnormalAccruals (predictor)
        abnormalaccruals_output = data_expanded[['permno', 'time_avail_m', 'AbnormalAccruals']].copy()
        abnormalaccruals_output = abnormalaccruals_output.dropna(subset=['AbnormalAccruals'])
        abnormalaccruals_output['yyyymm'] = pd.to_datetime(abnormalaccruals_output['time_avail_m']).dt.strftime('%Y%m').astype(int)
        abnormalaccruals_output = abnormalaccruals_output[['permno', 'yyyymm', 'AbnormalAccruals']]
        
        # For AbnormalAccrualsPercent (placebo)
        abnormalaccrualspercent_output = data_expanded[['permno', 'time_avail_m', 'AbnormalAccrualsPercent']].copy()
        abnormalaccrualspercent_output = abnormalaccrualspercent_output.dropna(subset=['AbnormalAccrualsPercent'])
        abnormalaccrualspercent_output['yyyymm'] = pd.to_datetime(abnormalaccrualspercent_output['time_avail_m']).dt.strftime('%Y%m').astype(int)
        abnormalaccrualspercent_output = abnormalaccrualspercent_output[['permno', 'yyyymm', 'AbnormalAccrualsPercent']]
        
        # Save results
        logger.info("Saving results")
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        
        # Save AbnormalAccruals (predictor)
        abnormalaccruals_file = output_path / "abnormalaccruals.csv"
        abnormalaccruals_output.to_csv(abnormalaccruals_file, index=False)
        logger.info(f"Saved AbnormalAccruals predictor to {abnormalaccruals_file}")
        logger.info(f"AbnormalAccruals: {len(abnormalaccruals_output)} observations")
        
        # Save AbnormalAccrualsPercent (placebo) - Note: This should go to Placebos directory
        placebos_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Placebos")
        placebos_path.mkdir(parents=True, exist_ok=True)
        abnormalaccrualspercent_file = placebos_path / "abnormalaccrualspercent.csv"
        abnormalaccrualspercent_output.to_csv(abnormalaccrualspercent_file, index=False)
        logger.info(f"Saved AbnormalAccrualsPercent placebo to {abnormalaccrualspercent_file}")
        logger.info(f"AbnormalAccrualsPercent: {len(abnormalaccrualspercent_output)} observations")
        
        logger.info("Successfully completed AbnormalAccruals and AbnormalAccrualsPercent predictor calculation")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct predictor zz2_abnormalaccruals_abnormalaccrualspercent: {e}")
        import traceback
        logger.error(f"Detailed traceback: {traceback.format_exc()}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    zz2_abnormalaccruals_abnormalaccrualspercent()
