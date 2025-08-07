"""
Python equivalent of ZZ2_PriceDelaySlope_PriceDelayRsq_PriceDelayTstat.do
Generated from: ZZ2_PriceDelaySlope_PriceDelayRsq_PriceDelayTstat.do

Original Stata file: ZZ2_PriceDelaySlope_PriceDelayRsq_PriceDelayTstat.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime
from sklearn.linear_model import LinearRegression
from scipy import stats

logger = logging.getLogger(__name__)

def zz2_pricedelayslope_pricedelayrsq_pricedelaytstat():
    """
    Python equivalent of ZZ2_PriceDelaySlope_PriceDelayRsq_PriceDelayTstat.do
    
    Constructs the PriceDelaySlope, PriceDelayRsq, and PriceDelayTstat predictor signals.
    """
    logger.info("Constructing predictor signal: zz2_pricedelayslope_pricedelayrsq_pricedelaytstat...")
    
    try:
        # Global parameters
        nlag = 4
        weightscale = 1
        
        # DATA LOAD
        logger.info("Loading daily Fama-French data")
        ff_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/dailyFF.csv")
        ff_data = pd.read_csv(ff_path, usecols=['time_d', 'mktrf', 'rf'])
        
        # Create market lag data
        logger.info("Creating market lag data")
        ff_data = ff_data.sort_values('time_d')
        for n in range(1, nlag + 1):
            ff_data[f'mktLag{n}'] = ff_data['mktrf'].shift(n)
        
        # Load daily CRSP data
        logger.info("Loading daily CRSP data")
        crsp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/dailyCRSP.csv")
        crsp_data = pd.read_csv(crsp_path, usecols=['permno', 'time_d', 'ret'])
        
        # Merge CRSP with Fama-French data
        logger.info("Merging CRSP with Fama-French data")
        data = crsp_data.merge(ff_data, on='time_d', how='inner')
        
        # Calculate excess returns
        data['ret'] = data['ret'] - data['rf']
        data = data.drop('rf', axis=1)
        
        # Set up for regressions in each June
        logger.info("Setting up time variables")
        data['time_d'] = pd.to_datetime(data['time_d'])
        data['time_m'] = data['time_d'].dt.to_period('M')
        data['time_avail_m'] = (data['time_m'] + 6).dt.to_timestamp()
        
        # REGRESSIONS
        logger.info("Running regressions")
        
        # Initialize result columns
        data['R2Restricted'] = np.nan
        data['_R2'] = np.nan
        data['_b_mktrf'] = np.nan
        for n in range(1, nlag + 1):
            data[f'_b_mktLag{n}'] = np.nan
            data[f'_se_mktLag{n}'] = np.nan
            data[f'_t_mktLag{n}'] = np.nan
        
        # Sort by time_avail_m and permno
        data = data.sort_values(['time_avail_m', 'permno'])
        
        # Run regressions for each permno-time_avail_m combination
        logger.info("Processing regressions for each stock-month")
        for (time_avail_m, permno), group in data.groupby(['time_avail_m', 'permno']):
            if len(group) >= 26:  # Minimum 26 observations
                group = group.dropna(subset=['ret', 'mktrf'] + [f'mktLag{n}' for n in range(1, nlag + 1)])
                
                if len(group) >= 26:
                    try:
                        # Restricted regression (lag slopes = 0)
                        X_restricted = group['mktrf'].values.reshape(-1, 1)
                        y = group['ret'].values
                        
                        model_restricted = LinearRegression()
                        model_restricted.fit(X_restricted, y)
                        y_pred_restricted = model_restricted.predict(X_restricted)
                        ss_res_restricted = np.sum((y - y_pred_restricted) ** 2)
                        ss_tot = np.sum((y - np.mean(y)) ** 2)
                        r2_restricted = 1 - (ss_res_restricted / ss_tot)
                        
                        # Unrestricted regression
                        X_unrestricted = group[['mktrf'] + [f'mktLag{n}' for n in range(1, nlag + 1)]].values
                        
                        model_unrestricted = LinearRegression()
                        model_unrestricted.fit(X_unrestricted, y)
                        y_pred_unrestricted = model_unrestricted.predict(X_unrestricted)
                        ss_res_unrestricted = np.sum((y - y_pred_unrestricted) ** 2)
                        r2_unrestricted = 1 - (ss_res_unrestricted / ss_tot)
                        
                        # Calculate standard errors and t-statistics
                        n = len(y)
                        p = X_unrestricted.shape[1]
                        mse = ss_res_unrestricted / (n - p)
                        
                        # Calculate covariance matrix
                        X_with_const = np.column_stack([np.ones(n), X_unrestricted])
                        cov_matrix = mse * np.linalg.inv(X_with_const.T @ X_with_const)
                        se_coeffs = np.sqrt(np.diag(cov_matrix))[1:]  # Skip intercept
                        
                        # Store results
                        data.loc[group.index, 'R2Restricted'] = r2_restricted
                        data.loc[group.index, '_R2'] = r2_unrestricted
                        data.loc[group.index, '_b_mktrf'] = model_unrestricted.coef_[0]
                        
                        for i, n in enumerate(range(1, nlag + 1)):
                            data.loc[group.index, f'_b_mktLag{n}'] = model_unrestricted.coef_[i + 1]
                            data.loc[group.index, f'_se_mktLag{n}'] = se_coeffs[i]
                            if se_coeffs[i] != 0:
                                data.loc[group.index, f'_t_mktLag{n}'] = model_unrestricted.coef_[i + 1] / se_coeffs[i]
                        
                    except:
                        continue
        
        # CONSTRUCT DELAY SIGNALS
        logger.info("Constructing delay signals")
        
        # Collapse to monthly (keep June observations)
        data = data[data['time_d'].dt.month == 6]  # Keep June observations
        data = data.dropna(subset=['_R2'])
        data = data.groupby(['permno', 'time_avail_m']).first().reset_index()
        
        # Construct PriceDelayRsq (D1)
        data['PriceDelayRsq'] = 1 - data['R2Restricted'] / data['_R2']
        
        # Construct PriceDelaySlope (D2)
        for n in range(1, nlag + 1):
            data[f'tempweighted{n}'] = (n / weightscale) * data[f'_b_mktLag{n}']
        
        tempweighted_cols = [f'tempweighted{n}' for n in range(1, nlag + 1)]
        mktlag_cols = [f'_b_mktLag{n}' for n in range(1, nlag + 1)]
        
        data['tempSum1'] = data[tempweighted_cols].sum(axis=1)
        data['tempSum2'] = data[mktlag_cols].sum(axis=1)
        data['PriceDelaySlope'] = data['tempSum1'] / (data['_b_mktrf'] + data['tempSum2'])
        
        # Drop temporary columns
        data = data.drop(tempweighted_cols + ['tempSum1', 'tempSum2'], axis=1)
        
        # Construct PriceDelayTstat (D3)
        for n in range(1, nlag + 1):
            data[f'tempweighted{n}'] = (n / weightscale) * data[f'_t_mktLag{n}']
        
        t_mktlag_cols = [f'_t_mktLag{n}' for n in range(1, nlag + 1)]
        
        data['tempSum1'] = data[tempweighted_cols].sum(axis=1)
        data['tempSum2'] = data[t_mktlag_cols].sum(axis=1)
        data['PriceDelayTstat'] = data['tempSum1'] / (data['_b_mktrf'] + data['tempSum2'])
        
        # Drop temporary columns
        data = data.drop(tempweighted_cols + ['tempSum1', 'tempSum2'], axis=1)
        
        # Skip one month (Hou and Moskowitz skip one month)
        data['time_avail_m'] = data['time_avail_m'] + pd.DateOffset(months=1)
        
        # Winsorize PriceDelayTstat
        logger.info("Winsorizing PriceDelayTstat")
        for time_avail_m in data['time_avail_m'].unique():
            mask = data['time_avail_m'] == time_avail_m
            subset = data.loc[mask, 'PriceDelayTstat']
            if len(subset) > 0:
                lower = subset.quantile(0.10)
                upper = subset.quantile(0.90)
                data.loc[mask, 'PriceDelayTstat'] = subset.clip(lower, upper)
        
        # Fill to monthly (forward fill)
        logger.info("Filling to monthly data")
        all_months = pd.date_range(
            data['time_avail_m'].min(),
            data['time_avail_m'].max(),
            freq='ME'
        )
        
        permno_list = data['permno'].unique()
        complete_data = pd.DataFrame([
            {'permno': permno, 'time_avail_m': month}
            for permno in permno_list
            for month in all_months
        ])
        
        # Merge with existing data
        complete_data = complete_data.merge(
            data[['permno', 'time_avail_m', 'PriceDelaySlope', 'PriceDelayRsq', 'PriceDelayTstat']], 
            on=['permno', 'time_avail_m'], 
            how='left'
        )
        
        # Forward fill missing values
        complete_data = complete_data.sort_values(['permno', 'time_avail_m'])
        for col in ['PriceDelaySlope', 'PriceDelayRsq', 'PriceDelayTstat']:
            complete_data[col] = complete_data.groupby('permno')[col].fillna(method='ffill')
        
        # Drop missing values
        complete_data = complete_data.dropna(subset=['PriceDelaySlope', 'PriceDelayRsq', 'PriceDelayTstat'])
        
        # Prepare output data
        logger.info("Preparing output data")
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        
        # Save PriceDelaySlope
        pricedelayslope_output = complete_data[['permno', 'time_avail_m', 'PriceDelaySlope']].copy()
        pricedelayslope_output['yyyymm'] = pricedelayslope_output['time_avail_m'].dt.strftime('%Y%m').astype(int)
        pricedelayslope_output = pricedelayslope_output[['permno', 'yyyymm', 'PriceDelaySlope']]
        pricedelayslope_output.to_csv(output_path / "pricedelayslope.csv", index=False)
        logger.info(f"Saved PriceDelaySlope predictor: {len(pricedelayslope_output)} observations")
        
        # Save PriceDelayRsq
        pricedelayrsq_output = complete_data[['permno', 'time_avail_m', 'PriceDelayRsq']].copy()
        pricedelayrsq_output['yyyymm'] = pricedelayrsq_output['time_avail_m'].dt.strftime('%Y%m').astype(int)
        pricedelayrsq_output = pricedelayrsq_output[['permno', 'yyyymm', 'PriceDelayRsq']]
        pricedelayrsq_output.to_csv(output_path / "pricedelayrsq.csv", index=False)
        logger.info(f"Saved PriceDelayRsq predictor: {len(pricedelayrsq_output)} observations")
        
        # Save PriceDelayTstat
        pricedelaytstat_output = complete_data[['permno', 'time_avail_m', 'PriceDelayTstat']].copy()
        pricedelaytstat_output['yyyymm'] = pricedelaytstat_output['time_avail_m'].dt.strftime('%Y%m').astype(int)
        pricedelaytstat_output = pricedelaytstat_output[['permno', 'yyyymm', 'PriceDelayTstat']]
        pricedelaytstat_output.to_csv(output_path / "pricedelaytstat.csv", index=False)
        logger.info(f"Saved PriceDelayTstat predictor: {len(pricedelaytstat_output)} observations")
        
        logger.info("Successfully completed PriceDelay predictors calculation")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct predictor zz2_pricedelayslope_pricedelayrsq_pricedelaytstat: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    zz2_pricedelayslope_pricedelayrsq_pricedelaytstat()
