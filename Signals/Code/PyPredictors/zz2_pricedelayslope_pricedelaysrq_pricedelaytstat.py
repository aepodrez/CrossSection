"""
ZZ2_PriceDelaySlope_PriceDelaySRQ_PriceDelayTstat Predictor Implementation

This script implements three price delay predictors based on Hou and Moskowitz:
- PriceDelaySlope: Price delay (slope-based measure)
- PriceDelaySRQ: Price delay (R-squared based measure)
- PriceDelayTstat: Price delay (t-statistic based measure)

The script calculates price delay measures using lagged market returns in CAPM regressions,
capturing the speed of price adjustment to market information.
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def zz2_pricedelayslope_pricedelaysrq_pricedelaytstat():
    """Main function to calculate PriceDelaySlope, PriceDelaySRQ, and PriceDelayTstat predictors."""
    
    # Define file paths
    base_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data")
    daily_ff_path = base_path / "Intermediate" / "dailyFF.csv"
    daily_crsp_path = base_path / "Intermediate" / "dailyCRSP.csv"
    temp_ff_path = base_path / "Temp" / "tempdailyff.csv"
    output_path = base_path / "Predictors"
    
    # Ensure directories exist
    temp_ff_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Global parameters
    nlag = 4
    weightscale = 1
    
    logger.info("Starting PriceDelaySlope, PriceDelaySRQ, PriceDelayTstat predictor calculation")
    
    try:
        # Prepare market lag data
        logger.info("Preparing market lag data")
        ff_data = pd.read_csv(daily_ff_path, usecols=['time_d', 'mktrf', 'rf'])
        ff_data = ff_data.sort_values('time_d')
        
        # Create lagged market returns
        for n in range(1, nlag + 1):
            ff_data[f'mktLag{n}'] = ff_data['mktrf'].shift(n)
        
        ff_data.to_csv(temp_ff_path, index=False)
        
        # Load daily CRSP data
        logger.info("Loading daily CRSP data")
        crsp_data = pd.read_csv(daily_crsp_path, usecols=['permno', 'time_d', 'ret'])
        
        # Merge with market data
        data = crsp_data.merge(ff_data, on='time_d', how='inner')
        
        # Calculate excess returns
        data['ret'] = data['ret'] - data['rf']
        data = data.drop('rf', axis=1)
        
        # Convert time_d to datetime
        data['time_d'] = pd.to_datetime(data['time_d'])
        
        # Set up for regressions in each June
        logger.info("Setting up time periods for regressions")
        data['time_m'] = data['time_d'].dt.to_period('M')
        data['time_avail_m'] = (data['time_m'] + 6).dt.to_timestamp()
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'].dt.year.astype(str) + '-06-30')
        data = data.drop('time_m', axis=1)
        
        # Sort by time_avail_m and permno
        data = data.sort_values(['time_avail_m', 'permno'])
        
        # Initialize results storage
        results = []
        
        # Run regressions for each time_avail_m and permno combination
        logger.info("Running restricted and unrestricted regressions")
        for time_period in data['time_avail_m'].unique():
            for permno in data[(data['time_avail_m'] == time_period)]['permno'].unique():
                firm_data = data[(data['time_avail_m'] == time_period) & (data['permno'] == permno)].copy()
                
                if len(firm_data) >= 26:  # Need at least 26 observations
                    try:
                        # Restricted regression (lag slopes = 0)
                        valid_data = firm_data.dropna(subset=['ret', 'mktrf'])
                        if len(valid_data) >= 26:
                            X_restricted = np.column_stack([np.ones(len(valid_data)), valid_data['mktrf'].values])
                            y = valid_data['ret'].values
                            
                            beta_restricted = np.linalg.lstsq(X_restricted, y, rcond=None)[0]
                            fitted_restricted = X_restricted @ beta_restricted
                            residuals_restricted = y - fitted_restricted
                            
                            ss_res_restricted = np.sum(residuals_restricted ** 2)
                            ss_tot = np.sum((y - np.mean(y)) ** 2)
                            r2_restricted = 1 - (ss_res_restricted / ss_tot)
                            
                            # Unrestricted regression (with lagged market returns)
                            lag_cols = [f'mktLag{n}' for n in range(1, nlag + 1)]
                            valid_data_unrestricted = firm_data.dropna(subset=['ret', 'mktrf'] + lag_cols)
                            
                            if len(valid_data_unrestricted) >= 26:
                                X_unrestricted = np.column_stack([
                                    np.ones(len(valid_data_unrestricted)),
                                    valid_data_unrestricted['mktrf'].values
                                ] + [valid_data_unrestricted[col].values for col in lag_cols])
                                
                                y_unrestricted = valid_data_unrestricted['ret'].values
                                
                                # Run regression and calculate statistics
                                beta_unrestricted = np.linalg.lstsq(X_unrestricted, y_unrestricted, rcond=None)[0]
                                fitted_unrestricted = X_unrestricted @ beta_unrestricted
                                residuals_unrestricted = y_unrestricted - fitted_unrestricted
                                
                                ss_res_unrestricted = np.sum(residuals_unrestricted ** 2)
                                r2_unrestricted = 1 - (ss_res_unrestricted / ss_tot)
                                
                                # Calculate standard errors and t-statistics
                                n_obs = len(y_unrestricted)
                                mse = ss_res_unrestricted / (n_obs - len(beta_unrestricted))
                                
                                # Calculate variance-covariance matrix
                                XtX_inv = np.linalg.inv(X_unrestricted.T @ X_unrestricted)
                                se_unrestricted = np.sqrt(mse * np.diag(XtX_inv))
                                t_stats = beta_unrestricted / se_unrestricted
                                
                                # Store results
                                result = {
                                    'permno': permno,
                                    'time_avail_m': time_period,
                                    'R2Restricted': r2_restricted,
                                    'R2Unrestricted': r2_unrestricted,
                                    'beta_mktrf': beta_unrestricted[1],
                                    'beta_lags': beta_unrestricted[2:],
                                    't_stats_lags': t_stats[2:],
                                    'last_obs_month': firm_data['time_d'].dt.month.iloc[-1]
                                }
                                results.append(result)
                    except:
                        continue
        
        # Convert results to DataFrame
        results_df = pd.DataFrame(results)
        
        # Keep only June observations
        results_df = results_df[results_df['last_obs_month'] == 6]
        
        # Construct delay signals
        logger.info("Constructing delay signals")
        
        # PriceDelaySRQ (R-squared based)
        results_df['PriceDelaySRQ'] = 1 - results_df['R2Restricted'] / results_df['R2Unrestricted']
        
        # PriceDelaySlope (slope-based)
        results_df['tempSum1'] = 0
        results_df['tempSum2'] = 0
        for n in range(1, nlag + 1):
            results_df['tempSum1'] += (n / weightscale) * results_df['beta_lags'].apply(lambda x: x[n-1] if len(x) > n-1 else 0)
            results_df['tempSum2'] += results_df['beta_lags'].apply(lambda x: x[n-1] if len(x) > n-1 else 0)
        
        results_df['PriceDelaySlope'] = results_df['tempSum1'] / (results_df['beta_mktrf'] + results_df['tempSum2'])
        
        # PriceDelayTstat (t-statistic based)
        results_df['tempSum1_t'] = 0
        results_df['tempSum2_t'] = 0
        for n in range(1, nlag + 1):
            results_df['tempSum1_t'] += (n / weightscale) * results_df['t_stats_lags'].apply(lambda x: x[n-1] if len(x) > n-1 else 0)
            results_df['tempSum2_t'] += results_df['t_stats_lags'].apply(lambda x: x[n-1] if len(x) > n-1 else 0)
        
        results_df['PriceDelayTstat'] = results_df['tempSum1_t'] / (results_df['beta_mktrf'] + results_df['tempSum2_t'])
        
        # Drop temporary columns
        temp_cols = [col for col in results_df.columns if col.startswith('temp')]
        results_df = results_df.drop(temp_cols, axis=1)
        
        # Add one month to time_avail_m (Hou and Moskowitz skip one month)
        results_df['time_avail_m'] = results_df['time_avail_m'] + pd.DateOffset(months=1)
        
        # Winsorize PriceDelayTstat
        results_df['PriceDelayTstat'] = results_df.groupby('time_avail_m')['PriceDelayTstat'].transform(
            lambda x: x.clip(lower=x.quantile(0.10), upper=x.quantile(0.90))
        )
        
        # Fill to monthly frequency
        logger.info("Filling to monthly frequency")
        all_months = pd.date_range(
            results_df['time_avail_m'].min(),
            results_df['time_avail_m'].max(),
            freq='M'
        )
        
        permno_list = results_df['permno'].unique()
        complete_data = pd.DataFrame([
            {'permno': permno, 'time_avail_m': month}
            for permno in permno_list
            for month in all_months
        ])
        
        # Merge with results and forward fill
        complete_data = complete_data.merge(results_df[['permno', 'time_avail_m', 'PriceDelaySlope', 'PriceDelaySRQ', 'PriceDelayTstat']], 
                                          on=['permno', 'time_avail_m'], how='left')
        
        complete_data = complete_data.sort_values(['permno', 'time_avail_m'])
        
        # Forward fill missing values
        for col in ['PriceDelaySlope', 'PriceDelaySRQ', 'PriceDelayTstat']:
            complete_data[col] = complete_data.groupby('permno')[col].fillna(method='ffill')
        
        # Prepare output data
        logger.info("Preparing output data")
        
        # For PriceDelaySlope
        pricedelayslope_output = complete_data[['permno', 'time_avail_m', 'PriceDelaySlope']].copy()
        pricedelayslope_output = pricedelayslope_output.dropna(subset=['PriceDelaySlope'])
        pricedelayslope_output['yyyymm'] = pricedelayslope_output['time_avail_m'].dt.strftime('%Y%m').astype(int)
        pricedelayslope_output = pricedelayslope_output[['permno', 'yyyymm', 'PriceDelaySlope']]
        
        # For PriceDelaySRQ
        pricedelaysrq_output = complete_data[['permno', 'time_avail_m', 'PriceDelaySRQ']].copy()
        pricedelaysrq_output = pricedelaysrq_output.dropna(subset=['PriceDelaySRQ'])
        pricedelaysrq_output['yyyymm'] = pricedelaysrq_output['time_avail_m'].dt.strftime('%Y%m').astype(int)
        pricedelaysrq_output = pricedelaysrq_output[['permno', 'yyyymm', 'PriceDelaySRQ']]
        
        # For PriceDelayTstat
        pricedelaytstat_output = complete_data[['permno', 'time_avail_m', 'PriceDelayTstat']].copy()
        pricedelaytstat_output = pricedelaytstat_output.dropna(subset=['PriceDelayTstat'])
        pricedelaytstat_output['yyyymm'] = pricedelaytstat_output['time_avail_m'].dt.strftime('%Y%m').astype(int)
        pricedelaytstat_output = pricedelaytstat_output[['permno', 'yyyymm', 'PriceDelayTstat']]
        
        # Save results
        logger.info("Saving results")
        
        # Save PriceDelaySlope
        pricedelayslope_file = output_path / "PriceDelaySlope.csv"
        pricedelayslope_output.to_csv(pricedelayslope_file, index=False)
        logger.info(f"Saved PriceDelaySlope predictor to {pricedelayslope_file}")
        logger.info(f"PriceDelaySlope: {len(pricedelayslope_output)} observations")
        
        # Save PriceDelaySRQ
        pricedelaysrq_file = output_path / "PriceDelaySRQ.csv"
        pricedelaysrq_output.to_csv(pricedelaysrq_file, index=False)
        logger.info(f"Saved PriceDelaySRQ predictor to {pricedelaysrq_file}")
        logger.info(f"PriceDelaySRQ: {len(pricedelaysrq_output)} observations")
        
        # Save PriceDelayTstat
        pricedelaytstat_file = output_path / "PriceDelayTstat.csv"
        pricedelaytstat_output.to_csv(pricedelaytstat_file, index=False)
        logger.info(f"Saved PriceDelayTstat predictor to {pricedelaytstat_file}")
        logger.info(f"PriceDelayTstat: {len(pricedelaytstat_output)} observations")
        
        # Clean up temporary file
        if temp_ff_path.exists():
            temp_ff_path.unlink()
        
        logger.info("Successfully completed PriceDelaySlope, PriceDelaySRQ, PriceDelayTstat predictor calculation")
        
    except Exception as e:
        logger.error(f"Error in PriceDelaySlope, PriceDelaySRQ, PriceDelayTstat calculation: {str(e)}")
        raise

if __name__ == "__main__":
    main() 