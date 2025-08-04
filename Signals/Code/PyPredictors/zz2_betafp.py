"""
ZZ2_BetaFP Predictor Implementation

This script implements the BetaFP predictor:
- BetaFP: Frazzini-Pedersen beta measure

The script calculates a sophisticated beta measure that combines correlation and volatility
ratios using rolling windows and log returns, following the Frazzini-Pedersen methodology.
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def zz2_betafp():
    """Main function to calculate BetaFP predictor."""
    
    # Define file paths
    base_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data")
    daily_crsp_path = base_path / "Intermediate" / "dailyCRSP.csv"
    daily_ff_path = base_path / "Intermediate" / "dailyFF.csv"
    output_path = base_path / "Predictors"
    
    # Ensure output directory exists
    output_path.mkdir(parents=True, exist_ok=True)
    
    logger.info("Starting BetaFP predictor calculation")
    
    try:
        # DATA LOAD
        logger.info("Loading daily CRSP data")
        crsp_data = pd.read_csv(daily_crsp_path, usecols=['permno', 'time_d', 'ret'])
        
        # Merge with Fama-French factors
        logger.info("Merging with Fama-French factors")
        ff_data = pd.read_csv(daily_ff_path, usecols=['time_d', 'rf', 'mktrf'])
        data = crsp_data.merge(ff_data, on='time_d', how='inner')
        
        # Convert time_d to datetime
        data['time_d'] = pd.to_datetime(data['time_d'])
        
        # Calculate excess returns
        data['ret'] = data['ret'] - data['rf']
        data = data.drop('rf', axis=1)
        
        # Sort by permno and time_d
        data = data.sort_values(['permno', 'time_d'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating log returns")
        
        # Calculate log returns
        data['LogRet'] = np.log(1 + data['ret'])
        data['LogMkt'] = np.log(1 + data['mktrf'])
        
        # Create time index for each permno
        data['time_temp'] = data.groupby('permno').cumcount() + 1
        
        # Calculate rolling standard deviations
        logger.info("Calculating rolling standard deviations")
        data['sd252_LogRet'] = data.groupby('permno')['LogRet'].rolling(window=252, min_periods=120).std().reset_index(0, drop=True)
        data['sd252_LogMkt'] = data.groupby('permno')['LogMkt'].rolling(window=252, min_periods=120).std().reset_index(0, drop=True)
        
        # Calculate lagged returns for correlation
        logger.info("Calculating lagged returns for correlation")
        data['LogRet_lag1'] = data.groupby('permno')['LogRet'].shift(1)
        data['LogRet_lag2'] = data.groupby('permno')['LogRet'].shift(2)
        data['LogMkt_lag1'] = data.groupby('permno')['LogMkt'].shift(1)
        data['LogMkt_lag2'] = data.groupby('permno')['LogMkt'].shift(2)
        
        # Calculate 3-day returns
        data['tempRi'] = data['LogRet_lag2'] + data['LogRet_lag1'] + data['LogRet']
        data['tempRm'] = data['LogMkt_lag2'] + data['LogMkt_lag1'] + data['LogMkt']
        
        # Initialize BetaFP
        data['BetaFP'] = np.nan
        
        # Run rolling regressions for each firm
        logger.info("Running rolling regressions for BetaFP calculation")
        for permno in data['permno'].unique():
            firm_data = data[data['permno'] == permno].copy()
            
            if len(firm_data) >= 750:  # Need at least 750 observations
                for i in range(1259, len(firm_data)):  # Start from 1260th observation
                    window_data = firm_data.iloc[i-1259:i+1]  # 1260-day window
                    
                    if len(window_data) == 1260:  # Ensure full window
                        try:
                            # Prepare regression variables
                            valid_data = window_data.dropna(subset=['tempRi', 'tempRm'])
                            
                            if len(valid_data) >= 750:  # Need at least 750 valid observations
                                X = np.column_stack([np.ones(len(valid_data)), valid_data['tempRm'].values])
                                y = valid_data['tempRi'].values
                                
                                # Run regression
                                beta = np.linalg.lstsq(X, y, rcond=None)[0]
                                fitted_values = X @ beta
                                residuals = y - fitted_values
                                
                                # Calculate R-squared
                                ss_res = np.sum(residuals ** 2)
                                ss_tot = np.sum((y - np.mean(y)) ** 2)
                                r_squared = 1 - (ss_res / ss_tot)
                                
                                # Calculate BetaFP
                                if not np.isnan(firm_data.iloc[i]['sd252_LogRet']) and not np.isnan(firm_data.iloc[i]['sd252_LogMkt']):
                                    data.loc[window_data.index[-1], 'BetaFP'] = np.sqrt(r_squared) * (firm_data.iloc[i]['sd252_LogRet'] / firm_data.iloc[i]['sd252_LogMkt'])
                        except:
                            continue
        
        # Convert to monthly frequency
        logger.info("Converting to monthly frequency")
        # Convert time_d to datetime if needed for period conversion
        if not pd.api.types.is_datetime64_any_dtype(data['time_d']):
            data['time_d'] = pd.to_datetime(data['time_d'])
        
        data['time_avail_m'] = data['time_d'].dt.to_period('M').dt.to_timestamp()
        
        # Aggregate to monthly level (keep last observation per month)
        monthly_data = data.sort_values(['permno', 'time_avail_m', 'time_d'])
        monthly_data = monthly_data.groupby(['permno', 'time_avail_m']).last().reset_index()
        
        # Prepare output data
        logger.info("Preparing output data")
        
        # For BetaFP
        betafp_output = monthly_data[['permno', 'time_avail_m', 'BetaFP']].copy()
        betafp_output = betafp_output.dropna(subset=['BetaFP'])
        # Convert time_avail_m to datetime if needed for strftime
        if not pd.api.types.is_datetime64_any_dtype(betafp_output['time_avail_m']):
            betafp_output['time_avail_m'] = pd.to_datetime(betafp_output['time_avail_m'])
        
        betafp_output['yyyymm'] = betafp_output['time_avail_m'].dt.strftime('%Y%m').astype(int)
        betafp_output = betafp_output[['permno', 'yyyymm', 'BetaFP']]
        
        # Save results
        logger.info("Saving results")
        
        # Save BetaFP
        betafp_file = output_path / "BetaFP.csv"
        betafp_output.to_csv(betafp_file, index=False)
        logger.info(f"Saved BetaFP predictor to {betafp_file}")
        logger.info(f"BetaFP: {len(betafp_output)} observations")
        
        logger.info("Successfully completed BetaFP predictor calculation")
        
    except Exception as e:
        logger.error(f"Error in BetaFP calculation: {str(e)}")
        raise

if __name__ == "__main__":
    main()
