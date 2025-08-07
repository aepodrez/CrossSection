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
from sklearn.linear_model import LinearRegression

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
        
        # Remove any missing values
        data = data.dropna(subset=['ret', 'mktrf'])
        
        logger.info(f"Processing {len(data)} observations for {data['permno'].nunique()} stocks")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating log returns")
        
        # Calculate log returns
        data['LogRet'] = np.log(1 + data['ret'])
        data['LogMkt'] = np.log(1 + data['mktrf'])
        
        # Initialize BetaFP
        data['BetaFP'] = np.nan
        
        # Process each stock separately to avoid memory issues
        unique_permnos = data['permno'].unique()
        total_stocks = len(unique_permnos)
        
        logger.info(f"Processing {total_stocks} stocks...")
        
        for i, permno in enumerate(unique_permnos):
            if i % 1000 == 0:
                logger.info(f"Processing stock {i+1}/{total_stocks} ({(i+1)/total_stocks*100:.1f}%)")
            
            # Get data for this stock
            stock_data = data[data['permno'] == permno].copy()
            
            if len(stock_data) < 1260:  # Need at least 1260 observations
                continue
                
            # Create time index
            stock_data = stock_data.reset_index(drop=True)
            
            # Calculate rolling standard deviations (252-day window)
            stock_data['sd252_LogRet'] = stock_data['LogRet'].rolling(window=252, min_periods=120).std()
            stock_data['sd252_LogMkt'] = stock_data['LogMkt'].rolling(window=252, min_periods=120).std()
            
            # Calculate lagged returns for correlation
            stock_data['LogRet_lag1'] = stock_data['LogRet'].shift(1)
            stock_data['LogRet_lag2'] = stock_data['LogRet'].shift(2)
            stock_data['LogMkt_lag1'] = stock_data['LogMkt'].shift(1)
            stock_data['LogMkt_lag2'] = stock_data['LogMkt'].shift(2)
            
            # Calculate 3-day returns
            stock_data['tempRi'] = stock_data['LogRet_lag2'] + stock_data['LogRet_lag1'] + stock_data['LogRet']
            stock_data['tempRm'] = stock_data['LogMkt_lag2'] + stock_data['LogMkt_lag1'] + stock_data['LogMkt']
            
            # Calculate rolling betas
            for j in range(1259, len(stock_data)):  # Start from 1260th observation
                window_data = stock_data.iloc[j-1259:j+1]  # 1260-day window
                
                if len(window_data) == 1260:
                    try:
                        # Prepare regression variables
                        valid_data = window_data.dropna(subset=['tempRi', 'tempRm'])
                        
                        if len(valid_data) >= 750:  # Need at least 750 valid observations
                            X = valid_data['tempRm'].values.reshape(-1, 1)
                            y = valid_data['tempRi'].values
                            
                            # Add constant term
                            X = np.column_stack([np.ones(len(X)), X])
                            
                            # Run regression
                            reg = LinearRegression(fit_intercept=False)
                            reg.fit(X, y)
                            
                            # Calculate R-squared
                            y_pred = reg.predict(X)
                            ss_res = np.sum((y - y_pred) ** 2)
                            ss_tot = np.sum((y - np.mean(y)) ** 2)
                            r_squared = 1 - (ss_res / ss_tot)
                            
                            # Calculate BetaFP
                            if (not np.isnan(stock_data.iloc[j]['sd252_LogRet']) and 
                                not np.isnan(stock_data.iloc[j]['sd252_LogMkt']) and
                                stock_data.iloc[j]['sd252_LogMkt'] != 0):
                                data.loc[stock_data.index[j], 'BetaFP'] = np.sqrt(r_squared) * (stock_data.iloc[j]['sd252_LogRet'] / stock_data.iloc[j]['sd252_LogMkt'])
                    except:
                        continue
        
        # Convert to monthly frequency
        logger.info("Converting to monthly frequency")
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
        betafp_file = output_path / "betafp.csv"
        betafp_output.to_csv(betafp_file, index=False)
        logger.info(f"Saved BetaFP predictor to {betafp_file}")
        logger.info(f"BetaFP: {len(betafp_output)} observations")
        
        logger.info("Successfully completed BetaFP predictor calculation")
        return True
        
    except Exception as e:
        logger.error(f"Error in BetaFP calculation: {str(e)}")
        return False

if __name__ == "__main__":
    zz2_betafp()
