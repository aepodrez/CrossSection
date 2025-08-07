"""
ZZ2_BetaVIX Predictor Implementation

This script implements the BetaVIX predictor:
- BetaVIX: Systematic volatility measure based on VIX sensitivity

The script calculates a beta measure that captures a stock's sensitivity to VIX changes,
representing systematic volatility risk using rolling regressions.
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path
from sklearn.linear_model import LinearRegression

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def zz2_betavix():
    """Main function to calculate BetaVIX predictor."""
    
    # Define file paths
    base_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data")
    daily_crsp_path = base_path / "Intermediate" / "dailyCRSP.csv"
    daily_ff_path = base_path / "Intermediate" / "dailyFF.csv"
    daily_vix_path = base_path / "Intermediate" / "d_vix.csv"
    output_path = base_path / "Predictors"
    
    # Ensure output directory exists
    output_path.mkdir(parents=True, exist_ok=True)
    
    logger.info("Starting BetaVIX predictor calculation")
    
    try:
        # DATA LOAD
        logger.info("Loading daily CRSP data")
        crsp_data = pd.read_csv(daily_crsp_path, usecols=['permno', 'time_d', 'ret'])
        
        # Merge with Fama-French factors
        logger.info("Merging with Fama-French factors")
        ff_data = pd.read_csv(daily_ff_path, usecols=['time_d', 'rf', 'mktrf'])
        data = crsp_data.merge(ff_data, on='time_d', how='inner')
        
        # Calculate excess returns
        data['ret'] = data['ret'] - data['rf']
        data = data.drop('rf', axis=1)
        
        # Merge with VIX data
        logger.info("Merging with VIX data")
        vix_data = pd.read_csv(daily_vix_path, usecols=['time_d', 'dVIX'])
        data = data.merge(vix_data, on='time_d', how='inner')
        
        # Convert time_d to datetime
        data['time_d'] = pd.to_datetime(data['time_d'])
        
        # Sort by permno and time_d
        data = data.sort_values(['permno', 'time_d'])
        
        # Remove any missing values
        data = data.dropna(subset=['ret', 'mktrf', 'dVIX'])
        
        logger.info(f"Processing {len(data)} observations for {data['permno'].nunique()} stocks")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating BetaVIX using rolling regressions")
        
        # Initialize BetaVIX column
        data['betaVIX'] = np.nan
        
        # Process each stock separately to avoid memory issues
        unique_permnos = data['permno'].unique()
        total_stocks = len(unique_permnos)
        
        logger.info(f"Processing {total_stocks} stocks...")
        
        for i, permno in enumerate(unique_permnos):
            if i % 1000 == 0:
                logger.info(f"Processing stock {i+1}/{total_stocks} ({(i+1)/total_stocks*100:.1f}%)")
            
            # Get data for this stock
            stock_data = data[data['permno'] == permno].copy()
            
            if len(stock_data) < 20:  # Need at least 20 observations
                continue
                
            # Create time index
            stock_data = stock_data.reset_index(drop=True)
            
            # Calculate rolling betas
            for j in range(19, len(stock_data)):  # Start from 20th observation
                window_data = stock_data.iloc[j-19:j+1]  # 20-day window
                
                if len(window_data) == 20 and window_data.dropna().shape[0] >= 15:
                    try:
                        # Prepare regression variables
                        X = window_data[['mktrf', 'dVIX']].values
                        y = window_data['ret'].values
                        
                        # Remove any rows with NaN
                        valid_mask = ~(np.isnan(X).any(axis=1) | np.isnan(y))
                        if valid_mask.sum() >= 15:
                            X_valid = X[valid_mask]
                            y_valid = y[valid_mask]
                            
                            # Add constant term
                            X_valid = np.column_stack([np.ones(len(X_valid)), X_valid])
                            
                            # Run regression
                            reg = LinearRegression(fit_intercept=False)
                            reg.fit(X_valid, y_valid)
                            
                            # Store the VIX beta (coefficient on dVIX, which is the 3rd coefficient)
                            if len(reg.coef_) >= 3:
                                data.loc[stock_data.index[j], 'betaVIX'] = reg.coef_[2]
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
        
        # For BetaVIX
        betavix_output = monthly_data[['permno', 'time_avail_m', 'betaVIX']].copy()
        betavix_output = betavix_output.dropna(subset=['betaVIX'])
        
        # Convert time_avail_m to datetime if needed for strftime
        if not pd.api.types.is_datetime64_any_dtype(betavix_output['time_avail_m']):
            betavix_output['time_avail_m'] = pd.to_datetime(betavix_output['time_avail_m'])
        
        betavix_output['yyyymm'] = betavix_output['time_avail_m'].dt.strftime('%Y%m').astype(int)
        betavix_output = betavix_output[['permno', 'yyyymm', 'betaVIX']]
        
        # Save results
        logger.info("Saving results")
        
        # Save BetaVIX
        betavix_file = output_path / "BetaVIX.csv"
        betavix_output.to_csv(betavix_file, index=False)
        logger.info(f"Saved BetaVIX predictor to {betavix_file}")
        logger.info(f"BetaVIX: {len(betavix_output)} observations")
        
        logger.info("Successfully completed BetaVIX predictor calculation")
        return True
        
    except Exception as e:
        logger.error(f"Error in BetaVIX calculation: {str(e)}")
        return False

if __name__ == "__main__":
    zz2_betavix()
