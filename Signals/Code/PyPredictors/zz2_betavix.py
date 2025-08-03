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
        
        # SIGNAL CONSTRUCTION
        logger.info("Setting up CAPM regression for systematic volatility")
        
        # Create time index for each permno
        data['time_temp'] = data.groupby('permno').cumcount() + 1
        
        # Initialize BetaVIX
        data['betaVIX'] = np.nan
        
        # Run rolling regressions for each firm
        logger.info("Running rolling regressions for BetaVIX calculation")
        for permno in data['permno'].unique():
            firm_data = data[data['permno'] == permno].copy()
            
            if len(firm_data) >= 15:  # Need at least 15 observations
                for i in range(19, len(firm_data)):  # Start from 20th observation
                    window_data = firm_data.iloc[i-19:i+1]  # 20-day window
                    
                    if len(window_data) == 20:  # Ensure full window
                        try:
                            # Prepare regression variables
                            valid_data = window_data.dropna(subset=['ret', 'mktrf', 'dVIX'])
                            
                            if len(valid_data) >= 15:  # Need at least 15 valid observations
                                X = np.column_stack([
                                    np.ones(len(valid_data)),
                                    valid_data['mktrf'].values,
                                    valid_data['dVIX'].values
                                ])
                                y = valid_data['ret'].values
                                
                                # Run regression: ret = α + β1*mktrf + β2*dVIX + ε
                                beta = np.linalg.lstsq(X, y, rcond=None)[0]
                                
                                # Store the VIX beta (coefficient on dVIX)
                                data.loc[window_data.index[-1], 'betaVIX'] = beta[2]
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
        
    except Exception as e:
        logger.error(f"Error in BetaVIX calculation: {str(e)}")
        raise

if __name__ == "__main__":
    main()
