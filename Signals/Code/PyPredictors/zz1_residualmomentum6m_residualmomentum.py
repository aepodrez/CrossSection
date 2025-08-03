"""
ZZ1_ResidualMomentum6m_ResidualMomentum Predictor Implementation

This script implements two residual momentum predictors:
- ResidualMomentum6m: 6-month residual momentum
- ResidualMomentum: 12-month residual momentum (momentum based on FF3 residuals)

The script calculates momentum measures using residuals from Fama-French 3-factor model
regressions, providing both 6-month and 12-month versions.
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def zz1_residualmomentum6m_residualmomentum():
    """Main function to calculate ResidualMomentum6m and ResidualMomentum predictors."""
    
    # Define file paths
    base_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data")
    crsp_path = base_path / "Intermediate" / "monthlyCRSP.csv"
    ff_path = base_path / "Intermediate" / "monthlyFF.csv"
    output_path = base_path / "Predictors"
    
    # Ensure output directory exists
    output_path.mkdir(parents=True, exist_ok=True)
    
    logger.info("Starting ResidualMomentum6m and ResidualMomentum predictor calculation")
    
    try:
        # DATA LOAD
        logger.info("Loading monthly CRSP data")
        required_vars = ['permno', 'time_avail_m', 'ret']
        data = pd.read_csv(crsp_path, usecols=required_vars)
        
        # Merge with Fama-French factors
        logger.info("Merging with Fama-French factors")
        ff_data = pd.read_csv(ff_path, usecols=['time_avail_m', 'rf', 'mktrf', 'hml', 'smb'])
        data = data.merge(ff_data, on='time_avail_m', how='inner')
        
        # Convert time_avail_m to datetime
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        
        # Sort by permno and time_avail_m
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating excess returns")
        
        # Calculate excess returns
        data['retrf'] = data['ret'] - data['rf']
        
        # Create time index for each firm
        data['time_temp'] = data.groupby('permno').cumcount() + 1
        
        # Initialize residual columns
        data['_residuals'] = np.nan
        
        # Run rolling FF3 regressions for each firm
        logger.info("Running rolling FF3 regressions")
        for permno in data['permno'].unique():
            firm_data = data[data['permno'] == permno].copy()
            
            if len(firm_data) >= 36:  # Need at least 36 observations
                for i in range(35, len(firm_data)):  # Start from 36th observation
                    window_data = firm_data.iloc[i-35:i+1]  # 36-month window
                    
                    if len(window_data) == 36:  # Ensure full window
                        try:
                            # Prepare regression variables
                            X = np.column_stack([
                                np.ones(36),
                                window_data['mktrf'].values,
                                window_data['hml'].values,
                                window_data['smb'].values
                            ])
                            y = window_data['retrf'].values
                            
                            # Run regression
                            beta = np.linalg.lstsq(X, y, rcond=None)[0]
                            fitted_values = X @ beta
                            residuals = y - fitted_values
                            
                            # Store the last residual (most recent month)
                            data.loc[window_data.index[-1], '_residuals'] = residuals[-1]
                        except:
                            continue
        
        # Create lagged residuals (skip most recent month)
        data['temp'] = data.groupby('permno')['_residuals'].shift(1)
        
        # Calculate 6-month residual momentum
        logger.info("Calculating 6-month residual momentum")
        data['mean6_temp'] = data.groupby('permno')['temp'].rolling(window=6, min_periods=6).mean().reset_index(0, drop=True)
        data['sd6_temp'] = data.groupby('permno')['temp'].rolling(window=6, min_periods=6).std().reset_index(0, drop=True)
        data['ResidualMomentum6m'] = data['mean6_temp'] / data['sd6_temp']
        
        # Calculate 12-month residual momentum
        logger.info("Calculating 12-month residual momentum")
        data['mean11_temp'] = data.groupby('permno')['temp'].rolling(window=11, min_periods=11).mean().reset_index(0, drop=True)
        data['sd11_temp'] = data.groupby('permno')['temp'].rolling(window=11, min_periods=11).std().reset_index(0, drop=True)
        data['ResidualMomentum'] = data['mean11_temp'] / data['sd11_temp']
        
        # Prepare output data
        logger.info("Preparing output data")
        
        # For ResidualMomentum6m (placebo)
        residualmomentum6m_output = data[['permno', 'time_avail_m', 'ResidualMomentum6m']].copy()
        residualmomentum6m_output = residualmomentum6m_output.dropna(subset=['ResidualMomentum6m'])
        residualmomentum6m_output['yyyymm'] = residualmomentum6m_output['time_avail_m'].dt.strftime('%Y%m').astype(int)
        residualmomentum6m_output = residualmomentum6m_output[['permno', 'yyyymm', 'ResidualMomentum6m']]
        
        # For ResidualMomentum (predictor)
        residualmomentum_output = data[['permno', 'time_avail_m', 'ResidualMomentum']].copy()
        residualmomentum_output = residualmomentum_output.dropna(subset=['ResidualMomentum'])
        residualmomentum_output['yyyymm'] = residualmomentum_output['time_avail_m'].dt.strftime('%Y%m').astype(int)
        residualmomentum_output = residualmomentum_output[['permno', 'yyyymm', 'ResidualMomentum']]
        
        # Save results
        logger.info("Saving results")
        
        # Save ResidualMomentum6m (placebo)
        residualmomentum6m_file = output_path / "ResidualMomentum6m.csv"
        residualmomentum6m_output.to_csv(residualmomentum6m_file, index=False)
        logger.info(f"Saved ResidualMomentum6m placebo to {residualmomentum6m_file}")
        logger.info(f"ResidualMomentum6m: {len(residualmomentum6m_output)} observations")
        
        # Save ResidualMomentum (predictor)
        residualmomentum_file = output_path / "ResidualMomentum.csv"
        residualmomentum_output.to_csv(residualmomentum_file, index=False)
        logger.info(f"Saved ResidualMomentum predictor to {residualmomentum_file}")
        logger.info(f"ResidualMomentum: {len(residualmomentum_output)} observations")
        
        logger.info("Successfully completed ResidualMomentum6m and ResidualMomentum predictor calculation")
        
    except Exception as e:
        logger.error(f"Error in ResidualMomentum6m and ResidualMomentum calculation: {str(e)}")
        raise

if __name__ == "__main__":
    main()
