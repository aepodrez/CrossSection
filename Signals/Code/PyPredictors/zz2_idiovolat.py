"""
ZZ2_IdioVolat Predictor Implementation

This script implements the IdioVolAHT predictor:
- IdioVolAHT: Idiosyncratic volatility measure based on Ali et al (2003)

The script calculates idiosyncratic volatility using the root mean squared error
from CAPM regressions, following the methodology cited in HXZ.
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Main function to calculate IdioVolAHT predictor."""
    
    # Define file paths
    base_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data")
    daily_crsp_path = base_path / "Intermediate" / "dailyCRSP.csv"
    daily_ff_path = base_path / "Intermediate" / "dailyFF.csv"
    output_path = base_path / "Predictors"
    
    # Ensure output directory exists
    output_path.mkdir(parents=True, exist_ok=True)
    
    logger.info("Starting IdioVolAHT predictor calculation")
    
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
        
        # Convert time_d to datetime
        data['time_d'] = pd.to_datetime(data['time_d'])
        
        # Sort by permno and time_d
        data = data.sort_values(['permno', 'time_d'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating idiosyncratic volatility")
        
        # Create time index for each permno
        data['time_temp'] = data.groupby('permno').cumcount() + 1
        
        # Initialize IdioVolAHT
        data['IdioVolAHT'] = np.nan
        
        # Run rolling regressions for each firm
        logger.info("Running rolling CAPM regressions")
        for permno in data['permno'].unique():
            firm_data = data[data['permno'] == permno].copy()
            
            if len(firm_data) >= 100:  # Need at least 100 observations
                for i in range(251, len(firm_data)):  # Start from 252nd observation
                    window_data = firm_data.iloc[i-251:i+1]  # 252-day window
                    
                    if len(window_data) == 252:  # Ensure full window
                        try:
                            # Prepare regression variables
                            valid_data = window_data.dropna(subset=['ret', 'mktrf'])
                            
                            if len(valid_data) >= 100:  # Need at least 100 valid observations
                                X = np.column_stack([
                                    np.ones(len(valid_data)),
                                    valid_data['mktrf'].values
                                ])
                                y = valid_data['ret'].values
                                
                                # Run CAPM regression: ret = α + β*mktrf + ε
                                beta = np.linalg.lstsq(X, y, rcond=None)[0]
                                fitted_values = X @ beta
                                residuals = y - fitted_values
                                
                                # Calculate root mean squared error (RMSE)
                                rmse = np.sqrt(np.mean(residuals ** 2))
                                
                                # Store the idiosyncratic volatility
                                data.loc[window_data.index[-1], 'IdioVolAHT'] = rmse
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
        
        # For IdioVolAHT
        idiovolat_output = monthly_data[['permno', 'time_avail_m', 'IdioVolAHT']].copy()
        idiovolat_output = idiovolat_output.dropna(subset=['IdioVolAHT'])
        idiovolat_output['yyyymm'] = idiovolat_output['time_avail_m'].dt.strftime('%Y%m').astype(int)
        idiovolat_output = idiovolat_output[['permno', 'yyyymm', 'IdioVolAHT']]
        
        # Save results
        logger.info("Saving results")
        
        # Save IdioVolAHT
        idiovolat_file = output_path / "IdioVolAHT.csv"
        idiovolat_output.to_csv(idiovolat_file, index=False)
        logger.info(f"Saved IdioVolAHT predictor to {idiovolat_file}")
        logger.info(f"IdioVolAHT: {len(idiovolat_output)} observations")
        
        logger.info("Successfully completed IdioVolAHT predictor calculation")
        
    except Exception as e:
        logger.error(f"Error in IdioVolAHT calculation: {str(e)}")
        raise

if __name__ == "__main__":
    main() 