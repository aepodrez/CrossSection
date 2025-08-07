"""
Python equivalent of ZZ2_IdioVolAHT.do
Generated from: ZZ2_IdioVolAHT.do

Original Stata file: ZZ2_IdioVolAHT.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime
from sklearn.linear_model import LinearRegression

logger = logging.getLogger(__name__)

def zz2_idiovolaht():
    """
    Python equivalent of ZZ2_IdioVolAHT.do
    
    Constructs the IdioVolAHT predictor signal.
    """
    logger.info("Constructing predictor signal: zz2_idiovolaht...")
    
    try:
        # DATA LOAD
        # Load daily CRSP data
        logger.info("Loading daily CRSP data")
        crsp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/dailyCRSP.csv")
        crsp_data = pd.read_csv(crsp_path, usecols=['permno', 'time_d', 'ret'])
        
        # Load daily Fama-French factors
        logger.info("Loading daily Fama-French factors")
        ff_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/dailyFF.csv")
        ff_data = pd.read_csv(ff_path, usecols=['time_d', 'rf', 'mktrf'])
        
        # Merge CRSP with Fama-French factors
        logger.info("Merging CRSP with Fama-French factors")
        data = crsp_data.merge(ff_data, on='time_d', how='inner')
        
        # Calculate excess returns
        data['ret'] = data['ret'] - data['rf']
        data = data.drop('rf', axis=1)
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating IdioVolAHT")
        
        # Create time index for each firm
        data = data.sort_values(['permno', 'time_d'])
        data['time_temp'] = data.groupby('permno').cumcount() + 1
        
        # Calculate idiosyncratic volatility using rolling regression
        # IdioVol as in HXZ citing Ali et al (2003)
        logger.info("Running rolling regressions for idiosyncratic volatility")
        
        # Initialize IdioVolAHT column
        data['IdioVolAHT'] = np.nan
        
        # Process each stock
        for permno in data['permno'].unique():
            stock_data = data[data['permno'] == permno].copy()
            
            if len(stock_data) >= 100:  # Need at least 100 observations
                for i in range(251, len(stock_data)):  # Start from 252nd observation
                    window_data = stock_data.iloc[i-251:i+1]  # 252-day window
                    
                    if len(window_data) == 252:  # Ensure full window
                        try:
                            # Prepare regression variables
                            X = window_data['mktrf'].values.reshape(-1, 1)
                            y = window_data['ret'].values
                            
                            # Run regression
                            model = LinearRegression()
                            model.fit(X, y)
                            y_pred = model.predict(X)
                            residuals = y - y_pred
                            
                            # Calculate RMSE (root mean square error)
                            rmse = np.sqrt(np.mean(residuals**2))
                            
                            # Store the RMSE for the last observation in the window
                            data.loc[window_data.index[-1], 'IdioVolAHT'] = rmse
                        except:
                            continue
        
        # Convert daily date to monthly date
        logger.info("Converting to monthly data")
        data['time_d'] = pd.to_datetime(data['time_d'])
        data['time_avail_m'] = data['time_d'].dt.to_period('M').dt.to_timestamp()
        
        # Sort and collapse to monthly data (keep last observation per month)
        data = data.sort_values(['permno', 'time_avail_m', 'time_d'])
        monthly_data = data.groupby(['permno', 'time_avail_m'])['IdioVolAHT'].last().reset_index()
        
        # Drop missing values
        monthly_data = monthly_data.dropna(subset=['IdioVolAHT'])
        
        # Prepare output data
        logger.info("Preparing output data")
        output_data = monthly_data.copy()
        output_data['yyyymm'] = output_data['time_avail_m'].dt.strftime('%Y%m').astype(int)
        output_data = output_data[['permno', 'yyyymm', 'IdioVolAHT']]
        
        # Save results
        logger.info("Saving results")
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        output_file = output_path / "idiovolaht.csv"
        output_data.to_csv(output_file, index=False)
        logger.info(f"Saved IdioVolAHT predictor to {output_file}")
        logger.info(f"IdioVolAHT: {len(output_data)} observations")
        
        logger.info("Successfully completed IdioVolAHT predictor calculation")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct predictor zz2_idiovolaht: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    zz2_idiovolaht()
