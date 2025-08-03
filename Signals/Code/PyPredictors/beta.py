"""
Python equivalent of Beta.do
Generated from: Beta.do

Original Stata file: Beta.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime
from sklearn.linear_model import LinearRegression

logger = logging.getLogger(__name__)

def beta():
    """
    Python equivalent of Beta.do
    
    Constructs the Beta predictor signal using CAPM beta estimation.
    """
    logger.info("Constructing predictor signal: Beta...")
    
    try:
        # DATA LOAD
        # Load monthly CRSP data
        crsp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/monthlyCRSP.csv")
        
        logger.info(f"Loading monthly CRSP data from: {crsp_path}")
        
        if not crsp_path.exists():
            logger.error(f"Monthly CRSP data not found: {crsp_path}")
            return False
        
        data = pd.read_csv(crsp_path, usecols=['permno', 'time_avail_m', 'ret'])
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Load monthly Fama-French data for risk-free rate
        ff_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/monthlyFF.csv")
        
        logger.info(f"Loading monthly Fama-French data from: {ff_path}")
        
        if not ff_path.exists():
            logger.error(f"Monthly Fama-French data not found: {ff_path}")
            return False
        
        ff_data = pd.read_csv(ff_path, usecols=['time_avail_m', 'rf'])
        logger.info(f"Successfully loaded Fama-French data with {len(ff_data)} records")
        
        # Load monthly market data
        market_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/monthlyMarket.csv")
        
        logger.info(f"Loading monthly market data from: {market_path}")
        
        if not market_path.exists():
            logger.error(f"Monthly market data not found: {market_path}")
            return False
        
        market_data = pd.read_csv(market_path, usecols=['time_avail_m', 'ewretd'])
        logger.info(f"Successfully loaded market data with {len(market_data)} records")
        
        # Merge datasets
        data = data.merge(ff_data, on='time_avail_m', how='inner')
        data = data.merge(market_data, on='time_avail_m', how='inner')
        logger.info(f"After merges: {len(data)} records")
        
        # Sort by permno and time_avail_m
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Convert time_avail_m to datetime
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Constructing Beta signal...")
        
        # Calculate excess returns (equivalent to Stata's "gen retrf = ret - rf")
        data['retrf'] = data['ret'] - data['rf']
        
        # Calculate market excess returns (equivalent to Stata's "gen ewmktrf = ewretd - rf")
        data['ewmktrf'] = data['ewretd'] - data['rf']
        
        # Create time index for each permno (equivalent to Stata's "bys permno (time_avail_m): gen time_temp = _n")
        data['time_temp'] = data.groupby('permno').cumcount() + 1
        
        # Calculate rolling beta using 60-month window with minimum 20 observations
        logger.info("Calculating rolling CAPM betas...")
        
        beta_results = []
        
        for permno in data['permno'].unique():
            permno_data = data[data['permno'] == permno].copy()
            permno_data = permno_data.sort_values('time_temp')
            
            # Calculate rolling beta with 60-month window, minimum 20 observations
            for i in range(len(permno_data)):
                window_start = max(0, i - 59)  # 60-month window
                window_data = permno_data.iloc[window_start:i+1]
                
                if len(window_data) >= 20:  # Minimum 20 observations
                    # Run CAPM regression: retrf = alpha + beta * ewmktrf
                    X = window_data['ewmktrf'].values.reshape(-1, 1)
                    y = window_data['retrf'].values
                    
                    try:
                        model = LinearRegression()
                        model.fit(X, y)
                        beta = model.coef_[0]
                        
                        beta_results.append({
                            'permno': permno,
                            'time_avail_m': permno_data.iloc[i]['time_avail_m'],
                            'Beta': beta
                        })
                    except:
                        # If regression fails, set beta to NaN
                        beta_results.append({
                            'permno': permno,
                            'time_avail_m': permno_data.iloc[i]['time_avail_m'],
                            'Beta': np.nan
                        })
                else:
                    # Not enough observations
                    beta_results.append({
                        'permno': permno,
                        'time_avail_m': permno_data.iloc[i]['time_avail_m'],
                        'Beta': np.nan
                    })
        
        # Convert results to DataFrame
        beta_df = pd.DataFrame(beta_results)
        logger.info("Successfully calculated Beta signal")
        
        # SAVE RESULTS
        logger.info("Saving Beta predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = beta_df.copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['Beta'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "Beta.csv"
        csv_data = output_data[['permno', 'yyyymm', 'Beta']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved Beta predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed Beta predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct Beta predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    beta()
