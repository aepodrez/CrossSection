"""
Python equivalent of BetaTailRisk.do
Generated from: BetaTailRisk.do

Original Stata file: BetaTailRisk.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime
from sklearn.linear_model import LinearRegression

logger = logging.getLogger(__name__)

def betatailrisk():
    """
    Python equivalent of BetaTailRisk.do
    
    Constructs the BetaTailRisk predictor signal using tail risk beta estimation.
    """
    logger.info("Constructing predictor signal: BetaTailRisk...")
    
    try:
        # DATA LOAD
        # Load daily CRSP data for tail risk calculation
        daily_crsp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/dailyCRSP.csv")
        
        logger.info(f"Loading daily CRSP data from: {daily_crsp_path}")
        
        if not daily_crsp_path.exists():
            logger.error(f"Daily CRSP data not found: {daily_crsp_path}")
            return False
        
        daily_data = pd.read_csv(daily_crsp_path, usecols=['permno', 'time_d', 'ret'])
        logger.info(f"Successfully loaded {len(daily_data)} daily records")
        
        # Convert time_d to datetime and create time_avail_m
        daily_data['time_d'] = pd.to_datetime(daily_data['time_d'])
        # Convert time_d to datetime if needed for period conversion
        if not pd.api.types.is_datetime64_any_dtype(daily_data['time_d']):
            daily_data['time_d'] = pd.to_datetime(daily_data['time_d'])
        
        daily_data['time_avail_m'] = daily_data['time_d'].dt.to_period('M')
        
        # Calculate 5th percentile returns by month (equivalent to Stata's "gcollapse (p5) ret, by(time_avail_m)")
        logger.info("Calculating monthly 5th percentile returns...")
        monthly_p5 = daily_data.groupby('time_avail_m')['ret'].quantile(0.05).reset_index()
        monthly_p5 = monthly_p5.rename(columns={'ret': 'retp5'})
        
        # Merge back to daily data
        daily_data = daily_data.merge(monthly_p5, on='time_avail_m', how='left')
        
        # Keep only tail events (ret <= retp5)
        tail_data = daily_data[daily_data['ret'] <= daily_data['retp5']].copy()
        
        # Calculate tail excess returns (equivalent to Stata's "gen tailex = log(ret/retp5)")
        tail_data['tailex'] = np.log(tail_data['ret'] / tail_data['retp5'])
        
        # Calculate monthly average tail excess (equivalent to Stata's "gcollapse (mean) tailex, by(time_avail_m)")
        monthly_tail = tail_data.groupby('time_avail_m')['tailex'].mean().reset_index()
        
        # Save tail risk data
        tail_risk_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/TailRisk.csv")
        monthly_tail.to_csv(tail_risk_path, index=False)
        logger.info(f"Saved tail risk data to: {tail_risk_path}")
        
        # Load monthly CRSP data for beta regression
        monthly_crsp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/monthlyCRSP.csv")
        
        logger.info(f"Loading monthly CRSP data from: {monthly_crsp_path}")
        
        if not monthly_crsp_path.exists():
            logger.error(f"Monthly CRSP data not found: {monthly_crsp_path}")
            return False
        
        data = pd.read_csv(monthly_crsp_path, usecols=['permno', 'time_avail_m', 'ret', 'shrcd'])
        logger.info(f"Successfully loaded {len(data)} monthly records")
        
        # Convert time_avail_m to datetime before merge
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        monthly_tail['time_avail_m'] = pd.to_datetime(monthly_tail['time_avail_m'])
        
        # Merge with tail risk data
        data = data.merge(monthly_tail, on='time_avail_m', how='inner')
        logger.info(f"After merge: {len(data)} records")
        
        # Sort by permno and time_avail_m
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Constructing BetaTailRisk signal...")
        
        # Calculate rolling tail risk beta using 120-month window with minimum 72 observations
        logger.info("Calculating rolling tail risk betas...")
        
        beta_results = []
        
        for permno in data['permno'].unique():
            permno_data = data[data['permno'] == permno].copy()
            permno_data = permno_data.sort_values('time_avail_m')
            
            # Calculate rolling beta with 120-month window, minimum 72 observations
            for i in range(len(permno_data)):
                window_start = max(0, i - 119)  # 120-month window
                window_data = permno_data.iloc[window_start:i+1]
                
                if len(window_data) >= 72:  # Minimum 72 observations
                    # Run regression: ret = alpha + beta * tailex
                    X = window_data['tailex'].values.reshape(-1, 1)
                    y = window_data['ret'].values
                    
                    try:
                        model = LinearRegression()
                        model.fit(X, y)
                        beta_tail = model.coef_[0]
                        
                        beta_results.append({
                            'permno': permno,
                            'time_avail_m': permno_data.iloc[i]['time_avail_m'],
                            'BetaTailRisk': beta_tail,
                            'shrcd': permno_data.iloc[i]['shrcd']
                        })
                    except:
                        # If regression fails, set beta to NaN
                        beta_results.append({
                            'permno': permno,
                            'time_avail_m': permno_data.iloc[i]['time_avail_m'],
                            'BetaTailRisk': np.nan,
                            'shrcd': permno_data.iloc[i]['shrcd']
                        })
                else:
                    # Not enough observations
                    beta_results.append({
                        'permno': permno,
                        'time_avail_m': permno_data.iloc[i]['time_avail_m'],
                        'BetaTailRisk': np.nan,
                        'shrcd': permno_data.iloc[i]['shrcd']
                    })
        
        # Convert results to DataFrame
        beta_df = pd.DataFrame(beta_results)
        
        # Set to missing if shrcd > 11 (equivalent to Stata's "replace BetaTailRisk = . if shrcd > 11")
        beta_df.loc[beta_df['shrcd'] > 11, 'BetaTailRisk'] = np.nan
        
        logger.info("Successfully calculated BetaTailRisk signal")
        
        # SAVE RESULTS
        logger.info("Saving BetaTailRisk predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = beta_df[['permno', 'time_avail_m', 'BetaTailRisk']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['BetaTailRisk'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "BetaTailRisk.csv"
        csv_data = output_data[['permno', 'yyyymm', 'BetaTailRisk']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved BetaTailRisk predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed BetaTailRisk predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct BetaTailRisk predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    betatailrisk()
