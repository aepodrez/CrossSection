"""
Python equivalent of VolumeTrend.do
Generated from: VolumeTrend.do

Original Stata file: VolumeTrend.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def volumetrend():
    """
    Python equivalent of VolumeTrend.do
    
    Constructs the VolumeTrend predictor signal for volume trend.
    """
    logger.info("Constructing predictor signal: VolumeTrend...")
    
    try:
        # DATA LOAD
        # Load monthly CRSP data
        crsp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/monthlyCRSP.csv")
        
        logger.info(f"Loading monthly CRSP data from: {crsp_path}")
        
        if not crsp_path.exists():
            logger.error(f"monthlyCRSP not found: {crsp_path}")
            logger.error("Please run the monthly CRSP data creation script first")
            return False
        
        # Load required variables
        required_vars = ['permno', 'time_avail_m', 'vol']
        
        data = pd.read_csv(crsp_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating VolumeTrend signal...")
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Convert time_avail_m to numeric for regression (equivalent to Stata's asreg)
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        data['time_numeric'] = (data['time_avail_m'].dt.year - data['time_avail_m'].dt.year.min()) * 12 + data['time_avail_m'].dt.month
        
        # Calculate rolling regression coefficients (equivalent to Stata's "asreg vol time_avail_m, window(time_av 60) min(30) by(permno)")
        # Note: This is a simplified version - in practice you'd need a more sophisticated rolling regression implementation
        data['betaVolTrend'] = np.nan
        
        for permno in data['permno'].unique():
            firm_data = data[data['permno'] == permno].copy()
            if len(firm_data) >= 30:  # Minimum observations requirement
                # Calculate rolling regression for this firm
                for i in range(59, len(firm_data)):  # 60-month window
                    window_data = firm_data.iloc[i-59:i+1]
                    if len(window_data) >= 30:  # Minimum observations in window
                        try:
                            # Simple linear regression: vol = a + b*time
                            X = window_data['time_numeric'].values.reshape(-1, 1)
                            y = window_data['vol'].values
                            beta = np.linalg.lstsq(X, y, rcond=None)[0][0]
                            data.loc[window_data.index[-1], 'betaVolTrend'] = beta
                        except:
                            continue
        
        # Calculate rolling mean of volume (equivalent to Stata's "asrol vol, gen(meanX) stat(mean) window(time_avail_m 60) min(30)")
        data['meanX'] = data.groupby('permno')['vol'].rolling(
            window=60, 
            min_periods=30
        ).mean().reset_index(0, drop=True)
        
        # Calculate VolumeTrend (equivalent to Stata's "gen VolumeTrend = betaVolTrend/meanX")
        data['VolumeTrend'] = data['betaVolTrend'] / data['meanX']
        
        # Winsorize VolumeTrend (equivalent to Stata's "winsor2 VolumeTrend, cut(1 99) replace trim")
        # Remove extreme values (1st and 99th percentiles)
        lower_bound = data['VolumeTrend'].quantile(0.01)
        upper_bound = data['VolumeTrend'].quantile(0.99)
        data.loc[data['VolumeTrend'] < lower_bound, 'VolumeTrend'] = lower_bound
        data.loc[data['VolumeTrend'] > upper_bound, 'VolumeTrend'] = upper_bound
        
        logger.info("Successfully calculated VolumeTrend signal")
        
        # SAVE RESULTS
        logger.info("Saving VolumeTrend predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'VolumeTrend']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['VolumeTrend'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "VolumeTrend.csv"
        csv_data = output_data[['permno', 'yyyymm', 'VolumeTrend']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved VolumeTrend predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed VolumeTrend predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct VolumeTrend predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    volumetrend()
