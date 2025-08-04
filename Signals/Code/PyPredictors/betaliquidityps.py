"""
Python equivalent of BetaLiquidityPS.do
Generated from: BetaLiquidityPS.do

Original Stata file: BetaLiquidityPS.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime
from sklearn.linear_model import LinearRegression

logger = logging.getLogger(__name__)

def betaliquidityps():
    """
    Python equivalent of BetaLiquidityPS.do
    
    Constructs the BetaLiquidityPS predictor signal using Pastor-Stambaugh liquidity beta.
    """
    logger.info("Constructing predictor signal: BetaLiquidityPS...")
    
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
        
        # Load monthly Fama-French data
        ff_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/monthlyFF.csv")
        
        logger.info(f"Loading monthly Fama-French data from: {ff_path}")
        
        if not ff_path.exists():
            logger.error(f"Monthly Fama-French data not found: {ff_path}")
            return False
        
        ff_data = pd.read_csv(ff_path, usecols=['time_avail_m', 'rf', 'mktrf', 'hml', 'smb'])
        logger.info(f"Successfully loaded Fama-French data with {len(ff_data)} records")
        
        # Load monthly liquidity data
        liquidity_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/monthlyLiquidity.csv")
        
        logger.info(f"Loading monthly liquidity data from: {liquidity_path}")
        
        if not liquidity_path.exists():
            logger.error(f"Monthly liquidity data not found: {liquidity_path}")
            return False
        
        liquidity_data = pd.read_csv(liquidity_path)
        logger.info(f"Successfully loaded liquidity data with {len(liquidity_data)} records")
        
        # Merge datasets
        data = data.merge(ff_data, on='time_avail_m', how='inner')
        data = data.merge(liquidity_data, on='time_avail_m', how='inner')
        logger.info(f"After merges: {len(data)} records")
        
        # Sort by permno and time_avail_m
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Convert time_avail_m to datetime
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Constructing BetaLiquidityPS signal...")
        
        # Calculate excess returns (equivalent to Stata's "gen retrf = ret - rf")
        data['retrf'] = data['ret'] - data['rf']
        
        # Create time index for each permno (equivalent to Stata's "bys permno (time_avail_m): gen time_temp = _n")
        data['time_temp'] = data.groupby('permno').cumcount() + 1
        
        # Calculate rolling liquidity beta using 60-month window with minimum 36 observations
        logger.info("Calculating rolling Pastor-Stambaugh liquidity betas...")
        
        beta_results = []
        
        for permno in data['permno'].unique():
            permno_data = data[data['permno'] == permno].copy()
            permno_data = permno_data.sort_values('time_temp')
            
            # Calculate rolling beta with 60-month window, minimum 36 observations
            for i in range(len(permno_data)):
                window_start = max(0, i - 59)  # 60-month window
                window_data = permno_data.iloc[window_start:i+1]
                
                if len(window_data) >= 36:  # Minimum 36 observations
                    # Run regression: retrf = alpha + beta1*ps_innov + beta2*mktrf + beta3*hml + beta4*smb
                    X = window_data[['ps_innov', 'mktrf', 'hml', 'smb']].values
                    y = window_data['retrf'].values
                    
                    try:
                        model = LinearRegression()
                        model.fit(X, y)
                        beta_liquidity = model.coef_[0]  # Coefficient on ps_innov
                        
                        beta_results.append({
                            'permno': permno,
                            'time_avail_m': permno_data.iloc[i]['time_avail_m'],
                            'BetaLiquidityPS': beta_liquidity
                        })
                    except:
                        # If regression fails, set beta to NaN
                        beta_results.append({
                            'permno': permno,
                            'time_avail_m': permno_data.iloc[i]['time_avail_m'],
                            'BetaLiquidityPS': np.nan
                        })
                else:
                    # Not enough observations
                    beta_results.append({
                        'permno': permno,
                        'time_avail_m': permno_data.iloc[i]['time_avail_m'],
                        'BetaLiquidityPS': np.nan
                    })
        
        # Convert results to DataFrame
        beta_df = pd.DataFrame(beta_results)
        logger.info("Successfully calculated BetaLiquidityPS signal")
        
        # SAVE RESULTS
        logger.info("Saving BetaLiquidityPS predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = beta_df.copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['BetaLiquidityPS'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "BetaLiquidityPS.csv"
        csv_data = output_data[['permno', 'yyyymm', 'BetaLiquidityPS']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved BetaLiquidityPS predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed BetaLiquidityPS predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct BetaLiquidityPS predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    betaliquidityps()
