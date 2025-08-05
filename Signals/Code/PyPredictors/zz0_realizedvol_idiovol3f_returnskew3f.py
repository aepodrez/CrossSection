"""
Python equivalent of ZZ0_RealizedVol_IdioVol3F_ReturnSkew3F.do
Generated from: ZZ0_RealizedVol_IdioVol3F_ReturnSkew3F.do

Original Stata file: ZZ0_RealizedVol_IdioVol3F_ReturnSkew3F.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime
from scipy import stats
from sklearn.linear_model import LinearRegression

logger = logging.getLogger(__name__)

def zz0_realizedvol_idiovol3f_returnskew3f():
    """
    Python equivalent of ZZ0_RealizedVol_IdioVol3F_ReturnSkew3F.do
    
    Constructs the RealizedVol, IdioVol3F, and ReturnSkew3F predictor signals.
    """
    logger.info("Constructing predictor signals: RealizedVol, IdioVol3F, ReturnSkew3F...")
    
    try:
        # DATA LOAD
        # Load daily CRSP data
        daily_crsp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/dailyCRSP.csv")
        
        logger.info(f"Loading daily CRSP data from: {daily_crsp_path}")
        
        if not daily_crsp_path.exists():
            logger.error(f"dailyCRSP not found: {daily_crsp_path}")
            logger.error("Please run the daily CRSP data creation script first")
            return False
        
        # Load required variables
        required_vars = ['permno', 'time_d', 'ret']
        
        data = pd.read_csv(daily_crsp_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} daily records")
        
        # Load daily Fama-French data
        daily_ff_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/dailyFF.csv")
        
        logger.info(f"Loading daily Fama-French data from: {daily_ff_path}")
        
        if not daily_ff_path.exists():
            logger.error(f"dailyFF not found: {daily_ff_path}")
            logger.error("Please run the daily Fama-French data creation script first")
            return False
        
        ff_data = pd.read_csv(daily_ff_path, usecols=['time_d', 'rf', 'mktrf', 'smb', 'hml'])
        
        # Merge with Fama-French data (equivalent to Stata's "merge m:1 time_d using "$pathDataIntermediate/dailyFF", nogenerate keep(match)keepusing(rf mktrf smb hml)")
        data = data.merge(ff_data, on='time_d', how='inner')
        logger.info(f"After merging with Fama-French data: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating volatility and skewness signals...")
        
        # Replace return with excess return (equivalent to Stata's "replace ret = ret - rf")
        data['ret'] = data['ret'] - data['rf']
        
        # Drop risk-free rate
        data = data.drop('rf', axis=1)
        
        # Sort data (equivalent to Stata's "sort permno time_d")
        data = data.sort_values(['permno', 'time_d'])
        
        # Create time_avail_m (equivalent to Stata's "gen time_avail_m = mofd(time_d)")
        data['time_d'] = pd.to_datetime(data['time_d'])
        data['time_avail_m'] = data['time_d'].dt.to_period('M').dt.to_timestamp()
        
        # Get FF3 residuals within each month (equivalent to Stata's "bys permno time_avail_m: asreg ret mktrf smb hml, fit")
        logger.info("Calculating FF3 residuals for each stock-month...")
        
        # Function to calculate residuals for a group
        def calculate_residuals(group):
            if len(group) < 15:  # Minimum observations requirement
                return pd.Series({'ret': group['ret'], '_residuals': np.nan, '_Nobs': len(group)})
            
            try:
                # Prepare data for regression
                X = group[['mktrf', 'smb', 'hml']].values
                y = group['ret'].values
                
                # Fit regression
                reg = LinearRegression(fit_intercept=True)
                reg.fit(X, y)
                
                # Calculate residuals
                residuals = y - reg.predict(X)
                
                return pd.Series({
                    'ret': group['ret'],
                    '_residuals': residuals,
                    '_Nobs': len(group)
                })
            except:
                return pd.Series({'ret': group['ret'], '_residuals': np.nan, '_Nobs': len(group)})
        
        # Apply regression to each group efficiently
        logger.info("Starting FF3 residual calculations...")
        start_time = datetime.now()
        
        # Group by permno and time_avail_m and apply regression
        grouped_data = data.groupby(['permno', 'time_avail_m']).apply(calculate_residuals).reset_index()
        
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(f"Completed FF3 residual calculations in {elapsed:.1f} seconds")
        
        # Keep only observations with sufficient data (equivalent to Stata's "keep if _Nobs >= 15")
        grouped_data = grouped_data[grouped_data['_Nobs'] >= 15]
        logger.info(f"After filtering for minimum observations: {len(grouped_data)} records")
        
        # Collapse into second and third moments (equivalent to Stata's gcollapse)
        logger.info("Calculating monthly volatility and skewness measures...")
        
        # Calculate monthly statistics using vectorized operations
        monthly_stats = grouped_data.groupby(['permno', 'time_avail_m']).agg({
            'ret': 'std',  # RealizedVol
            '_residuals': ['std', lambda x: stats.skew(x) if len(x) > 2 else np.nan]  # IdioVol3F and ReturnSkew3F
        }).reset_index()
        
        # Flatten column names
        monthly_stats.columns = ['permno', 'time_avail_m', 'RealizedVol', 'IdioVol3F', 'ReturnSkew3F']
        
        logger.info("Successfully calculated volatility and skewness signals")
        
        # SAVE RESULTS
        logger.info("Saving predictor signals...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Save RealizedVol
        realizedvol_data = monthly_stats[['permno', 'time_avail_m', 'RealizedVol']].copy()
        realizedvol_data = realizedvol_data.dropna(subset=['RealizedVol'])
        realizedvol_data['yyyymm'] = realizedvol_data['time_avail_m'].dt.year * 100 + realizedvol_data['time_avail_m'].dt.month
        csv_output_path = predictors_dir / "RealizedVol.csv"
        realizedvol_data[['permno', 'yyyymm', 'RealizedVol']].to_csv(csv_output_path, index=False)
        logger.info(f"Saved RealizedVol predictor to: {csv_output_path}")
        
        # Save IdioVol3F
        idiovol3f_data = monthly_stats[['permno', 'time_avail_m', 'IdioVol3F']].copy()
        idiovol3f_data = idiovol3f_data.dropna(subset=['IdioVol3F'])
        idiovol3f_data['yyyymm'] = idiovol3f_data['time_avail_m'].dt.year * 100 + idiovol3f_data['time_avail_m'].dt.month
        csv_output_path = predictors_dir / "IdioVol3F.csv"
        idiovol3f_data[['permno', 'yyyymm', 'IdioVol3F']].to_csv(csv_output_path, index=False)
        logger.info(f"Saved IdioVol3F predictor to: {csv_output_path}")
        
        # Save ReturnSkew3F
        returnskew3f_data = monthly_stats[['permno', 'time_avail_m', 'ReturnSkew3F']].copy()
        returnskew3f_data = returnskew3f_data.dropna(subset=['ReturnSkew3F'])
        returnskew3f_data['yyyymm'] = returnskew3f_data['time_avail_m'].dt.year * 100 + returnskew3f_data['time_avail_m'].dt.month
        csv_output_path = predictors_dir / "ReturnSkew3F.csv"
        returnskew3f_data[['permno', 'yyyymm', 'ReturnSkew3F']].to_csv(csv_output_path, index=False)
        logger.info(f"Saved ReturnSkew3F predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed all three predictor signals")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct volatility and skewness predictors: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    zz0_realizedvol_idiovol3f_returnskew3f()
