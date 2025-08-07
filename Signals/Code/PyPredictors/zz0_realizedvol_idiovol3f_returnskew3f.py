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
        
        logger.info("Reading daily CRSP data...")
        data = pd.read_csv(daily_crsp_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} daily records")
        
        # Load daily Fama-French data
        daily_ff_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/dailyFF.csv")
        
        logger.info(f"Loading daily Fama-French data from: {daily_ff_path}")
        
        if not daily_ff_path.exists():
            logger.error(f"dailyFF not found: {daily_ff_path}")
            logger.error("Please run the daily Fama-French data creation script first")
            return False
        
        logger.info("Reading daily Fama-French data...")
        ff_data = pd.read_csv(daily_ff_path, usecols=['time_d', 'rf', 'mktrf', 'smb', 'hml'])
        logger.info(f"Successfully loaded {len(ff_data)} Fama-French records")
        
        # Merge with Fama-French data (equivalent to Stata's "merge m:1 time_d using "$pathDataIntermediate/dailyFF", nogenerate keep(match)keepusing(rf mktrf smb hml)")
        logger.info("Merging CRSP and Fama-French data...")
        data = data.merge(ff_data, on='time_d', how='inner')
        logger.info(f"After merging with Fama-French data: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating volatility and skewness signals...")
        
        # Replace return with excess return (equivalent to Stata's "replace ret = ret - rf")
        data['ret'] = data['ret'] - data['rf']
        
        # Drop risk-free rate
        data = data.drop('rf', axis=1)
        
        # Sort data (equivalent to Stata's "sort permno time_d")
        logger.info("Sorting data by permno and time_d...")
        data = data.sort_values(['permno', 'time_d'])
        
        # Create time_avail_m (equivalent to Stata's "gen time_avail_m = mofd(time_d)")
        logger.info("Converting dates to monthly periods...")
        data['time_d'] = pd.to_datetime(data['time_d'])
        data['time_avail_m'] = data['time_d'].dt.to_period('M').dt.to_timestamp()
        
        # Get FF3 residuals within each month (equivalent to Stata's "bys permno time_avail_m: asreg ret mktrf smb hml, fit")
        logger.info("Calculating FF3 residuals for each stock-month...")
        
        # Initialize residuals column
        data['_residuals'] = np.nan
        data['_Nobs'] = 0
        
        # Process each group separately for better memory management
        logger.info("Starting FF3 residual calculations...")
        start_time = datetime.now()
        
        # Get unique groups
        groups = data.groupby(['permno', 'time_avail_m'])
        total_groups = len(groups)
        logger.info(f"Processing {total_groups} stock-month groups...")
        
        processed_count = 0
        successful_groups = 0
        failed_groups = 0
        
        for (permno, time_avail_m), group in groups:
            if len(group) >= 15:  # Minimum observations requirement
                try:
                    # Prepare data for regression
                    X = group[['mktrf', 'smb', 'hml']].values
                    y = group['ret'].values
                    
                    # Remove any rows with NaN values
                    valid_mask = ~(np.isnan(X).any(axis=1) | np.isnan(y))
                    if valid_mask.sum() >= 15:
                        X_valid = X[valid_mask]
                        y_valid = y[valid_mask]
                        
                        # Fit regression
                        reg = LinearRegression(fit_intercept=True)
                        reg.fit(X_valid, y_valid)
                        
                        # Calculate residuals
                        residuals = y_valid - reg.predict(X_valid)
                        
                        # Store results
                        data.loc[group.index[valid_mask], '_residuals'] = residuals
                        data.loc[group.index, '_Nobs'] = valid_mask.sum()
                        successful_groups += 1
                    else:
                        failed_groups += 1
                except Exception as e:
                    failed_groups += 1
                    if processed_count % 10000 == 0:
                        logger.warning(f"Error in group {permno}-{time_avail_m}: {e}")
            else:
                failed_groups += 1
            
            processed_count += 1
            if processed_count % 10000 == 0:
                elapsed = (datetime.now() - start_time).total_seconds()
                logger.info(f"Processed {processed_count}/{total_groups} groups ({processed_count/total_groups*100:.1f}%) in {elapsed:.1f} seconds")
                logger.info(f"  Successful: {successful_groups}, Failed: {failed_groups}")
        
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(f"Completed FF3 residual calculations in {elapsed:.1f} seconds")
        logger.info(f"Final stats: Successful groups: {successful_groups}, Failed groups: {failed_groups}")
        
        # Keep only observations with sufficient data (equivalent to Stata's "keep if _Nobs >= 15")
        data = data[data['_Nobs'] >= 15]
        logger.info(f"After filtering for minimum observations: {len(data)} records")
        
        # Collapse into second and third moments (equivalent to Stata's gcollapse)
        logger.info("Calculating monthly volatility and skewness measures...")
        
        # Calculate monthly statistics using groupby operations
        monthly_stats = data.groupby(['permno', 'time_avail_m']).agg({
            'ret': 'std',  # RealizedVol
            '_residuals': ['std', lambda x: stats.skew(x) if len(x) > 2 else np.nan]  # IdioVol3F and ReturnSkew3F
        }).reset_index()
        
        # Flatten column names
        monthly_stats.columns = ['permno', 'time_avail_m', 'RealizedVol', 'IdioVol3F', 'ReturnSkew3F']
        
        logger.info("Successfully calculated volatility and skewness signals")
        logger.info(f"Generated {len(monthly_stats)} monthly observations")
        
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
        logger.info(f"Saved RealizedVol predictor to: {csv_output_path} ({len(realizedvol_data)} records)")
        
        # Save IdioVol3F
        idiovol3f_data = monthly_stats[['permno', 'time_avail_m', 'IdioVol3F']].copy()
        idiovol3f_data = idiovol3f_data.dropna(subset=['IdioVol3F'])
        idiovol3f_data['yyyymm'] = idiovol3f_data['time_avail_m'].dt.year * 100 + idiovol3f_data['time_avail_m'].dt.month
        csv_output_path = predictors_dir / "IdioVol3F.csv"
        idiovol3f_data[['permno', 'yyyymm', 'IdioVol3F']].to_csv(csv_output_path, index=False)
        logger.info(f"Saved IdioVol3F predictor to: {csv_output_path} ({len(idiovol3f_data)} records)")
        
        # Save ReturnSkew3F
        returnskew3f_data = monthly_stats[['permno', 'time_avail_m', 'ReturnSkew3F']].copy()
        returnskew3f_data = returnskew3f_data.dropna(subset=['ReturnSkew3F'])
        returnskew3f_data['yyyymm'] = returnskew3f_data['time_avail_m'].dt.year * 100 + returnskew3f_data['time_avail_m'].dt.month
        csv_output_path = predictors_dir / "ReturnSkew3F.csv"
        returnskew3f_data[['permno', 'yyyymm', 'ReturnSkew3F']].to_csv(csv_output_path, index=False)
        logger.info(f"Saved ReturnSkew3F predictor to: {csv_output_path} ({len(returnskew3f_data)} records)")
        
        logger.info("Successfully constructed all three predictor signals")
        return True
        
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"Failed to construct volatility and skewness predictors: {e}")
        logger.error(f"Detailed error: {error_details}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    zz0_realizedvol_idiovol3f_returnskew3f()
