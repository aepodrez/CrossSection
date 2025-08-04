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
        # Convert time_d to datetime if needed for period conversion
        if not pd.api.types.is_datetime64_any_dtype(data['time_d']):
            data['time_d'] = pd.to_datetime(data['time_d'])
        
        data['time_avail_m'] = data['time_d'].dt.to_period('M').dt.to_timestamp()
        
        # Get FF3 residuals within each month (equivalent to Stata's "bys permno time_avail_m: asreg ret mktrf smb hml, fit")
        # Note: This is a simplified version - in practice you'd need a more sophisticated regression implementation
        data['_residuals'] = np.nan
        data['_Nobs'] = 0
        
        # Add progress logging for the existing loop
        unique_permnos = data['permno'].unique()
        total_permnos = len(unique_permnos)
        logger.info(f"Starting FF3 residual calculations for {total_permnos} unique stocks...")
        
        # Progress tracking variables
        processed_permnos = 0
        successful_regressions = 0
        start_time = datetime.now()
        
        for permno in data['permno'].unique():
            # Progress logging every 1000 stocks or every 5%
            if processed_permnos % 1000 == 0 or processed_permnos % max(1, total_permnos // 20) == 0:
                elapsed = (datetime.now() - start_time).total_seconds()
                if processed_permnos > 0:
                    avg_time_per_stock = elapsed / processed_permnos
                    remaining_stocks = total_permnos - processed_permnos
                    eta_seconds = remaining_stocks * avg_time_per_stock
                    eta_minutes = eta_seconds / 60
                    logger.info(f"Progress: {processed_permnos}/{total_permnos} stocks ({processed_permnos/total_permnos*100:.1f}%) - "
                              f"Processed {successful_regressions} regressions - "
                              f"ETA: {eta_minutes:.1f} minutes")
                else:
                    logger.info(f"Progress: {processed_permnos}/{total_permnos} stocks ({processed_permnos/total_permnos*100:.1f}%) - "
                              f"Processed {successful_regressions} regressions")
            
            for time_avail_m in data[data['permno'] == permno]['time_avail_m'].unique():
                month_data = data[(data['permno'] == permno) & (data['time_avail_m'] == time_avail_m)]
                
                if len(month_data) >= 15:  # Minimum observations requirement
                    try:
                        # Simple linear regression: ret = a + b1*mktrf + b2*smb + b3*hml
                        X = month_data[['mktrf', 'smb', 'hml']].values
                        y = month_data['ret'].values
                        
                        # Add constant term
                        X = np.column_stack([np.ones(len(X)), X])
                        
                        # Calculate residuals
                        beta = np.linalg.lstsq(X, y, rcond=None)[0]
                        residuals = y - X @ beta
                        
                        # Store residuals and observation count
                        data.loc[month_data.index, '_residuals'] = residuals
                        data.loc[month_data.index, '_Nobs'] = len(month_data)
                        successful_regressions += 1
                    except:
                        continue
            
            processed_permnos += 1
        
        # Final progress report
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(f"Completed FF3 residual calculations:")
        logger.info(f"  - Processed {processed_permnos}/{total_permnos} stocks")
        logger.info(f"  - Successful {successful_regressions} regressions")
        logger.info(f"  - Total time: {elapsed/60:.1f} minutes")
        logger.info(f"  - Average time per stock: {elapsed/processed_permnos:.2f} seconds")
        
        # Keep only observations with sufficient data (equivalent to Stata's "keep if _Nobs >= 15")
        data = data[data['_Nobs'] >= 15]
        logger.info(f"After filtering for minimum observations: {len(data)} records")
        
        # Collapse into second and third moments (equivalent to Stata's gcollapse)
        logger.info("Calculating monthly volatility and skewness measures...")
        
        # Calculate monthly statistics
        monthly_stats = data.groupby(['permno', 'time_avail_m']).agg({
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
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(realizedvol_data['time_avail_m']):
            realizedvol_data['time_avail_m'] = pd.to_datetime(realizedvol_data['time_avail_m'])
        
        realizedvol_data['yyyymm'] = realizedvol_data['time_avail_m'].dt.year * 100 + realizedvol_data['time_avail_m'].dt.month
        csv_output_path = predictors_dir / "RealizedVol.csv"
        realizedvol_data[['permno', 'yyyymm', 'RealizedVol']].to_csv(csv_output_path, index=False)
        logger.info(f"Saved RealizedVol predictor to: {csv_output_path}")
        
        # Save IdioVol3F
        idiovol3f_data = monthly_stats[['permno', 'time_avail_m', 'IdioVol3F']].copy()
        idiovol3f_data = idiovol3f_data.dropna(subset=['IdioVol3F'])
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(idiovol3f_data['time_avail_m']):
            idiovol3f_data['time_avail_m'] = pd.to_datetime(idiovol3f_data['time_avail_m'])
        
        idiovol3f_data['yyyymm'] = idiovol3f_data['time_avail_m'].dt.year * 100 + idiovol3f_data['time_avail_m'].dt.month
        csv_output_path = predictors_dir / "IdioVol3F.csv"
        idiovol3f_data[['permno', 'yyyymm', 'IdioVol3F']].to_csv(csv_output_path, index=False)
        logger.info(f"Saved IdioVol3F predictor to: {csv_output_path}")
        
        # Save ReturnSkew3F
        returnskew3f_data = monthly_stats[['permno', 'time_avail_m', 'ReturnSkew3F']].copy()
        returnskew3f_data = returnskew3f_data.dropna(subset=['ReturnSkew3F'])
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(returnskew3f_data['time_avail_m']):
            returnskew3f_data['time_avail_m'] = pd.to_datetime(returnskew3f_data['time_avail_m'])
        
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
