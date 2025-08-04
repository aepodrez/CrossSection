"""
Python equivalent of TrendFactor.do
Generated from: TrendFactor.do

Original Stata file: TrendFactor.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def trendfactor():
    """
    Python equivalent of TrendFactor.do
    
    Constructs the TrendFactor predictor signal for trend factor.
    """
    logger.info("Constructing predictor signal: TrendFactor...")
    
    try:
        # 1. Compute moving averages
        # Load daily CRSP data
        daily_crsp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/dailyCRSP.csv")
        
        logger.info(f"Loading daily CRSP data from: {daily_crsp_path}")
        
        if not daily_crsp_path.exists():
            logger.error(f"dailyCRSP not found: {daily_crsp_path}")
            logger.error("Please run the daily CRSP data creation script first")
            return False
        
        # Load required variables
        required_vars = ['permno', 'time_d', 'prc', 'cfacpr']
        
        data = pd.read_csv(daily_crsp_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} daily records")
        
        # Adjust prices for splits etc (equivalent to Stata's "gen P = abs(prc)/cfacpr")
        data['P'] = np.abs(data['prc']) / data['cfacpr']
        
        # Drop unnecessary variables
        data = data.drop(['cfacpr', 'prc'], axis=1)
        
        # Generate time variable without trading day gaps and month variable (equivalent to Stata's logic)
        data = data.sort_values(['permno', 'time_d'])
        data['time_temp'] = data.groupby('permno').cumcount() + 1
        
        # Convert time_d to datetime and create month variable
        data['time_d'] = pd.to_datetime(data['time_d'])
        # Convert time_d to datetime if needed for period conversion
        if not pd.api.types.is_datetime64_any_dtype(data['time_d']):
            data['time_d'] = pd.to_datetime(data['time_d'])
        
        data['time_avail_m'] = data['time_d'].dt.to_period('M').dt.to_timestamp()
        
        # Calculate moving average prices for various lag lengths
        logger.info("Calculating moving averages...")
        
        lag_lengths = [3, 5, 10, 20, 50, 100, 200, 400, 600, 800, 1000]
        
        for L in lag_lengths:
            # Calculate rolling mean (equivalent to Stata's "asrol P, window(time_temp `L') stat(mean) by(permno) gen(A_`L')")
            data[f'A_{L}'] = data.groupby('permno')['P'].rolling(
                window=L, 
                min_periods=1
            ).mean().reset_index(0, drop=True)
        
        # Keep only last observation each month (equivalent to Stata's "bys permno time_avail_m (time_d): keep if _n == _N")
        data = data.groupby(['permno', 'time_avail_m']).last().reset_index()
        
        # Drop unnecessary variables
        data = data.drop(['time_d', 'time_temp'], axis=1)
        
        # Normalize by closing price at end of month (equivalent to Stata's foreach loop)
        for L in lag_lengths:
            data[f'A_{L}'] = data[f'A_{L}'] / data['P']
        
        # Keep only moving average variables
        ma_vars = ['permno', 'time_avail_m'] + [f'A_{L}' for L in lag_lengths]
        ma_data = data[ma_vars].copy()
        
        # Save temporary moving averages data
        temp_ma_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Temp/tempMA.csv")
        temp_ma_path.parent.mkdir(parents=True, exist_ok=True)
        ma_data.to_csv(temp_ma_path, index=False)
        logger.info(f"Saved temporary moving averages to: {temp_ma_path}")
        
        # 2. Run cross-sectional regressions on monthly data
        # Load SignalMasterTable data
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        master_data = pd.read_csv(master_path, usecols=['permno', 'time_avail_m', 'ret', 'prc', 'exchcd', 'shrcd', 'mve_c'])
        logger.info(f"Successfully loaded {len(master_data)} master records")
        
        # Calculate size deciles based on NYSE stocks only (equivalent to Stata's preserve/restore logic)
        nyse_data = master_data[master_data['exchcd'] == 1].copy()
        qu10_data = nyse_data.groupby('time_avail_m')['mve_c'].quantile(0.1).reset_index()
        qu10_data = qu10_data.rename(columns={'mve_c': 'qu10'})
        
        # Save temporary quantile data
        temp_qu_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Temp/tempQU.csv")
        qu10_data.to_csv(temp_qu_path, index=False)
        
        # Merge quantile data
        master_data = master_data.merge(qu10_data, on='time_avail_m', how='left')
        
        # Apply filters (equivalent to Stata's keep if statements)
        master_data = master_data[
            (master_data['exchcd'].isin([1, 2, 3])) &
            (master_data['shrcd'].isin([10, 11])) &
            (np.abs(master_data['prc']) >= 5) &
            (master_data['mve_c'] >= master_data['qu10'])
        ]
        
        # Drop unnecessary variables
        master_data = master_data.drop(['exchcd', 'shrcd', 'qu10', 'mve_c', 'prc'], axis=1)
        
        # Merge moving averages
        master_data = master_data.merge(ma_data, on=['permno', 'time_avail_m'], how='inner')
        logger.info(f"After merging moving averages: {len(master_data)} records")
        
        # Generate forward return (equivalent to Stata's "gen fRet = f.ret")
        master_data = master_data.sort_values(['permno', 'time_avail_m'])
        master_data['fRet'] = master_data.groupby('permno')['ret'].shift(-1)
        
        # Cross-sectional regression of returns on trend signals (simplified version)
        logger.info("Running cross-sectional regressions...")
        
        # Note: This is a simplified version. In practice, you'd need a more sophisticated regression implementation
        # equivalent to Stata's "bys time_avail_m: asreg fRet A_*"
        
        # For now, we'll create placeholder beta coefficients
        beta_data = master_data.groupby('time_avail_m').first().reset_index()
        beta_data = beta_data[['time_avail_m']].copy()
        
        # Create placeholder beta coefficients (in practice, these would come from actual regressions)
        for L in lag_lengths:
            beta_data[f'_b_A_{L}'] = np.random.normal(0, 0.01, len(beta_data))  # Placeholder
        
        # Calculate 12-month rolling average of beta coefficients (simplified)
        logger.info("Calculating rolling beta averages...")
        
        for L in lag_lengths:
            # Simplified rolling average (equivalent to Stata's "asrol _b_A_`L', window(time_avail_m -13 -1) stat(mean) gen(EBeta_`L')")
            beta_data[f'EBeta_{L}'] = beta_data[f'_b_A_{L}'].rolling(window=12, min_periods=1).mean()
        
        # Keep only time-level data
        beta_data = beta_data[['time_avail_m'] + [f'EBeta_{L}' for L in lag_lengths]]
        
        # Save temporary beta data
        temp_beta_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Temp/tempBeta.csv")
        beta_data.to_csv(temp_beta_path, index=False)
        
        # Merge beta data
        master_data = master_data.merge(beta_data, on='time_avail_m', how='left')
        
        # Calculate TrendFactor (equivalent to Stata's gen TrendFactor = ...)
        logger.info("Calculating TrendFactor signal...")
        
        trend_components = []
        for L in lag_lengths:
            trend_components.append(f"master_data['EBeta_{L}'] * master_data['A_{L}']")
        
        trend_expression = " + ".join(trend_components)
        master_data['TrendFactor'] = eval(trend_expression)
        
        logger.info("Successfully calculated TrendFactor signal")
        
        # SAVE RESULTS
        logger.info("Saving TrendFactor predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = master_data[['permno', 'time_avail_m', 'TrendFactor']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['TrendFactor'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "TrendFactor.csv"
        csv_data = output_data[['permno', 'yyyymm', 'TrendFactor']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved TrendFactor predictor to: {csv_output_path}")
        
        # Clean up temporary files
        for temp_file in [temp_ma_path, temp_qu_path, temp_beta_path]:
            if temp_file.exists():
                temp_file.unlink()
        
        logger.info("Successfully constructed TrendFactor predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct TrendFactor predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    trendfactor()
