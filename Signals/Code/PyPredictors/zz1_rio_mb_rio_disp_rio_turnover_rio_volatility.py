"""
ZZ1_RIO_MB_RIO_Disp_RIO_Turnover_RIO_Volatility Predictor Implementation

This script implements four residual institutional ownership (RIO) predictors:
- RIO_MB: Institutional ownership and market-to-book
- RIO_Disp: Institutional ownership and forecast dispersion
- RIO_Turnover: Institutional ownership and turnover
- RIO_Volatility: Institutional ownership and volatility

The script calculates RIO measures and combines them with various firm characteristics
to create interaction-based predictors.
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def zz1_rio_mb_rio_disp_rio_turnover_rio_volatility():
    """Main function to calculate RIO_MB, RIO_Disp, RIO_Turnover, and RIO_Volatility predictors."""
    
    # Define file paths
    base_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data")
    ibes_path = base_path / "Intermediate" / "IBES_EPS_Unadj.csv"
    master_path = base_path / "Intermediate" / "SignalMasterTable.csv"
    tr13f_path = base_path / "Intermediate" / "TR_13F.csv"
    compustat_path = base_path / "Intermediate" / "m_aCompustat.csv"
    crsp_path = base_path / "Intermediate" / "monthlyCRSP.csv"
    temp_path = base_path / "Temp" / "tempIBES.csv"
    output_path = base_path / "Predictors"
    
    # Ensure directories exist
    temp_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.mkdir(parents=True, exist_ok=True)
    
    logger.info("Starting RIO_MB, RIO_Disp, RIO_Turnover, RIO_Volatility predictor calculation")
    
    try:
        # Prep IBES data
        logger.info("Preparing IBES data")
        ibes_data = pd.read_csv(ibes_path)
        ibes_data = ibes_data[ibes_data['fpi'] == '1']
        ibes_data.to_csv(temp_path, index=False)
        
        # DATA LOAD
        logger.info("Loading SignalMasterTable data")
        required_vars = ['permno', 'tickerIBES', 'time_avail_m', 'exchcd', 'mve_c']
        data = pd.read_csv(master_path, usecols=required_vars)
        
        # Merge with TR_13F for institutional ownership
        logger.info("Merging with TR_13F for institutional ownership")
        tr13f_data = pd.read_csv(tr13f_path, usecols=['permno', 'time_avail_m', 'instown_perc'])
        data = data.merge(tr13f_data, on=['permno', 'time_avail_m'], how='inner')
        
        # Merge with Compustat
        logger.info("Merging with Compustat data")
        compustat_vars = ['permno', 'time_avail_m', 'at', 'ceq', 'txditc']
        compustat_data = pd.read_csv(compustat_path, usecols=compustat_vars)
        data = data.merge(compustat_data, on=['permno', 'time_avail_m'], how='inner')
        
        # Merge with monthly CRSP
        logger.info("Merging with monthly CRSP")
        crsp_vars = ['permno', 'time_avail_m', 'vol', 'shrout', 'ret']
        crsp_data = pd.read_csv(crsp_path, usecols=crsp_vars)
        data = data.merge(crsp_data, on=['permno', 'time_avail_m'], how='inner')
        
        # Merge with IBES data
        logger.info("Merging with IBES data")
        ibes_temp_data = pd.read_csv(temp_path, usecols=['tickerIBES', 'time_avail_m', 'stdev'])
        data = data.merge(ibes_temp_data, on=['tickerIBES', 'time_avail_m'], how='inner')
        
        # Convert time_avail_m to datetime
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        
        # Filter below 20th percentile NYSE market equity
        logger.info("Applying size filter")
        nyse_data = data[data['exchcd'].isin([1, 2])].copy()
        nyse_data['sizecat'] = nyse_data.groupby('time_avail_m')['mve_c'].transform(
            lambda x: pd.qcut(x, q=5, labels=False, duplicates='drop') + 1
        )
        data = data.merge(nyse_data[['permno', 'time_avail_m', 'sizecat']], on=['permno', 'time_avail_m'], how='left')
        data = data[data['sizecat'] != 1]  # Drop smallest size category
        data = data.drop('sizecat', axis=1)
        
        # Sort by permno and time_avail_m
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating residual institutional ownership")
        
        # Calculate RIO (Residual Institutional Ownership)
        data['temp'] = data['instown_perc'] / 100
        data['temp'] = data['temp'].fillna(0)
        data.loc[data['temp'] > 0.9999, 'temp'] = 0.9999
        data.loc[data['temp'] < 0.0001, 'temp'] = 0.0001
        
        data['RIO'] = np.log(data['temp'] / (1 - data['temp'])) + 23.66 - 2.89 * np.log(data['mve_c']) + 0.08 * (np.log(data['mve_c'])) ** 2
        
        # Calculate lagged RIO and create quintiles
        data['RIOlag'] = data.groupby('permno')['RIO'].shift(6)
        data['cat_RIO'] = data.groupby('time_avail_m')['RIOlag'].transform(
            lambda x: pd.qcut(x, q=5, labels=False, duplicates='drop') + 1
        )
        
        # Calculate firm characteristics
        logger.info("Calculating firm characteristics")
        
        # Market-to-book ratio
        data['txditc'] = data['txditc'].fillna(0)
        data['MB'] = data['mve_c'] / (data['ceq'] + data['txditc'])
        data.loc[(data['ceq'] + data['txditc']) < 0, 'MB'] = np.nan
        
        # Forecast dispersion
        data['Disp'] = np.nan
        data.loc[data['stdev'] > 0, 'Disp'] = data.loc[data['stdev'] > 0, 'stdev'] / data.loc[data['stdev'] > 0, 'at']
        
        # Turnover
        data['Turnover'] = data['vol'] / data['shrout']
        
        # Volatility (12-month rolling standard deviation of returns)
        data['Volatility'] = data.groupby('permno')['ret'].rolling(window=12, min_periods=6).std().reset_index(0, drop=True)
        
        # Create quintiles for each characteristic
        characteristics = ['MB', 'Disp', 'Volatility', 'Turnover']
        for char in characteristics:
            data[f'cat_{char}'] = data.groupby('time_avail_m')[char].transform(
                lambda x: pd.qcut(x, q=5, labels=False, duplicates='drop') + 1
            )
        
        # Create RIO interaction measures
        logger.info("Creating RIO interaction measures")
        
        # RIO_MB: Institutional ownership and market-to-book
        data['RIO_MB'] = np.nan
        data.loc[data['cat_MB'] == 5, 'RIO_MB'] = data.loc[data['cat_MB'] == 5, 'cat_RIO']
        
        # RIO_Disp: Institutional ownership and forecast dispersion
        data['RIO_Disp'] = np.nan
        data.loc[data['cat_Disp'] == 5, 'RIO_Disp'] = data.loc[data['cat_Disp'] == 5, 'cat_RIO']
        # Patch for Dispersion: also include 4th quintile
        data.loc[data['cat_Disp'] >= 4, 'RIO_Disp'] = data.loc[data['cat_Disp'] >= 4, 'cat_RIO']
        
        # RIO_Turnover: Institutional ownership and turnover
        data['RIO_Turnover'] = np.nan
        data.loc[data['cat_Turnover'] == 5, 'RIO_Turnover'] = data.loc[data['cat_Turnover'] == 5, 'cat_RIO']
        
        # RIO_Volatility: Institutional ownership and volatility
        data['RIO_Volatility'] = np.nan
        data.loc[data['cat_Volatility'] == 5, 'RIO_Volatility'] = data.loc[data['cat_Volatility'] == 5, 'cat_RIO']
        
        # Prepare output data
        logger.info("Preparing output data")
        
        # For RIO_MB
        rio_mb_output = data[['permno', 'time_avail_m', 'RIO_MB']].copy()
        rio_mb_output = rio_mb_output.dropna(subset=['RIO_MB'])
        # Convert time_avail_m to datetime if needed for strftime
        if not pd.api.types.is_datetime64_any_dtype(rio_mb_output['time_avail_m']):
            rio_mb_output['time_avail_m'] = pd.to_datetime(rio_mb_output['time_avail_m'])
        
        rio_mb_output['yyyymm'] = rio_mb_output['time_avail_m'].dt.strftime('%Y%m').astype(int)
        rio_mb_output = rio_mb_output[['permno', 'yyyymm', 'RIO_MB']]
        
        # For RIO_Disp
        rio_disp_output = data[['permno', 'time_avail_m', 'RIO_Disp']].copy()
        rio_disp_output = rio_disp_output.dropna(subset=['RIO_Disp'])
        # Convert time_avail_m to datetime if needed for strftime
        if not pd.api.types.is_datetime64_any_dtype(rio_disp_output['time_avail_m']):
            rio_disp_output['time_avail_m'] = pd.to_datetime(rio_disp_output['time_avail_m'])
        
        rio_disp_output['yyyymm'] = rio_disp_output['time_avail_m'].dt.strftime('%Y%m').astype(int)
        rio_disp_output = rio_disp_output[['permno', 'yyyymm', 'RIO_Disp']]
        
        # For RIO_Turnover
        rio_turnover_output = data[['permno', 'time_avail_m', 'RIO_Turnover']].copy()
        rio_turnover_output = rio_turnover_output.dropna(subset=['RIO_Turnover'])
        # Convert time_avail_m to datetime if needed for strftime
        if not pd.api.types.is_datetime64_any_dtype(rio_turnover_output['time_avail_m']):
            rio_turnover_output['time_avail_m'] = pd.to_datetime(rio_turnover_output['time_avail_m'])
        
        rio_turnover_output['yyyymm'] = rio_turnover_output['time_avail_m'].dt.strftime('%Y%m').astype(int)
        rio_turnover_output = rio_turnover_output[['permno', 'yyyymm', 'RIO_Turnover']]
        
        # For RIO_Volatility
        rio_volatility_output = data[['permno', 'time_avail_m', 'RIO_Volatility']].copy()
        rio_volatility_output = rio_volatility_output.dropna(subset=['RIO_Volatility'])
        # Convert time_avail_m to datetime if needed for strftime
        if not pd.api.types.is_datetime64_any_dtype(rio_volatility_output['time_avail_m']):
            rio_volatility_output['time_avail_m'] = pd.to_datetime(rio_volatility_output['time_avail_m'])
        
        rio_volatility_output['yyyymm'] = rio_volatility_output['time_avail_m'].dt.strftime('%Y%m').astype(int)
        rio_volatility_output = rio_volatility_output[['permno', 'yyyymm', 'RIO_Volatility']]
        
        # Save results
        logger.info("Saving results")
        
        # Save RIO_MB
        rio_mb_file = output_path / "RIO_MB.csv"
        rio_mb_output.to_csv(rio_mb_file, index=False)
        logger.info(f"Saved RIO_MB predictor to {rio_mb_file}")
        logger.info(f"RIO_MB: {len(rio_mb_output)} observations")
        
        # Save RIO_Disp
        rio_disp_file = output_path / "RIO_Disp.csv"
        rio_disp_output.to_csv(rio_disp_file, index=False)
        logger.info(f"Saved RIO_Disp predictor to {rio_disp_file}")
        logger.info(f"RIO_Disp: {len(rio_disp_output)} observations")
        
        # Save RIO_Turnover
        rio_turnover_file = output_path / "RIO_Turnover.csv"
        rio_turnover_output.to_csv(rio_turnover_file, index=False)
        logger.info(f"Saved RIO_Turnover predictor to {rio_turnover_file}")
        logger.info(f"RIO_Turnover: {len(rio_turnover_output)} observations")
        
        # Save RIO_Volatility
        rio_volatility_file = output_path / "RIO_Volatility.csv"
        rio_volatility_output.to_csv(rio_volatility_file, index=False)
        logger.info(f"Saved RIO_Volatility predictor to {rio_volatility_file}")
        logger.info(f"RIO_Volatility: {len(rio_volatility_output)} observations")
        
        # Clean up temporary file
        if temp_path.exists():
            temp_path.unlink()
        
        logger.info("Successfully completed RIO_MB, RIO_Disp, RIO_Turnover, RIO_Volatility predictor calculation")
        
    except Exception as e:
        logger.error(f"Error in RIO_MB, RIO_Disp, RIO_Turnover, RIO_Volatility calculation: {str(e)}")
        raise

if __name__ == "__main__":
    main()
