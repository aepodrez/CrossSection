"""
Python equivalent of MS.do
Generated from: MS.do

Original Stata file: MS.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def ms():
    """
    Python equivalent of MS.do
    
    Constructs the MS predictor signal for Mohanram G-score.
    """
    logger.info("Constructing predictor signal: MS...")
    
    try:
        # DATA LOAD
        # Load annual Compustat data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading annual Compustat data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"m_aCompustat not found: {compustat_path}")
            logger.error("Please run the Compustat data creation script first")
            return False
        
        # Load required variables
        required_vars = ['permno', 'gvkey', 'time_avail_m', 'datadate', 'at', 'ceq', 'ni', 'oancf', 'fopt', 'wcapch', 'ib', 'dp', 'xrd', 'capx', 'xad', 'revt']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'])
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # Load SignalMasterTable for market value and SIC codes
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        master_data = pd.read_csv(master_path, usecols=['permno', 'gvkey', 'time_avail_m', 'mve_c', 'sicCRSP'])
        
        # Merge with SignalMasterTable
        data = data.merge(master_data, on=['permno', 'gvkey', 'time_avail_m'], how='inner')
        logger.info(f"After merging with SignalMasterTable: {len(data)} records")
        
        # Load quarterly Compustat data
        qcompustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_QCompustat.csv")
        
        logger.info(f"Loading quarterly Compustat data from: {qcompustat_path}")
        
        if not qcompustat_path.exists():
            logger.error(f"m_QCompustat not found: {qcompustat_path}")
            logger.error("Please run the quarterly Compustat data creation script first")
            return False
        
        qcompustat_data = pd.read_csv(qcompustat_path, usecols=['gvkey', 'time_avail_m', 'niq', 'atq', 'saleq', 'oancfy', 'capxy', 'xrdq', 'fyearq', 'fqtr', 'datafqtr', 'datadateq'])
        
        # Merge with quarterly data
        data = data.merge(qcompustat_data, on=['gvkey', 'time_avail_m'], how='left')
        logger.info(f"After merging with quarterly data: {len(data)} records")
        
        # SAMPLE SELECTION
        logger.info("Applying sample selection criteria...")
        
        # Calculate book-to-market ratio
        data['BM'] = np.log(data['ceq'] / data['mve_c'])
        
        # Create quintiles and keep only lowest BM quintile
        data['temp'] = data.groupby('time_avail_m')['BM'].transform(
            lambda x: pd.qcut(x, q=5, labels=False, duplicates='drop') + 1
        )
        data = data[(data['temp'] == 1) & (data['ceq'] > 0)]
        data = data.drop('temp', axis=1)
        logger.info(f"After BM quintile filter: {len(data)} records")
        
        # Keep only if at least 3 firms in SIC2D
        data['sicCRSP'] = data['sicCRSP'].astype(str)
        data['sic2D'] = data['sicCRSP'].str[:2]
        data['tempN'] = data.groupby(['sic2D', 'time_avail_m'])['at'].transform('count')
        data = data[data['tempN'] >= 3]
        data = data.drop('tempN', axis=1)
        logger.info(f"After SIC2D filter: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating MS signal...")
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Prepare variables
        data['xad'] = data['xad'].fillna(0)
        data['xrdq'] = data['xrdq'].fillna(0)
        
        # Calculate quarterly capital expenditures
        data['capxq'] = np.where(data['fqtr'] == 1, data['capxy'], 
                                np.where(data['fqtr'] > 1, data['capxy'] - data.groupby('permno')['capxy'].shift(3), np.nan))
        
        # Calculate quarterly operating cash flow
        data['oancfq'] = np.where(data['fqtr'] == 1, data['oancfy'],
                                 np.where(data['fqtr'] > 1, data['oancfy'] - data.groupby('permno')['oancfy'].shift(3), np.nan))
        
        # Aggregate quarterly variables (12-month rolling mean)
        for var in ['niq', 'xrdq', 'oancfq', 'capxq']:
            data[f'{var}sum'] = data.groupby('permno')[var].rolling(window=12, min_periods=12).mean().reset_index(0, drop=True)
            data[f'{var}sum'] = data[f'{var}sum'] * 4  # Annualize
        
        # Special handling for oancfqsum for years <= 1988
        data.loc[data['datadate'].dt.year <= 1988, 'oancfqsum'] = data.loc[data['datadate'].dt.year <= 1988, 'fopt'] - data.loc[data['datadate'].dt.year <= 1988, 'wcapch']
        
        # Calculate profitability and cash flow signals
        data['atdenom'] = (data['atq'] + data.groupby('permno')['atq'].shift(3)) / 2
        
        data['roa'] = data['niqsum'] / data['atdenom']
        data['cfroa'] = data['oancfqsum'] / data['atdenom']
        
        # Calculate industry medians
        for var in ['roa', 'cfroa']:
            data[f'md_{var}'] = data.groupby(['sic2D', 'time_avail_m'])[var].transform('median')
        
        # Create binary signals
        data['m1'] = (data['roa'] > data['md_roa']).astype(int)
        data['m2'] = (data['cfroa'] > data['md_cfroa']).astype(int)
        data['m3'] = (data['oancfqsum'] > data['niqsum']).astype(int)
        
        # Calculate volatility measures
        data['roaq'] = data['niq'] / data['atq']
        data['sg'] = data['saleq'] / data.groupby('permno')['saleq'].shift(3)
        
        # Rolling standard deviations
        data['niVol'] = data.groupby('permno')['roaq'].rolling(window=48, min_periods=18).std().reset_index(0, drop=True)
        data['revVol'] = data.groupby('permno')['sg'].rolling(window=48, min_periods=18).std().reset_index(0, drop=True)
        
        # Calculate industry medians for volatility
        for var in ['niVol', 'revVol']:
            data[f'md_{var}'] = data.groupby(['sic2D', 'time_avail_m'])[var].transform('median')
        
        data['m4'] = (data['niVol'] < data['md_niVol']).astype(int)
        data['m5'] = (data['revVol'] < data['md_revVol']).astype(int)
        
        # Calculate conservatism measures
        data['atdenom2'] = data.groupby('permno')['atq'].shift(3)
        data['xrdint'] = data['xrdqsum'] / data['atdenom2']
        data['capxint'] = data['capxqsum'] / data['atdenom2']
        data['xadint'] = data['xad'] / data['atdenom2']
        
        # Calculate industry medians for conservatism
        for var in ['xrdint', 'capxint', 'xadint']:
            data[f'md_{var}'] = data.groupby(['sic2D', 'time_avail_m'])[var].transform('median')
        
        data['m6'] = (data['xrdint'] > data['md_xrdint']).astype(int)
        data['m7'] = (data['capxint'] > data['md_capxint']).astype(int)
        data['m8'] = (data['xadint'] > data['md_xadint']).astype(int)
        
        # Calculate total score
        data['tempMS'] = data['m1'] + data['m2'] + data['m3'] + data['m4'] + data['m5'] + data['m6'] + data['m7'] + data['m8']
        
        # Fix tempMS at most recent data release for entire year
        # This is a complex timing adjustment that matches OP's approach
        data['month_avail'] = data['time_avail_m'].dt.month
        data['month_date'] = (data['datadate'].dt.month + 6) % 12
        data.loc[data['month_avail'] != data['month_date'], 'tempMS'] = np.nan
        
        # Forward fill missing values within each permno
        data['tempMS'] = data.groupby('permno')['tempMS'].ffill()
        
        # Create final MS score
        data['MS'] = data['tempMS']
        data.loc[(data['tempMS'] >= 6) & (data['tempMS'] <= 8), 'MS'] = 6
        data.loc[data['tempMS'] <= 1, 'MS'] = 1
        
        logger.info("Successfully calculated MS signal")
        
        # SAVE RESULTS
        logger.info("Saving MS predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'MS']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['MS'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "MS.csv"
        csv_data = output_data[['permno', 'yyyymm', 'MS']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved MS predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed MS predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct MS predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    ms()
