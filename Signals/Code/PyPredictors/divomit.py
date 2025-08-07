"""
Python equivalent of DivOmit.do
Generated from: DivOmit.do

Original Stata file: DivOmit.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def divomit():
    """
    Python equivalent of DivOmit.do
    
    Constructs the DivOmit predictor signal for dividend omission.
    """
    logger.info("Constructing predictor signal: DivOmit...")
    
    try:
        # PREP DISTRIBUTIONS DATA
        # Load CRSP distributions data
        distributions_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/CRSPdistributions.csv")
        
        logger.info(f"Loading CRSP distributions data from: {distributions_path}")
        
        if not distributions_path.exists():
            logger.error(f"CRSP distributions file not found: {distributions_path}")
            logger.error("Please run the CRSP distributions download script first")
            return False
        
        # Load distributions data
        dist_data = pd.read_csv(distributions_path)
        logger.info(f"Successfully loaded {len(dist_data)} distribution records")
        
        # Keep cash dividends only (equivalent to Stata's "keep if cd2 == 2 | cd2 == 3")
        dist_data = dist_data[dist_data['cd2'].isin([2, 3])]
        logger.info(f"After filtering for cash dividends: {len(dist_data)} records")
        
        # Create time_avail_m from exdt (equivalent to Stata's "gen time_avail_m = mofd(exdt)")
        dist_data['time_avail_m'] = pd.to_datetime(dist_data['exdt']).dt.to_period('M').astype(str)
        
        # Drop missing time_avail_m or divamt (equivalent to Stata's "drop if time_avail_m == . | divamt == .")
        dist_data = dist_data.dropna(subset=['time_avail_m', 'divamt'])
        
        # Sum dividends by permno and time_avail_m (equivalent to Stata's "gcollapse (sum) divamt, by(permno time_avail_m)")
        div_summary = dist_data.groupby(['permno', 'time_avail_m'])['divamt'].sum().reset_index()
        logger.info(f"After collapsing dividends: {len(div_summary)} records")
        
        # Save temporary file (equivalent to Stata's "save "$pathtemp/tempdivamt", replace")
        temp_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Temp")
        temp_dir.mkdir(parents=True, exist_ok=True)
        div_summary.to_csv(temp_dir / "tempdivamt.csv", index=False)
        
        # DATA LOAD
        # Load SignalMasterTable
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load required variables from SignalMasterTable
        master_vars = ['permno', 'time_avail_m', 'exchcd', 'shrcd']
        data = pd.read_csv(master_path, usecols=master_vars)
        logger.info(f"Successfully loaded {len(data)} records from SignalMasterTable")
        
        # Convert time_avail_m to datetime before merge
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        div_summary['time_avail_m'] = pd.to_datetime(div_summary['time_avail_m'])
        
        # Merge with dividend data (equivalent to Stata's "merge 1:1 permno time_avail_m using "$pathtemp/tempdivamt", keep(master match)")
        merged_data = data.merge(
            div_summary,
            on=['permno', 'time_avail_m'],
            how='left'
        )
        
        logger.info(f"Successfully merged data: {len(merged_data)} observations")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating DivOmit signal...")
        
        # Create div and divind (equivalent to Stata's "gen div = divamt" and "replace div = 0 if divamt ==." and "gen divind = div > 0")
        merged_data['div'] = merged_data['divamt'].fillna(0)
        merged_data['divind'] = (merged_data['div'] > 0).astype(int)
        
        # Sort for rolling calculations
        merged_data = merged_data.sort_values(['permno', 'time_avail_m'])
        
        # Quarterly omission detection (3-month window)
        logger.info("Calculating quarterly omission detection...")
        
        # Calculate 3-month rolling sum of divind
        merged_data['sum3_divind'] = merged_data.groupby('permno')['divind'].rolling(window=3, min_periods=1).sum().reset_index(0, drop=True)
        
        # Create temppaid flag
        merged_data['temppaid'] = (merged_data['sum3_divind'] == 1).astype(int)
        
        # Calculate 18-month rolling mean of temppaid
        merged_data['mean18_temppaid'] = merged_data.groupby('permno')['temppaid'].rolling(window=18, min_periods=1).mean().reset_index(0, drop=True)
        
        # Create temppayer flag
        merged_data['temppayer'] = (merged_data['mean18_temppaid'] == 1).astype(int)
        
        # Calculate lags
        merged_data['sum3_divind_lag1'] = merged_data.groupby('permno')['sum3_divind'].shift(1)
        merged_data['temppayer_lag3'] = merged_data.groupby('permno')['temppayer'].shift(3)
        
        # Create omit_3 flag
        merged_data['omit_3'] = ((merged_data['sum3_divind'] == 0) & 
                                (merged_data['sum3_divind_lag1'] > 0) & 
                                (merged_data['temppayer_lag3'] == 1)).astype(int)
        
        # Semi-annual omission detection (6-month window)
        logger.info("Calculating semi-annual omission detection...")
        
        # Calculate 6-month rolling sum of divind
        merged_data['sum6_divind'] = merged_data.groupby('permno')['divind'].rolling(window=6, min_periods=1).sum().reset_index(0, drop=True)
        
        # Create temppaid flag
        merged_data['temppaid'] = (merged_data['sum6_divind'] == 1).astype(int)
        
        # Calculate 18-month rolling mean of temppaid
        merged_data['mean18_temppaid'] = merged_data.groupby('permno')['temppaid'].rolling(window=18, min_periods=1).mean().reset_index(0, drop=True)
        
        # Create temppayer flag
        merged_data['temppayer'] = (merged_data['mean18_temppaid'] == 1).astype(int)
        
        # Calculate lags
        merged_data['sum6_divind_lag1'] = merged_data.groupby('permno')['sum6_divind'].shift(1)
        merged_data['temppayer_lag6'] = merged_data.groupby('permno')['temppayer'].shift(6)
        
        # Create omit_6 flag
        merged_data['omit_6'] = ((merged_data['sum6_divind'] == 0) & 
                                (merged_data['sum6_divind_lag1'] > 0) & 
                                (merged_data['temppayer_lag6'] == 1)).astype(int)
        
        # Annual omission detection (12-month window)
        logger.info("Calculating annual omission detection...")
        
        # Calculate 12-month rolling sum of divind
        merged_data['sum12_divind'] = merged_data.groupby('permno')['divind'].rolling(window=12, min_periods=1).sum().reset_index(0, drop=True)
        
        # Create temppaid flag
        merged_data['temppaid'] = (merged_data['sum12_divind'] == 1).astype(int)
        
        # Calculate 24-month rolling mean of temppaid
        merged_data['mean24_temppaid'] = merged_data.groupby('permno')['temppaid'].rolling(window=24, min_periods=1).mean().reset_index(0, drop=True)
        
        # Create temppayer flag
        merged_data['temppayer'] = (merged_data['mean24_temppaid'] == 1).astype(int)
        
        # Calculate lags
        merged_data['sum12_divind_lag1'] = merged_data.groupby('permno')['sum12_divind'].shift(1)
        merged_data['temppayer_lag12'] = merged_data.groupby('permno')['temppayer'].shift(12)
        
        # Create omit_12 flag
        merged_data['omit_12'] = ((merged_data['sum12_divind'] == 0) & 
                                 (merged_data['sum12_divind_lag1'] > 0) & 
                                 (merged_data['temppayer_lag12'] == 1)).astype(int)
        
        # Combine all omission flags (equivalent to Stata's "gen omitnow = omit_3 == 1 | omit_6 == 1 | omit_12 == 1")
        merged_data['omitnow'] = ((merged_data['omit_3'] == 1) | 
                                 (merged_data['omit_6'] == 1) | 
                                 (merged_data['omit_12'] == 1)).astype(int)
        
        # Calculate 2-month rolling sum of omitnow (equivalent to Stata's "asrol omitnow, window(time_avail_m 2) stat(sum) gen(temp)")
        merged_data['temp'] = merged_data.groupby('permno')['omitnow'].rolling(window=2, min_periods=1).sum().reset_index(0, drop=True)
        
        # Create DivOmit (equivalent to Stata's "gen DivOmit = temp == 1")
        merged_data['DivOmit'] = (merged_data['temp'] == 1).astype(int)
        
        logger.info("Successfully calculated DivOmit signal")
        
        # SAVE RESULTS
        logger.info("Saving DivOmit predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = merged_data[['permno', 'time_avail_m', 'DivOmit']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['DivOmit'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "DivOmit.csv"
        csv_data = output_data[['permno', 'yyyymm', 'DivOmit']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved DivOmit predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed DivOmit predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct DivOmit predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    divomit()
