"""
Python equivalent of CBOperProf.do
Generated from: CBOperProf.do

Original Stata file: CBOperProf.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def cboperprof():
    """
    Python equivalent of CBOperProf.do
    
    Constructs the CBOperProf predictor signal for cash-based operating profitability.
    """
    logger.info("Constructing predictor signal: CBOperProf...")
    
    try:
        # DATA LOAD
        # Load SignalMasterTable
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load required variables from SignalMasterTable
        master_vars = ['permno', 'gvkey', 'time_avail_m', 'exchcd', 'sicCRSP', 'shrcd', 'mve_c']
        master_data = pd.read_csv(master_path, usecols=master_vars)
        
        # Load Compustat annual data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        if not compustat_path.exists():
            logger.error(f"Compustat data not found: {compustat_path}")
            logger.error("Please run the Compustat data download scripts first")
            return False
        
        # Load required variables from Compustat
        compustat_vars = ['permno', 'time_avail_m', 'revt', 'cogs', 'xsga', 'xrd', 'rect', 'invt', 'xpp', 'drc', 'drlt', 'ap', 'xacc', 'at', 'ceq']
        compustat_data = pd.read_csv(compustat_path, usecols=compustat_vars)
        
        logger.info(f"Successfully loaded {len(master_data)} master records and {len(compustat_data)} Compustat records")
        
        # Merge data
        merged_data = master_data.merge(
            compustat_data, 
            on=['permno', 'time_avail_m'], 
            how='left'
        )
        
        logger.info(f"Successfully merged data: {len(merged_data)} observations")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating CBOperProf signal...")
        
        # Replace missing values with 0 for specified variables (equivalent to Stata's foreach loop)
        variables_to_zero = ['revt', 'cogs', 'xsga', 'xrd', 'rect', 'invt', 'xpp', 'drc', 'drlt', 'ap', 'xacc']
        for var in variables_to_zero:
            merged_data[var] = merged_data[var].fillna(0)
        
        # Sort by permno and time_avail_m for lag calculations
        merged_data = merged_data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate 12-month lags for required variables
        lag_vars = ['rect', 'invt', 'xpp', 'drc', 'drlt', 'ap', 'xacc']
        for var in lag_vars:
            merged_data[f'{var}_lag12'] = merged_data.groupby('permno')[var].shift(12)
        
        # Calculate CBOperProf (equivalent to Stata's complex formula)
        merged_data['CBOperProf'] = (
            (merged_data['revt'] - merged_data['cogs'] - (merged_data['xsga'] - merged_data['xrd'])) -
            (merged_data['rect'] - merged_data['rect_lag12']) -
            (merged_data['invt'] - merged_data['invt_lag12']) -
            (merged_data['xpp'] - merged_data['xpp_lag12']) +
            (merged_data['drc'] + merged_data['drlt'] - merged_data['drc_lag12'] - merged_data['drlt_lag12']) +
            (merged_data['ap'] - merged_data['ap_lag12']) +
            (merged_data['xacc'] - merged_data['xacc_lag12'])
        ) / merged_data['at']
        
        # Calculate BM for filtering
        merged_data['BM'] = np.log(merged_data['ceq'] / merged_data['mve_c'])
        
        # Apply filters (equivalent to Stata's replace conditions)
        merged_data.loc[
            (merged_data['shrcd'] > 11) |
            (merged_data['mve_c'].isna()) |
            (merged_data['BM'].isna()) |
            (merged_data['at'].isna()) |
            ((merged_data['sicCRSP'] >= 6000) & (merged_data['sicCRSP'] < 7000)),
            'CBOperProf'
        ] = np.nan
        
        logger.info("Successfully calculated CBOperProf signal")
        
        # SAVE RESULTS
        logger.info("Saving CBOperProf predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = merged_data[['permno', 'time_avail_m', 'CBOperProf']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['CBOperProf'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "CBOperProf.csv"
        csv_data = output_data[['permno', 'yyyymm', 'CBOperProf']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved CBOperProf predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed CBOperProf predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct CBOperProf predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    cboperprof()
