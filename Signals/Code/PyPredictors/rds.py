"""
Python equivalent of RDS.do
Generated from: RDS.do

Original Stata file: RDS.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def rds():
    """
    Python equivalent of RDS.do
    
    Constructs the RDS predictor signal for real dirty surplus.
    """
    logger.info("Constructing predictor signal: RDS...")
    
    try:
        # DATA LOAD
        # Load annual Compustat data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading annual Compustat data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"m_aCompustat not found: {compustat_path}")
            logger.error("Please run the annual Compustat data creation script first")
            return False
        
        # Load required variables
        required_vars = ['permno', 'gvkey', 'time_avail_m', 'recta', 'ceq', 'ni', 'dvp', 'dvc', 'prcc_f', 'csho', 'msa']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Create year variable (equivalent to Stata's "gen year = yofd(dofm(time_avail_m))")
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        # Convert time_avail_m to datetime if needed for year extraction
        if not pd.api.types.is_datetime64_any_dtype(data['time_avail_m']):
            data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        
        data['year'] = data['time_avail_m'].dt.year
        
        # Load Compustat pensions data
        pensions_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/CompustatPensions.csv")
        
        logger.info(f"Loading Compustat pensions data from: {pensions_path}")
        
        if not pensions_path.exists():
            logger.error(f"CompustatPensions not found: {pensions_path}")
            logger.error("Please run the Compustat pensions data creation script first")
            return False
        
        pensions_data = pd.read_csv(pensions_path, usecols=['gvkey', 'year', 'pcupsu', 'paddml'])
        
        # Merge with pensions data
        data = data.merge(pensions_data, on=['gvkey', 'year'], how='left')
        logger.info(f"After merging with pensions data: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating RDS signal...")
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate 12-month lags (equivalent to Stata's "l12." variables)
        data['msa_lag12'] = data.groupby('permno')['msa'].shift(12)
        data['recta_lag12'] = data.groupby('permno')['recta'].shift(12)
        data['pcupsu_lag12'] = data.groupby('permno')['pcupsu'].shift(12)
        data['paddml_lag12'] = data.groupby('permno')['paddml'].shift(12)
        data['ceq_lag12'] = data.groupby('permno')['ceq'].shift(12)
        data['ni_lag12'] = data.groupby('permno')['ni'].shift(12)
        data['dvp_lag12'] = data.groupby('permno')['dvp'].shift(12)
        data['csho_lag12'] = data.groupby('permno')['csho'].shift(12)
        
        # Replace missing recta with 0 (equivalent to Stata's "replace recta = 0 if recta == .")
        data['recta'] = data['recta'].fillna(0)
        
        # Calculate DS (dirty surplus) (equivalent to Stata's complex formula)
        # DS = (msa - l12.msa) + (recta - l12.recta) + 0.65*(min(pcupsu - paddml,0) - min(l12.pcupsu - l12.paddml,0))
        data['DS'] = ((data['msa'] - data['msa_lag12']) + 
                      (data['recta'] - data['recta_lag12']) + 
                      0.65 * (np.minimum(data['pcupsu'] - data['paddml'], 0) - 
                              np.minimum(data['pcupsu_lag12'] - data['paddml_lag12'], 0)))
        
        # Calculate RDS (real dirty surplus) (equivalent to Stata's formula)
        # RDS = (ceq - l12.ceq) - DS - (ni - dvp) + dvc - prcc_f*(csho - l12.csho)
        data['RDS'] = ((data['ceq'] - data['ceq_lag12']) - 
                       data['DS'] - 
                       (data['ni'] - data['dvp']) + 
                       data['dvc'] - 
                       data['prcc_f'] * (data['csho'] - data['csho_lag12']))
        
        logger.info("Successfully calculated RDS signal")
        
        # SAVE RESULTS
        logger.info("Saving RDS predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'RDS']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['RDS'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "RDS.csv"
        csv_data = output_data[['permno', 'yyyymm', 'RDS']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved RDS predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed RDS predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct RDS predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    rds()
