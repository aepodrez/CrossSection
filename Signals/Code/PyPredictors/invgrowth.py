"""
Python equivalent of InvGrowth.do
Generated from: InvGrowth.do

Original Stata file: InvGrowth.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def invgrowth():
    """
    Python equivalent of InvGrowth.do
    
    Constructs the InvGrowth predictor signal for inventory growth.
    """
    logger.info("Constructing predictor signal: InvGrowth...")
    
    try:
        # DATA LOAD
        # Load Compustat annual data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading Compustat annual data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"Input file not found: {compustat_path}")
            logger.error("Please run the Compustat data download scripts first")
            return False
        
        # Load the required variables
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'invt', 'sic', 'ppent', 'at']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Merge with GNP deflator data
        gnp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/GNPdefl.csv")
        
        logger.info(f"Loading GNP deflator data from: {gnp_path}")
        
        if not gnp_path.exists():
            logger.error(f"GNP deflator file not found: {gnp_path}")
            logger.error("Please run the GNP deflator download scripts first")
            return False
        
        gnp_data = pd.read_csv(gnp_path)
        
        # Merge data (equivalent to Stata's "merge m:1 time_avail_m using "$pathDataIntermediate/GNPdefl", keep(match) nogenerate")
        data = data.merge(
            gnp_data,
            on='time_avail_m',
            how='inner'  # keep(match)
        )
        
        logger.info(f"After merging with GNP deflator: {len(data)} observations")
        
        # Deflate inventory (equivalent to Stata's "replace invt = invt/gnpdefl")
        data['invt'] = data['invt'] / data['gnpdefl']
        
        # SAMPLE SELECTION
        logger.info("Applying sample selection filters...")
        
        # Drop transportation and public utilities (equivalent to Stata's "drop if substr(sic,1,1) == "4"")
        data = data[data['sic'].astype(str).str[0] != '4']
        
        # Drop financial services (equivalent to Stata's "drop if substr(sic,1,1) == "6"")
        data = data[data['sic'].astype(str).str[0] != '6']
        
        # Drop if assets or PPE <= 0 (equivalent to Stata's "drop if at <= 0 | ppent <= 0")
        data = data[(data['at'] > 0) & (data['ppent'] > 0)]
        
        logger.info(f"After sample selection: {len(data)} observations")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating InvGrowth signal...")
        
        # Sort data for time series operations (equivalent to Stata's "xtset permno time_avail_m")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate 12-month lag of inventory (equivalent to Stata's "l12.invt")
        data['invt_lag12'] = data.groupby('permno')['invt'].shift(12)
        
        # Calculate inventory growth (equivalent to Stata's "gen InvGrowth = invt/l12.invt - 1")
        data['InvGrowth'] = data['invt'] / data['invt_lag12'] - 1
        
        logger.info("Successfully calculated InvGrowth signal")
        
        # SAVE RESULTS
        logger.info("Saving InvGrowth predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'InvGrowth']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['InvGrowth'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "InvGrowth.csv"
        csv_data = output_data[['permno', 'yyyymm', 'InvGrowth']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved InvGrowth predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed InvGrowth predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct InvGrowth predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    invgrowth()
