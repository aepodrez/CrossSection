"""
Python equivalent of IndRetBig.do
Generated from: IndRetBig.do

Original Stata file: IndRetBig.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def indretbig():
    """
    Python equivalent of IndRetBig.do
    
    Constructs the IndRetBig predictor signal for industry returns of big companies.
    """
    logger.info("Constructing predictor signal: IndRetBig...")
    
    try:
        # DATA LOAD
        # Load SignalMasterTable data
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load the required variables
        required_vars = ['permno', 'time_avail_m', 'ret', 'sicCRSP', 'mve_c']
        
        data = pd.read_csv(master_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating IndRetBig signal...")
        
        # Convert SIC to string and create 2-digit SIC (equivalent to Stata's "tostring sicCRSP, replace" and "gen sic2D = substr(sicCRSP,1,2)")
        data['sicCRSP'] = data['sicCRSP'].astype(str)
        data['sic2D'] = data['sicCRSP'].str[:2]
        
        # Replace missing returns with 0 (equivalent to Stata's "replace ret = 0 if mi(ret)")
        data['ret'] = data['ret'].fillna(0)
        
        # Calculate relative rank of market value within industry-month (equivalent to Stata's "egen temp = rank(mve_c), by(sic2D time_avail_m)")
        data['temp'] = data.groupby(['sic2D', 'time_avail_m'])['mve_c'].rank(pct=True)
        
        # Calculate industry return for big companies (top 30% by market value)
        # Group by industry and time, calculate weighted mean for top 30%
        def weighted_mean_big(x):
            # Get top 30% by market value
            threshold = x['temp'].quantile(0.7)  # Top 30%
            big_firms = x[x['temp'] >= threshold]
            if len(big_firms) > 0:
                return np.average(big_firms['ret'], weights=big_firms['mve_c'])
            else:
                return np.nan
        
        data['IndRetBig'] = data.groupby(['sic2D', 'time_avail_m']).apply(weighted_mean_big).reset_index(level=[0, 1], drop=True)
        
        # Set IndRetBig to missing for big firms themselves (equivalent to Stata's logic where big firms don't get the industry return)
        data.loc[data['temp'] >= data.groupby(['sic2D', 'time_avail_m'])['temp'].transform('quantile', 0.7), 'IndRetBig'] = np.nan
        
        logger.info("Successfully calculated IndRetBig signal")
        
        # SAVE RESULTS
        logger.info("Saving IndRetBig predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'IndRetBig']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['IndRetBig'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "IndRetBig.csv"
        csv_data = output_data[['permno', 'yyyymm', 'IndRetBig']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved IndRetBig predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed IndRetBig predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct IndRetBig predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    indretbig()
