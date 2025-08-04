"""
Python equivalent of ExclExp.do
Generated from: ExclExp.do

Original Stata file: ExclExp.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def exclexp():
    """
    Python equivalent of ExclExp.do
    
    Constructs the ExclExp predictor signal for excluded expenses.
    """
    logger.info("Constructing predictor signal: ExclExp...")
    
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
        required_vars = ['permno', 'gvkey', 'time_avail_m', 'tickerIBES']
        
        data = pd.read_csv(master_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Keep only non-missing gvkey (equivalent to Stata's "keep if !mi(gvkey)")
        data = data.dropna(subset=['gvkey'])
        logger.info(f"After filtering for non-missing gvkey: {len(data)} records")
        
        # Merge with quarterly Compustat data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_QCompustat.csv")
        
        logger.info(f"Loading quarterly Compustat data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"Quarterly Compustat file not found: {compustat_path}")
            logger.error("Please run the Compustat data download scripts first")
            return False
        
        # Load required variables from quarterly Compustat
        compustat_vars = ['gvkey', 'time_avail_m', 'epspiq']
        compustat_data = pd.read_csv(compustat_path, usecols=compustat_vars)
        
        # Merge data (equivalent to Stata's "merge 1:1 gvkey time_avail_m using "$pathDataIntermediate/m_QCompustat", keepusing(epspiq) nogenerate keep(match)")
        data = data.merge(
            compustat_data,
            on=['gvkey', 'time_avail_m'],
            how='inner'  # keep(match)
        )
        
        logger.info(f"After merging with Compustat: {len(data)} observations")
        
        # Merge with IBES unadjusted actuals
        ibes_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/IBES_UnadjustedActuals.csv")
        
        logger.info(f"Loading IBES unadjusted actuals from: {ibes_path}")
        
        if not ibes_path.exists():
            logger.error(f"IBES unadjusted actuals file not found: {ibes_path}")
            logger.error("Please run the IBES data download scripts first")
            return False
        
        # Load required variables from IBES
        ibes_vars = ['tickerIBES', 'time_avail_m', 'int0a']
        ibes_data = pd.read_csv(ibes_path, usecols=ibes_vars)
        
        # Merge data (equivalent to Stata's "merge m:1 tickerIBES time_avail_m using "$pathDataIntermediate/IBES_UnadjustedActuals", keep(master match) nogenerate keepusing(int0a)")
        data = data.merge(
            ibes_data,
            on=['tickerIBES', 'time_avail_m'],
            how='inner'  # keep(master match)
        )
        
        logger.info(f"After merging with IBES: {len(data)} observations")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating ExclExp signal...")
        
        # Calculate ExclExp (equivalent to Stata's "gen ExclExp = int0a - epspiq")
        data['ExclExp'] = data['int0a'] - data['epspiq']
        
        # Winsorize at 1st and 99th percentiles (equivalent to Stata's "winsor2 ExclExp, replace cut(1 99) trim")
        lower_bound = data['ExclExp'].quantile(0.01)
        upper_bound = data['ExclExp'].quantile(0.99)
        data['ExclExp'] = data['ExclExp'].clip(lower=lower_bound, upper=upper_bound)
        
        logger.info("Successfully calculated ExclExp signal")
        
        # SAVE RESULTS
        logger.info("Saving ExclExp predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'ExclExp']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['ExclExp'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "ExclExp.csv"
        csv_data = output_data[['permno', 'yyyymm', 'ExclExp']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved ExclExp predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed ExclExp predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct ExclExp predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    exclexp()
