"""
Python equivalent of OperProf.do
Generated from: OperProf.do

Original Stata file: OperProf.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def operprof():
    """
    Python equivalent of OperProf.do
    
    Constructs the OperProf predictor signal for operating profitability.
    """
    logger.info("Constructing predictor signal: OperProf...")
    
    try:
        # DATA LOAD
        # Load SignalMasterTable data
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load required variables
        required_vars = ['permno', 'gvkey', 'time_avail_m', 'mve_c']
        
        data = pd.read_csv(master_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Drop observations with missing gvkey (equivalent to Stata's "drop if mi(gvkey)")
        data = data.dropna(subset=['gvkey'])
        logger.info(f"After dropping missing gvkey: {len(data)} records")
        
        # Load Compustat data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading Compustat data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"m_aCompustat not found: {compustat_path}")
            logger.error("Please run the Compustat data creation script first")
            return False
        
        compustat_data = pd.read_csv(compustat_path, usecols=['gvkey', 'time_avail_m', 'revt', 'cogs', 'xsga', 'xint', 'ceq'])
        
        # Merge with Compustat data
        data = data.merge(compustat_data, on=['gvkey', 'time_avail_m'], how='inner')
        logger.info(f"After merging with Compustat data: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating OperProf signal...")
        
        # Calculate temporary profitability (equivalent to Stata's "gen tempprof = (revt - cogs - xsga - xint)/ceq")
        data['tempprof'] = (data['revt'] - data['cogs'] - data['xsga'] - data['xint']) / data['ceq']
        
        # Create size terciles (equivalent to Stata's "egen tempsizeq = fastxtile(mve_c), by(time_avail_m) n(3)")
        data['tempsizeq'] = data.groupby('time_avail_m')['mve_c'].transform(
            lambda x: pd.qcut(x, q=3, labels=False, duplicates='drop') + 1
        )
        
        # Set tempprof to missing for small cap stocks (equivalent to Stata's "replace tempprof = . if tempsizeq == 1")
        data.loc[data['tempsizeq'] == 1, 'tempprof'] = np.nan
        
        # Create final OperProf signal (equivalent to Stata's "gen OperProf = tempprof")
        data['OperProf'] = data['tempprof']
        
        logger.info("Successfully calculated OperProf signal")
        
        # SAVE RESULTS
        logger.info("Saving OperProf predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'OperProf']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['OperProf'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "OperProf.csv"
        csv_data = output_data[['permno', 'yyyymm', 'OperProf']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved OperProf predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed OperProf predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct OperProf predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    operprof()
