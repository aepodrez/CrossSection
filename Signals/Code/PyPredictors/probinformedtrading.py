"""
Python equivalent of ProbInformedTrading.do
Generated from: ProbInformedTrading.do

Original Stata file: ProbInformedTrading.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def probinformedtrading():
    """
    Python equivalent of ProbInformedTrading.do
    
    Constructs the ProbInformedTrading predictor signal for probability of informed trading.
    """
    logger.info("Constructing predictor signal: ProbInformedTrading...")
    
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
        
        # Create year variable (equivalent to Stata's "gen year = yofd(dofm(time_avail_m))")
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        # Convert time_avail_m to datetime if needed for year extraction
        if not pd.api.types.is_datetime64_any_dtype(data['time_avail_m']):
            data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        
        data['year'] = data['time_avail_m'].dt.year
        
        # Load PIN monthly data
        pin_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/pin_monthly.csv")
        
        logger.info(f"Loading PIN monthly data from: {pin_path}")
        
        if not pin_path.exists():
            logger.error(f"pin_monthly not found: {pin_path}")
            logger.error("Please run the PIN monthly data creation script first")
            return False
        
        pin_data = pd.read_csv(pin_path)
        
        # Merge with PIN data
        data = data.merge(pin_data, on=['permno', 'time_avail_m'], how='left')
        logger.info(f"After merging with PIN data: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating ProbInformedTrading signal...")
        
        # Calculate PIN measure (equivalent to Stata's "gen pin = (a*u) / (a*u + es + eb)")
        data['pin'] = (data['a'] * data['u']) / (data['a'] * data['u'] + data['es'] + data['eb'])
        
        # Create size categories (equivalent to Stata's "egen tempsize = fastxtile(mve_c), by(time_avail_m) n(2)")
        data['tempsize'] = data.groupby('time_avail_m')['mve_c'].transform(
            lambda x: pd.qcut(x, q=2, labels=False, duplicates='drop') + 1
        )
        
        # Replace PIN with missing for large firms (equivalent to Stata's "replace pin = . if tempsize == 2")
        data.loc[data['tempsize'] == 2, 'pin'] = np.nan
        
        # Rename pin to ProbInformedTrading (equivalent to Stata's "rename pin ProbInformedTrading")
        data['ProbInformedTrading'] = data['pin']
        
        logger.info("Successfully calculated ProbInformedTrading signal")
        
        # SAVE RESULTS
        logger.info("Saving ProbInformedTrading predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'ProbInformedTrading']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['ProbInformedTrading'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "ProbInformedTrading.csv"
        csv_data = output_data[['permno', 'yyyymm', 'ProbInformedTrading']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved ProbInformedTrading predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed ProbInformedTrading predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct ProbInformedTrading predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    probinformedtrading()
