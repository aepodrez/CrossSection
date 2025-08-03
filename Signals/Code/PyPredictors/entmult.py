"""
Python equivalent of EntMult.do
Generated from: EntMult.do

Original Stata file: EntMult.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def entmult():
    """
    Python equivalent of EntMult.do
    
    Constructs the EntMult predictor signal for enterprise multiple.
    """
    logger.info("Constructing predictor signal: EntMult...")
    
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
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'dltt', 'dlc', 'dc', 'che', 'oibdp', 'ceq']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Merge with SignalMasterTable to get mve_c (equivalent to Stata's merge)
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load required variables from SignalMasterTable
        master_vars = ['permno', 'time_avail_m', 'mve_c']
        master_data = pd.read_csv(master_path, usecols=master_vars)
        
        # Merge data (equivalent to Stata's "merge 1:1 permno time_avail_m using "$pathDataIntermediate/SignalMasterTable", keep(using match)")
        data = data.merge(
            master_data,
            on=['permno', 'time_avail_m'],
            how='inner'  # keep(using match)
        )
        
        logger.info(f"Successfully merged data: {len(data)} observations")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating EntMult signal...")
        
        # Calculate EntMult (equivalent to Stata's "gen EntMult = (mve_c + dltt + dlc + dc - che)/oibdp")
        data['EntMult'] = (data['mve_c'] + data['dltt'] + data['dlc'] + data['dc'] - data['che']) / data['oibdp']
        
        # Apply filters (equivalent to Stata's "replace EntMult = . if ceq < 0 | oibdp < 0")
        data.loc[(data['ceq'] < 0) | (data['oibdp'] < 0), 'EntMult'] = np.nan
        
        logger.info("Successfully calculated EntMult signal")
        
        # SAVE RESULTS
        logger.info("Saving EntMult predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'EntMult']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['EntMult'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "EntMult.csv"
        csv_data = output_data[['permno', 'yyyymm', 'EntMult']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved EntMult predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed EntMult predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct EntMult predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    entmult()
