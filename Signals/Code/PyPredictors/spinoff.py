"""
Python equivalent of Spinoff.do
Generated from: Spinoff.do

Original Stata file: Spinoff.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def spinoff():
    """
    Python equivalent of Spinoff.do
    
    Constructs the Spinoff predictor signal for spinoff companies.
    """
    logger.info("Constructing predictor signal: Spinoff...")
    
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
        required_vars = ['permno', 'time_avail_m']
        
        data = pd.read_csv(master_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Load CRSP Acquisitions data
        acquisitions_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_CRSPAcquisitions.csv")
        
        logger.info(f"Loading CRSP Acquisitions data from: {acquisitions_path}")
        
        if not acquisitions_path.exists():
            logger.error(f"m_CRSPAcquisitions not found: {acquisitions_path}")
            logger.error("Please run the CRSP Acquisitions data creation script first")
            return False
        
        acquisitions_data = pd.read_csv(acquisitions_path)
        
        # Merge with CRSP Acquisitions data (equivalent to Stata's "merge m:1 permno using "$pathDataIntermediate/m_CRSPAcquisitions", keep(master match) nogenerate")
        data = data.merge(acquisitions_data, on='permno', how='left')
        logger.info(f"After merging with CRSP Acquisitions data: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating Spinoff signal...")
        
        # Sort data for time series operations (equivalent to Stata's "bys permno (time_avail_m)")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Generate firm age without screening (equivalent to Stata's "bys permno (time_avail_m): gen FirmAgeNoScreen = _n")
        data['FirmAgeNoScreen'] = data.groupby('permno').cumcount() + 1
        
        # Create Spinoff indicator (equivalent to Stata's logic)
        data['Spinoff'] = np.nan
        data.loc[(data['SpinoffCo'] == 1) & (data['FirmAgeNoScreen'] <= 24), 'Spinoff'] = 1
        
        # Replace missing values with 0 (equivalent to Stata's "replace Spinoff = 0 if Spinoff ==.")
        data.loc[data['Spinoff'].isna(), 'Spinoff'] = 0
        
        logger.info("Successfully calculated Spinoff signal")
        
        # SAVE RESULTS
        logger.info("Saving Spinoff predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'Spinoff']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['Spinoff'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "Spinoff.csv"
        csv_data = output_data[['permno', 'yyyymm', 'Spinoff']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved Spinoff predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed Spinoff predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct Spinoff predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    spinoff()
