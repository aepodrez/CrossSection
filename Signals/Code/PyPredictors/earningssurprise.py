"""
Python equivalent of EarningsSurprise.do
Generated from: EarningsSurprise.do

Original Stata file: EarningsSurprise.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def earningssurprise():
    """
    Python equivalent of EarningsSurprise.do
    
    Constructs the EarningsSurprise predictor signal for earnings surprise.
    """
    logger.info("Constructing predictor signal: EarningsSurprise...")
    
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
        master_vars = ['permno', 'gvkey', 'time_avail_m']
        data = pd.read_csv(master_path, usecols=master_vars)
        logger.info(f"Successfully loaded {len(data)} records from SignalMasterTable")
        
        # Keep if gvkey is not missing (equivalent to Stata's "keep if !mi(gvkey)")
        data = data[data['gvkey'].notna()]
        logger.info(f"After filtering for non-missing gvkey: {len(data)} records")
        
        # Merge with quarterly Compustat data (equivalent to Stata's merge)
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_QCompustat.csv")
        
        if not compustat_path.exists():
            logger.error(f"Quarterly Compustat file not found: {compustat_path}")
            logger.error("Please run the Compustat data download script first")
            return False
        
        compustat_data = pd.read_csv(compustat_path, usecols=['gvkey', 'time_avail_m', 'epspxq'])
        
        data = data.merge(
            compustat_data,
            on=['gvkey', 'time_avail_m'],
            how='inner'  # keep(match)
        )
        
        logger.info(f"Successfully merged data: {len(data)} observations")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating EarningsSurprise signal...")
        
        # Sort for lag calculations (equivalent to Stata's "xtset permno time_avail_m")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate 12-month lag of epspxq
        data['epspxq_lag12'] = data.groupby('permno')['epspxq'].shift(12)
        
        # Calculate GrTemp (equivalent to Stata's "gen GrTemp = (epspxq - l12.epspxq)")
        data['GrTemp'] = data['epspxq'] - data['epspxq_lag12']
        
        # Calculate lags of GrTemp for different periods (equivalent to Stata's foreach loop)
        for n in [3, 6, 9, 12, 15, 18, 21, 24]:
            data[f'temp{n}'] = data.groupby('permno')['GrTemp'].shift(n)
        
        # Calculate Drift as row mean (equivalent to Stata's "egen Drift = rowmean(temp*)")
        temp_cols = [f'temp{n}' for n in [3, 6, 9, 12, 15, 18, 21, 24]]
        data['Drift'] = data[temp_cols].mean(axis=1)
        
        # Calculate EarningsSurprise (equivalent to Stata's "gen EarningsSurprise = epspxq - l12.epspxq - Drift")
        data['EarningsSurprise'] = data['epspxq'] - data['epspxq_lag12'] - data['Drift']
        
        # Drop temporary columns (equivalent to Stata's "cap drop temp*")
        data = data.drop(columns=temp_cols)
        
        # Calculate lags of EarningsSurprise for different periods (equivalent to Stata's second foreach loop)
        for n in [3, 6, 9, 12, 15, 18, 21, 24]:
            data[f'temp{n}'] = data.groupby('permno')['EarningsSurprise'].shift(n)
        
        # Calculate SD as row standard deviation (equivalent to Stata's "egen SD = rowsd(temp*)")
        temp_cols = [f'temp{n}' for n in [3, 6, 9, 12, 15, 18, 21, 24]]
        data['SD'] = data[temp_cols].std(axis=1)
        
        # Normalize EarningsSurprise by SD (equivalent to Stata's "replace EarningsSurprise = EarningsSurprise/SD")
        data['EarningsSurprise'] = data['EarningsSurprise'] / data['SD']
        
        logger.info("Successfully calculated EarningsSurprise signal")
        
        # SAVE RESULTS
        logger.info("Saving EarningsSurprise predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'EarningsSurprise']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['EarningsSurprise'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "earningssurprise.csv"
        csv_data = output_data[['permno', 'yyyymm', 'EarningsSurprise']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved EarningsSurprise predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed EarningsSurprise predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct EarningsSurprise predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    earningssurprise()
