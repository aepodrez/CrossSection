"""
Python equivalent of realestate.do
Generated from: realestate.do

Original Stata file: realestate.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def realestate():
    """
    Python equivalent of realestate.do
    
    Constructs the realestate predictor signal for real estate holdings.
    """
    logger.info("Constructing predictor signal: realestate...")
    
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
        required_vars = ['permno', 'time_avail_m', 'ppenb', 'ppenls', 'fatb', 'fatl', 'ppegt', 'ppent', 'at']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Load SignalMasterTable for SIC codes
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        master_data = pd.read_csv(master_path, usecols=['permno', 'time_avail_m', 'sicCRSP'])
        
        # Merge with SignalMasterTable
        data = data.merge(master_data, on=['permno', 'time_avail_m'], how='inner')
        logger.info(f"After merging with SignalMasterTable: {len(data)} records")
        
        # SAMPLE SELECTION
        logger.info("Applying sample selection criteria...")
        
        # Convert SIC to string and extract 2-digit SIC (equivalent to Stata's "tostring sicCRSP, replace" and "gen sic2D = substr(sicCRSP,1,2)")
        data['sicCRSP'] = data['sicCRSP'].astype(str)
        data['sic2D'] = data['sicCRSP'].str[:2]
        
        # Count observations per SIC2D-time_avail_m (equivalent to Stata's "egen tempN = count(at), by(sic2D time_avail_m)")
        data['tempN'] = data.groupby(['sic2D', 'time_avail_m'])['at'].transform('count')
        
        # Keep only SIC2D-time_avail_m combinations with at least 5 observations (equivalent to Stata's "keep if tempN >= 5")
        data = data[data['tempN'] >= 5]
        logger.info(f"After keeping SIC2D-time_avail_m with at least 5 observations: {len(data)} records")
        
        # Drop if at is missing (equivalent to Stata's "drop if at == .")
        data = data.dropna(subset=['at'])
        logger.info(f"After dropping missing at: {len(data)} records")
        
        # Drop if both ppent and ppegt are missing (equivalent to Stata's "drop if ppent == . & ppegt == .")
        data = data[~((data['ppent'].isna()) & (data['ppent'].isna()))]
        logger.info(f"After dropping missing ppent and ppegt: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating realestate signal...")
        
        # Calculate old real estate measure (equivalent to Stata's "gen re_old = (ppenb+ppenls)/ppent")
        data['re_old'] = (data['ppenb'] + data['ppenls']) / data['ppent']
        
        # Calculate new real estate measure (equivalent to Stata's "gen re_new = (fatb+fatl)/ppegt")
        data['re_new'] = (data['fatb'] + data['fatl']) / data['ppegt']
        
        # Use new measure, fall back to old if new is missing (equivalent to Stata's logic)
        data['re'] = data['re_new']
        data.loc[data['re_new'].isna(), 're'] = data.loc[data['re_new'].isna(), 're_old']
        
        # Create year and decade variables (equivalent to Stata's "gen year = year(dofm(time_avail_m))" and "gen decade = floor(year/10)*10")
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        # Convert time_avail_m to datetime if needed for year extraction
        if not pd.api.types.is_datetime64_any_dtype(data['time_avail_m']):
            data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        
        data['year'] = data['time_avail_m'].dt.year
        data['decade'] = (data['year'] // 10) * 10
        
        # Calculate industry-adjusted real estate (equivalent to Stata's "egen tempMean = mean(re), by(sic2D time_avail_m)" and "gen realestate = re - tempMean")
        data['tempMean'] = data.groupby(['sic2D', 'time_avail_m'])['re'].transform('mean')
        data['realestate'] = data['re'] - data['tempMean']
        
        logger.info("Successfully calculated realestate signal")
        
        # SAVE RESULTS
        logger.info("Saving realestate predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'realestate']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['realestate'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "realestate.csv"
        csv_data = output_data[['permno', 'yyyymm', 'realestate']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved realestate predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed realestate predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct realestate predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    realestate()
