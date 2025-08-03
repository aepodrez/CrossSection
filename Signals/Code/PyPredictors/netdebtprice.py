"""
Python equivalent of NetDebtPrice.do
Generated from: NetDebtPrice.do

Original Stata file: NetDebtPrice.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def netdebtprice():
    """
    Python equivalent of NetDebtPrice.do
    
    Constructs the NetDebtPrice predictor signal for net debt to price ratio.
    """
    logger.info("Constructing predictor signal: NetDebtPrice...")
    
    try:
        # DATA LOAD
        # Load Compustat data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading Compustat data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"m_aCompustat not found: {compustat_path}")
            logger.error("Please run the Compustat data creation script first")
            return False
        
        # Load required variables
        required_vars = ['permno', 'time_avail_m', 'at', 'dltt', 'dlc', 'pstk', 'dvpa', 'tstkp', 'che', 'sic', 'ib', 'csho', 'ceq', 'prcc_f']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'])
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # Load SignalMasterTable for market value
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        master_data = pd.read_csv(master_path, usecols=['permno', 'time_avail_m', 'mve_c'])
        
        # Merge with SignalMasterTable
        data = data.merge(master_data, on=['permno', 'time_avail_m'], how='inner')
        logger.info(f"After merging with SignalMasterTable: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating NetDebtPrice signal...")
        
        # Convert SIC to numeric (equivalent to Stata's "destring sic, replace")
        data['sic'] = pd.to_numeric(data['sic'], errors='coerce')
        
        # Calculate NetDebtPrice (equivalent to Stata's "gen NetDebtPrice = ((dltt + dlc + pstk + dvpa - tstkp) - che)/mve_c")
        data['NetDebtPrice'] = ((data['dltt'] + data['dlc'] + data['pstk'] + data['dvpa'] - data['tstkp']) - data['che']) / data['mve_c']
        
        # Exclude financial firms (equivalent to Stata's "replace NetDebtPrice = . if sic >= 6000 & sic <= 6999")
        data.loc[(data['sic'] >= 6000) & (data['sic'] <= 6999), 'NetDebtPrice'] = np.nan
        
        # Exclude firms with missing key variables (equivalent to Stata's "replace NetDebtPrice = . if mi(at) | mi(ib) | mi(csho) | mi(ceq) | mi(prcc_f)")
        missing_vars = ['at', 'ib', 'csho', 'ceq', 'prcc_f']
        for var in missing_vars:
            data.loc[data[var].isna(), 'NetDebtPrice'] = np.nan
        
        # Calculate book-to-market ratio (equivalent to Stata's "gen BM = log(ceq/mve_c)")
        data['BM'] = np.log(data['ceq'] / data['mve_c'])
        
        # Create quintiles and exclude bottom 2 quintiles (equivalent to Stata's "egen tempsort = fastxtile(BM), by(time_avail_m) n(5)" and "replace NetDebtPrice = . if tempsort <= 2")
        data['tempsort'] = data.groupby('time_avail_m')['BM'].transform(
            lambda x: pd.qcut(x, q=5, labels=False, duplicates='drop') + 1
        )
        data.loc[data['tempsort'] <= 2, 'NetDebtPrice'] = np.nan
        
        logger.info("Successfully calculated NetDebtPrice signal")
        
        # SAVE RESULTS
        logger.info("Saving NetDebtPrice predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'NetDebtPrice']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['NetDebtPrice'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "NetDebtPrice.csv"
        csv_data = output_data[['permno', 'yyyymm', 'NetDebtPrice']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved NetDebtPrice predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed NetDebtPrice predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct NetDebtPrice predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    netdebtprice()
