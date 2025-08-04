"""
Python equivalent of ZZ1_Activism1_Activism2.do
Generated from: ZZ1_Activism1_Activism2.do

Original Stata file: ZZ1_Activism1_Activism2.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def zz1_activism1_activism2():
    """
    Python equivalent of ZZ1_Activism1_Activism2.do
    
    Constructs the Activism1 and Activism2 predictor signals for shareholder activism.
    """
    logger.info("Constructing predictor signals: Activism1, Activism2...")
    
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
        required_vars = ['permno', 'time_avail_m', 'ticker', 'exchcd', 'mve_c']
        
        data = pd.read_csv(master_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Merge with TR_13F data
        tr_13f_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/TR_13F.csv")
        
        logger.info(f"Loading TR_13F data from: {tr_13f_path}")
        
        if not tr_13f_path.exists():
            logger.error(f"TR_13F not found: {tr_13f_path}")
            logger.error("Please run the TR_13F data creation script first")
            return False
        
        tr_13f_data = pd.read_csv(tr_13f_path, usecols=['permno', 'time_avail_m', 'maxinstown_perc'])
        
        data = data.merge(tr_13f_data, on=['permno', 'time_avail_m'], how='left')
        logger.info(f"After merging with TR_13F data: {len(data)} records")
        
        # Merge with monthly CRSP data
        crsp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/monthlyCRSP.csv")
        
        logger.info(f"Loading monthly CRSP data from: {crsp_path}")
        
        if not crsp_path.exists():
            logger.error(f"monthlyCRSP not found: {crsp_path}")
            logger.error("Please run the monthly CRSP data creation script first")
            return False
        
        crsp_data = pd.read_csv(crsp_path, usecols=['permno', 'time_avail_m', 'shrcls'])
        
        data = data.merge(crsp_data, on=['permno', 'time_avail_m'], how='left')
        logger.info(f"After merging with monthly CRSP data: {len(data)} records")
        
        # Preserve observations with missing ticker (equivalent to Stata's preserve/restore logic)
        missing_ticker_data = data[data['ticker'].isna()].copy()
        logger.info(f"Preserved {len(missing_ticker_data)} records with missing ticker")
        
        # Drop observations with missing ticker for merging
        data = data.dropna(subset=['ticker'])
        logger.info(f"After dropping missing ticker: {len(data)} records")
        
        # Load GovIndex data
        govindex_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/GovIndex.csv")
        
        logger.info(f"Loading GovIndex data from: {govindex_path}")
        
        if not govindex_path.exists():
            logger.error(f"GovIndex not found: {govindex_path}")
            logger.error("Please run the GovIndex data creation script first")
            return False
        
        govindex_data = pd.read_csv(govindex_path)
        
        # Merge with GovIndex data (equivalent to Stata's "merge m:1 ticker time_avail_m using "$pathDataIntermediate/GovIndex", keep(master match) nogenerate")
        data = data.merge(govindex_data, on=['ticker', 'time_avail_m'], how='left')
        logger.info(f"After merging with GovIndex data: {len(data)} records")
        
        # Append back the missing ticker data (equivalent to Stata's "append using "$pathtemp/temp"")
        data = pd.concat([data, missing_ticker_data], ignore_index=True)
        logger.info(f"After appending missing ticker data: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating activism signals...")
        
        # Shareholder activism proxy 1
        # Create tempBLOCK (equivalent to Stata's "gen tempBLOCK = maxinstown_perc if maxinstown_perc > 5")
        data['tempBLOCK'] = np.nan
        data.loc[data['maxinstown_perc'] > 5, 'tempBLOCK'] = data.loc[data['maxinstown_perc'] > 5, 'maxinstown_perc']
        
        # Replace missing with 0 (equivalent to Stata's "replace tempBLOCK = 0 if tempBLOCK == .")
        data['tempBLOCK'] = data['tempBLOCK'].fillna(0)
        
        # Create quartiles (equivalent to Stata's "egen tempBLOCKQuant = fastxtile(tempBLOCK), n(4) by(time_avail_m)")
        data['tempBLOCKQuant'] = data.groupby('time_avail_m')['tempBLOCK'].transform(
            lambda x: pd.qcut(x, q=4, labels=False, duplicates='drop') + 1
        )
        
        # Calculate tempEXT (equivalent to Stata's logic)
        data['tempEXT'] = 24 - data['G']
        data.loc[data['G'].isna(), 'tempEXT'] = np.nan
        data.loc[data['tempBLOCKQuant'] <= 3, 'tempEXT'] = np.nan
        data.loc[data['shrcls'].notna(), 'tempEXT'] = np.nan  # Exclude dual class shares
        
        # Create Activism1
        data['Activism1'] = data['tempEXT']
        
        # Drop temporary variables
        data = data.drop(['tempBLOCK', 'tempBLOCKQuant', 'tempEXT'], axis=1)
        
        # Shareholder activism proxy 2
        # Recreate tempBLOCK
        data['tempBLOCK'] = np.nan
        data.loc[data['maxinstown_perc'] > 5, 'tempBLOCK'] = data.loc[data['maxinstown_perc'] > 5, 'maxinstown_perc']
        data['tempBLOCK'] = data['tempBLOCK'].fillna(0)
        
        # Apply filters (equivalent to Stata's logic)
        data.loc[data['G'].isna(), 'tempBLOCK'] = np.nan
        data.loc[data['shrcls'].notna(), 'tempBLOCK'] = np.nan  # Exclude dual class shares
        data.loc[(24 - data['G']) < 19, 'tempBLOCK'] = np.nan
        
        # Create Activism2
        data['Activism2'] = data['tempBLOCK']
        
        # Drop temporary variables
        data = data.drop(['tempBLOCK'], axis=1)
        
        logger.info("Successfully calculated activism signals")
        
        # SAVE RESULTS
        logger.info("Saving activism predictor signals...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Save Activism1
        activism1_data = data[['permno', 'time_avail_m', 'Activism1']].copy()
        activism1_data = activism1_data.dropna(subset=['Activism1'])
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(activism1_data['time_avail_m']):
            activism1_data['time_avail_m'] = pd.to_datetime(activism1_data['time_avail_m'])
        
        activism1_data['yyyymm'] = activism1_data['time_avail_m'].dt.year * 100 + activism1_data['time_avail_m'].dt.month
        csv_output_path = predictors_dir / "Activism1.csv"
        activism1_data[['permno', 'yyyymm', 'Activism1']].to_csv(csv_output_path, index=False)
        logger.info(f"Saved Activism1 predictor to: {csv_output_path}")
        
        # Save Activism2
        activism2_data = data[['permno', 'time_avail_m', 'Activism2']].copy()
        activism2_data = activism2_data.dropna(subset=['Activism2'])
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(activism2_data['time_avail_m']):
            activism2_data['time_avail_m'] = pd.to_datetime(activism2_data['time_avail_m'])
        
        activism2_data['yyyymm'] = activism2_data['time_avail_m'].dt.year * 100 + activism2_data['time_avail_m'].dt.month
        csv_output_path = predictors_dir / "Activism2.csv"
        activism2_data[['permno', 'yyyymm', 'Activism2']].to_csv(csv_output_path, index=False)
        logger.info(f"Saved Activism2 predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed activism predictor signals")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct activism predictors: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    zz1_activism1_activism2()
