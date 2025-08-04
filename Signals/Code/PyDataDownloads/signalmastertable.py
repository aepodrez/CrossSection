"""
Python equivalent of SignalMasterTable.do
Generated from: SignalMasterTable.do

Original Stata file: SignalMasterTable.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def signalmastertable():
    """
    Python equivalent of SignalMasterTable.do
    
    Creates the SignalMasterTable which holds monthly list of firms with identifiers and meta information.
    This is a critical file that many predictors depend on.
    """
    logger.info("Creating SignalMasterTable...")
    
    try:
        # Load monthly CRSP data
        crsp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/monthlyCRSP.csv")
        
        logger.info(f"Loading monthly CRSP data from: {crsp_path}")
        
        if not crsp_path.exists():
            logger.error(f"Monthly CRSP data not found: {crsp_path}")
            logger.error("Please run the CRSP monthly download script first")
            return False
        
        # Load required variables from CRSP
        required_vars = ['permno', 'ticker', 'exchcd', 'shrcd', 'time_avail_m', 'mve_c', 'prc', 'ret', 'sicCRSP']
        
        crsp_data = pd.read_csv(crsp_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(crsp_data)} CRSP records")
        
        # Screen on Stock market information: common stocks and major exchanges
        # (equivalent to Stata's "keep if (shrcd == 10 | shrcd == 11 | shrcd == 12) & (exchcd == 1 | exchcd == 2 | exchcd == 3)")
        common_stocks = (crsp_data['shrcd'] == 10) | (crsp_data['shrcd'] == 11) | (crsp_data['shrcd'] == 12)
        major_exchanges = (crsp_data['exchcd'] == 1) | (crsp_data['exchcd'] == 2) | (crsp_data['exchcd'] == 3)
        crsp_data = crsp_data[common_stocks & major_exchanges]
        logger.info(f"After screening for common stocks and major exchanges: {len(crsp_data)} records")
        
        # Load Compustat data for merge
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading Compustat data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"Compustat data not found: {compustat_path}")
            logger.error("Please run the Compustat annual download script first")
            return False
        
        # Load required variables from Compustat
        compustat_vars = ['permno', 'time_avail_m', 'gvkey', 'sic']
        compustat_data = pd.read_csv(compustat_path, usecols=compustat_vars)
        logger.info(f"Successfully loaded {len(compustat_data)} Compustat records")
        
        # Merge CRSP with Compustat (equivalent to Stata's "merge 1:1 permno time_avail_m using m_aCompustat, keepusing(gvkey sic) keep(master match) nogenerate")
        crsp_data = crsp_data.merge(compustat_data[['permno', 'time_avail_m', 'gvkey', 'sic']], 
                                   on=['permno', 'time_avail_m'], 
                                   how='left')
        
        # Rename sic to sicCS (equivalent to Stata's "rename sic sicCS")
        crsp_data = crsp_data.rename(columns={'sic': 'sicCS'})
        logger.info(f"After merging with Compustat: {len(crsp_data)} records")
        
        # Add auxiliary variables
        # NYSE indicator (equivalent to Stata's "gen NYSE = exchcd == 1")
        crsp_data['NYSE'] = (crsp_data['exchcd'] == 1).astype(int)
        
        # Sort by permno and time_avail_m for lag calculations
        crsp_data = crsp_data.sort_values(['permno', 'time_avail_m'])
        
        # Future buy and hold return (equivalent to Stata's "gen bh1m = f.ret")
        crsp_data['bh1m'] = crsp_data.groupby('permno')['ret'].shift(-1)
        
        # Keep required variables (equivalent to Stata's "keep gvkey permno ticker time_avail_m ret bh1m mve_c prc NYSE exchcd shrcd sicCS sicCRSP")
        final_vars = ['gvkey', 'permno', 'ticker', 'time_avail_m', 'ret', 'bh1m', 'mve_c', 'prc', 'NYSE', 'exchcd', 'shrcd', 'sicCS', 'sicCRSP']
        crsp_data = crsp_data[final_vars]
        
        # Add IBES ticker if available (equivalent to Stata's conditional merge)
        ibes_link_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/IBESCRSPLinkingTable.csv")
        
        if ibes_link_path.exists():
            logger.info("Adding IBES-CRSP linking table...")
            ibes_link = pd.read_csv(ibes_link_path)
            crsp_data = crsp_data.merge(ibes_link, on='permno', how='left')
            logger.info("Successfully added IBES-CRSP link")
        else:
            logger.warning("IBES-CRSP linking table not found. Some signals cannot be generated.")
        
        # Add OptionMetrics secid if available (equivalent to Stata's conditional merge)
        optionmetrics_link_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/OPTIONMETRICSCRSPLinkingTable.csv")
        
        if optionmetrics_link_path.exists():
            logger.info("Adding OptionMetrics-CRSP linking table...")
            optionmetrics_link = pd.read_csv(optionmetrics_link_path)
            crsp_data = crsp_data.merge(optionmetrics_link, on='permno', how='left')
            logger.info("Successfully added OptionMetrics-CRSP link")
        else:
            logger.warning("OptionMetrics-CRSP linking table not found. Some signals cannot be generated.")
        
        # Final sort (equivalent to Stata's "xtset permno time_avail_m")
        crsp_data = crsp_data.sort_values(['permno', 'time_avail_m'])
        
        # Save SignalMasterTable
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        crsp_data.to_csv(output_path, index=False)
        logger.info(f"Saved SignalMasterTable to: {output_path}")
        
        # Log summary statistics
        logger.info("SignalMasterTable summary:")
        logger.info(f"  Total records: {len(crsp_data)}")
        logger.info(f"  Unique firms (permno): {crsp_data['permno'].nunique()}")
        logger.info(f"  Time range: {crsp_data['time_avail_m'].min()} to {crsp_data['time_avail_m'].max()}")
        logger.info(f"  NYSE firms: {crsp_data['NYSE'].sum()}")
        logger.info(f"  Non-missing market value: {crsp_data['mve_c'].notna().sum()}")
        logger.info(f"  Non-missing returns: {crsp_data['ret'].notna().sum()}")
        
        logger.info("Successfully created SignalMasterTable")
        return True
        
    except Exception as e:
        logger.error(f"Failed to create SignalMasterTable: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the SignalMasterTable creation function
    signalmastertable() 