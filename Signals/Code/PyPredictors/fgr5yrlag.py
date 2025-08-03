"""
Python equivalent of fgr5yrLag.do
Generated from: fgr5yrLag.do

Original Stata file: fgr5yrLag.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def fgr5yrlag():
    """
    Python equivalent of fgr5yrLag.do
    
    Constructs the fgr5yrLag predictor signal for long-term EPS forecast lagged.
    """
    logger.info("Constructing predictor signal: fgr5yrLag...")
    
    try:
        # Prep IBES data
        logger.info("Preparing IBES data...")
        
        # Load IBES EPS unadjusted data
        ibes_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/IBES_EPS_Unadj.csv")
        
        if not ibes_path.exists():
            logger.error(f"IBES EPS unadjusted file not found: {ibes_path}")
            logger.error("Please run the IBES data download scripts first")
            return False
        
        # Load and filter IBES data (equivalent to Stata's "keep if fpi == '0'")
        ibes_data = pd.read_csv(ibes_path)
        ibes_data = ibes_data[ibes_data['fpi'] == '0']
        
        # Rename meanest to fgr5yr (equivalent to Stata's "rename meanest fgr5yr")
        ibes_data = ibes_data.rename(columns={'meanest': 'fgr5yr'})
        
        # Keep required variables
        ibes_data = ibes_data[['tickerIBES', 'time_avail_m', 'fgr5yr']]
        
        logger.info(f"Prepared IBES data: {len(ibes_data)} records")
        
        # DATA LOAD
        # Load Compustat annual data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading Compustat annual data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"Compustat annual file not found: {compustat_path}")
            logger.error("Please run the Compustat data download scripts first")
            return False
        
        # Load the required variables
        required_vars = ['permno', 'time_avail_m', 'ceq', 'ib', 'txdi', 'dv', 'sale', 'ni', 'dp']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # Merge with SignalMasterTable to get tickerIBES
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load required variables from SignalMasterTable
        master_vars = ['permno', 'time_avail_m', 'tickerIBES']
        master_data = pd.read_csv(master_path, usecols=master_vars)
        
        # Merge data (equivalent to Stata's "merge 1:1 permno time_avail_m using "$pathDataIntermediate/SignalMasterTable", keep(match) nogenerate keepusing(tickerIBES)")
        data = data.merge(
            master_data,
            on=['permno', 'time_avail_m'],
            how='inner'  # keep(match)
        )
        
        logger.info(f"After merging with SignalMasterTable: {len(data)} observations")
        
        # Merge with prepared IBES data
        data = data.merge(
            ibes_data,
            on=['tickerIBES', 'time_avail_m'],
            how='inner'  # keep(match)
        )
        
        logger.info(f"After merging with IBES: {len(data)} observations")
        
        # Drop missing values (equivalent to Stata's "drop if ceq == . | ib == . | txdi == . | dv == . | sale == . | ni == . | dp == . | fgr5yr == .")
        data = data.dropna(subset=['ceq', 'ib', 'txdi', 'dv', 'sale', 'ni', 'dp', 'fgr5yr'])
        
        # Drop unnecessary columns (equivalent to Stata's "drop ceq dp dv ib ni sale txdi")
        data = data.drop(['ceq', 'dp', 'dv', 'ib', 'ni', 'sale', 'txdi'], axis=1)
        
        logger.info(f"After dropping missing values and unnecessary columns: {len(data)} observations")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating fgr5yrLag signal...")
        
        # Sort data for lag calculations (equivalent to Stata's "xtset permno time_avail_m")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate 6-month lag of fgr5yr (equivalent to Stata's "gen fgr5yrLag = l6.fgr5yr")
        data['fgr5yrLag'] = data.groupby('permno')['fgr5yr'].shift(6)
        
        # Keep only June observations (equivalent to Stata's "keep if month(dofm(time_avail_m)) == 6")
        data['month'] = pd.to_datetime(data['time_avail_m']).dt.month
        data = data[data['month'] == 6]
        data = data.drop('month', axis=1)
        
        logger.info(f"After keeping June observations: {len(data)} observations")
        
        # Expand to monthly (equivalent to Stata's expand logic)
        logger.info("Expanding June data to monthly...")
        
        # Create monthly observations
        monthly_data = []
        for _, row in data.iterrows():
            for month in range(12):
                new_row = row.copy()
                new_row['time_avail_m'] = row['time_avail_m'] + pd.DateOffset(months=month)
                monthly_data.append(new_row)
        
        data = pd.DataFrame(monthly_data)
        
        logger.info(f"After expanding to monthly: {len(data)} observations")
        
        # SAVE RESULTS
        logger.info("Saving fgr5yrLag predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'fgr5yrLag']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['fgr5yrLag'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "fgr5yrLag.csv"
        csv_data = output_data[['permno', 'yyyymm', 'fgr5yrLag']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved fgr5yrLag predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed fgr5yrLag predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct fgr5yrLag predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    fgr5yrlag()
