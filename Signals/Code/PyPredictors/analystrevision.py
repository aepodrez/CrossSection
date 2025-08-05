"""
Python equivalent of AnalystRevision.do
Generated from: AnalystRevision.do

Original Stata file: AnalystRevision.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def analystrevision():
    """
    Python equivalent of AnalystRevision.do
    
    Constructs the AnalystRevision predictor signal for analyst forecast revisions.
    """
    logger.info("Constructing predictor signal: AnalystRevision...")
    
    try:
        # DATA LOAD
        # Load IBES EPS Unadjusted data
        ibes_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/IBES_EPS_Unadj.csv")
        
        logger.info(f"Loading IBES EPS Unadjusted data from: {ibes_path}")
        
        if not ibes_path.exists():
            logger.error(f"IBES EPS Unadjusted data not found: {ibes_path}")
            return False
        
        # Load and filter IBES data (equivalent to Stata's "use IBES_EPS_Unadj, replace; keep if fpi == '1'")
        ibes_data = pd.read_csv(ibes_path)
        ibes_data = ibes_data[ibes_data['fpi'] == '1'].copy()
        ibes_data = ibes_data[['tickerIBES', 'time_avail_m', 'meanest']].copy()
        logger.info(f"Successfully loaded and filtered IBES data: {len(ibes_data)} records")
        
        # Load SignalMasterTable
        signal_master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {signal_master_path}")
        
        if not signal_master_path.exists():
            logger.error(f"SignalMasterTable not found: {signal_master_path}")
            return False
        
        data = pd.read_csv(signal_master_path, usecols=['permno', 'ticker', 'time_avail_m'])
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Merge with IBES data (equivalent to Stata's "merge m:1 tickerIBES time_avail_m using temp")
        data = data.merge(ibes_data, on=['tickerIBES', 'time_avail_m'], how='inner')
        logger.info(f"After merge: {len(data)} records")
        
        # Sort by permno and time_avail_m for lag calculations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Convert time_avail_m to datetime for proper lagging
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Constructing AnalystRevision signal...")
        
        # Calculate lagged meanest (equivalent to Stata's "gen AnalystRevision = meanest/l.meanest")
        data['meanest_lag'] = data.groupby('permno')['meanest'].shift(1)
        
        # Calculate AnalystRevision ratio
        data['AnalystRevision'] = data['meanest'] / data['meanest_lag']
        
        logger.info("Successfully calculated AnalystRevision signal")
        
        # SAVE RESULTS
        logger.info("Saving AnalystRevision predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'AnalystRevision']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['AnalystRevision'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "AnalystRevision.csv"
        csv_data = output_data[['permno', 'yyyymm', 'AnalystRevision']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved AnalystRevision predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed AnalystRevision predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct AnalystRevision predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    analystrevision()
