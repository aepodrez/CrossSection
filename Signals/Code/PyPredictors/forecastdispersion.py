"""
Python equivalent of ForecastDispersion.do
Generated from: ForecastDispersion.do

Original Stata file: ForecastDispersion.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def forecastdispersion():
    """
    Python equivalent of ForecastDispersion.do
    
    Constructs the ForecastDispersion predictor signal for EPS forecast dispersion.
    """
    logger.info("Constructing predictor signal: ForecastDispersion...")
    
    try:
        # Prep IBES data
        logger.info("Preparing IBES data...")
        
        # Load IBES EPS unadjusted data
        ibes_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/IBES_EPS_Unadj.csv")
        
        if not ibes_path.exists():
            logger.error(f"IBES EPS unadjusted file not found: {ibes_path}")
            logger.error("Please run the IBES data download scripts first")
            return False
        
        # Load and filter IBES data (equivalent to Stata's "keep if fpi == '1'")
        ibes_data = pd.read_csv(ibes_path)
        ibes_data = ibes_data[ibes_data['fpi'] == '1']
        
        # Keep only non-missing fpedats (equivalent to Stata's "keep if fpedats != .")
        ibes_data = ibes_data.dropna(subset=['fpedats'])
        
        # Keep required variables
        ibes_data = ibes_data[['tickerIBES', 'time_avail_m', 'stdev', 'meanest']]
        
        logger.info(f"Prepared IBES data: {len(ibes_data)} records")
        
        # DATA LOAD
        # Load SignalMasterTable data
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load the required variables
        required_vars = ['permno', 'time_avail_m', 'tickerIBES']
        
        data = pd.read_csv(master_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Merge with prepared IBES data (equivalent to Stata's "merge m:1 tickerIBES time_avail_m using "$pathtemp/temp", keep(master match) nogenerate keepusing(stdev meanest)")
        data = data.merge(
            ibes_data,
            on=['tickerIBES', 'time_avail_m'],
            how='inner'  # keep(master match)
        )
        
        logger.info(f"After merging with IBES: {len(data)} observations")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating ForecastDispersion signal...")
        
        # Calculate ForecastDispersion (equivalent to Stata's "gen ForecastDispersion = stdev/abs(meanest)")
        data['ForecastDispersion'] = data['stdev'] / data['meanest'].abs()
        
        logger.info("Successfully calculated ForecastDispersion signal")
        
        # SAVE RESULTS
        logger.info("Saving ForecastDispersion predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'ForecastDispersion']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['ForecastDispersion'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "ForecastDispersion.csv"
        csv_data = output_data[['permno', 'yyyymm', 'ForecastDispersion']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved ForecastDispersion predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed ForecastDispersion predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct ForecastDispersion predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    forecastdispersion()
