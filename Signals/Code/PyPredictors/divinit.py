"""
Python equivalent of DivInit.do
Generated from: DivInit.do

Original Stata file: DivInit.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def divinit():
    """
    Python equivalent of DivInit.do
    
    Constructs the DivInit predictor signal for dividend initiation.
    """
    logger.info("Constructing predictor signal: DivInit...")
    
    try:
        # PREP DISTRIBUTIONS DATA
        # Load CRSP distributions data
        distributions_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/CRSPdistributions.csv")
        
        logger.info(f"Loading CRSP distributions data from: {distributions_path}")
        
        if not distributions_path.exists():
            logger.error(f"CRSP distributions file not found: {distributions_path}")
            logger.error("Please run the CRSP distributions download script first")
            return False
        
        # Load distributions data
        dist_data = pd.read_csv(distributions_path)
        logger.info(f"Successfully loaded {len(dist_data)} distribution records")
        
        # Keep cash dividends only (equivalent to Stata's "keep if cd2 == 2 | cd2 == 3")
        dist_data = dist_data[dist_data['cd2'].isin([2, 3])]
        logger.info(f"After filtering for cash dividends: {len(dist_data)} records")
        
        # Create time_avail_m from exdt (equivalent to Stata's "gen time_avail_m = mofd(exdt)")
        dist_data['time_avail_m'] = pd.to_datetime(dist_data['exdt']).dt.to_period('M')
        
        # Drop missing time_avail_m or divamt (equivalent to Stata's "drop if time_avail_m == . | divamt == .")
        dist_data = dist_data.dropna(subset=['time_avail_m', 'divamt'])
        
        # Sum dividends by permno and time_avail_m (equivalent to Stata's "gcollapse (sum) divamt, by(permno time_avail_m)")
        div_summary = dist_data.groupby(['permno', 'time_avail_m'])['divamt'].sum().reset_index()
        logger.info(f"After collapsing dividends: {len(div_summary)} records")
        
        # Save temporary file (equivalent to Stata's "save "$pathtemp/tempdivamt", replace")
        temp_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Temp")
        temp_dir.mkdir(parents=True, exist_ok=True)
        div_summary.to_csv(temp_dir / "tempdivamt.csv", index=False)
        
        # DATA LOAD
        # Load SignalMasterTable
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load required variables from SignalMasterTable
        master_vars = ['permno', 'time_avail_m', 'exchcd', 'shrcd']
        data = pd.read_csv(master_path, usecols=master_vars)
        logger.info(f"Successfully loaded {len(data)} records from SignalMasterTable")
        
        # Merge with dividend data (equivalent to Stata's "merge 1:1 permno time_avail_m using "$pathtemp/tempdivamt", keep(master match)")
        merged_data = data.merge(
            div_summary,
            on=['permno', 'time_avail_m'],
            how='left'
        )
        
        logger.info(f"Successfully merged data: {len(merged_data)} observations")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating DivInit signal...")
        
        # Replace missing divamt with 0 (equivalent to Stata's "replace divamt = 0 if divamt == .")
        merged_data['divamt'] = merged_data['divamt'].fillna(0)
        
        # Sort for rolling calculations
        merged_data = merged_data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate 24-month rolling sum of dividends (equivalent to Stata's "asrol divamt, window(time_avail_m 24) stat(sum) gen(divsum)")
        merged_data['divsum'] = merged_data.groupby('permno')['divamt'].rolling(window=24, min_periods=1).sum().reset_index(0, drop=True)
        
        # Calculate 1-month lag of divsum (equivalent to Stata's "l1.divsum")
        merged_data['divsum_lag1'] = merged_data.groupby('permno')['divsum'].shift(1)
        
        # Create temp flag (equivalent to Stata's "gen temp = divamt > 0 & l1.divsum == 0")
        merged_data['temp'] = (merged_data['divamt'] > 0) & (merged_data['divsum_lag1'] == 0)
        
        # Calculate 6-month rolling sum of temp (equivalent to Stata's "asrol temp, window(time_avail_m 6) stat(sum) gen(initsum)")
        merged_data['initsum'] = merged_data.groupby('permno')['temp'].rolling(window=6, min_periods=1).sum().reset_index(0, drop=True)
        
        # Create DivInit (equivalent to Stata's "gen DivInit = initsum == 1")
        merged_data['DivInit'] = (merged_data['initsum'] == 1).astype(int)
        
        logger.info("Successfully calculated DivInit signal")
        
        # SAVE RESULTS
        logger.info("Saving DivInit predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = merged_data[['permno', 'time_avail_m', 'DivInit']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['DivInit'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "DivInit.csv"
        csv_data = output_data[['permno', 'yyyymm', 'DivInit']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved DivInit predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed DivInit predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct DivInit predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    divinit()
