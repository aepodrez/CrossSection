"""
Python equivalent of EarningsStreak.do
Generated from: EarningsStreak.do

Original Stata file: EarningsStreak.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def earningsstreak():
    """
    Python equivalent of EarningsStreak.do
    
    Constructs the EarningsStreak predictor signal for earnings surprise among earnings streaks.
    """
    logger.info("Constructing predictor signal: EarningsStreak...")
    
    try:
        # PROCESS ACTUALS
        # Load IBES EPS adjusted data
        ibes_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/IBES_EPS_Adj.csv")
        
        logger.info(f"Loading IBES EPS adjusted data from: {ibes_path}")
        
        if not ibes_path.exists():
            logger.error(f"IBES EPS adjusted file not found: {ibes_path}")
            logger.error("Please run the IBES data download script first")
            return False
        
        # Load IBES data
        data = pd.read_csv(ibes_path)
        logger.info(f"Successfully loaded {len(data)} IBES records")
        
        # Keep fpi == "6" (equivalent to Stata's "keep if fpi == '6'")
        data = data[data['fpi'] == "6"]
        logger.info(f"After filtering for fpi==6: {len(data)} records")
        
        # Drop missing values (equivalent to Stata's "drop if actual == . | meanest == . | price == .")
        data = data.dropna(subset=['actual', 'meanest', 'price'])
        logger.info(f"After dropping missing values: {len(data)} records")
        
        # Use actual release date as date of availability (equivalent to Stata's date manipulation)
        data = data.drop(columns=['time_avail_m'])
        data['time_avail_m'] = pd.to_datetime(data['anndats_act']).dt.to_period('M').astype(str)
        
        # Keep the last forecast before the actual release (equivalent to Stata's sort and keep logic)
        data = data.sort_values(['tickerIBES', 'time_avail_m', 'anndats_act', 'statpers'])
        data = data.drop_duplicates(subset=['tickerIBES', 'time_avail_m'], keep='last')
        logger.info(f"After keeping last forecast: {len(data)} records")
        
        # Define Surp and Streak (equivalent to Stata's streak calculation)
        data['surp'] = (data['actual'] - data['meanest']) / data['price']
        
        # Sort for streak calculation
        data = data.sort_values(['tickerIBES', 'anndats_act'])
        
        # Calculate streak (equivalent to Stata's "gen streak = sign(surp) == sign(surp[_n-1])")
        data['surp_lag'] = data.groupby('tickerIBES')['surp'].shift(1)
        data['streak'] = (np.sign(data['surp']) == np.sign(data['surp_lag'])).astype(int)
        
        # Keep only streak == 1 (equivalent to Stata's "keep if streak == 1")
        data = data[data['streak'] == 1]
        logger.info(f"After keeping only streak==1: {len(data)} records")
        
        # Save temporary file (equivalent to Stata's "save "$pathtemp/tempibes", replace")
        temp_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Temp")
        temp_dir.mkdir(parents=True, exist_ok=True)
        data.to_csv(temp_dir / "tempibes.csv", index=False)
        
        # FILL TO MONTHLY AND ADD PERMNOS
        # Load SignalMasterTable
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load required variables from SignalMasterTable
        master_vars = ['permno', 'time_avail_m', 'tickerIBES']
        master_data = pd.read_csv(master_path, usecols=master_vars)
        
        # Merge with IBES data (equivalent to Stata's "merge m:1 tickerIBES time_avail_m using "$pathtemp/tempibes", keep(master match)")
        merged_data = master_data.merge(
            data,
            on=['tickerIBES', 'time_avail_m'],
            how='inner'  # keep(master match)
        )
        
        logger.info(f"Successfully merged data: {len(merged_data)} observations")
        
        # Drop unnecessary columns (equivalent to Stata's "drop fpi tickerIBES")
        merged_data = merged_data.drop(columns=['fpi', 'tickerIBES'])
        
        # Sort for lag calculations (equivalent to Stata's "xtset permno time_avail_m")
        merged_data = merged_data.sort_values(['permno', 'time_avail_m'])
        
        # Forward fill anndats_act (equivalent to Stata's "replace anndats_act = l1.anndats_act if anndats_act == .")
        merged_data['anndats_act'] = merged_data.groupby('permno')['anndats_act'].ffill()
        
        # Drop missing anndats_act (equivalent to Stata's "drop if anndats_act == .")
        merged_data = merged_data.dropna(subset=['anndats_act'])
        
        # Drop stale data (equivalent to Stata's "drop if time_avail_m - mofd(anndats_act) > 6")
        merged_data['anndats_act_m'] = pd.to_datetime(merged_data['anndats_act']).dt.to_period('M')
        merged_data['months_diff'] = merged_data['time_avail_m'].astype(int) - merged_data['anndats_act_m'].astype(int)
        merged_data = merged_data[merged_data['months_diff'] <= 6]
        logger.info(f"After dropping stale data: {len(merged_data)} observations")
        
        # Create EarningsStreak signal (equivalent to Stata's signal creation)
        merged_data['EarningsStreak'] = merged_data['surp']
        
        # Forward fill EarningsStreak (equivalent to Stata's "replace EarningsStreak = l1.EarningsStreak if EarningsStreak == .")
        merged_data['EarningsStreak'] = merged_data.groupby('permno')['EarningsStreak'].ffill()
        
        logger.info("Successfully calculated EarningsStreak signal")
        
        # SAVE RESULTS
        logger.info("Saving EarningsStreak predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = merged_data[['permno', 'time_avail_m', 'EarningsStreak']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['EarningsStreak'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "earningsstreak.csv"
        csv_data = output_data[['permno', 'yyyymm', 'EarningsStreak']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved EarningsStreak predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed EarningsStreak predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct EarningsStreak predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    earningsstreak()
