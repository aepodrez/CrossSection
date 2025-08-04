"""
Python equivalent of DivYieldST.do
Generated from: DivYieldST.do

Original Stata file: DivYieldST.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def divyieldst():
    """
    Python equivalent of DivYieldST.do
    
    Constructs the DivYieldST predictor signal for predicted dividend yield next month.
    """
    logger.info("Constructing predictor signal: DivYieldST...")
    
    try:
        # PREP DISTRIBUTIONS DATA
        # Load CRSP distributions data
        distributions_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/CRSPdistributions.csv")
        
        logger.info(f"Loading CRSP distributions data from: {distributions_path}")
        
        if not distributions_path.exists():
            logger.error(f"CRSP distributions file not found: {distributions_path}")
            logger.error("Please run the CRSP distributions download script first")
            return False
        
        # Load required variables from distributions
        dist_vars = ['permno', 'cd1', 'cd2', 'cd3', 'divamt', 'exdt']
        dist_data = pd.read_csv(distributions_path, usecols=dist_vars)
        logger.info(f"Successfully loaded {len(dist_data)} distribution records")
        
        # Keep regular cash dividends (equivalent to Stata's "keep if cd1 == 1 & cd2 == 2")
        dist_data = dist_data[(dist_data['cd1'] == 1) & (dist_data['cd2'] == 2)]
        logger.info(f"After filtering for regular cash dividends: {len(dist_data)} records")
        
        # Keep quarterly, semi-annual, and annual dividends (equivalent to Stata's "keep if cd3 == 3 | cd3 == 4 | cd3 == 5")
        dist_data = dist_data[dist_data['cd3'].isin([3, 4, 5])]
        logger.info(f"After filtering for quarterly/semi-annual/annual dividends: {len(dist_data)} records")
        
        # Create time_avail_m from exdt (equivalent to Stata's "gen time_avail_m = mofd(exdt)")
        dist_data['time_avail_m'] = pd.to_datetime(dist_data['exdt']).dt.to_period('M')
        
        # Drop missing time_avail_m or divamt (equivalent to Stata's "drop if time_avail_m == . | divamt == .")
        dist_data = dist_data.dropna(subset=['time_avail_m', 'divamt'])
        
        # Sum dividends by permno, cd3, and time_avail_m (equivalent to Stata's "gcollapse (sum) divamt, by(permno cd3 time_avail_m)")
        div_summary = dist_data.groupby(['permno', 'cd3', 'time_avail_m'])['divamt'].sum().reset_index()
        
        # Clean up odd two-frequency permno-months by keeping the quarterly code (equivalent to Stata's sort and keep logic)
        div_summary = div_summary.sort_values(['permno', 'time_avail_m', 'cd3'])
        div_summary = div_summary.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
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
        master_vars = ['permno', 'time_avail_m', 'prc']
        data = pd.read_csv(master_path, usecols=master_vars)
        logger.info(f"Successfully loaded {len(data)} records from SignalMasterTable")
        
        # Merge with dividend data (equivalent to Stata's "merge 1:1 permno time_avail_m using "$pathtemp/tempdivamt", keep(master match)")
        merged_data = data.merge(
            div_summary[['permno', 'time_avail_m', 'cd3', 'divamt']],
            on=['permno', 'time_avail_m'],
            how='left'
        )
        
        # Merge with monthly CRSP data (equivalent to Stata's "merge 1:1 permno time_avail_m using "$pathDataIntermediate/monthlyCRSP", keep(master match)")
        crsp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/monthlyCRSP.csv")
        
        if not crsp_path.exists():
            logger.error(f"Monthly CRSP file not found: {crsp_path}")
            logger.error("Please run the CRSP data download script first")
            return False
        
        crsp_vars = ['permno', 'time_avail_m', 'ret', 'retx']
        crsp_data = pd.read_csv(crsp_path, usecols=crsp_vars)
        
        merged_data = merged_data.merge(
            crsp_data,
            on=['permno', 'time_avail_m'],
            how='left'
        )
        
        logger.info(f"Successfully merged data: {len(merged_data)} observations")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating DivYieldST signal...")
        
        # Sort for lag calculations
        merged_data = merged_data.sort_values(['permno', 'time_avail_m'])
        
        # Replace missing cd3 with lagged value (equivalent to Stata's "replace cd3 = l1.cd3 if cd3 == .")
        merged_data['cd3'] = merged_data.groupby('permno')['cd3'].ffill()
        
        # Replace missing divamt with 0 (equivalent to Stata's "replace divamt = 0 if divamt == .")
        merged_data['divamt'] = merged_data['divamt'].fillna(0)
        
        # Calculate 12-month rolling sum of dividends (equivalent to Stata's "asrol divamt, window(time_avail_m 12) stat(sum) gen(div12)")
        merged_data['div12'] = merged_data.groupby('permno')['divamt'].rolling(window=12, min_periods=1).sum().reset_index(0, drop=True)
        
        # Keep only dividend payers (equivalent to Stata's "drop if div12 == 0 | div12 == .")
        merged_data = merged_data[(merged_data['div12'] > 0) & (merged_data['div12'].notna())]
        logger.info(f"After filtering for dividend payers: {len(merged_data)} observations")
        
        # Calculate lags for expected dividend
        merged_data['divamt_lag2'] = merged_data.groupby('permno')['divamt'].shift(2)
        merged_data['divamt_lag5'] = merged_data.groupby('permno')['divamt'].shift(5)
        merged_data['divamt_lag11'] = merged_data.groupby('permno')['divamt'].shift(11)
        
        # Calculate expected dividend based on frequency (equivalent to Stata's Ediv1 logic)
        merged_data['Ediv1'] = np.nan
        
        # Quarterly, unknown, and missing frequency (assumed quarterly)
        quarterly_mask = merged_data['cd3'].isin([3, 0, 1])
        merged_data.loc[quarterly_mask, 'Ediv1'] = merged_data.loc[quarterly_mask, 'divamt_lag2']
        
        # Semi-annual
        semi_annual_mask = merged_data['cd3'] == 4
        merged_data.loc[semi_annual_mask, 'Ediv1'] = merged_data.loc[semi_annual_mask, 'divamt_lag5']
        
        # Annual
        annual_mask = merged_data['cd3'] == 5
        merged_data.loc[annual_mask, 'Ediv1'] = merged_data.loc[annual_mask, 'divamt_lag11']
        
        # Calculate expected dividend yield (equivalent to Stata's "gen Edy1 = Ediv1/abs(prc)")
        merged_data['Edy1'] = merged_data['Ediv1'] / merged_data['prc'].abs()
        
        # Create positive expected dividend yield (equivalent to Stata's "gen Edy1pos = Edy1 if Edy1 > 0")
        merged_data['Edy1pos'] = merged_data['Edy1'].where(merged_data['Edy1'] > 0)
        
        # Create 3-quintile groups for positive expected dividend yield (equivalent to Stata's "egen DivYieldST = fastxtile(Edy1pos), by(time_avail_m) n(3)")
        merged_data['DivYieldST'] = merged_data.groupby('time_avail_m')['Edy1pos'].transform(
            lambda x: pd.qcut(x, q=3, labels=[1, 2, 3], duplicates='drop')
        )
        
        # Set DivYieldST to 0 for stocks with zero expected dividend yield (equivalent to Stata's "replace DivYieldST = 0 if Edy1 == 0")
        merged_data.loc[merged_data['Edy1'] == 0, 'DivYieldST'] = 0
        
        logger.info("Successfully calculated DivYieldST signal")
        
        # SAVE RESULTS
        logger.info("Saving DivYieldST predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = merged_data[['permno', 'time_avail_m', 'DivYieldST']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['DivYieldST'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "DivYieldST.csv"
        csv_data = output_data[['permno', 'yyyymm', 'DivYieldST']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved DivYieldST predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed DivYieldST predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct DivYieldST predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    divyieldst()
