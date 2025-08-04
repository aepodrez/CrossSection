"""
Python equivalent of PS.do
Generated from: PS.do

Original Stata file: PS.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def ps():
    """
    Python equivalent of PS.do
    
    Constructs the PS predictor signal for Piotroski F-score.
    """
    logger.info("Constructing predictor signal: PS...")
    
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
        required_vars = ['permno', 'time_avail_m', 'fopt', 'oancf', 'ib', 'at', 'dltt', 'act', 'lct', 'txt', 'xint', 'sale', 'ceq']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Load SignalMasterTable for market value data
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
        
        # Load monthly CRSP data for shares outstanding
        crsp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/monthlyCRSP.csv")
        
        logger.info(f"Loading monthly CRSP data from: {crsp_path}")
        
        if not crsp_path.exists():
            logger.error(f"monthlyCRSP not found: {crsp_path}")
            logger.error("Please run the monthly CRSP data creation script first")
            return False
        
        crsp_data = pd.read_csv(crsp_path, usecols=['permno', 'time_avail_m', 'shrout'])
        
        # Merge with CRSP data
        data = data.merge(crsp_data, on=['permno', 'time_avail_m'], how='inner')
        logger.info(f"After merging with CRSP data: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating PS signal...")
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Replace missing fopt with oancf (equivalent to Stata's "replace fopt = oancf if fopt == .")
        data['fopt'] = data['fopt'].fillna(data['oancf'])
        
        # Calculate 12-month lags (equivalent to Stata's "l12." variables)
        data['ib_lag12'] = data.groupby('permno')['ib'].shift(12)
        data['at_lag12'] = data.groupby('permno')['at'].shift(12)
        data['dltt_lag12'] = data.groupby('permno')['dltt'].shift(12)
        data['act_lag12'] = data.groupby('permno')['act'].shift(12)
        data['lct_lag12'] = data.groupby('permno')['lct'].shift(12)
        data['sale_lag12'] = data.groupby('permno')['sale'].shift(12)
        data['shrout_lag12'] = data.groupby('permno')['shrout'].shift(12)
        
        # Calculate Piotroski F-score components (equivalent to Stata's p1-p9 calculations)
        
        # p1: Positive net income
        data['p1'] = (data['ib'] > 0).astype(int)
        
        # p2: Positive operating cash flow
        data['p2'] = (data['fopt'] > 0).astype(int)
        
        # p3: Increase in ROA
        data['p3'] = ((data['ib'] / data['at']) > (data['ib_lag12'] / data['at_lag12'])).astype(int)
        
        # p4: Operating cash flow exceeds net income
        data['p4'] = (data['fopt'] > data['ib']).astype(int)
        
        # p5: Decrease in leverage
        data['p5'] = ((data['dltt'] / data['at']) < (data['dltt_lag12'] / data['at_lag12'])).astype(int)
        
        # p6: Increase in current ratio
        data['p6'] = ((data['act'] / data['lct']) > (data['act_lag12'] / data['lct_lag12'])).astype(int)
        
        # p7: Increase in gross margin
        data['tempebit'] = data['ib'] + data['txt'] + data['xint']
        data['p7'] = ((data['tempebit'] / data['sale']) > (data['tempebit'] / data['sale_lag12'])).astype(int)
        
        # p8: Increase in asset turnover
        data['p8'] = ((data['sale'] / data['at']) > (data['sale_lag12'] / data['at_lag12'])).astype(int)
        
        # p9: No new equity issuance
        data['p9'] = (data['shrout'] <= data['shrout_lag12']).astype(int)
        
        # Calculate PS (Piotroski F-score) (equivalent to Stata's "gen PS = p1 + p2 + p3 + p4 + p5 + p6 + p7 + p8 + p9")
        data['PS'] = data['p1'] + data['p2'] + data['p3'] + data['p4'] + data['p5'] + data['p6'] + data['p7'] + data['p8'] + data['p9']
        
        # Replace PS with missing if any required variables are missing (equivalent to Stata's complex replace logic)
        missing_vars = ['fopt', 'ib', 'at', 'dltt', 'sale', 'act', 'tempebit', 'shrout']
        for var in missing_vars:
            data.loc[data[var].isna(), 'PS'] = np.nan
        
        # Calculate book-to-market ratio (equivalent to Stata's "gen BM = log(ceq/mve_c)")
        data['BM'] = np.log(data['ceq'] / data['mve_c'])
        
        # Create BM quintiles (equivalent to Stata's "egen temp = fastxtile(BM), by(time_avail_m) n(5)")
        data['temp'] = data.groupby('time_avail_m')['BM'].transform(
            lambda x: pd.qcut(x, q=5, labels=False, duplicates='drop') + 1
        )
        
        # Keep only highest BM quintile (equivalent to Stata's "replace PS = . if temp != 5")
        data.loc[data['temp'] != 5, 'PS'] = np.nan
        
        logger.info("Successfully calculated PS signal")
        
        # SAVE RESULTS
        logger.info("Saving PS predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'PS']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['PS'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "PS.csv"
        csv_data = output_data[['permno', 'yyyymm', 'PS']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved PS predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed PS predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct PS predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    ps()
