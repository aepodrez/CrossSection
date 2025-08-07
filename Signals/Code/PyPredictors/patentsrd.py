"""
Python equivalent of PatentsRD.do
Generated from: PatentsRD.do

Original Stata file: PatentsRD.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def patentsrd():
    """
    Python equivalent of PatentsRD.do
    
    Constructs the PatentsRD predictor signal for Patents to R&D capital ratio.
    """
    logger.info("Constructing predictor signal: PatentsRD...")
    
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
        required_vars = ['permno', 'gvkey', 'time_avail_m', 'mve_c', 'sicCRSP', 'exchcd']
        
        data = pd.read_csv(master_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Create year variable (equivalent to Stata's "gen year = yofd(dofm(time_avail_m))")
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        # Convert time_avail_m to datetime if needed for year extraction
        if not pd.api.types.is_datetime64_any_dtype(data['time_avail_m']):
            data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        
        data['year'] = data['time_avail_m'].dt.year
        
        # Load annual Compustat data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading annual Compustat data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"m_aCompustat not found: {compustat_path}")
            logger.error("Please run the annual Compustat data creation script first")
            return False
        
        compustat_data = pd.read_csv(compustat_path, usecols=['permno', 'time_avail_m', 'gvkey', 'xrd', 'sich', 'datadate', 'ceq'])
        
        # Convert time_avail_m to datetime before merge
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        compustat_data['time_avail_m'] = pd.to_datetime(compustat_data['time_avail_m'])
        
        # Merge with Compustat data
        # Since both datasets have gvkey, we need to handle the column conflict
        data = data.merge(compustat_data, on=['permno', 'time_avail_m'], how='left', suffixes=('', '_compustat'))
        logger.info(f"After merging with Compustat data: {len(data)} records")
        
        # If gvkey_compustat exists, use it instead of gvkey (Compustat gvkey is more reliable)
        if 'gvkey_compustat' in data.columns:
            data['gvkey'] = data['gvkey_compustat']
            data = data.drop('gvkey_compustat', axis=1)
        
        # Drop if gvkey is missing (equivalent to Stata's "drop if gvkey == .")
        data = data.dropna(subset=['gvkey'])
        logger.info(f"After dropping missing gvkey: {len(data)} records")
        
        # Load patent data
        patent_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/PatentDataProcessed.csv")
        
        logger.info(f"Loading patent data from: {patent_path}")
        
        if not patent_path.exists():
            logger.error(f"PatentDataProcessed not found: {patent_path}")
            logger.error("Please run the patent data processing script first")
            return False
        
        patent_data = pd.read_csv(patent_path, usecols=['gvkey', 'year', 'npat'])
        
        # Check if patent data is empty (placeholder file)
        if len(patent_data) == 0:
            logger.warning("Patent data is empty (placeholder file). Creating empty patent column.")
            data['npat'] = np.nan
        else:
            # Merge with patent data
            data = data.merge(patent_data, on=['gvkey', 'year'], how='left')
            logger.info(f"After merging with patent data: {len(data)} records")
        
        # Sort data for time series operations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate 6-month lag of npat (equivalent to Stata's "gen temp = l6.npat")
        data['npat_lag6'] = data.groupby('permno')['npat'].shift(6)
        
        # Replace missing values with 0 (equivalent to Stata's "replace temp = 0 if mi(temp)")
        data['npat_lag6'] = data['npat_lag6'].fillna(0)
        
        # Replace npat with lagged value (equivalent to Stata's "replace npat = temp")
        data['npat'] = data['npat_lag6']
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating PatentsRD signal...")
        
        # Keep only June observations (equivalent to Stata's "keep if month(dofm(time_avail_m)) == 6")
        data = data[data['time_avail_m'].dt.month == 6].copy()
        logger.info(f"After keeping June observations: {len(data)} records")
        
        # Drop observations before 1975 (equivalent to Stata's "drop if time_avail_m < ym(1975,1)")
        data = data[data['time_avail_m'] >= pd.Timestamp('1975-01-01')]
        logger.info(f"After dropping pre-1975 observations: {len(data)} records")
        
        # Replace missing xrd with 0 (equivalent to Stata's "replace xrd = 0 if mi(xrd)")
        data['xrd'] = data['xrd'].fillna(0)
        
        # Calculate R&D capital components (equivalent to Stata's complex lag calculations)
        data['xrd_lag24'] = data.groupby('permno')['xrd'].shift(24)
        data['xrd_lag36'] = data.groupby('permno')['xrd'].shift(36)
        data['xrd_lag48'] = data.groupby('permno')['xrd'].shift(48)
        data['xrd_lag60'] = data.groupby('permno')['xrd'].shift(60)
        data['xrd_lag72'] = data.groupby('permno')['xrd'].shift(72)
        
        # Calculate components (equivalent to Stata's comp1-comp5 calculations)
        data['comp1'] = data['xrd_lag24'].fillna(0)
        data['comp2'] = 0.8 * data['xrd_lag36'].fillna(0)
        data['comp3'] = 0.6 * data['xrd_lag48'].fillna(0)
        data['comp4'] = 0.4 * data['xrd_lag60'].fillna(0)
        data['comp5'] = 0.2 * data['xrd_lag72'].fillna(0)
        
        # Calculate R&D capital (equivalent to Stata's "gen RDcap = comp1 + comp2 + comp3 + comp4 + comp5")
        data['RDcap'] = data['comp1'] + data['comp2'] + data['comp3'] + data['comp4'] + data['comp5']
        
        # Calculate PatentsRD ratio (equivalent to Stata's "gen tempPatentsRD = npat/RDcap if RDcap > 0")
        data['tempPatentsRD'] = np.where(data['RDcap'] > 0, data['npat'] / data['RDcap'], np.nan)
        
        # Filter: drop first 2 observations per gvkey (equivalent to Stata's "bysort gvkey (time_avail_m): drop if _n <= 2")
        data = data.groupby('gvkey').apply(lambda x: x.iloc[2:]).reset_index(drop=True)
        logger.info(f"After dropping first 2 observations per gvkey: {len(data)} records")
        
        # Drop financial firms (equivalent to Stata's "drop if sicCRSP >= 6000 & sicCRSP <= 6999")
        data = data[~((data['sicCRSP'] >= 6000) & (data['sicCRSP'] <= 6999))]
        logger.info(f"After dropping financial firms: {len(data)} records")
        
        # Drop negative common equity (equivalent to Stata's "drop if ceq < 0")
        data = data[data['ceq'] >= 0]
        logger.info(f"After dropping negative common equity: {len(data)} records")
        
        # Create size categories (equivalent to Stata's "bys time_avail_m: astile sizecat = mve_c, qc(exchcd == 1) nq(2)")
        data['sizecat'] = data.groupby('time_avail_m').apply(
            lambda x: pd.qcut(x['mve_c'], q=2, labels=False, duplicates='drop') + 1
        ).reset_index(level=0, drop=True)
        
        # Create main categories (equivalent to Stata's "egen maincat = fastxtile(tempPatentsRD), by(time_avail_m) n(3)")
        data['maincat'] = data.groupby('time_avail_m')['tempPatentsRD'].transform(
            lambda x: pd.qcut(x, q=3, labels=False, duplicates='drop') + 1
        )
        
        # Create PatentsRD signal (equivalent to Stata's binary logic)
        data['PatentsRD'] = np.nan
        data.loc[(data['sizecat'] == 1) & (data['maincat'] == 3), 'PatentsRD'] = 1  # Small/High
        data.loc[(data['sizecat'] == 1) & (data['maincat'] == 1), 'PatentsRD'] = 0  # Small/Low
        
        logger.info("Successfully calculated PatentsRD signal")
        
        # Expand to monthly (equivalent to Stata's "expand temp" and time adjustment)
        logger.info("Expanding to monthly frequency...")
        
        # Create monthly expansion
        monthly_data = []
        for _, row in data.iterrows():
            for month_offset in range(12):
                new_row = row.copy()
                new_row['time_avail_m'] = row['time_avail_m'] + pd.DateOffset(months=month_offset)
                monthly_data.append(new_row)
        
        data = pd.DataFrame(monthly_data)
        logger.info(f"After expanding to monthly: {len(data)} records")
        
        # SAVE RESULTS
        logger.info("Saving PatentsRD predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'PatentsRD']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['PatentsRD'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "patentsrd.csv"
        csv_data = output_data[['permno', 'yyyymm', 'PatentsRD']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved PatentsRD predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed PatentsRD predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct PatentsRD predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    patentsrd()
