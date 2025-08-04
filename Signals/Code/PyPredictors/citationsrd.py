"""
Python equivalent of CitationsRD.do
Generated from: CitationsRD.do

Original Stata file: CitationsRD.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def citationsrd():
    """
    Python equivalent of CitationsRD.do
    
    Constructs the CitationsRD predictor signal for citations to R&D expenses.
    """
    logger.info("Constructing predictor signal: CitationsRD...")
    
    try:
        # DATA LOAD
        # Load SignalMasterTable
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load required variables from SignalMasterTable
        master_vars = ['permno', 'gvkey', 'time_avail_m', 'mve_c', 'sicCRSP', 'exchcd']
        master_data = pd.read_csv(master_path, usecols=master_vars)
        
        # Create year variable
        master_data['time_avail_m'] = pd.to_datetime(master_data['time_avail_m'])
        # Convert time_avail_m to datetime if needed for year extraction
        if not pd.api.types.is_datetime64_any_dtype(master_data['time_avail_m']):
            master_data['time_avail_m'] = pd.to_datetime(master_data['time_avail_m'])
        
        master_data['year'] = master_data['time_avail_m'].dt.year
        
        # Load Compustat annual data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        if not compustat_path.exists():
            logger.error(f"Compustat data not found: {compustat_path}")
            logger.error("Please run the Compustat data download scripts first")
            return False
        
        # Load required variables from Compustat
        compustat_vars = ['permno', 'time_avail_m', 'xrd', 'sich', 'datadate', 'ceq']
        compustat_data = pd.read_csv(compustat_path, usecols=compustat_vars)
        
        # Merge with Compustat data
        merged_data = master_data.merge(
            compustat_data, 
            on=['permno', 'time_avail_m'], 
            how='left'  # equivalent to Stata's keep(master match)
        )
        
        # Drop if gvkey is missing
        merged_data = merged_data[merged_data['gvkey'].notna()]
        
        logger.info(f"Successfully merged with Compustat data: {len(merged_data)} observations")
        
        # Load patent citation dataset
        patent_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/PatentDataProcessed.csv")
        
        if not patent_path.exists():
            logger.error(f"Patent data not found: {patent_path}")
            logger.error("Please run the patent data download scripts first")
            return False
        
        # Load required variables from patent data
        patent_vars = ['gvkey', 'year', 'ncitscale']
        patent_data = pd.read_csv(patent_path, usecols=patent_vars)
        
        # Merge with patent data
        merged_data = merged_data.merge(
            patent_data, 
            on=['gvkey', 'year'], 
            how='left'  # equivalent to Stata's keep(master match)
        )
        
        logger.info(f"Successfully merged with patent data: {len(merged_data)} observations")
        
        # Sort by permno and time_avail_m for lag calculations
        merged_data = merged_data.sort_values(['permno', 'time_avail_m'])
        
        # Lag patent citations by 6 months (equivalent to Stata's "gen temp = l6.ncitscale")
        merged_data['ncitscale_lag6'] = merged_data.groupby('permno')['ncitscale'].shift(6)
        merged_data['ncitscale_lag6'] = merged_data['ncitscale_lag6'].fillna(0)
        merged_data['ncitscale'] = merged_data['ncitscale_lag6']
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating CitationsRD signal...")
        
        # Filter for June observations and after 1975
        merged_data = merged_data[merged_data['time_avail_m'].dt.month == 6]
        merged_data = merged_data[merged_data['time_avail_m'].dt.year >= 1975]
        
        # Calculate 24-month lag of xrd
        merged_data['xrd_lag24'] = merged_data.groupby('permno')['xrd'].shift(24)
        merged_data['xrd_lag24'] = merged_data['xrd_lag24'].fillna(0)
        
        # Calculate rolling sums (equivalent to Stata's asrol with 48-month window)
        # For simplicity, we'll use a 48-month rolling sum
        merged_data['sum_xrd'] = merged_data.groupby('permno')['xrd_lag24'].rolling(window=48, min_periods=1).sum().reset_index(0, drop=True)
        merged_data['sum_ncit'] = merged_data.groupby('permno')['ncitscale'].rolling(window=48, min_periods=1).sum().reset_index(0, drop=True)
        
        # Calculate tempCitationsRD
        merged_data['tempCitationsRD'] = np.where(
            merged_data['sum_xrd'] > 0,
            merged_data['sum_ncit'] / merged_data['sum_xrd'],
            np.nan
        )
        
        # Filter: drop first 2 observations per gvkey, exclude financials, exclude negative equity
        merged_data = merged_data.sort_values(['gvkey', 'time_avail_m'])
        merged_data = merged_data.groupby('gvkey').tail(-2)  # Drop first 2 observations
        merged_data = merged_data[~((merged_data['sicCRSP'] >= 6000) & (merged_data['sicCRSP'] <= 6999))]  # Exclude financials
        merged_data = merged_data[merged_data['ceq'] >= 0]  # Exclude negative equity
        
        # Create size categories (equivalent to Stata's astile)
        merged_data['sizecat'] = merged_data.groupby('time_avail_m').apply(
            lambda x: pd.qcut(x[x['exchcd'] == 1]['mve_c'], q=2, labels=False, duplicates='drop') + 1
            if len(x[x['exchcd'] == 1]) > 0 else np.nan
        ).reset_index(0, drop=True)
        
        # Create main categories (equivalent to Stata's fastxtile)
        merged_data['maincat'] = merged_data.groupby('time_avail_m')['tempCitationsRD'].transform(
            lambda x: pd.qcut(x, q=3, labels=False, duplicates='drop') + 1
        )
        
        # Calculate CitationsRD (equivalent to Stata's binary logic)
        merged_data['CitationsRD'] = np.nan
        merged_data.loc[(merged_data['sizecat'] == 1) & (merged_data['maincat'] == 3), 'CitationsRD'] = 1
        merged_data.loc[(merged_data['sizecat'] == 1) & (merged_data['maincat'] == 1), 'CitationsRD'] = 0
        
        # Expand back to monthly (equivalent to Stata's expand logic)
        logger.info("Expanding annual data to monthly...")
        
        expanded_data = []
        for _, row in merged_data.iterrows():
            for month in range(12):
                new_row = row.copy()
                new_row['time_avail_m'] = row['time_avail_m'] + pd.DateOffset(months=month)
                expanded_data.append(new_row)
        
        merged_data = pd.DataFrame(expanded_data)
        
        # Remove duplicates: keep first observation per gvkey-time_avail_m
        merged_data = merged_data.drop_duplicates(subset=['gvkey', 'time_avail_m'], keep='first')
        
        logger.info("Successfully calculated CitationsRD signal")
        
        # SAVE RESULTS
        logger.info("Saving CitationsRD predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = merged_data[['permno', 'time_avail_m', 'CitationsRD']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['CitationsRD'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "CitationsRD.csv"
        csv_data = output_data[['permno', 'yyyymm', 'CitationsRD']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved CitationsRD predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed CitationsRD predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct CitationsRD predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    citationsrd()
