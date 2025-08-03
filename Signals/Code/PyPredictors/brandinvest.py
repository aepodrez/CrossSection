"""
Python equivalent of BrandInvest.do
Generated from: BrandInvest.do

Original Stata file: BrandInvest.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def brandinvest():
    """
    Python equivalent of BrandInvest.do
    
    Constructs the BrandInvest predictor signal for brand investment rate.
    """
    logger.info("Constructing predictor signal: BrandInvest...")
    
    try:
        # DATA LOAD
        # Load Compustat annual data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/a_aCompustat.csv")
        
        logger.info(f"Loading Compustat annual data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"Input file not found: {compustat_path}")
            logger.error("Please run the Compustat data download scripts first")
            return False
        
        # Load the required variables
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'fyear', 'datadate', 'xad', 'xad0', 'at', 'sic']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Constructing BrandInvest signal...")
        
        # Sort by gvkey and fyear for lag calculations
        data = data.sort_values(['gvkey', 'fyear'])
        
        # Create OK flag for non-missing xad (equivalent to Stata's "gen byte OK = !missing(xad)")
        data['OK'] = ~data['xad'].isna()
        
        # Calculate BrandCapital for first observation with advertising (equivalent to Stata's BrandCapital calculation)
        data['BrandCapital'] = np.nan
        data.loc[(data['OK'] == True) & (data.groupby('gvkey').cumcount() == 0), 'BrandCapital'] = (
            data.loc[(data['OK'] == True) & (data.groupby('gvkey').cumcount() == 0), 'xad'] / (0.5 + 0.1)
        )
        
        # Create tempYear for first advertising year
        data['tempYear'] = np.nan
        data.loc[(data['OK'] == True) & (data.groupby('gvkey').cumcount() == 0), 'tempYear'] = (
            data.loc[(data['OK'] == True) & (data.groupby('gvkey').cumcount() == 0), 'fyear']
        )
        
        # Calculate FirstNMyear (equivalent to Stata's "egen FirstNMyear = min(tempYear), by(gvkey)")
        data['FirstNMyear'] = data.groupby('gvkey')['tempYear'].transform('min')
        
        # Create tempxad with missing values set to 0
        data['tempxad'] = data['xad'].fillna(0)
        
        # Set BrandCapital to 0 if missing
        data['BrandCapital'] = data['BrandCapital'].fillna(0)
        
        # Calculate BrandCapital for subsequent years (equivalent to Stata's recursive calculation)
        for gvkey in data['gvkey'].unique():
            gvkey_data = data[data['gvkey'] == gvkey].copy()
            gvkey_data = gvkey_data.sort_values('fyear')
            
            for i in range(1, len(gvkey_data)):
                if not pd.isna(gvkey_data.iloc[i-1]['BrandCapital']):
                    gvkey_data.iloc[i, gvkey_data.columns.get_loc('BrandCapital')] = (
                        (1 - 0.5) * gvkey_data.iloc[i-1]['BrandCapital'] + gvkey_data.iloc[i]['tempxad']
                    )
            
            data.loc[data['gvkey'] == gvkey, 'BrandCapital'] = gvkey_data['BrandCapital'].values
        
        # Set to missing if FirstNMyear is missing or fyear < FirstNMyear
        data.loc[data['FirstNMyear'].isna() | (data['fyear'] < data['FirstNMyear']), 'BrandCapital'] = np.nan
        
        # Set to missing if xad is missing
        data.loc[data['xad'].isna(), 'BrandCapital'] = np.nan
        
        # Scale by total assets
        data['BrandCapital'] = data['BrandCapital'] / data['at']
        
        # Calculate BrandInvest (equivalent to Stata's "gen BrandInvest = xad0/l.BrandCapital")
        data['BrandCapital_lag'] = data.groupby('gvkey')['BrandCapital'].shift(1)
        data['BrandInvest'] = data['xad0'] / data['BrandCapital_lag']
        
        # Filter by SIC codes (equivalent to Stata's SIC filtering)
        data['sic'] = pd.to_numeric(data['sic'], errors='coerce')
        data = data[~((data['sic'] >= 4900) & (data['sic'] <= 4999))]  # Utilities
        data = data[~((data['sic'] >= 6000) & (data['sic'] <= 6999))]  # Financials
        
        # Keep only December observations (equivalent to Stata's "keep if month(datadate) == 12")
        data['datadate'] = pd.to_datetime(data['datadate'])
        data = data[data['datadate'].dt.month == 12]
        
        # Expand to monthly (equivalent to Stata's expand logic)
        logger.info("Expanding annual data to monthly...")
        
        expanded_data = []
        for _, row in data.iterrows():
            for month in range(12):
                new_row = row.copy()
                new_row['time_avail_m'] = row['time_avail_m'] + month
                expanded_data.append(new_row)
        
        data = pd.DataFrame(expanded_data)
        
        # Keep last observation per gvkey-time_avail_m (equivalent to Stata's deduplication)
        data = data.sort_values(['gvkey', 'time_avail_m', 'datadate'])
        data = data.drop_duplicates(subset=['gvkey', 'time_avail_m'], keep='last')
        
        # Keep first observation per permno-time_avail_m
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        
        logger.info("Successfully calculated BrandInvest signal")
        
        # SAVE RESULTS
        logger.info("Saving BrandInvest predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'BrandInvest']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['BrandInvest'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "BrandInvest.csv"
        csv_data = output_data[['permno', 'yyyymm', 'BrandInvest']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved BrandInvest predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed BrandInvest predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct BrandInvest predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    brandinvest()
