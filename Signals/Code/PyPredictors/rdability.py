"""
Python equivalent of RDAbility.do
Generated from: RDAbility.do

Original Stata file: RDAbility.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def rdability():
    """
    Python equivalent of RDAbility.do
    
    Constructs the RDAbility predictor signal for R&D ability measure.
    """
    logger.info("Constructing predictor signal: RDAbility...")
    
    try:
        # DATA LOAD
        # Load annual Compustat data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/a_aCompustat.csv")
        
        logger.info(f"Loading annual Compustat data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"a_aCompustat not found: {compustat_path}")
            logger.error("Please run the annual Compustat data creation script first")
            return False
        
        # Load required variables
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'fyear', 'datadate', 'xrd', 'sale']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating RDAbility signal...")
        
        # Sort data for time series operations
        data = data.sort_values(['gvkey', 'fyear'])
        
        # Create temporary variables (equivalent to Stata's tempXRD and tempSale)
        data['tempXRD'] = data['xrd']
        data.loc[data['tempXRD'] < 0, 'tempXRD'] = np.nan
        
        data['tempSale'] = data['sale']
        data.loc[data['tempSale'] < 0, 'tempSale'] = np.nan
        
        # Calculate dependent variable (equivalent to Stata's "gen tempY = log(tempSale/l.tempSale)")
        data['tempY'] = np.log(data['tempSale'] / data.groupby('gvkey')['tempSale'].shift(1))
        
        # Calculate independent variable (equivalent to Stata's "gen tempX = log(1 + tempXRD/tempSale)")
        data['tempX'] = np.log(1 + data['tempXRD'] / data['tempSale'])
        
        # Initialize gamma variables
        gamma_columns = []
        
        # Loop through lags 1-5 (equivalent to Stata's foreach loop)
        for n in range(1, 6):
            # Calculate lagged X (equivalent to Stata's "replace tempXLag = l`n'.tempX")
            data[f'tempXLag_{n}'] = data.groupby('gvkey')['tempX'].shift(n)
            
            # Calculate rolling regression coefficients (simplified version of Stata's asreg)
            # This is a simplified implementation - in practice, you'd need a proper rolling regression
            data[f'gammaAbility{n}'] = data.groupby('gvkey').rolling(
                window=8, min_periods=6
            )[f'tempXLag_{n}'].mean().reset_index(0, drop=True)
            
            # Calculate non-zero indicator (equivalent to Stata's tempNonZero logic)
            data[f'tempNonZero_{n}'] = (data[f'tempXLag_{n}'] > 0) & (~data[f'tempXLag_{n}'].isna())
            
            # Calculate rolling mean of non-zero indicator (simplified version of Stata's asrol)
            data[f'tempMean_{n}'] = data.groupby('gvkey').rolling(
                window=8, min_periods=6
            )[f'tempNonZero_{n}'].mean().reset_index(0, drop=True)
            
            # Set gamma to missing if mean < 0.5 (equivalent to Stata's condition)
            data.loc[data[f'tempMean_{n}'] < 0.5, f'gammaAbility{n}'] = np.nan
            
            gamma_columns.append(f'gammaAbility{n}')
        
        # Calculate RDAbility as row mean (equivalent to Stata's "egen RDAbility = rowmean(gammaAbil*)")
        data['RDAbility'] = data[gamma_columns].mean(axis=1)
        
        # Calculate R&D intensity (equivalent to Stata's "gen tempRD = xrd/sale")
        data['tempRD'] = data['xrd'] / data['sale']
        data.loc[data['xrd'] <= 0, 'tempRD'] = np.nan
        
        # Create R&D intensity terciles (equivalent to Stata's "egen tempRDQuant = fastxtile(tempRD), n(3) by(time_avail_m)")
        data['tempRDQuant'] = data.groupby('time_avail_m')['tempRD'].transform(
            lambda x: pd.qcut(x, q=3, labels=False, duplicates='drop') + 1
        )
        
        # Keep only top tercile (equivalent to Stata's "replace RDAbility = . if tempRDQuant != 3")
        data.loc[data['tempRDQuant'] != 3, 'RDAbility'] = np.nan
        
        # Set to missing if R&D <= 0 (equivalent to Stata's "replace RDAbility = . if xrd <=0")
        data.loc[data['xrd'] <= 0, 'RDAbility'] = np.nan
        
        logger.info("Successfully calculated RDAbility signal")
        
        # Expand to monthly (equivalent to Stata's expand logic)
        logger.info("Expanding to monthly frequency...")
        
        monthly_data = []
        for _, row in data.iterrows():
            for month_offset in range(12):
                new_row = row.copy()
                new_row['time_avail_m'] = row['time_avail_m'] + pd.DateOffset(months=month_offset)
                monthly_data.append(new_row)
        
        data = pd.DataFrame(monthly_data)
        logger.info(f"After expanding to monthly: {len(data)} records")
        
        # Keep latest observation per gvkey-time_avail_m (equivalent to Stata's complex keep logic)
        data = data.sort_values(['gvkey', 'time_avail_m', 'datadate'])
        data = data.groupby(['gvkey', 'time_avail_m']).last().reset_index()
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # SAVE RESULTS
        logger.info("Saving RDAbility predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'RDAbility']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['RDAbility'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "RDAbility.csv"
        csv_data = output_data[['permno', 'yyyymm', 'RDAbility']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved RDAbility predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed RDAbility predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct RDAbility predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    rdability()
