"""
ZZ1_RIVolSpread Predictor Implementation

This script implements the RIVolSpread predictor based on Bali-Hovak (2009):
- RIVolSpread: Realized minus Implied Volatility Spread

The script calculates the difference between realized volatility (from RealizedVol.csv)
and implied volatility (from OptionMetrics data), creating a volatility spread measure.
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def zz1_rivolspread():
    """Main function to calculate RIVolSpread predictor."""
    
    # Define file paths
    base_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data")
    optionmetrics_path = base_path / "Intermediate" / "OptionMetricsBH.csv"
    realizedvol_path = base_path / "Predictors" / "RealizedVol.csv"
    master_path = base_path / "Intermediate" / "SignalMasterTable.csv"
    temp_path = base_path / "Temp" / "temp_rivolspread.csv"
    temp2_path = base_path / "Temp" / "temp2_rivolspread.csv"
    output_path = base_path / "Predictors"
    
    # Ensure directories exist
    temp_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.mkdir(parents=True, exist_ok=True)
    
    logger.info("Starting RIVolSpread predictor calculation")
    
    try:
        # Clean OptionMetrics data
        logger.info("Cleaning OptionMetrics data")
        option_data = pd.read_csv(optionmetrics_path)
        
        # Rename mean_imp_vol to impvol
        option_data = option_data.rename(columns={'mean_imp_vol': 'impvol'})
        
        # Drop unnecessary columns
        option_data = option_data.drop(['mean_day', 'nobs', 'ticker'], axis=1, errors='ignore')
        
        # Reshape wide by cp_flag
        option_data_wide = option_data.pivot_table(
            index=['secid', 'time_avail_m'], 
            columns='cp_flag', 
            values='impvol', 
            aggfunc='first'
        ).reset_index()
        
        # Rename columns
        option_data_wide = option_data_wide.rename(columns={'C': 'impvolC', 'P': 'impvolP'})
        
        # Calculate implied volatility (many stage version)
        option_data_wide['impvol'] = (option_data_wide['impvolC'] + option_data_wide['impvolP']) / 2
        option_data_wide.loc[option_data_wide['impvol'].isna() & option_data_wide['impvolC'].notna(), 'impvol'] = option_data_wide.loc[option_data_wide['impvol'].isna() & option_data_wide['impvolC'].notna(), 'impvolC']
        option_data_wide.loc[option_data_wide['impvol'].isna() & option_data_wide['impvolP'].notna(), 'impvol'] = option_data_wide.loc[option_data_wide['impvol'].isna() & option_data_wide['impvolP'].notna(), 'impvolP']
        
        # Convert time_avail_m to datetime in option_data_wide
        option_data_wide['time_avail_m'] = pd.to_datetime(option_data_wide['time_avail_m'])
        
        # Keep only necessary columns
        option_data_clean = option_data_wide[['secid', 'time_avail_m', 'impvol']].copy()
        option_data_clean.to_csv(temp_path, index=False)
        
        # Clean Realized vol data
        logger.info("Cleaning RealizedVol data")
        realizedvol_data = pd.read_csv(realizedvol_path)
        
        # Convert yyyymm to time_avail_m
        realizedvol_data['time_avail_m'] = pd.to_datetime(
            realizedvol_data['yyyymm'].astype(str).str[:4] + '-' + 
            realizedvol_data['yyyymm'].astype(str).str[4:6] + '-01'
        )
        realizedvol_data = realizedvol_data.drop('yyyymm', axis=1)
        
        # Annualize realized volatility (multiply by sqrt(252))
        realizedvol_data['RealizedVol'] = realizedvol_data['RealizedVol'] * np.sqrt(252)
        realizedvol_data.to_csv(temp2_path, index=False)
        
        # DATA LOAD
        logger.info("Loading SignalMasterTable data")
        required_vars = ['permno', 'time_avail_m', 'secid', 'sicCRSP']
        data = pd.read_csv(master_path, usecols=required_vars)
        
        # Drop missing secid observations
        data = data.dropna(subset=['secid'])
        
        # Convert time_avail_m to datetime for consistent merging
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        
        # Merge with cleaned OptionMetrics data
        logger.info("Merging with OptionMetrics data")
        data = data.merge(option_data_clean, on=['secid', 'time_avail_m'], how='inner')
        
        # Apply SIC filters
        logger.info("Applying SIC filters")
        # Drop closed-end funds (6720-6730) and REITs (6798)
        data = data[
            (data['sicCRSP'] < 6720) | (data['sicCRSP'] > 6730)
        ]
        data = data[data['sicCRSP'] != 6798]
        
        # Drop missing secid or impvol observations
        data = data.dropna(subset=['secid', 'impvol'])
        
        # Merge with realized volatility data
        logger.info("Merging with realized volatility data")
        data = data.merge(realizedvol_data, on=['permno', 'time_avail_m'], how='inner')
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating RIVolSpread")
        
        # Calculate RIVolSpread = realized volatility - implied volatility
        data['RIVolSpread'] = data['RealizedVol'] - data['impvol']
        
        # Drop missing RIVolSpread observations
        data = data.dropna(subset=['RIVolSpread'])
        
        # Prepare output data
        logger.info("Preparing output data")
        
        # For RIVolSpread
        rivolspread_output = data[['permno', 'time_avail_m', 'RIVolSpread']].copy()
        # Convert time_avail_m to datetime if needed for strftime
        if not pd.api.types.is_datetime64_any_dtype(rivolspread_output['time_avail_m']):
            rivolspread_output['time_avail_m'] = pd.to_datetime(rivolspread_output['time_avail_m'])
        
        rivolspread_output['yyyymm'] = rivolspread_output['time_avail_m'].dt.strftime('%Y%m').astype(int)
        rivolspread_output = rivolspread_output[['permno', 'yyyymm', 'RIVolSpread']]
        
        # Save results
        logger.info("Saving results")
        
        # Save RIVolSpread
        rivolspread_file = output_path / "RIVolSpread.csv"
        rivolspread_output.to_csv(rivolspread_file, index=False)
        logger.info(f"Saved RIVolSpread predictor to {rivolspread_file}")
        logger.info(f"RIVolSpread: {len(rivolspread_output)} observations")
        
        # Clean up temporary files
        if temp_path.exists():
            temp_path.unlink()
        if temp2_path.exists():
            temp2_path.unlink()
        
        logger.info("Successfully completed RIVolSpread predictor calculation")
        
    except Exception as e:
        logger.error(f"Error in RIVolSpread calculation: {str(e)}")
        raise

if __name__ == "__main__":
    zz1_rivolspread()
