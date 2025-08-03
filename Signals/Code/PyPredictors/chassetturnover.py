"""
Python equivalent of ChAssetTurnover.do
Generated from: ChAssetTurnover.do

Original Stata file: ChAssetTurnover.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def chassetturnover():
    """
    Python equivalent of ChAssetTurnover.do
    
    Constructs the ChAssetTurnover predictor signal for change in asset turnover.
    """
    logger.info("Constructing predictor signal: ChAssetTurnover...")
    
    try:
        # DATA LOAD
        # Load Compustat annual data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading Compustat annual data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"Input file not found: {compustat_path}")
            logger.error("Please run the Compustat data download scripts first")
            return False
        
        # Load the required variables
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'rect', 'invt', 'aco', 'ppent', 'intan', 'ap', 'lco', 'lo', 'sale']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates: keep first observation per permno-time_avail_m
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        
        # Sort by permno and time_avail_m for lag calculations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating ChAssetTurnover signal...")
        
        # Calculate temp variable (equivalent to Stata's "gen temp = (rect + invt + aco + ppent + intan - ap - lco - lo)")
        data['temp'] = data['rect'] + data['invt'] + data['aco'] + data['ppent'] + data['intan'] - data['ap'] - data['lco'] - data['lo']
        
        # Calculate 12-month lag of temp
        data['temp_lag12'] = data.groupby('permno')['temp'].shift(12)
        
        # Calculate AssetTurnover (equivalent to Stata's "gen AssetTurnover = sale/((temp + l12.temp)/2)")
        data['AssetTurnover'] = data['sale'] / ((data['temp'] + data['temp_lag12']) / 2)
        
        # Set AssetTurnover to missing if negative (equivalent to Stata's "replace AssetTurnover = . if AssetTurnover < 0")
        data.loc[data['AssetTurnover'] < 0, 'AssetTurnover'] = np.nan
        
        # Calculate 12-month lag of AssetTurnover
        data['AssetTurnover_lag12'] = data.groupby('permno')['AssetTurnover'].shift(12)
        
        # Calculate ChAssetTurnover (equivalent to Stata's "gen ChAssetTurnover = AssetTurnover - l12.AssetTurnover")
        data['ChAssetTurnover'] = data['AssetTurnover'] - data['AssetTurnover_lag12']
        
        logger.info("Successfully calculated ChAssetTurnover signal")
        
        # SAVE RESULTS
        logger.info("Saving ChAssetTurnover predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'ChAssetTurnover']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['ChAssetTurnover'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "ChAssetTurnover.csv"
        csv_data = output_data[['permno', 'yyyymm', 'ChAssetTurnover']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved ChAssetTurnover predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed ChAssetTurnover predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct ChAssetTurnover predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    chassetturnover()
