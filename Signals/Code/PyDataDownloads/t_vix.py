"""
Python equivalent of T_VIX.do
Generated from: T_VIX.do

Original Stata file: T_VIX.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime
import fredapi

logger = logging.getLogger(__name__)

def t_vix():
    """
    Python equivalent of T_VIX.do
    
    Downloads and processes VIX data from FRED
    """
    logger.info("Downloading VIX data from FRED...")
    
    try:
        # Use global FRED connection from master.py
        from master import fred
        if fred is None:
            logger.error("FRED connection not available. Please run master.py")
            return False
        
        # FRED series IDs from original Stata file
        # VIXCLS: VIX (CBOE Volatility Index)
        # VXOCLS: VXO (CBOE S&P 100 Volatility Index) - discontinued in 2021
        series_ids = ['VIXCLS', 'VXOCLS']
        
        logger.info(f"Downloading VIX data for series: {series_ids}")
        
        # Download data from FRED
        data_list = []
        for series_id in series_ids:
            try:
                series_data = fred.get_series(series_id)
                series_data = series_data.reset_index()
                series_data.columns = ['time_d', series_id]
                data_list.append(series_data)
                logger.info(f"Downloaded {len(series_data)} records for {series_id}")
            except Exception as e:
                logger.warning(f"Failed to download {series_id}: {e}")
                # Create empty DataFrame for missing series
                empty_df = pd.DataFrame(columns=['time_d', series_id])
                data_list.append(empty_df)
        
        # Merge the series data
        if len(data_list) == 2:
            data = data_list[0].merge(data_list[1], on='time_d', how='outer')
        else:
            data = data_list[0]
        
        logger.info(f"Combined data has {len(data)} records")
        
        # Create vix column (equivalent to Stata's "gen vix = VXOCLS")
        data['vix'] = data['VXOCLS']
        
        # Replace vix with VIXCLS where VXOCLS is missing and date >= 2021-09-23
        # Equivalent to Stata's "replace vix = VIXCLS if mi(VXOCLS) & daten >= dmy(23, 9, 2021)"
        cutoff_date = pd.to_datetime('2021-09-23')
        mask = (data['VXOCLS'].isna()) & (data['time_d'] >= cutoff_date)
        data.loc[mask, 'vix'] = data.loc[mask, 'VIXCLS']
        
        logger.info(f"Applied VIXCLS replacement for {mask.sum()} records after 2021-09-23")
        
        # Drop unnecessary columns (equivalent to Stata's "drop datestr VIXCLS VXOCLS")
        columns_to_drop = ['VIXCLS', 'VXOCLS']
        for col in columns_to_drop:
            if col in data.columns:
                data = data.drop(columns=[col])
        
        # Calculate dVIX (VIX change) - equivalent to Stata's lag calculation
        data = data.sort_values('time_d')
        data['dVIX'] = data['vix'].diff()
        
        logger.info(f"Calculated VIX changes (dVIX) for {len(data)} records")
        
        # Save to intermediate file
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/d_vix.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved VIX data to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/d_vix.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log some summary statistics
        logger.info("VIX data summary:")
        logger.info(f"  Total records: {len(data)}")
        logger.info(f"  Time range: {data['time_d'].min()} to {data['time_d'].max()}")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to download VIX data: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the download function
    t_vix()
