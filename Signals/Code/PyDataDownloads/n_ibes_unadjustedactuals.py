"""
Python equivalent of N_IBES_UnadjustedActuals.do
Generated from: N_IBES_UnadjustedActuals.do

Original Stata file: N_IBES_UnadjustedActuals.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def n_ibes_unadjustedactuals(wrds_conn=None):
    """
    Python equivalent of N_IBES_UnadjustedActuals.do
    
    Downloads and processes IBES unadjusted actuals data from WRDS
    """
    logger.info("Downloading IBES unadjusted actuals data...")
    
    try:
        # Check if WRDS connection is provided
        if wrds_conn is None:
            logger.error("WRDS connection not available. Please run master.py")
            return False
        conn = wrds_conn
        
        # SQL query from original Stata file
        query = """
        SELECT a.*
        FROM ibes.actpsumu_epsus as a
        WHERE a.measure = 'EPS'
        """
        
        # Execute query
        data = conn.raw_sql(query, date_cols=['statpers', 'fy0edats'])
        logger.info(f"Downloaded {len(data)} IBES unadjusted actuals records")
        
        # Rename shout to shoutIBESUnadj
        if 'shout' in data.columns:
            data = data.rename(columns={'shout': 'shoutIBESUnadj'})
        
        # Set up in monthly time and fill gaps
        data['time_avail_m'] = pd.to_datetime(data['statpers']).dt.to_period('M')
        
        # Create unique ID for each ticker
        data['id'] = data['ticker'].astype('category').cat.codes
        
        # Keep first observation per ticker-month
        data = data.sort_values(['id', 'time_avail_m', 'statpers'])
        data = data.drop_duplicates(subset=['id', 'time_avail_m'], keep='first')
        logger.info(f"After keeping first observation per ticker-month: {len(data)} records")
        
        # Forward fill missing values within each ticker
        # This replicates the Stata logic: replace `v' = `v'[_n-1] if id == id[_n-1] & mi(`v')
        fill_columns = ['int0a', 'fy0a', 'shoutIBESUnadj', 'ticker']
        available_columns = [col for col in fill_columns if col in data.columns]
        
        for col in available_columns:
            data['col'] = data.groupby('id')['col'].ffill()
            logger.info(f"Forward filled missing values in {col}")
        
        # Drop id and statpers columns
        data = data.drop(['id', 'statpers'], axis=1)
        
        # Prepare for match with other files - rename ticker to tickerIBES
        data = data.rename(columns={'ticker': 'tickerIBES'})
        
        # Save to intermediate file
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/IBES_UnadjustedActuals.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved IBES unadjusted actuals data to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/IBES_UnadjustedActuals.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log some summary statistics
        logger.info("IBES unadjusted actuals data summary:")
        logger.info(f"  Total records: {len(data)}")
        logger.info(f"  Unique tickers: {data['tickerIBES'].nunique()}")
        logger.info(f"  Time range: {data['time_avail_m'].min()} to {data['time_avail_m'].max()}")
        
        # Check data availability
        if 'int0a' in data.columns:
            non_missing_int0a = data['int0a'].notna().sum()
            logger.info(f"  Non-missing int0a: {non_missing_int0a} ({non_missing_int0a/len(data)*100:.1f}%)")
        
        if 'fy0a' in data.columns:
            non_missing_fy0a = data['fy0a'].notna().sum()
            logger.info(f"  Non-missing fy0a: {non_missing_fy0a} ({non_missing_fy0a/len(data)*100:.1f}%)")
        
        if 'shoutIBESUnadj' in data.columns:
            non_missing_shout = data['shoutIBESUnadj'].notna().sum()
            logger.info(f"  Non-missing shoutIBESUnadj: {non_missing_shout} ({non_missing_shout/len(data)*100:.1f}%)")
        
        if 'price' in data.columns:
            non_missing_price = data['price'].notna().sum()
            logger.info(f"  Non-missing price: {non_missing_price} ({non_missing_price/len(data)*100:.1f}%)")
        
        logger.info("Successfully downloaded and processed IBES unadjusted actuals data")
        logger.info("Note: Unadjusted for splits, forward filled missing values within tickers")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download IBES unadjusted actuals data: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the download function
    n_ibes_unadjustedactuals()
