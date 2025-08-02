"""
Python equivalent of J_CRSPdaily.do
Generated from: J_CRSPdaily.do

Original Stata file: J_CRSPdaily.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime
import time

logger = logging.getLogger(__name__)

def j_crspdaily():
    """
    Python equivalent of J_CRSPdaily.do
    
    Downloads and processes CRSP daily data from WRDS
    """
    logger.info("Downloading CRSP daily data...")
    
    try:
        # Use global WRDS connection from master.py
        from master import wrds_conn
        if wrds_conn is None:
            logger.error("WRDS connection not available. Please run master.py")
            return False
        conn = wrds_conn
        
        # Get current year for the loop
        current_year = datetime.now().year
        logger.info(f"Downloading data from 1926 to {current_year}")
        
        # Initialize empty DataFrames for different purposes
        daily_data_full = []
        daily_data_price = []
        
        # Loop over years to avoid memory issues
        for year in range(1926, current_year + 1):
            logger.info(f"Processing year {year}...")
            
            # SQL query for each year
            query = f"""
            SELECT 
                a.permno, a.date, a.ret, a.vol, a.shrout, a.prc, a.cfacshr, a.cfacpr
            FROM crsp.dsf as a
            WHERE date >= '{year}-01-01' AND date <= '{year}-12-31'
            """
            
            try:
                # Execute query
                year_data = conn.raw_sql(query, date_cols=['date'])
                
                if len(year_data) > 0:
                    logger.info(f"  Downloaded {len(year_data)} records for {year}")
                    
                    # Full dataset (for betas and liquidity)
                    daily_data_full.append(year_data)
                    
                    # Price-only dataset (for High 52, Zero trade)
                    price_data = year_data[['permno', 'date', 'prc', 'cfacpr', 'shrout']].copy()
                    daily_data_price.append(price_data)
                    
                else:
                    logger.info(f"  No data for {year}")
                
                # Sleep to avoid login errors (equivalent to Stata sleep 1000)
                time.sleep(1)
                
            except Exception as e:
                logger.warning(f"  Error processing year {year}: {e}")
                continue
        
        # Combine all years
        if daily_data_full:
            logger.info("Combining all years...")
            
            # Full dataset
            full_data = pd.concat(daily_data_full, ignore_index=True)
            logger.info(f"Combined full dataset: {len(full_data)} records")
            
            # Rename date to time_d
            full_data = full_data.rename(columns={'date': 'time_d'})
            
            # Save full dataset
            output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/dailyCRSP.csv")
            output_path.parent.mkdir(parents=True, exist_ok=True)
            full_data.to_csv(output_path, index=False)
            logger.info(f"Saved full daily CRSP data to {output_path}")
            
            # Price-only dataset
            price_data = pd.concat(daily_data_price, ignore_index=True)
            logger.info(f"Combined price dataset: {len(price_data)} records")
            
            # Rename date to time_d
            price_data = price_data.rename(columns={'date': 'time_d'})
            
            # Save price-only dataset
            price_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/dailyCRSPprc.csv")
            price_data.to_csv(price_output_path, index=False)
            logger.info(f"Saved price-only daily CRSP data to {price_output_path}")
            
            # Also save to main data directory for compatibility
            main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/dailyCRSP.csv")
            full_data.to_csv(main_output_path, index=False)
            logger.info(f"Saved to main data directory: {main_output_path}")
            
            # Log some summary statistics
            logger.info("CRSP daily data summary:")
            logger.info(f"  Total records: {len(full_data)}")
            logger.info(f"  Unique firms (permno): {full_data['permno'].nunique()}")
            logger.info(f"  Time range: {full_data['time_d'].min()} to {full_data['time_d'].max()}")
            
            # Check data availability
            if 'ret' in full_data.columns:
                non_missing_ret = full_data['ret'].notna().sum()
                logger.info(f"  Non-missing returns: {non_missing_ret} ({non_missing_ret/len(full_data)*100:.1f}%)")
            
            if 'prc' in full_data.columns:
                non_missing_prc = full_data['prc'].notna().sum()
                logger.info(f"  Non-missing prices: {non_missing_prc} ({non_missing_prc/len(full_data)*100:.1f}%)")
            
            if 'vol' in full_data.columns:
                non_missing_vol = full_data['vol'].notna().sum()
                logger.info(f"  Non-missing volume: {non_missing_vol} ({non_missing_vol/len(full_data)*100:.1f}%)")
            
            logger.info("Successfully downloaded and processed CRSP daily data")
            logger.info("Note: Data downloaded year-by-year to avoid memory issues")
            return True
            
        else:
            logger.error("No data downloaded")
            return False
        
    except Exception as e:
        logger.error(f"Failed to download CRSP daily data: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the download function
    j_crspdaily()
