"""
Python equivalent of L2_IBES_EPS_Adj.do
Generated from: L2_IBES_EPS_Adj.do

Original Stata file: L2_IBES_EPS_Adj.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def l2_ibes_eps_adj(wrds_conn=None):
    """
    Python equivalent of L2_IBES_EPS_Adj.do
    
    Downloads and processes IBES EPS adjusted data from WRDS
    """
    logger.info("Downloading IBES EPS adjusted data...")
    
    try:
        # Check if WRDS connection is provided
        if wrds_conn is None:
            logger.error("WRDS connection not available. Please run master.py")
            return False
        conn = wrds_conn
        
        # SQL query from original Stata file
        query = """
        SELECT 
            a.fpi, a.ticker, a.statpers, a.fpedats, a.anndats_act,
            a.meanest, a.actual, a.medest, a.stdev, a.numest,
            b.prdays, b.price, b.shout
        FROM ibes.statsum_epsus as a 
        LEFT JOIN ibes.actpsum_epsus as b
        ON a.ticker = b.ticker AND a.statpers = b.statpers
        """
        
        # Execute query
        data = conn.raw_sql(query, date_cols=['statpers', 'fpedats', 'anndats_act'])
        logger.info(f"Downloaded {len(data)} IBES EPS adjusted records")
        
        # Set up linking variables
        data['time_avail_m'] = pd.to_datetime(data['statpers']).dt.to_period('M')
        
        # Rename ticker to tickerIBES
        data = data.rename(columns={'ticker': 'tickerIBES'})
        
        # Keep last observation each month (drop if meanest is missing for sanity)
        data = data.dropna(subset=['meanest'])
        logger.info(f"After dropping missing meanest: {len(data)} records")
        
        # Sort and keep last observation per ticker-fpi-month
        data = data.sort_values(['tickerIBES', 'fpi', 'time_avail_m', 'statpers'])
        data = data.drop_duplicates(subset=['tickerIBES', 'fpi', 'time_avail_m'], keep='last')
        logger.info(f"After keeping last observation per month: {len(data)} records")
        
        # Save to intermediate file
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/IBES_EPS_Adj.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved IBES EPS adjusted data to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/IBES_EPS_Adj.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log some summary statistics
        logger.info("IBES EPS adjusted data summary:")
        logger.info(f"  Total records: {len(data)}")
        logger.info(f"  Unique tickers: {data['tickerIBES'].nunique()}")
        logger.info(f"  Time range: {data['time_avail_m'].min()} to {data['time_avail_m'].max()}")
        
        # FPI distribution
        if 'fpi' in data.columns:
            fpi_counts = data['fpi'].value_counts()
            logger.info("  FPI distribution:")
            for fpi, count in fpi_counts.items():
                logger.info(f"    FPI {fpi}: {count} records")
        
        # Check data availability
        if 'meanest' in data.columns:
            non_missing_meanest = data['meanest'].notna().sum()
            logger.info(f"  Non-missing mean estimates: {non_missing_meanest} ({non_missing_meanest/len(data)*100:.1f}%)")
        
        if 'actual' in data.columns:
            non_missing_actual = data['actual'].notna().sum()
            logger.info(f"  Non-missing actual earnings: {non_missing_actual} ({non_missing_actual/len(data)*100:.1f}%)")
        
        if 'numest' in data.columns:
            non_missing_numest = data['numest'].notna().sum()
            logger.info(f"  Non-missing number of estimates: {non_missing_numest} ({non_missing_numest/len(data)*100:.1f}%)")
        
        # Check join success
        if 'price' in data.columns:
            non_missing_price = data['price'].notna().sum()
            logger.info(f"  Non-missing prices (from join): {non_missing_price} ({non_missing_price/len(data)*100:.1f}%)")
        
        logger.info("Successfully downloaded and processed IBES EPS adjusted data")
        logger.info("Note: Adjusted for splits, includes actual earnings and price data from join")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download IBES EPS adjusted data: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the download function
    l2_ibes_eps_adj()
