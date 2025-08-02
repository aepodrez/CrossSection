"""
Python equivalent of M_IBES_Recommendations.do
Generated from: M_IBES_Recommendations.do

Original Stata file: M_IBES_Recommendations.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def m_ibes_recommendations():
    """
    Python equivalent of M_IBES_Recommendations.do
    
    Downloads and processes IBES recommendations data from WRDS
    """
    logger.info("Downloading IBES recommendations data...")
    
    try:
        # Use global WRDS connection from master.py
        from master import wrds_conn
        if wrds_conn is None:
            logger.error("WRDS connection not available. Please run master.py")
            return False
        conn = wrds_conn
        
        # SQL query from original Stata file
        query = """
        SELECT 
            a.ticker, a.estimid, a.ereccd, a.etext, a.ireccd, a.itext, a.emaskcd, 
            a.amaskcd, a.anndats, a.actdats
        FROM ibes.recddet as a
        WHERE a.usfirm = '1'
        """
        
        # Execute query
        data = conn.raw_sql(query, date_cols=['anndats', 'actdats'])
        logger.info(f"Downloaded {len(data)} IBES recommendations records")
        
        # Convert ireccd to numeric and drop missing values
        data['ireccd'] = pd.to_numeric(data['ireccd'], errors='coerce')
        data = data.dropna(subset=['ireccd'])
        logger.info(f"After dropping missing ireccd: {len(data)} records")
        
        # Clean up - rename ticker to tickerIBES
        data = data.rename(columns={'ticker': 'tickerIBES'})
        
        # Create time_avail_m (month of announcement date)
        data['time_avail_m'] = pd.to_datetime(data['anndats']).dt.to_period('M')
        
        # Reorder columns to put important stuff first
        column_order = ['tickerIBES', 'amaskcd', 'anndats', 'time_avail_m', 'ireccd']
        other_columns = [col for col in data.columns if col not in column_order]
        data = data[column_order + other_columns]
        
        # Save to intermediate file
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/IBES_Recommendations.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved IBES recommendations data to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/IBES_Recommendations.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log some summary statistics
        logger.info("IBES recommendations data summary:")
        logger.info(f"  Total records: {len(data)}")
        logger.info(f"  Unique tickers: {data['tickerIBES'].nunique()}")
        logger.info(f"  Unique analysts: {data['amaskcd'].nunique()}")
        logger.info(f"  Time range: {data['time_avail_m'].min()} to {data['time_avail_m'].max()}")
        
        # Recommendation code distribution
        if 'ireccd' in data.columns:
            rec_counts = data['ireccd'].value_counts().sort_index()
            logger.info("  Recommendation code distribution:")
            rec_labels = {
                1: "Strong Buy",
                2: "Buy", 
                3: "Hold",
                4: "Underperform",
                5: "Sell"
            }
            for code, count in rec_counts.items():
                label = rec_labels.get(code, f"Code {code}")
                logger.info(f"    {code} ({label}): {count} records")
        
        # Check data availability
        if 'ireccd' in data.columns:
            non_missing_ireccd = data['ireccd'].notna().sum()
            logger.info(f"  Non-missing recommendation codes: {non_missing_ireccd} ({non_missing_ireccd/len(data)*100:.1f}%)")
        
        if 'anndats' in data.columns:
            non_missing_anndats = data['anndats'].notna().sum()
            logger.info(f"  Non-missing announcement dates: {non_missing_anndats} ({non_missing_anndats/len(data)*100:.1f}%)")
        
        logger.info("Successfully downloaded and processed IBES recommendations data")
        logger.info("Note: Data begins in 1993, recommendation codes: 1=Strong Buy, 2=Buy, 3=Hold, 4=Underperform, 5=Sell")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download IBES recommendations data: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the download function
    m_ibes_recommendations()
