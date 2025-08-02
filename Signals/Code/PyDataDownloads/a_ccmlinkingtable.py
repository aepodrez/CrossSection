"""
Python equivalent of A_CCMLinkingTable.do
Generated from: A_CCMLinkingTable.do

Original Stata file: A_CCMLinkingTable.do
"""

import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def a_ccmlinkingtable():
    """
    Python equivalent of A_CCMLinkingTable.do
    
    Downloads CRSP-Compustat linking table from WRDS
    """
    logger.info("Downloading CRSP-Compustat linking table...")
    
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
            a.gvkey, 
            a.conm, 
            a.tic, 
            a.cusip, 
            a.cik, 
            a.sic, 
            a.naics, 
            b.linkprim, 
            b.linktype, 
            b.liid, 
            b.lpermno, 
            b.lpermco, 
            b.linkdt, 
            b.linkenddt
        FROM comp.names as a
        INNER JOIN crsp.ccmxpf_lnkhist as b
        ON a.gvkey = b.gvkey
        WHERE b.linktype in ('LC', 'LU')
        AND b.linkprim in ('P', 'C')
        ORDER BY a.gvkey
        """
        
        # Execute query
        data = conn.raw_sql(query, date_cols=['linkdt', 'linkenddt'])
        logger.info(f"Downloaded {len(data)} linking table records")
        
        # Rename columns to match original Stata output
        data = data.rename(columns={
            'linkdt': 'timeLinkStart_d',
            'linkenddt': 'timeLinkEnd_d',
            'lpermno': 'permno'
        })
        
        # Save to intermediate file (CSV format)
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/CCMLinkingTable.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved linking table to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/CCMLinkingTable.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved linking table to {main_output_path}")
        
        logger.info("Successfully downloaded CRSP-Compustat linking table")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download CRSP-Compustat linking table: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the download function
    a_ccmlinkingtable()
