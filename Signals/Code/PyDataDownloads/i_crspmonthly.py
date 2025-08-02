"""
Python equivalent of I_CRSPmonthly.do
Generated from: I_CRSPmonthly.do

Original Stata file: I_CRSPmonthly.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def i_crspmonthly():
    """
    Python equivalent of I_CRSPmonthly.do
    
    Downloads and processes CRSP monthly data from WRDS
    """
    logger.info("Downloading CRSP monthly data...")
    
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
            a.permno, a.permco, a.date, a.ret, a.retx, a.vol, a.shrout, a.prc, a.cfacshr, a.bidlo, a.askhi,
            b.shrcd, b.exchcd, b.siccd, b.ticker, b.shrcls, 
            c.dlstcd, c.dlret                               
        FROM crsp.msf as a
        LEFT JOIN crsp.msenames as b
        ON a.permno=b.permno AND b.namedt<=a.date AND a.date<=b.nameendt
        LEFT JOIN crsp.msedelist as c
        ON a.permno=c.permno AND date_trunc('month', a.date) = date_trunc('month', c.dlstdt)
        """
        
        # Execute query
        data = conn.raw_sql(query, date_cols=['date'])
        logger.info(f"Downloaded {len(data)} CRSP monthly records")
        
        # Save intermediate file (CSV format)
        intermediate_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/mCRSP.csv")
        intermediate_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(intermediate_path, index=False)
        logger.info(f"Saved intermediate data to {intermediate_path}")
        
        # Make 2 digit SIC
        data = data.rename(columns={'siccd': 'sicCRSP'})
        data['sicCRSP'] = data['sicCRSP'].astype(str)
        data['sic2D'] = data['sicCRSP'].str[:2]
        
        # Convert SIC columns to numeric
        sic_columns = ['sicCRSP', 'sic2D']
        for col in sic_columns:
            data[col] = pd.to_numeric(data[col], errors='coerce')
        
        # Create monthly date
        data['time_avail_m'] = pd.to_datetime(data['date']).dt.to_period('M')
        data = data.drop('date', axis=1)
        
        # Incorporate delisting return
        # Replace dlret = -.35 if dlret is missing and (dlstcd == 500 | (dlstcd >=520 & dlstcd <=584)) and (exchcd == 1 | exchcd == 2)
        mask1 = (data['dlret'].isna() & 
                ((data['dlstcd'] == 500) | ((data['dlstcd'] >= 520) & (data['dlstcd'] <= 584))) &
                ((data['exchcd'] == 1) | (data['exchcd'] == 2)))
        data.loc[mask1, 'dlret'] = -0.35
        
        # Replace dlret = -.55 if dlret is missing and (dlstcd == 500 | (dlstcd >=520 & dlstcd <=584)) and exchcd == 3
        mask2 = (data['dlret'].isna() & 
                ((data['dlstcd'] == 500) | ((data['dlstcd'] >= 520) & (data['dlstcd'] <= 584))) &
                (data['exchcd'] == 3))
        data.loc[mask2, 'dlret'] = -0.55
        
        # Replace dlret = -1 if dlret < -1 and dlret is not missing
        mask3 = (data['dlret'] < -1) & data['dlret'].notna()
        data.loc[mask3, 'dlret'] = -1
        
        # Replace dlret = 0 if dlret is missing
        data.loc[data['dlret'].isna(), 'dlret'] = 0
        
        # Update return to incorporate delisting return
        # ret = (1+ret)*(1+dlret) - 1
        data['ret'] = (1 + data['ret']) * (1 + data['dlret']) - 1
        
        # Replace ret with dlret if ret is missing and dlret is not 0
        mask4 = data['ret'].isna() & (data['dlret'] != 0)
        data.loc[mask4, 'ret'] = data.loc[mask4, 'dlret']
        
        # Compute market value of equity
        # Converting units
        data['shrout'] = data['shrout'] / 1000  # Convert to thousands
        data['vol'] = data['vol'] / 10000  # Convert to 10^4
        
        # Market value of equity = shares outstanding * absolute price
        data['mve_c'] = data['shrout'] * data['prc'].abs()
        
        # Housekeeping - drop unnecessary columns
        data = data.drop(['dlret', 'dlstcd', 'permco'], axis=1)
        
        # Save to intermediate file
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/monthlyCRSP.csv")
        data.to_csv(output_path, index=False)
        logger.info(f"Saved monthly CRSP data to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/monthlyCRSP.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log some summary statistics
        logger.info("CRSP monthly data summary:")
        logger.info(f"  Total records: {len(data)}")
        logger.info(f"  Unique firms (permno): {data['permno'].nunique()}")
        logger.info(f"  Time range: {data['time_avail_m'].min()} to {data['time_avail_m'].max()}")
        logger.info(f"  Unique exchanges: {data['exchcd'].nunique()}")
        logger.info(f"  Unique share codes: {data['shrcd'].nunique()}")
        
        # Check data availability
        if 'ret' in data.columns:
            non_missing_ret = data['ret'].notna().sum()
            logger.info(f"  Non-missing returns: {non_missing_ret} ({non_missing_ret/len(data)*100:.1f}%)")
        
        if 'mve_c' in data.columns:
            non_missing_mve = data['mve_c'].notna().sum()
            logger.info(f"  Non-missing market value: {non_missing_mve} ({non_missing_mve/len(data)*100:.1f}%)")
        
        logger.info("Successfully downloaded and processed CRSP monthly data")
        logger.info("Note: Delisting returns incorporated, market value computed, units converted")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download CRSP monthly data: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the download function
    i_crspmonthly()
