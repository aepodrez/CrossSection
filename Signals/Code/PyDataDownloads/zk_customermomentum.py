"""
Python equivalent of ZKR_CustomerSegments.R
Generated from: ZKR_CustomerSegments.R

Original R file: ZKR_CustomerSegments.R
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path
import re
from datetime import datetime

logger = logging.getLogger(__name__)

def zk_customermomentum():
    """
    Python equivalent of ZKR_CustomerSegments.R
    
    Creates customer momentum using Compustat customer segment data
    Creates customerMom.csv with time_avail_m, permno, CustMom
    """
    logger.info("Processing customer momentum data...")
    
    try:
        # Use the global PROJECT_PATH from master.py
        from master import PROJECT_PATH
        arg1 = PROJECT_PATH
        
        logger.info(f"Using project path: {arg1}")
        
        # Create intermediate directory
        intermediate_dir = Path(arg1) / "Signals" / "Data" / "Intermediate"
        intermediate_dir.mkdir(parents=True, exist_ok=True)
        
        # Read customer segment data
        logger.info("Reading customer segment data...")
        seg_customer = pd.read_csv(intermediate_dir / "CompustatSegmentDataCustomers.csv")
        seg_customer['datadate'] = pd.to_datetime(seg_customer['datadate'])
        
        # Read CCM linking table
        ccm = pd.read_csv(intermediate_dir / "CCMLinkingTable.csv")
        # The CCM linking table has different column names than expected in the R script
        # We need to use the actual column names from the file
        ccm['linkdt'] = pd.to_datetime(ccm['timeLinkStart_d'])
        ccm['linkenddt'] = pd.to_datetime(ccm['timeLinkEnd_d'])
        ccm['lpermno'] = ccm['permno']  # Use permno as lpermno
        
        # Read monthly CRSP data
        m_crsp = pd.read_csv(intermediate_dir / "mCRSP.csv")
        m_crsp['date'] = pd.to_datetime(m_crsp['date'])
        
        # Clean data (equivalent to R's cleaning logic)
        logger.info("Cleaning customer segment data...")
        seg_customer['cnms'] = seg_customer['cnms'].str.upper()
        seg_customer = seg_customer[seg_customer['ctype'] == "COMPANY"]
        
        # Remove punctuation (equivalent to R's removePunctuation)
        seg_customer['cnms'] = seg_customer['cnms'].str.replace(r'[^\w\s]', '', regex=True)
        
        # Filter out unwanted entries
        seg_customer = seg_customer[
            (seg_customer['cnms'] != "NOT REPORTED") & 
            (~seg_customer['cnms'].str.endswith("CUSTOMERS", na=False)) & 
            (~seg_customer['cnms'].str.endswith("CUSTOMER", na=False))
        ]
        
        # Remove common suffixes (equivalent to R's str_remove logic)
        suffixes_to_remove = [
            " INC$", " INC THE$", " CORP$", " LLC$", " PLC$", " LLP$", 
            " LTD$", " CO$", " SA$", " AG$", " AB$", " CO LTD$", " GROUP$"
        ]
        
        for suffix in suffixes_to_remove:
            seg_customer['cnms'] = seg_customer['cnms'].str.replace(suffix, "", regex=True)
        
        # Remove all spaces
        seg_customer['cnms'] = seg_customer['cnms'].str.replace(" ", "", regex=True)
        
        # Replace common abbreviations
        seg_customer['cnms'] = seg_customer['cnms'].str.replace("MTR", "MOTORS", regex=True)
        seg_customer['cnms'] = seg_customer['cnms'].str.replace("MOTOR$", "MOTORS", regex=True)
        
        seg_customer = seg_customer[['gvkey', 'datadate', 'cnms']]
        
        # Clean CCM data
        ccm0 = ccm.copy()
        ccm0['conm'] = ccm0['conm'].str.upper()
        ccm0['conm'] = ccm0['conm'].str.replace(r'[^\w\s]', '', regex=True)
        
        # Remove common suffixes
        for suffix in suffixes_to_remove:
            ccm0['conm'] = ccm0['conm'].str.replace(suffix, "", regex=True)
        
        # Remove all spaces
        ccm0['conm'] = ccm0['conm'].str.replace(" ", "", regex=True)
        
        # Replace common abbreviations
        ccm0['conm'] = ccm0['conm'].str.replace("MTR", "MOTORS", regex=True)
        ccm0['conm'] = ccm0['conm'].str.replace("MOTOR$", "MOTORS", regex=True)
        
        # Add permno data (both firm and customer)
        logger.info("Adding permno data...")
        seg_customer2 = seg_customer.merge(ccm0, on='gvkey', how='inner')
        seg_customer2 = seg_customer2[
            (seg_customer2['datadate'] >= seg_customer2['linkdt']) & 
            (seg_customer2['datadate'] <= seg_customer2['linkenddt'])
        ]
        seg_customer2 = seg_customer2[['gvkey', 'cnms', 'datadate', 'lpermno']]
        seg_customer2 = seg_customer2.rename(columns={'lpermno': 'permno'})
        
        # Add customer permno
        ccm0_cust = ccm0.rename(columns={'lpermno': 'cust_permno'})
        ccm0_cust = ccm0_cust[['conm', 'cust_permno', 'linkdt', 'linkenddt']]
        
        seg_customer2 = seg_customer2.merge(
            ccm0_cust, 
            left_on='cnms', 
            right_on='conm', 
            how='left'
        )
        
        seg_customer2 = seg_customer2.dropna(subset=['cust_permno'])
        seg_customer2 = seg_customer2[
            (seg_customer2['datadate'] >= seg_customer2['linkdt']) & 
            (seg_customer2['datadate'] <= seg_customer2['linkenddt'])
        ]
        seg_customer2 = seg_customer2[['permno', 'datadate', 'cust_permno']]
        seg_customer2 = seg_customer2.sort_values(['permno', 'datadate'])
        
        # Set day to 28 (equivalent to R's day() function)
        seg_customer2['datadate'] = seg_customer2['datadate'].apply(lambda x: x.replace(day=28))
        
        # Interpolate customer data to monthly w.r.t. time_avail_m (datadate+6 months)
        logger.info("Interpolating customer data to monthly...")
        tempm0 = m_crsp[m_crsp['permno'].isin(seg_customer2['permno'].unique())].copy()
        tempm0['time_avail_m'] = tempm0['date'] - pd.DateOffset(months=1)
        tempm0 = tempm0[['permno', 'time_avail_m']]
        tempm0['time_avail_m'] = tempm0['time_avail_m'].apply(lambda x: x.replace(day=28))
        
        # Make customer data wide so we have one entry per permno-datadate
        logger.info("Creating wide customer data...")
        temp1 = seg_customer2.sort_values(['permno', 'datadate', 'cust_permno']).copy()
        temp1['customeri'] = temp1.groupby(['permno', 'datadate']).cumcount() + 1
        
        # Pivot to wide format
        temp1_wide = temp1.pivot_table(
            index=['permno', 'datadate'], 
            columns='customeri', 
            values='cust_permno', 
            aggfunc='first'
        ).reset_index()
        
        # Fill missing values with -1
        temp1_wide = temp1_wide.fillna(-1)
        
        # Check if there is customer data next year, if not, make a row of -1
        logger.info("Adding missing year entries...")
        temp1b = temp1_wide.sort_values(['permno', 'datadate']).copy()
        temp1b['diffpermno'] = temp1b['permno'].shift(-1) - temp1b['permno']
        temp1b['dyear'] = temp1b['datadate'].shift(-1).dt.year - temp1b['datadate'].dt.year
        temp1b['lastentry'] = (temp1b['diffpermno'] > 0) & (temp1b['dyear'] != 1)
        
        tempstop = temp1b[temp1b['lastentry']].copy()
        tempstop['datadate'] = tempstop['datadate'] + pd.DateOffset(years=1)
        tempstop = tempstop.drop(['diffpermno', 'dyear', 'lastentry'], axis=1)
        
        # Fill customer columns with -1
        customer_cols = [col for col in tempstop.columns if col not in ['permno', 'datadate']]
        tempstop[customer_cols] = -1
        
        temp1c = pd.concat([temp1_wide, tempstop], ignore_index=True)
        temp1c = temp1c.sort_values(['permno', 'datadate'])
        
        # Use crsp permno-dates as a frame and merge on wide customer data with lag
        logger.info("Merging with CRSP data...")
        temp1c['time_avail_m'] = temp1c['datadate'] + pd.DateOffset(months=6)
        
        tempm1 = tempm0.merge(
            temp1c, 
            on=['permno', 'time_avail_m'], 
            how='left'
        )
        tempm1 = tempm1.drop('datadate', axis=1)
        
        # Fill in with most recent available customer
        seg_customer3 = tempm1.sort_values(['permno', 'time_avail_m']).copy()
        
        # Forward fill customer data
        customer_cols = [col for col in seg_customer3.columns if col not in ['permno', 'time_avail_m']]
        seg_customer3[customer_cols] = seg_customer3.groupby('permno')[customer_cols].fillna(method='ffill')
        
        # Convert back to long, remove na's and -1's
        seg_customer3_long = seg_customer3.melt(
            id_vars=['permno', 'time_avail_m'], 
            var_name='customeri', 
            value_name='cust_permno'
        )
        seg_customer3_long = seg_customer3_long[
            (seg_customer3_long['cust_permno'].notna()) & 
            (seg_customer3_long['cust_permno'] > 0)
        ]
        seg_customer3_long = seg_customer3_long[['permno', 'time_avail_m', 'cust_permno']]
        
        # Merge m_crsp returns and compute average customer portfolio returns
        logger.info("Computing customer momentum...")
        tempc = m_crsp[m_crsp['permno'].isin(seg_customer3_long['cust_permno'].unique())].copy()
        tempc['time_avail_m'] = tempc['date']
        tempc['cust_permno'] = tempc['permno']
        tempc['cust_ret'] = tempc['ret']
        tempc = tempc[['cust_permno', 'cust_ret', 'time_avail_m']]
        tempc = tempc.dropna(subset=['cust_ret'])
        tempc['time_avail_m'] = tempc['time_avail_m'].apply(lambda x: x.replace(day=28))
        
        customerMom = seg_customer3_long.merge(
            tempc, 
            on=['time_avail_m', 'cust_permno'], 
            how='left'
        )
        customerMom = customerMom.dropna(subset=['cust_ret'])
        
        # Calculate average customer momentum by permno and time
        customerMom = customerMom.groupby(['time_avail_m', 'permno'])['cust_ret'].mean().reset_index()
        customerMom = customerMom.rename(columns={'cust_ret': 'CustMom'})
        
        # Save to CSV format in the main Data directory (as expected by master script)
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/customerMom.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        customerMom.to_csv(output_path, index=False)
        
        # Also save to intermediate directory for compatibility
        intermediate_path = intermediate_dir / "customerMom.csv"
        customerMom.to_csv(intermediate_path, index=False)
        
        logger.info(f"Saved customerMom.csv to {output_path}")
        logger.info(f"Also saved customerMom.csv to {intermediate_path}")
        logger.info(f"Final dataset: {len(customerMom)} records")
        logger.info(f"Unique companies: {customerMom['permno'].nunique()}")
        logger.info(f"Time range: {customerMom['time_avail_m'].min()} to {customerMom['time_avail_m'].max()}")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to process customer momentum data: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the function
    zk_customermomentum()
