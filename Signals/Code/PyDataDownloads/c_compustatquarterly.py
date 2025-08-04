"""
Python equivalent of C_CompustatQuarterly.do
Generated from: C_CompustatQuarterly.do

Original Stata file: C_CompustatQuarterly.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def c_compustatquarterly(wrds_conn=None):
    """
    Python equivalent of C_CompustatQuarterly.do
    
    Downloads and processes Compustat quarterly data from WRDS
    """
    logger.info("Downloading Compustat quarterly data...")
    
    try:
        # Check if WRDS connection is provided
        if wrds_conn is None:
            logger.error("WRDS connection not available. Please run master.py")
            return False
        conn = wrds_conn
        
        # SQL query from original Stata file
        query = """
        SELECT 
            a.gvkey, a.datadate, a.fyearq, a.fqtr, a.datacqtr, a.datafqtr, a.acoq,
            a.actq,a.ajexq,a.apq,a.atq,a.ceqq,a.cheq,a.cogsq,a.cshoq,a.cshprq,
            a.dlcq,a.dlttq,a.dpq,a.drcq,a.drltq,a.dvpsxq,a.dvpq,a.dvy,a.epspiq,a.epspxq,a.fopty,
            a.gdwlq,a.ibq,a.invtq,a.intanq,a.ivaoq,a.lcoq,a.lctq,a.loq,a.ltq,a.mibq,
            a.niq,a.oancfy,a.oiadpq,a.oibdpq,a.piq,a.ppentq,a.ppegtq,a.prstkcy,a.prccq,
            a.pstkq,a.rdq,a.req,a.rectq,a.revtq,a.saleq,a.seqq,a.sstky,a.txdiq,
            a.txditcq,a.txpq,a.txtq,a.xaccq,a.xintq,a.xsgaq,a.xrdq, a.capxy
        FROM COMP.FUNDQ as a
        WHERE a.consol = 'C'
        AND a.popsrc = 'D'
        AND a.datafmt = 'STD'
        AND a.curcdq = 'USD'
        AND a.indfmt = 'INDL'
        """
        
        # Execute query
        data = conn.raw_sql(query, date_cols=['datadate', 'datacqtr', 'datafqtr', 'rdq'])
        logger.info(f"Downloaded {len(data)} Compustat quarterly records")
        
        # Keep only the most recent data for each fiscal quarter
        data = data.sort_values(['gvkey', 'fyearq', 'fqtr', 'datadate'])
        data = data.drop_duplicates(subset=['gvkey', 'fyearq', 'fqtr'], keep='last')
        logger.info(f"After keeping most recent per quarter: {len(data)} records")
        
        # Data availability assumed as discussed in https://github.com/OpenSourceAP/CrossSection/issues/50
        # Assume data available with a 3 month lag (equivalent to Stata's "gen time_avail_m = mofd(datadate) + 3")
        data['time_avail_m'] = pd.to_datetime(data['datadate']) + pd.DateOffset(months=3)
        data['time_avail_m'] = data['time_avail_m'].dt.to_period('M')
        
        # Patch cases with earlier data availability (equivalent to Stata's "replace time_avail_m = mofd(rdq) if !mi(rdq) & mofd(rdq) > time_avail_m")
        rdq_available = data['rdq'].notna()
        rdq_time_avail = pd.to_datetime(data['rdq']) + pd.DateOffset(months=3)
        rdq_time_avail = rdq_time_avail.dt.to_period('M')
        rdq_earlier = rdq_time_avail > data['time_avail_m']
        mask = rdq_available & rdq_earlier
        data.loc[mask, 'time_avail_m'] = rdq_time_avail[mask]
        
        # Drop cases with very late release (equivalent to Stata's "drop if mofd(rdq) - mofd(datadate) > 6 & !mi(rdq)")
        rdq_available = data['rdq'].notna()
        if rdq_available.any():
            date_diff = (pd.to_datetime(data['rdq']) - pd.to_datetime(data['datadate'])).dt.days
            late_release = date_diff > 180  # 6 months = 180 days
            mask = rdq_available & late_release
            data = data[~mask]
        logger.info(f"After dropping very late releases: {len(data)} records")
        
        # Keep only the most recent info for each gvkey-time_avail_m combination
        data = data.sort_values(['gvkey', 'time_avail_m', 'datadate'])
        data = data.drop_duplicates(subset=['gvkey', 'time_avail_m'], keep='last')
        logger.info(f"After keeping most recent per month: {len(data)} records")
        
        # For these variables, missing is assumed to be 0
        zero_vars = ['acoq', 'actq', 'apq', 'cheq', 'dpq', 'drcq', 'invtq', 'intanq', 'ivaoq',
                     'gdwlq', 'lcoq', 'lctq', 'loq', 'mibq', 'prstkcy', 'rectq', 'sstky', 'txditcq']
        
        for var in zero_vars:
            if var in data.columns:
                data[var] = data[var].fillna(0)
        
        # Prepare year-to-date items (equivalent to Stata's foreach loop)
        data = data.sort_values(['gvkey', 'fyearq', 'fqtr'])
        
        ytd_vars = ['sstky', 'prstkcy', 'oancfy', 'fopty']
        for var in ytd_vars:
            if var in data.columns:
                # Create quarterly version
                q_var = f'{var}q'
                data[q_var] = np.nan
                
                # For Q1, use the value as is (equivalent to Stata's "gen `v'q = `v' if fqtr == 1")
                mask = data['fqtr'] == 1
                data.loc[mask, q_var] = data.loc[mask, var]
                
                # For other quarters, calculate quarter-over-quarter change (equivalent to Stata's "by gvkey fyearq: replace `v'q = `v' - `v'[_n-1] if fqtr !=1")
                for gvkey in data['gvkey'].unique():
                    gvkey_mask = data['gvkey'] == gvkey
                    gvkey_data = data[gvkey_mask].copy()
                    gvkey_data = gvkey_data.sort_values(['fyearq', 'fqtr'])
                    
                    for i in range(1, len(gvkey_data)):
                        if gvkey_data.iloc[i]['fqtr'] != 1:  # Not Q1
                            # Calculate change from previous quarter
                            current_val = gvkey_data.iloc[i][var]
                            prev_val = gvkey_data.iloc[i-1][var]
                            
                            # Only calculate if both values are not NaN (equivalent to Stata's behavior)
                            if pd.notna(current_val) and pd.notna(prev_val):
                                data.loc[gvkey_data.index[i], q_var] = current_val - prev_val
                            elif pd.notna(current_val):
                                # If only current value exists, use it as is
                                data.loc[gvkey_data.index[i], q_var] = current_val
        
        # Expand to monthly (equivalent to Stata's "expand 3")
        # Create a temporary time_avail_m column for the expansion logic
        data['tempTimeAvailM'] = data['time_avail_m']
        
        # Expand each row to 3 rows (equivalent to Stata's "expand 3")
        expanded_data = []
        for _, row in data.iterrows():
            for i in range(3):
                new_row = row.copy()
                if i > 0:  # For rows after the first one
                    new_row['time_avail_m'] = row['time_avail_m'] + i
                expanded_data.append(new_row)
        
        data = pd.DataFrame(expanded_data)
        
        # Keep only the most recent info for each gvkey-time_avail_m combination after expanding
        # (equivalent to Stata's "bysort gvkey time_avail_m (datadate): keep if _n == _N")
        data = data.sort_values(['gvkey', 'time_avail_m', 'datadate'])
        data = data.drop_duplicates(subset=['gvkey', 'time_avail_m'], keep='last')
        logger.info(f"After expanding to monthly: {len(data)} records")
        
        # Drop temporary column (equivalent to Stata's "drop temp*")
        data = data.drop('tempTimeAvailM', axis=1)
        
        # Rename datadate to datadateq (equivalent to Stata's "rename datadate datadateq")
        data = data.rename(columns={'datadate': 'datadateq'})
        
        # Convert gvkey to numeric (equivalent to Stata's "destring gvkey, replace")
        data['gvkey'] = pd.to_numeric(data['gvkey'], errors='coerce')
        
        # Save to intermediate file
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_QCompustat.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved quarterly data to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/m_QCompustat.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        logger.info("Successfully downloaded and processed Compustat quarterly data")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download Compustat quarterly data: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the download function
    c_compustatquarterly()
