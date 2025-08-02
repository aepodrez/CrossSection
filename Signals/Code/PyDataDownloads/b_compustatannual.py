"""
Python equivalent of B_CompustatAnnual.do
Generated from: B_CompustatAnnual.do

Original Stata file: B_CompustatAnnual.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def b_compustatannual():
    """
    Python equivalent of B_CompustatAnnual.do
    
    Downloads and processes Compustat annual data from WRDS
    """
    logger.info("Downloading Compustat annual data...")
    
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
            a.gvkey, a.datadate, a.conm, a.fyear, a.tic, a.cusip, a.naicsh, a.sich, 
            a.aco,a.act,a.ajex,a.am,a.ao,a.ap,a.at,a.capx,a.ceq,a.ceqt,a.che,a.cogs,
            a.csho,a.cshrc,a.dcpstk,a.dcvt,a.dlc,a.dlcch,a.dltis,a.dltr,
            a.dltt,a.dm,a.dp,a.drc,a.drlt,a.dv,a.dvc,a.dvp,a.dvpa,a.dvpd,
            a.dvpsx_c,a.dvt,a.ebit,a.ebitda,a.emp,a.epspi,a.epspx,a.fatb,a.fatl,
            a.ffo,a.fincf,a.fopt,a.gdwl,a.gdwlia,a.gdwlip,a.gwo,a.ib,a.ibcom,
            a.intan,a.invt,a.ivao,a.ivncf,a.ivst,a.lco,a.lct,a.lo,a.lt,a.mib,
            a.msa,a.ni,a.nopi,a.oancf,a.ob,a.oiadp,a.oibdp,a.pi,a.ppenb,a.ppegt,
            a.ppenls,a.ppent,a.prcc_c,a.prcc_f,a.prstkc,a.prstkcc,a.pstk,a.pstkl,a.pstkrv,
            a.re,a.rect,a.recta,a.revt,a.sale,a.scstkc,a.seq,a.spi,a.sstk,
            a.tstkp,a.txdb,a.txdi,a.txditc,a.txfo,a.txfed,a.txp,a.txt,
            a.wcap,a.wcapch,a.xacc,a.xad,a.xint,a.xrd,a.xpp,a.xsga
        FROM COMP.FUNDA as a
        WHERE a.consol = 'C'
        AND a.popsrc = 'D'
        AND a.datafmt = 'STD'
        AND a.curcd = 'USD'
        AND a.indfmt = 'INDL'
        """
        
        # Execute query
        data = conn.raw_sql(query, date_cols=['datadate'])
        logger.info(f"Downloaded {len(data)} Compustat annual records")
        
        # Save intermediate file (CSV format)
        intermediate_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/CompustatAnnual.csv")
        intermediate_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(intermediate_path, index=False)
        logger.info(f"Saved intermediate data to {intermediate_path}")
        
        # Require some reasonable amount of information
        data = data.dropna(subset=['at', 'prcc_c', 'ni'])
        logger.info(f"After dropping missing key variables: {len(data)} records")
        
        # 6 digit CUSIP
        data['cnum'] = data['cusip'].str[:6]
        
        # ----------------------------------------------------------------------------
        # Replace missing values if reasonable
        # ----------------------------------------------------------------------------
        
        # Deferred revenue
        data['dr'] = np.nan
        mask = (data['drc'].notna() & data['drlt'].notna())
        data.loc[mask, 'dr'] = data.loc[mask, 'drc'] + data.loc[mask, 'drlt']
        mask = (data['drc'].notna() & data['drlt'].isna())
        data.loc[mask, 'dr'] = data.loc[mask, 'drc']
        mask = (data['drc'].isna() & data['drlt'].notna())
        data.loc[mask, 'dr'] = data.loc[mask, 'drlt']
        
        # Convertible debt
        data['dc'] = np.nan
        mask = ((data['dcpstk'] > data['pstk']) & data['pstk'].notna() & 
                data['dcpstk'].notna() & data['dcvt'].isna())
        data.loc[mask, 'dc'] = data.loc[mask, 'dcpstk'] - data.loc[mask, 'pstk']
        mask = (data['pstk'].isna() & data['dcpstk'].notna() & data['dcvt'].isna())
        data.loc[mask, 'dc'] = data.loc[mask, 'dcpstk']
        mask = (data['dc'].isna() & data['dcvt'].notna())
        data.loc[mask, 'dc'] = data.loc[mask, 'dcvt']
        
        # Interest expense
        data['xint0'] = 0
        mask = data['xint'].notna()
        data.loc[mask, 'xint0'] = data.loc[mask, 'xint']
        
        # Selling, general and administrative expenses
        data['xsga0'] = 0
        mask = data['xsga'].notna()
        data.loc[mask, 'xsga0'] = data.loc[mask, 'xsga']
        
        # Advertising expense
        data['xad0'] = data['xad']
        data.loc[data['xad'].isna(), 'xad0'] = 0
        
        # For these variables, missing is assumed to be 0
        zero_vars = ['nopi', 'dvt', 'ob', 'dm', 'dc', 'aco', 'ap', 'intan', 'ao', 
                     'lco', 'lo', 'rect', 'invt', 'drc', 'spi', 'gdwl', 'che', 
                     'dp', 'act', 'lct', 'tstkp', 'dvpa', 'scstkc', 'sstk', 'mib',
                     'ivao', 'prstkc', 'prstkcc', 'txditc', 'ivst']
        
        for var in zero_vars:
            if var in data.columns:
                data[var] = data[var].fillna(0)
        
        # Add identifiers for merging
        linking_table_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/CCMLinkingTable.csv")
        if linking_table_path.exists():
            linking_table = pd.read_csv(linking_table_path)
            data = data.merge(linking_table, on='gvkey', how='inner')
            logger.info(f"After merging with linking table: {len(data)} records")
            
            # Use only if data date is within the validity period of the link
            data['temp'] = ((data['timeLinkStart_d'] <= data['datadate']) & 
                           (data['datadate'] <= data['timeLinkEnd_d']))
            data = data[data['temp'] == True]
            data = data.drop('temp', axis=1)
            logger.info(f"After filtering by link validity: {len(data)} records")
        else:
            logger.warning("CCMLinkingTable.csv not found, skipping merge")
        
        # Create two versions: Annual and monthly (monthly makes matching to monthly CRSP easier)
        
        # Annual version
        annual_data = data.copy()
        if 'timeLinkStart_d' in annual_data.columns:
            annual_data = annual_data.drop(['timeLinkStart_d', 'timeLinkEnd_d', 'linkprim', 'liid', 'linktype'], axis=1)
        
        # Convert gvkey to numeric
        annual_data['gvkey'] = pd.to_numeric(annual_data['gvkey'], errors='coerce')
        
        # Create time_avail_m (assuming 6 month reporting lag)
        annual_data['time_avail_m'] = pd.to_datetime(annual_data['datadate']) + pd.DateOffset(months=6)
        annual_data['time_avail_m'] = annual_data['time_avail_m'].dt.to_period('M')
        
        # Save annual version
        annual_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/a_aCompustat.csv")
        annual_data.to_csv(annual_path, index=False)
        logger.info(f"Saved annual version to {annual_path}")
        
        # Monthly version
        monthly_data = annual_data.copy()
        
        # Expand to monthly (12 months per observation)
        monthly_list = []
        for _, row in monthly_data.iterrows():
            for i in range(12):
                new_row = row.copy()
                new_row['time_avail_m'] = row['time_avail_m'] + i
                monthly_list.append(new_row)
        
        monthly_data = pd.DataFrame(monthly_list)
        
        # Keep only the most recent info for each gvkey-time_avail_m combination
        monthly_data = monthly_data.sort_values(['gvkey', 'time_avail_m', 'datadate'])
        monthly_data = monthly_data.drop_duplicates(subset=['gvkey', 'time_avail_m'], keep='last')
        
        # Also keep only the most recent info for each permno-time_avail_m combination
        if 'permno' in monthly_data.columns:
            monthly_data = monthly_data.sort_values(['permno', 'time_avail_m', 'datadate'])
            monthly_data = monthly_data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='last')
        
        # Save monthly version
        monthly_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        monthly_data.to_csv(monthly_path, index=False)
        logger.info(f"Saved monthly version to {monthly_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/m_aCompustat.csv")
        monthly_data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        logger.info("Successfully downloaded and processed Compustat annual data")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download Compustat annual data: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the download function
    b_compustatannual()
