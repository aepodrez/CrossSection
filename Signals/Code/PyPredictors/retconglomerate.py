"""
Python equivalent of retConglomerate.do
Generated from: retConglomerate.do

Original Stata file: retConglomerate.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def retconglomerate():
    """
    Python equivalent of retConglomerate.do
    
    Constructs the retConglomerate predictor signal for conglomerate returns.
    """
    logger.info("Constructing predictor signal: retConglomerate...")
    
    try:
        # DATA LOAD
        logger.info("Loading data sources...")
        
        # Prepare crosswalk
        logger.info("Loading CCMLinkingTable...")
        ccm_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/CCMLinkingTable.csv")
        
        if not ccm_path.exists():
            logger.error(f"CCMLinkingTable not found: {ccm_path}")
            logger.error("Please run the CCMLinkingTable creation script first")
            return False
        
        ccm_data = pd.read_csv(ccm_path, usecols=['gvkey', 'permno', 'timeLinkStart_d', 'timeLinkEnd_d'])
        ccm_data['gvkey'] = pd.to_numeric(ccm_data['gvkey'], errors='coerce')
        logger.info(f"Successfully loaded {len(ccm_data)} CCM records")
        
        # Prepare returns
        logger.info("Loading monthly CRSP returns...")
        crsp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/monthlyCRSP.csv")
        
        if not crsp_path.exists():
            logger.error(f"monthlyCRSP not found: {crsp_path}")
            logger.error("Please run the monthly CRSP data creation script first")
            return False
        
        crsp_data = pd.read_csv(crsp_path, usecols=['permno', 'time_avail_m', 'ret'])
        logger.info(f"Successfully loaded {len(crsp_data)} CRSP records")
        
        # Annual sales from Compustat
        logger.info("Loading annual Compustat sales...")
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/a_aCompustat.csv")
        
        if not compustat_path.exists():
            logger.error(f"a_aCompustat not found: {compustat_path}")
            logger.error("Please run the annual Compustat data creation script first")
            return False
        
        compustat_data = pd.read_csv(compustat_path, usecols=['gvkey', 'permno', 'sale', 'fyear'])
        compustat_data = compustat_data.rename(columns={'sale': 'saleACS'})
        compustat_data = compustat_data[(compustat_data['saleACS'] >= 0) & (~compustat_data['saleACS'].isna())]
        logger.info(f"Successfully loaded {len(compustat_data)} Compustat records")
        
        # Conglomerates from Compustat segment data
        logger.info("Loading Compustat segments...")
        segments_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/CompustatSegments.csv")
        
        if not segments_path.exists():
            logger.error(f"CompustatSegments not found: {segments_path}")
            logger.error("Please run the Compustat segments data creation script first")
            return False
        
        segments_data = pd.read_csv(segments_path, usecols=['gvkey', 'datadate', 'stype', 'sics1', 'sales'])
        
        # Keep only operating and business segments
        segments_data = segments_data[segments_data['stype'].isin(['OPSEG', 'BUSSEG'])]
        segments_data = segments_data[(segments_data['sales'] >= 0) & (~segments_data['sales'].isna())]
        
        # Create 2-digit SIC
        segments_data['sics1'] = segments_data['sics1'].astype(str)
        segments_data['sic2D'] = segments_data['sics1'].str[:2]
        
        # Collapse by gvkey, sic2D, datadate
        segments_data = segments_data.groupby(['gvkey', 'sic2D', 'datadate'])['sales'].sum().reset_index()
        
        # Create fiscal year
        segments_data['datadate'] = pd.to_datetime(segments_data['datadate'])
        segments_data['fyear'] = segments_data['datadate'].dt.year
        
        # Merge with Compustat sales
        segments_data = segments_data.merge(compustat_data, on=['gvkey', 'fyear'], how='inner')
        
        # Calculate total sales and segment share
        segments_data['temptotalSales'] = segments_data.groupby(['gvkey', 'fyear'])['sales'].transform('sum')
        segments_data['tempCSSegmentShare'] = segments_data['sales'] / segments_data['saleACS']
        
        # Count industries per firm-date
        segments_data['tempNInd'] = segments_data.groupby(['gvkey', 'datadate']).transform('size')
        
        # Create conglomerate indicator
        segments_data['Conglomerate'] = np.nan
        segments_data.loc[(segments_data['tempNInd'] == 1) & (segments_data['tempCSSegmentShare'] > 0.8), 'Conglomerate'] = 0
        segments_data.loc[(segments_data['tempNInd'] > 1) & (segments_data['tempCSSegmentShare'] > 0.8), 'Conglomerate'] = 1
        
        # Drop missing conglomerate values
        segments_data = segments_data.dropna(subset=['Conglomerate'])
        logger.info(f"Conglomerate classification: {segments_data['Conglomerate'].value_counts().to_dict()}")
        
        # Industry returns from stand-alones
        standalone_data = segments_data[segments_data['Conglomerate'] == 0].copy()
        
        # Add identifiers for merging with stock returns
        standalone_data = standalone_data.merge(ccm_data, on='gvkey', how='inner')
        
        # Use only if data date is within the validity period of the link
        standalone_data['datadate'] = pd.to_datetime(standalone_data['datadate'])
        standalone_data['timeLinkStart_d'] = pd.to_datetime(standalone_data['timeLinkStart_d'])
        standalone_data['timeLinkEnd_d'] = pd.to_datetime(standalone_data['timeLinkEnd_d'])
        
        standalone_data['temp'] = ((standalone_data['timeLinkStart_d'] <= standalone_data['datadate']) & 
                                  (standalone_data['datadate'] <= standalone_data['timeLinkEnd_d']))
        standalone_data = standalone_data[standalone_data['temp'] == True]
        
        # Merge stock returns
        standalone_data = standalone_data[['permno', 'sic2D', 'fyear']].drop_duplicates()
        standalone_data = standalone_data.rename(columns={'sic2D': 'sic2DCSS'})
        standalone_data = standalone_data.merge(crsp_data, on='permno', how='inner')
        
        # Keep if fiscal year matches calendar year
        standalone_data['time_avail_m'] = pd.to_datetime(standalone_data['time_avail_m'])
        # Convert time_avail_m to datetime if needed for year extraction
        if not pd.api.types.is_datetime64_any_dtype(standalone_data['time_avail_m']):
            standalone_data['time_avail_m'] = pd.to_datetime(standalone_data['time_avail_m'])
        
        standalone_data['year'] = standalone_data['time_avail_m'].dt.year
        standalone_data = standalone_data[standalone_data['fyear'] == standalone_data['year']]
        
        # Collapse to industry-month returns
        industry_returns = standalone_data.groupby(['sic2DCSS', 'time_avail_m'])['ret'].mean().reset_index()
        industry_returns = industry_returns[industry_returns['sic2DCSS'] != '.']
        logger.info(f"Created industry returns for {len(industry_returns)} industry-month combinations")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating conglomerate returns...")
        
        # Match industry returns to conglomerates
        conglomerate_data = segments_data[segments_data['Conglomerate'] == 1].copy()
        conglomerate_data = conglomerate_data[['permno', 'sic2D', 'sales', 'fyear']]
        conglomerate_data = conglomerate_data.rename(columns={'sic2D': 'sic2DCSS'})
        conglomerate_data = conglomerate_data[conglomerate_data['sic2DCSS'] != '.']
        
        # Merge with industry returns
        conglomerate_data = conglomerate_data.merge(industry_returns, on='sic2DCSS', how='inner')
        
        # Keep if fiscal year matches calendar year
        conglomerate_data['time_avail_m'] = pd.to_datetime(conglomerate_data['time_avail_m'])
        # Convert time_avail_m to datetime if needed for year extraction
        if not pd.api.types.is_datetime64_any_dtype(conglomerate_data['time_avail_m']):
            conglomerate_data['time_avail_m'] = pd.to_datetime(conglomerate_data['time_avail_m'])
        
        conglomerate_data['year'] = conglomerate_data['time_avail_m'].dt.year
        conglomerate_data = conglomerate_data[conglomerate_data['fyear'] == conglomerate_data['year']]
        
        # Calculate weighted returns
        conglomerate_data['tempTotal'] = conglomerate_data.groupby(['permno', 'time_avail_m'])['sales'].transform('sum')
        conglomerate_data['tempweight'] = conglomerate_data['sales'] / conglomerate_data['tempTotal']
        
        # Collapse to weighted average returns
        conglomerate_returns = conglomerate_data.groupby(['permno', 'time_avail_m']).apply(
            lambda x: np.average(x['ret'], weights=x['tempweight'])
        ).reset_index()
        conglomerate_returns.columns = ['permno', 'time_avail_m', 'retConglomerate']
        
        logger.info("Successfully calculated conglomerate returns")
        
        # SAVE RESULTS
        logger.info("Saving retConglomerate predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = conglomerate_returns.copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['retConglomerate'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "retConglomerate.csv"
        csv_data = output_data[['permno', 'yyyymm', 'retConglomerate']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved retConglomerate predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed retConglomerate predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct retConglomerate predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    retconglomerate()
