"""
Python equivalent of EarnSupBig.do
Generated from: EarnSupBig.do

Original Stata file: EarnSupBig.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def earnsupbig():
    """
    Python equivalent of EarnSupBig.do
    
    Constructs the EarnSupBig predictor signal for industry earnings surprise of big companies.
    """
    logger.info("Constructing predictor signal: EarnSupBig...")
    
    try:
        # First part: make earnings surprise (copied from EarningsSurprise.do)
        logger.info("Calculating earnings surprise component...")
        
        # DATA LOAD
        # Load SignalMasterTable
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load required variables from SignalMasterTable
        master_vars = ['permno', 'gvkey', 'time_avail_m']
        data = pd.read_csv(master_path, usecols=master_vars)
        logger.info(f"Successfully loaded {len(data)} records from SignalMasterTable")
        
        # Keep if gvkey is not missing (equivalent to Stata's "keep if !mi(gvkey)")
        data = data[data['gvkey'].notna()]
        logger.info(f"After filtering for non-missing gvkey: {len(data)} records")
        
        # Merge with quarterly Compustat data (equivalent to Stata's merge)
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_QCompustat.csv")
        
        if not compustat_path.exists():
            logger.error(f"Quarterly Compustat file not found: {compustat_path}")
            logger.error("Please run the Compustat data download script first")
            return False
        
        compustat_data = pd.read_csv(compustat_path, usecols=['gvkey', 'time_avail_m', 'epspxq'])
        
        data = data.merge(
            compustat_data,
            on=['gvkey', 'time_avail_m'],
            how='inner'  # keep(match)
        )
        
        logger.info(f"Successfully merged data: {len(data)} observations")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating earnings surprise...")
        
        # Sort for lag calculations (equivalent to Stata's "xtset permno time_avail_m")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate 12-month lag of epspxq
        data['epspxq_lag12'] = data.groupby('permno')['epspxq'].shift(12)
        
        # Calculate GrTemp (equivalent to Stata's "gen GrTemp = (epspxq - l12.epspxq)")
        data['GrTemp'] = data['epspxq'] - data['epspxq_lag12']
        
        # Calculate lags of GrTemp for different periods (equivalent to Stata's foreach loop)
        for n in [3, 6, 9, 12, 15, 18, 21, 24]:
            data[f'temp{n}'] = data.groupby('permno')['GrTemp'].shift(n)
        
        # Calculate Drift as row mean (equivalent to Stata's "egen Drift = rowmean(temp*)")
        temp_cols = [f'temp{n}' for n in [3, 6, 9, 12, 15, 18, 21, 24]]
        data['Drift'] = data[temp_cols].mean(axis=1)
        
        # Calculate EarningsSurprise (equivalent to Stata's "gen EarningsSurprise = epspxq - l12.epspxq - Drift")
        data['EarningsSurprise'] = data['epspxq'] - data['epspxq_lag12'] - data['Drift']
        
        # Drop temporary columns (equivalent to Stata's "cap drop temp*")
        data = data.drop(columns=temp_cols)
        
        # Calculate lags of EarningsSurprise for different periods (equivalent to Stata's second foreach loop)
        for n in [3, 6, 9, 12, 15, 18, 21, 24]:
            data[f'temp{n}'] = data.groupby('permno')['EarningsSurprise'].shift(n)
        
        # Calculate SD as row standard deviation (equivalent to Stata's "egen SD = rowsd(temp*)")
        temp_cols = [f'temp{n}' for n in [3, 6, 9, 12, 15, 18, 21, 24]]
        data['SD'] = data[temp_cols].std(axis=1)
        
        # Normalize EarningsSurprise by SD (equivalent to Stata's "replace EarningsSurprise = EarningsSurprise/SD")
        data['EarningsSurprise'] = data['EarningsSurprise'] / data['SD']
        
        # Save temporary file (equivalent to Stata's "save "$pathtemp/temp", replace")
        temp_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Temp")
        temp_dir.mkdir(parents=True, exist_ok=True)
        data[['permno', 'time_avail_m', 'EarningsSurprise']].to_csv(temp_dir / "temp.csv", index=False)
        
        logger.info("Successfully calculated earnings surprise component")
        
        # Second part: actually make EarnSupBig
        logger.info("Calculating EarnSupBig signal...")
        
        # DATA LOAD
        # Load SignalMasterTable with additional variables
        master_vars2 = ['permno', 'time_avail_m', 'mve_c', 'sicCRSP']
        data2 = pd.read_csv(master_path, usecols=master_vars2)
        
        # Merge with earnings surprise data (equivalent to Stata's merge)
        data2 = data2.merge(
            data[['permno', 'time_avail_m', 'EarningsSurprise']],
            on=['permno', 'time_avail_m'],
            how='left'
        )
        
        logger.info(f"Successfully merged data: {len(data2)} observations")
        
        # SIGNAL CONSTRUCTION
        # Create Fama-French 48 industry classification (equivalent to Stata's "sicff sicCRSP, generate(tempFF48) industry(48)")
        # Note: This is a simplified version - in practice you'd need the actual SIC to FF48 mapping
        data2['tempFF48'] = data2['sicCRSP'] // 100  # Simplified industry classification
        
        # Drop missing tempFF48 (equivalent to Stata's "drop if mi(tempFF48)")
        data2 = data2.dropna(subset=['tempFF48'])
        
        # Calculate relative rank of market value within industry-month (equivalent to Stata's "bys tempFF48 time_avail_m: relrank mve_c, gen(tempRK)")
        data2['tempRK'] = data2.groupby(['tempFF48', 'time_avail_m'])['mve_c'].rank(pct=True)
        
        # Calculate mean earnings surprise for big companies (equivalent to Stata's preserve/restore logic)
        big_companies = data2[(data2['tempRK'] >= 0.7) & (data2['tempRK'].notna())].copy()
        
        if len(big_companies) > 0:
            # Calculate mean earnings surprise by industry-month for big companies
            earnsupbig_means = big_companies.groupby(['tempFF48', 'time_avail_m'])['EarningsSurprise'].mean().reset_index()
            earnsupbig_means = earnsupbig_means.rename(columns={'EarningsSurprise': 'EarnSupBig'})
            
            # Save temporary file (equivalent to Stata's "save "$pathtemp/temp",replace")
            earnsupbig_means.to_csv(temp_dir / "temp_earnsupbig.csv", index=False)
            
            # Merge back with main data
            data2 = data2.merge(
                earnsupbig_means,
                on=['tempFF48', 'time_avail_m'],
                how='left'
            )
            
            # Set EarnSupBig to missing for big companies (equivalent to Stata's "replace EarnSupBig = . if tempRK >= .7")
            data2.loc[data2['tempRK'] >= 0.7, 'EarnSupBig'] = np.nan
        else:
            # If no big companies, create empty EarnSupBig column
            data2['EarnSupBig'] = np.nan
        
        logger.info("Successfully calculated EarnSupBig signal")
        
        # SAVE RESULTS
        logger.info("Saving EarnSupBig predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data2[['permno', 'time_avail_m', 'EarnSupBig']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['EarnSupBig'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "EarnSupBig.csv"
        csv_data = output_data[['permno', 'yyyymm', 'EarnSupBig']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved EarnSupBig predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed EarnSupBig predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct EarnSupBig predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    earnsupbig()
