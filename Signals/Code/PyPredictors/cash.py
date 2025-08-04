"""
Python equivalent of Cash.do
Generated from: Cash.do

Original Stata file: Cash.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def cash():
    """
    Python equivalent of Cash.do
    
    Constructs the Cash predictor signal for cash to assets ratio.
    """
    logger.info("Constructing predictor signal: Cash...")
    
    try:
        # DATA LOAD
        # Load Compustat quarterly data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_QCompustat.csv")
        
        logger.info(f"Loading Compustat quarterly data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"Input file not found: {compustat_path}")
            logger.error("Please run the Compustat data download scripts first")
            return False
        
        # Load the required variables
        required_vars = ['gvkey', 'rdq', 'cheq', 'atq']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Drop duplicates: there are a lot since m_QCompustat is a monthly version
        data = data.sort_values(['gvkey', 'rdq'])
        data['dup'] = data.groupby(['gvkey', 'rdq']).cumcount() + 1
        data = data[
            (data['gvkey'].notna()) & 
            (data['dup'] == 1) & 
            (data['atq'].notna())
        ]
        data = data.drop('dup', axis=1)
        
        # Define time_avail_m from rdq (equivalent to Stata's "gen time_avail_m = mofd(rdq)")
        data['rdq'] = pd.to_datetime(data['rdq'])
        data['time_avail_m'] = data['rdq'].dt.to_period('M')
        
        # Expand back to monthly (equivalent to Stata's expand logic)
        logger.info("Expanding quarterly data to monthly...")
        
        expanded_data = []
        for _, row in data.iterrows():
            for i in range(3):
                new_row = row.copy()
                new_row['time_avail_m'] = row['time_avail_m'] + i
                expanded_data.append(new_row)
        
        data = pd.DataFrame(expanded_data)
        
        # Remove duplicates: keep newest rdq (most updated announcement)
        data = data.sort_values(['gvkey', 'time_avail_m', 'rdq'], ascending=[True, True, False])
        data['dup'] = data.groupby(['gvkey', 'time_avail_m']).cumcount()
        data = data[data['dup'] == 0]
        data = data.drop('dup', axis=1)
        
        # Save temporary file (equivalent to Stata's "save tempCash")
        temp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Temp/tempCash.csv")
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(temp_path, index=False)
        
        # Merge with SignalMasterTable
        logger.info("Merging with SignalMasterTable...")
        
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        master_data = pd.read_csv(master_path, usecols=['permno', 'gvkey', 'time_avail_m'])
        master_data = master_data[master_data['gvkey'].notna()]
        
        # Merge data
        merged_data = master_data.merge(
            data[['gvkey', 'time_avail_m', 'atq', 'cheq', 'rdq']], 
            on=['gvkey', 'time_avail_m'], 
            how='inner'
        )
        
        logger.info(f"Successfully merged data: {len(merged_data)} observations")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating Cash signal...")
        
        # Calculate Cash ratio (equivalent to Stata's "gen Cash = cheq/atq")
        merged_data['Cash'] = merged_data['cheq'] / merged_data['atq']
        
        logger.info("Successfully calculated Cash signal")
        
        # SAVE RESULTS
        logger.info("Saving Cash predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = merged_data[['permno', 'time_avail_m', 'Cash']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['Cash'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "Cash.csv"
        csv_data = output_data[['permno', 'yyyymm', 'Cash']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved Cash predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed Cash predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct Cash predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    cash()
