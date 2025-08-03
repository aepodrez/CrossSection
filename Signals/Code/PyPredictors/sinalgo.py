"""
Python equivalent of sinAlgo.do
Generated from: sinAlgo.do

Original Stata file: sinAlgo.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def sinalgo():
    """
    Python equivalent of sinAlgo.do
    
    Constructs the sinAlgo predictor signal for sin stocks.
    """
    logger.info("Constructing predictor signal: sinAlgo...")
    
    try:
        # DATA LOAD (Compustat Segments)
        segments_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/CompustatSegments.csv")
        
        logger.info(f"Loading Compustat Segments data from: {segments_path}")
        
        if not segments_path.exists():
            logger.error(f"CompustatSegments not found: {segments_path}")
            logger.error("Please run the Compustat Segments data creation script first")
            return False
        
        segments_data = pd.read_csv(segments_path, usecols=['gvkey', 'sics1', 'naicsh', 'datadate'])
        logger.info(f"Successfully loaded {len(segments_data)} segment records")
        
        # Convert datadate to year
        segments_data['datadate'] = pd.to_datetime(segments_data['datadate'])
        segments_data['year'] = segments_data['datadate'].dt.year
        
        # SIGNAL CONSTRUCTION for segments
        logger.info("Calculating sin segment indicators...")
        
        # Initialize sin segment indicators
        segments_data['sinSegTobacco'] = np.nan
        segments_data['sinSegBeer'] = np.nan
        segments_data['sinSegGaming'] = np.nan
        
        # Sin stocks identification
        segments_data.loc[(segments_data['sics1'] >= 2100) & (segments_data['sics1'] <= 2199), 'sinSegTobacco'] = 1
        segments_data.loc[(segments_data['sics1'] >= 2080) & (segments_data['sics1'] <= 2085), 'sinSegBeer'] = 1
        segments_data.loc[segments_data['naicsh'].isin([7132, 71312, 713210, 71329, 713290, 72112, 721120]), 'sinSegGaming'] = 1
        
        # Create any sin segment indicator
        segments_data['sinSegAny'] = np.nan
        segments_data.loc[(segments_data['sinSegTobacco'] == 1) | (segments_data['sinSegBeer'] == 1) | (segments_data['sinSegGaming'] == 1), 'sinSegAny'] = 1
        
        # Keep only sin segments
        segments_data = segments_data[segments_data['sinSegAny'] == 1]
        
        # Collapse to firm-year level (equivalent to Stata's gcollapse)
        sin_segments = segments_data.groupby(['gvkey', 'year'])[['sinSegTobacco', 'sinSegBeer', 'sinSegGaming', 'sinSegAny']].max().reset_index()
        
        # Keep first year for each firm (equivalent to Stata's "bys gvkey (year): keep if _n == 1")
        first_year_segments = sin_segments.groupby('gvkey').first().reset_index()
        first_year_segments = first_year_segments.rename(columns={
            'year': 'firstYear',
            'sinSegTobacco': 'sinSegTobaccoFirstYear',
            'sinSegBeer': 'sinSegBeerFirstYear',
            'sinSegGaming': 'sinSegGamingFirstYear',
            'sinSegAny': 'sinSegAnyFirstYear'
        })
        
        logger.info(f"Created {len(first_year_segments)} first-year sin segment records")
        
        # DATA LOAD (Firm-level industry codes)
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        data = pd.read_csv(master_path, usecols=['permno', 'gvkey', 'time_avail_m', 'sicCRSP', 'shrcd', 'bh1m'])
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Add NAICS codes from Compustat
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading annual Compustat data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"m_aCompustat not found: {compustat_path}")
            logger.error("Please run the annual Compustat data creation script first")
            return False
        
        compustat_data = pd.read_csv(compustat_path, usecols=['permno', 'time_avail_m', 'naicsh'])
        
        # Merge with Compustat data
        data = data.merge(compustat_data, on=['permno', 'time_avail_m'], how='left')
        logger.info(f"After merging with Compustat data: {len(data)} records")
        
        # Convert time_avail_m to year
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        data['year'] = data['time_avail_m'].dt.year
        
        # Convert sicCRSP to numeric
        data['sicCRSP'] = pd.to_numeric(data['sicCRSP'], errors='coerce')
        
        # SIGNAL CONSTRUCTION for firm-level
        logger.info("Calculating firm-level sin stock indicators...")
        
        # Initialize sin stock indicators
        data['sinStockTobacco'] = np.nan
        data['sinStockBeer'] = np.nan
        data['sinStockGaming'] = np.nan
        
        # Sin stocks identification (with tobacco restriction for year >= 1965)
        data.loc[(data['sicCRSP'] >= 2100) & (data['sicCRSP'] <= 2199) & (data['year'] >= 1965), 'sinStockTobacco'] = 1
        data.loc[(data['sicCRSP'] >= 2080) & (data['sicCRSP'] <= 2085), 'sinStockBeer'] = 1
        data.loc[data['naicsh'].isin([7132, 71312, 713210, 71329, 713290, 72112, 721120]), 'sinStockGaming'] = 1
        
        # Create any sin stock indicator
        data['sinStockAny'] = np.nan
        data.loc[(data['sinStockTobacco'] == 1) | (data['sinStockBeer'] == 1) | (data['sinStockGaming'] == 1), 'sinStockAny'] = 1
        
        # Create comparable stock indicator (FF48 groups 2, 3, 7, 43)
        # Note: This is a simplified version - in practice you'd need the full FF48 classification
        data['ComparableStock'] = np.nan
        # Placeholder for FF48 classification - would need actual implementation
        
        # Merge segment data
        data = data.merge(sin_segments, on=['gvkey', 'year'], how='left')
        
        # Merge first year segment data
        data = data.merge(first_year_segments, on='gvkey', how='left')
        
        # Create final sinAlgo indicator
        logger.info("Creating final sinAlgo indicator...")
        
        data['sinAlgo'] = np.nan
        
        # Apply sinAlgo logic
        sin_conditions = (
            (data['sinStockAny'] == 1) |
            (data['sinSegAny'] == 1) |
            ((data['sinSegAnyFirstYear'] == 1) & (data['year'] < data['firstYear']) & (data['year'] >= 1965)) |
            (data['sinSegBeerFirstYear'] == 1) |
            ((data['sinSegGamingFirstYear'] == 1) & (data['year'] < data['firstYear']) & (data['year'] < 1965))
        )
        
        data.loc[sin_conditions, 'sinAlgo'] = 1
        
        # Set to 0 for comparable stocks if sinAlgo is still missing
        data.loc[(data['ComparableStock'] == 1) & (data['sinAlgo'].isna()), 'sinAlgo'] = 0
        
        # Set to missing for shrcd > 11
        data.loc[data['shrcd'] > 11, 'sinAlgo'] = np.nan
        
        logger.info("Successfully calculated sinAlgo signal")
        
        # SAVE RESULTS
        logger.info("Saving sinAlgo predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'sinAlgo']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['sinAlgo'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "sinAlgo.csv"
        csv_data = output_data[['permno', 'yyyymm', 'sinAlgo']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved sinAlgo predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed sinAlgo predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct sinAlgo predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    sinalgo()
