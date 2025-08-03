"""
Python equivalent of MeanRankRevGrowth.do
Generated from: MeanRankRevGrowth.do

Original Stata file: MeanRankRevGrowth.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def meanrankrevgrowth():
    """
    Python equivalent of MeanRankRevGrowth.do
    
    Constructs the MeanRankRevGrowth predictor signal for average revenue growth rank.
    """
    logger.info("Constructing predictor signal: MeanRankRevGrowth...")
    
    try:
        # DATA LOAD
        # Load Compustat annual data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading Compustat annual data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"Input file not found: {compustat_path}")
            logger.error("Please run the Compustat data download scripts first")
            return False
        
        # Load the required variables
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'revt']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating MeanRankRevGrowth signal...")
        
        # Sort data for time series operations (equivalent to Stata's "xtset permno time_avail_m")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate 12-month lag of revenue (equivalent to Stata's "l12.revt")
        data['revt_lag12'] = data.groupby('permno')['revt'].shift(12)
        
        # Calculate log revenue growth (equivalent to Stata's "gen temp = log(revt) - log(l12.revt)")
        data['temp'] = np.log(data['revt']) - np.log(data['revt_lag12'])
        
        # Calculate ranks within each month (equivalent to Stata's "gsort time_avail_m -temp" and "by time_avail_m: gen tempRank = _n if temp !=.")
        # Sort by time_avail_m and temp (descending) within each month
        data = data.sort_values(['time_avail_m', 'temp'], ascending=[True, False])
        
        # Create ranks within each month, only for non-missing temp
        data['tempRank'] = data.groupby('time_avail_m')['temp'].rank(method='dense', ascending=False)
        
        # Set tempRank to missing where temp is missing
        data.loc[data['temp'].isna(), 'tempRank'] = np.nan
        
        # Sort back for lag calculations
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate lags of tempRank (12, 24, 36, 48, 60 months)
        data['tempRank_lag12'] = data.groupby('permno')['tempRank'].shift(12)
        data['tempRank_lag24'] = data.groupby('permno')['tempRank'].shift(24)
        data['tempRank_lag36'] = data.groupby('permno')['tempRank'].shift(36)
        data['tempRank_lag48'] = data.groupby('permno')['tempRank'].shift(48)
        data['tempRank_lag60'] = data.groupby('permno')['tempRank'].shift(60)
        
        # Calculate weighted average rank (equivalent to Stata's formula)
        # MeanRankRevGrowth = (5*l12.tempRank + 4*l24.tempRank + 3*l36.tempRank + 2*l48.tempRank + l60.tempRank)/15
        data['MeanRankRevGrowth'] = (5 * data['tempRank_lag12'] + 
                                     4 * data['tempRank_lag24'] + 
                                     3 * data['tempRank_lag36'] + 
                                     2 * data['tempRank_lag48'] + 
                                     1 * data['tempRank_lag60']) / 15
        
        logger.info("Successfully calculated MeanRankRevGrowth signal")
        
        # SAVE RESULTS
        logger.info("Saving MeanRankRevGrowth predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'MeanRankRevGrowth']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['MeanRankRevGrowth'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "MeanRankRevGrowth.csv"
        csv_data = output_data[['permno', 'yyyymm', 'MeanRankRevGrowth']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved MeanRankRevGrowth predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed MeanRankRevGrowth predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct MeanRankRevGrowth predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    meanrankrevgrowth()
