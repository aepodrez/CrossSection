"""
Python equivalent of tang.do
Generated from: tang.do

Original Stata file: tang.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def tang():
    """
    Python equivalent of tang.do
    
    Constructs the tang predictor signal for tangibility.
    """
    logger.info("Constructing predictor signal: tang...")
    
    try:
        # DATA LOAD
        # Load annual Compustat data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading annual Compustat data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"m_aCompustat not found: {compustat_path}")
            logger.error("Please run the annual Compustat data creation script first")
            return False
        
        # Load required variables
        required_vars = ['permno', 'time_avail_m', 'che', 'rect', 'invt', 'ppegt', 'at', 'sic']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating tang signal...")
        
        # Convert SIC to numeric (equivalent to Stata's "destring sic, replace")
        data['sic'] = pd.to_numeric(data['sic'], errors='coerce')
        
        # Keep only manufacturing firms (equivalent to Stata's "drop if sic < 2000 | sic > 3999")
        data = data[(data['sic'] >= 2000) & (data['sic'] <= 3999)]
        logger.info(f"After filtering for manufacturing firms: {len(data)} records")
        
        # Create size deciles (equivalent to Stata's "egen tempFC = fastxtile(at), n(10) by(time_avail_m)")
        data['tempFC'] = data.groupby('time_avail_m')['at'].transform(
            lambda x: pd.qcut(x, q=10, labels=False, duplicates='drop') + 1
        )
        
        # Create financial constraint indicator (equivalent to Stata's logic)
        data['FC'] = np.nan
        data.loc[data['tempFC'] <= 3, 'FC'] = 1  # Lower three deciles are financially constrained
        data.loc[(data['tempFC'] >= 8) & (data['tempFC'].notna()), 'FC'] = 0  # Upper deciles are not constrained
        
        # Calculate tangibility (equivalent to Stata's "gen tang = (che + .715*rect + .547*invt + .535*ppegt)/at")
        data['tang'] = (data['che'] + 0.715 * data['rect'] + 0.547 * data['invt'] + 0.535 * data['ppegt']) / data['at']
        
        logger.info("Successfully calculated tang signal")
        
        # SAVE RESULTS
        logger.info("Saving tang predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'tang']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['tang'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "tang.csv"
        csv_data = output_data[['permno', 'yyyymm', 'tang']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved tang predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed tang predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct tang predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    tang()
