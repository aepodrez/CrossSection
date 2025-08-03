"""
Python equivalent of GrSaleToGrOverhead.do
Generated from: GrSaleToGrOverhead.do

Original Stata file: GrSaleToGrOverhead.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def grsaletogroverhead():
    """
    Python equivalent of GrSaleToGrOverhead.do
    
    Constructs the GrSaleToGrOverhead predictor signal for sales growth over overhead growth.
    """
    logger.info("Constructing predictor signal: GrSaleToGrOverhead...")
    
    try:
        # DATA LOAD
        # Load Compustat annual data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading Compustat annual data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"Compustat annual file not found: {compustat_path}")
            logger.error("Please run the Compustat data download scripts first")
            return False
        
        # Load the required variables
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'sale', 'xsga']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating GrSaleToGrOverhead signal...")
        
        # Sort data for time series operations (equivalent to Stata's "xtset permno time_avail_m")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate lags for sale and xsga
        data['sale_lag12'] = data.groupby('permno')['sale'].shift(12)
        data['sale_lag24'] = data.groupby('permno')['sale'].shift(24)
        data['xsga_lag12'] = data.groupby('permno')['xsga'].shift(12)
        data['xsga_lag24'] = data.groupby('permno')['xsga'].shift(24)
        
        # Calculate GrSaleToGrOverhead using the primary formula
        # ((sale- (.5*(l12.sale + l24.sale)))/(.5*(l12.sale + l24.sale))) - ((xsga- (.5*(l12.xsga+l24.xsga))) /(.5*(l12.xsga+l24.xsga)))
        sale_avg = 0.5 * (data['sale_lag12'] + data['sale_lag24'])
        xsga_avg = 0.5 * (data['xsga_lag12'] + data['xsga_lag24'])
        
        sale_growth = (data['sale'] - sale_avg) / sale_avg
        xsga_growth = (data['xsga'] - xsga_avg) / xsga_avg
        
        data['GrSaleToGrOverhead'] = sale_growth - xsga_growth
        
        # Apply fallback formula for missing values (equivalent to Stata's "replace GrSaleToGrOverhead = ((sale-l12.sale)/l12.sale)-( (xsga-l12.xsga) /l12.xsga ) if mi(GrSaleToGrOverhead)")
        fallback_sale_growth = (data['sale'] - data['sale_lag12']) / data['sale_lag12']
        fallback_xsga_growth = (data['xsga'] - data['xsga_lag12']) / data['xsga_lag12']
        fallback_gr_sale_to_gr_overhead = fallback_sale_growth - fallback_xsga_growth
        
        # Replace missing values with fallback calculation
        data.loc[data['GrSaleToGrOverhead'].isna(), 'GrSaleToGrOverhead'] = fallback_gr_sale_to_gr_overhead[data['GrSaleToGrOverhead'].isna()]
        
        logger.info("Successfully calculated GrSaleToGrOverhead signal")
        
        # SAVE RESULTS
        logger.info("Saving GrSaleToGrOverhead predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'GrSaleToGrOverhead']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['GrSaleToGrOverhead'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "GrSaleToGrOverhead.csv"
        csv_data = output_data[['permno', 'yyyymm', 'GrSaleToGrOverhead']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved GrSaleToGrOverhead predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed GrSaleToGrOverhead predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct GrSaleToGrOverhead predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    grsaletogroverhead()
