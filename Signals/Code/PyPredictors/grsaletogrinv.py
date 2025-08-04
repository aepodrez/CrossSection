"""
Python equivalent of GrSaleToGrInv.do
Generated from: GrSaleToGrInv.do

Original Stata file: GrSaleToGrInv.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def grsaletogrinv():
    """
    Python equivalent of GrSaleToGrInv.do
    
    Constructs the GrSaleToGrInv predictor signal for sales growth over inventory growth.
    """
    logger.info("Constructing predictor signal: GrSaleToGrInv...")
    
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
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'sale', 'invt']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        logger.info(f"After removing duplicates: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating GrSaleToGrInv signal...")
        
        # Sort data for time series operations (equivalent to Stata's "xtset permno time_avail_m")
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # Calculate lags for sale and invt
        data['sale_lag12'] = data.groupby('permno')['sale'].shift(12)
        data['sale_lag24'] = data.groupby('permno')['sale'].shift(24)
        data['invt_lag12'] = data.groupby('permno')['invt'].shift(12)
        data['invt_lag24'] = data.groupby('permno')['invt'].shift(24)
        
        # Calculate GrSaleToGrInv using the primary formula
        # ((sale- (.5*(l12.sale + l24.sale)))/(.5*(l12.sale + l24.sale))) - ((invt- (.5*(l12.invt + l24.invt)))/(.5*(l12.invt + l24.invt)))
        sale_avg = 0.5 * (data['sale_lag12'] + data['sale_lag24'])
        invt_avg = 0.5 * (data['invt_lag12'] + data['invt_lag24'])
        
        sale_growth = (data['sale'] - sale_avg) / sale_avg
        invt_growth = (data['invt'] - invt_avg) / invt_avg
        
        data['GrSaleToGrInv'] = sale_growth - invt_growth
        
        # Apply fallback formula for missing values (equivalent to Stata's "replace GrSaleToGrInv = ((sale-l12.sale)/l12.sale)-((invt-l12.invt)/l12.invt) if mi(GrSaleToGrInv)")
        fallback_sale_growth = (data['sale'] - data['sale_lag12']) / data['sale_lag12']
        fallback_invt_growth = (data['invt'] - data['invt_lag12']) / data['invt_lag12']
        fallback_gr_sale_to_gr_inv = fallback_sale_growth - fallback_invt_growth
        
        # Replace missing values with fallback calculation
        data.loc[data['GrSaleToGrInv'].isna(), 'GrSaleToGrInv'] = fallback_gr_sale_to_gr_inv[data['GrSaleToGrInv'].isna()]
        
        logger.info("Successfully calculated GrSaleToGrInv signal")
        
        # SAVE RESULTS
        logger.info("Saving GrSaleToGrInv predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'GrSaleToGrInv']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['GrSaleToGrInv'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "GrSaleToGrInv.csv"
        csv_data = output_data[['permno', 'yyyymm', 'GrSaleToGrInv']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved GrSaleToGrInv predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed GrSaleToGrInv predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct GrSaleToGrInv predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    grsaletogrinv()
