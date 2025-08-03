"""
ZZ1_grcapx_grcapx1y_grcapx3y Predictor Implementation

This script implements three capital expenditure growth predictors:
- grcapx: Change in capex (two years)
- grcapx1y: Change in capex (one year) 
- grcapx3y: Change in capex (three years)

The script loads Compustat data, calculates firm age, handles missing capex values,
and computes the three growth measures using different time horizons.
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def zz1_grcapx_grcapx1y_grcapx3y():
    """Main function to calculate grcapx, grcapx1y, and grcapx3y predictors."""
    
    # Define file paths
    base_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data")
    compustat_path = base_path / "Intermediate" / "m_aCompustat.csv"
    master_path = base_path / "Intermediate" / "SignalMasterTable.csv"
    output_path = base_path / "Predictors"
    
    # Ensure output directory exists
    output_path.mkdir(parents=True, exist_ok=True)
    
    logger.info("Starting grcapx, grcapx1y, grcapx3y predictor calculation")
    
    try:
        # DATA LOAD
        logger.info("Loading Compustat data")
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'capx', 'ppent', 'at']
        data = pd.read_csv(compustat_path, usecols=required_vars)
        
        # Remove duplicates
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='first')
        
        # Merge with SignalMasterTable for exchcd
        logger.info("Merging with SignalMasterTable")
        master_data = pd.read_csv(master_path, usecols=['permno', 'time_avail_m', 'exchcd'])
        data = data.merge(master_data, on=['permno', 'time_avail_m'], how='left')
        
        # Convert time_avail_m to datetime
        data['time_avail_m'] = pd.to_datetime(data['time_avail_m'])
        
        # Sort by permno and time_avail_m
        data = data.sort_values(['permno', 'time_avail_m'])
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating firm age and growth measures")
        
        # Calculate Firm Age
        data['FirmAge'] = data.groupby('permno').cumcount() + 1
        
        # Remove observations that started with CRSP (July 1926)
        data['tempcrsptime'] = (data['time_avail_m'].dt.year - 1926) * 12 + (data['time_avail_m'].dt.month - 7) + 1
        data.loc[data['tempcrsptime'] == data['FirmAge'], 'FirmAge'] = np.nan
        
        # Handle missing capx values for firms with age >= 24
        data['ppent_lag12'] = data.groupby('permno')['ppent'].shift(12)
        missing_capx_condition = (data['capx'].isna()) & (data['FirmAge'] >= 24)
        data.loc[missing_capx_condition, 'capx'] = data.loc[missing_capx_condition, 'ppent'] - data.loc[missing_capx_condition, 'ppent_lag12']
        
        # Calculate lagged values for growth measures
        data['capx_lag12'] = data.groupby('permno')['capx'].shift(12)
        data['capx_lag24'] = data.groupby('permno')['capx'].shift(24)
        data['capx_lag36'] = data.groupby('permno')['capx'].shift(36)
        
        # Calculate growth measures
        # grcapx: Change in capex (two years)
        data['grcapx'] = (data['capx'] - data['capx_lag24']) / data['capx_lag24']
        
        # grcapx1y: Change in capex (one year)
        data['grcapx1y'] = (data['capx_lag12'] - data['capx_lag24']) / data['capx_lag24']
        
        # grcapx3y: Change in capex (three years)
        data['grcapx3y'] = data['capx'] / (data['capx_lag12'] + data['capx_lag24'] + data['capx_lag36']) * 3
        
        # Prepare output data
        logger.info("Preparing output data")
        
        # For grcapx (predictor)
        grcapx_output = data[['permno', 'time_avail_m', 'grcapx']].copy()
        grcapx_output = grcapx_output.dropna(subset=['grcapx'])
        grcapx_output['yyyymm'] = grcapx_output['time_avail_m'].dt.strftime('%Y%m').astype(int)
        grcapx_output = grcapx_output[['permno', 'yyyymm', 'grcapx']]
        
        # For grcapx1y (placebo)
        grcapx1y_output = data[['permno', 'time_avail_m', 'grcapx1y']].copy()
        grcapx1y_output = grcapx1y_output.dropna(subset=['grcapx1y'])
        grcapx1y_output['yyyymm'] = grcapx1y_output['time_avail_m'].dt.strftime('%Y%m').astype(int)
        grcapx1y_output = grcapx1y_output[['permno', 'yyyymm', 'grcapx1y']]
        
        # For grcapx3y (predictor)
        grcapx3y_output = data[['permno', 'time_avail_m', 'grcapx3y']].copy()
        grcapx3y_output = grcapx3y_output.dropna(subset=['grcapx3y'])
        grcapx3y_output['yyyymm'] = grcapx3y_output['time_avail_m'].dt.strftime('%Y%m').astype(int)
        grcapx3y_output = grcapx3y_output[['permno', 'yyyymm', 'grcapx3y']]
        
        # Save results
        logger.info("Saving results")
        
        # Save grcapx (predictor)
        grcapx_file = output_path / "grcapx.csv"
        grcapx_output.to_csv(grcapx_file, index=False)
        logger.info(f"Saved grcapx predictor to {grcapx_file}")
        logger.info(f"grcapx: {len(grcapx_output)} observations")
        
        # Save grcapx1y (placebo)
        grcapx1y_file = output_path / "grcapx1y.csv"
        grcapx1y_output.to_csv(grcapx1y_file, index=False)
        logger.info(f"Saved grcapx1y placebo to {grcapx1y_file}")
        logger.info(f"grcapx1y: {len(grcapx1y_output)} observations")
        
        # Save grcapx3y (predictor)
        grcapx3y_file = output_path / "grcapx3y.csv"
        grcapx3y_output.to_csv(grcapx3y_file, index=False)
        logger.info(f"Saved grcapx3y predictor to {grcapx3y_file}")
        logger.info(f"grcapx3y: {len(grcapx3y_output)} observations")
        
        logger.info("Successfully completed grcapx, grcapx1y, grcapx3y predictor calculation")
        
    except Exception as e:
        logger.error(f"Error in grcapx, grcapx1y, grcapx3y calculation: {str(e)}")
        raise

if __name__ == "__main__":
    main()
