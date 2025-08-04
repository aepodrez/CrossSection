"""
Python equivalent of EquityDuration.do
Generated from: EquityDuration.do

Original Stata file: EquityDuration.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def equityduration():
    """
    Python equivalent of EquityDuration.do
    
    Constructs the EquityDuration predictor signal for equity duration.
    """
    logger.info("Constructing predictor signal: EquityDuration...")
    
    try:
        # DATA LOAD
        # Load Compustat annual data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/a_aCompustat.csv")
        
        logger.info(f"Loading Compustat annual data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"Input file not found: {compustat_path}")
            logger.error("Please run the Compustat data download scripts first")
            return False
        
        # Load the required variables
        required_vars = ['gvkey', 'permno', 'time_avail_m', 'fyear', 'datadate', 'ceq', 'ib', 'sale', 'prcc_f', 'csho']
        
        data = pd.read_csv(compustat_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating EquityDuration signal...")
        
        # Sort data for lag calculations (equivalent to Stata's "xtset gvkey fyear")
        data = data.sort_values(['gvkey', 'fyear'])
        
        # Calculate lagged values
        data['ceq_lag'] = data.groupby('gvkey')['ceq'].shift(1)
        data['sale_lag'] = data.groupby('gvkey')['sale'].shift(1)
        
        # Compute ROE, book equity growth, and cash distributions to equity
        # (equivalent to Stata's calculations)
        data['tempRoE'] = data['ib'] / data['ceq_lag']  # ROE
        data['temp_g_eq'] = data['sale'] / data['sale_lag'] - 1  # Growth in equity
        data['tempCD'] = data['ceq_lag'] * (data['tempRoE'] - data['temp_g_eq'])  # Cash distribution to equity
        
        # Project variables forward (years 1-10)
        # Year 1
        data['tempRoE1'] = 0.57 * data['tempRoE'] + 0.12 * (1 - 0.57)
        data['temp_g_eq1'] = 0.24 * data['temp_g_eq'] + 0.06 * (1 - 0.24)
        data['tempBV1'] = data['ceq'] * (1 + data['temp_g_eq1'])
        data['tempCD1'] = data['ceq'] - data['tempBV1'] + data['ceq'] * data['tempRoE1']
        
        # Years 2-10
        for t in range(2, 11):
            j = t - 1
            data[f'tempRoE{t}'] = 0.57 * data[f'tempRoE{j}'] + 0.12 * (1 - 0.57)
            data[f'temp_g_eq{t}'] = 0.24 * data[f'temp_g_eq{j}'] + 0.06 * (1 - 0.24)
            data[f'tempBV{t}'] = data[f'tempBV{j}'] * (1 + data[f'temp_g_eq{t}'])
            data[f'tempCD{t}'] = data[f'tempBV{j}'] - data[f'tempBV{t}'] + data[f'tempBV{j}'] * data[f'tempRoE{t}']
        
        # Calculate MD_Part1 and PV_Part1
        md_terms = []
        pv_terms = []
        for t in range(1, 11):
            md_terms.append(f"{t}*tempCD{t}/(1+0.12)**{t}")
            pv_terms.append(f"tempCD{t}/(1+0.12)**{t}")
        
        # Evaluate the expressions
        data['MD_Part1'] = 0
        data['PV_Part1'] = 0
        for t in range(1, 11):
            data['MD_Part1'] += t * data[f'tempCD{t}'] / ((1 + 0.12) ** t)
            data['PV_Part1'] += data[f'tempCD{t}'] / ((1 + 0.12) ** t)
        
        # Compute equity duration
        data['tempME'] = data['prcc_f'] * data['csho']  # Market equity
        
        # Calculate equity duration (equivalent to Stata's formula)
        data['tempED'] = (data['MD_Part1'] / data['tempME'] + 
                         (10 + (1 + 0.12) / 0.12) * (1 - data['PV_Part1'] / data['tempME']))
        
        data['EquityDuration'] = data['tempED']
        
        logger.info("Successfully calculated EquityDuration signal")
        
        # Expand to monthly (equivalent to Stata's expand logic)
        logger.info("Expanding annual data to monthly...")
        
        # Create monthly observations
        monthly_data = []
        for _, row in data.iterrows():
            for month in range(12):
                new_row = row.copy()
                new_row['time_avail_m'] = row['time_avail_m'] + pd.DateOffset(months=month)
                monthly_data.append(new_row)
        
        data = pd.DataFrame(monthly_data)
        
        # Remove duplicates (equivalent to Stata's "bysort permno time_avail_m: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno', 'time_avail_m'], keep='last')
        
        logger.info(f"After expanding to monthly: {len(data)} observations")
        
        # SAVE RESULTS
        logger.info("Saving EquityDuration predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'EquityDuration']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['EquityDuration'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "EquityDuration.csv"
        csv_data = output_data[['permno', 'yyyymm', 'EquityDuration']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved EquityDuration predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed EquityDuration predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct EquityDuration predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    equityduration()
