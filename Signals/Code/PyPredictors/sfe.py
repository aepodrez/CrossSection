"""
Python equivalent of SFE.do
Generated from: SFE.do

Original Stata file: SFE.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def sfe():
    """
    Python equivalent of SFE.do
    
    Constructs the sfe predictor signal for earnings forecast.
    """
    logger.info("Constructing predictor signal: sfe...")
    
    try:
        # Prep IBES data
        logger.info("Preparing IBES data...")
        
        # Load IBES EPS data
        ibes_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/IBES_EPS_Unadj.csv")
        
        logger.info(f"Loading IBES EPS data from: {ibes_path}")
        
        if not ibes_path.exists():
            logger.error(f"IBES_EPS_Unadj not found: {ibes_path}")
            logger.error("Please run the IBES EPS data creation script first")
            return False
        
        ibes_data = pd.read_csv(ibes_path)
        logger.info(f"Successfully loaded {len(ibes_data)} IBES records")
        
        # Keep only fpi == "1" and March forecasts (equivalent to Stata's "keep if fpi == '1' & month(statpers) == 3")
        ibes_data = ibes_data[ibes_data['fpi'] == '1']
        ibes_data['statpers'] = pd.to_datetime(ibes_data['statpers'])
        ibes_data = ibes_data[ibes_data['statpers'].dt.month == 3]
        logger.info(f"After keeping fpi == '1' and March forecasts: {len(ibes_data)} records")
        
        # Keep only forecasts past June (equivalent to Stata's "keep if fpedats != . & fpedats > statpers + 90")
        ibes_data = ibes_data[(ibes_data['fpedats'].notna()) & 
                              (ibes_data['fpedats'] > ibes_data['statpers'] + pd.Timedelta(days=90))]
        logger.info(f"After keeping forecasts past June: {len(ibes_data)} records")
        
        # Create prc_time for merge with December stock price (equivalent to Stata's "gen prc_time = time_avail_m - 3")
        ibes_data['time_avail_m'] = pd.to_datetime(ibes_data['time_avail_m'])
        ibes_data['prc_time'] = ibes_data['time_avail_m'] - pd.DateOffset(months=3)
        
        # Save temporary file (equivalent to Stata's "save '$pathtemp/temp', replace")
        temp_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/temp_sfe.csv")
        ibes_data.to_csv(temp_path, index=False)
        logger.info(f"Saved temporary IBES data to: {temp_path}")
        
        # Merge with CRSP/Compustat
        logger.info("Loading main data sources...")
        
        # Load SignalMasterTable
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        master_data = pd.read_csv(master_path, usecols=['permno', 'time_avail_m', 'tickerIBES', 'prc', 'mve_c'])
        logger.info(f"Successfully loaded {len(master_data)} master records")
        
        # Rename time_avail_m to prc_time for merge (equivalent to Stata's "rename time_avail_m prc_time")
        master_data = master_data.rename(columns={'time_avail_m': 'prc_time'})
        
        # Merge with temporary IBES data
        data = master_data.merge(ibes_data[['tickerIBES', 'prc_time', 'medest']], 
                                on=['tickerIBES', 'prc_time'], how='inner')
        logger.info(f"After merging with IBES data: {len(data)} records")
        
        # Load annual Compustat data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading annual Compustat data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"m_aCompustat not found: {compustat_path}")
            logger.error("Please run the annual Compustat data creation script first")
            return False
        
        compustat_data = pd.read_csv(compustat_path, usecols=['permno', 'time_avail_m', 'datadate'])
        
        # Merge with Compustat data
        data = data.merge(compustat_data, on=['permno', 'time_avail_m'], how='inner')
        logger.info(f"After merging with Compustat data: {len(data)} records")
        
        # Keep only December fiscal year ends (equivalent to Stata's "keep if month(datadate) == 12")
        data['datadate'] = pd.to_datetime(data['datadate'])
        data = data[data['datadate'].dt.month == 12]
        logger.info(f"After keeping December fiscal year ends: {len(data)} records")
        
        # Lower analyst coverage only (equivalent to Stata's "egen tempcoverage = fastxtile(numest), by(time_avail_m) n(2)" and "keep if tempcoverage == 1")
        data['tempcoverage'] = data.groupby('time_avail_m')['numest'].transform(
            lambda x: pd.qcut(x, q=2, labels=False, duplicates='drop') + 1
        )
        data = data[data['tempcoverage'] == 1]
        logger.info(f"After keeping lower analyst coverage: {len(data)} records")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating sfe signal...")
        
        # Calculate sfe (equivalent to Stata's "gen sfe = medest/abs(prc)")
        data['sfe'] = data['medest'] / np.abs(data['prc'])
        
        # Keep only required variables (equivalent to Stata's "keep permno time_avail_m sfe")
        data = data[['permno', 'time_avail_m', 'sfe']]
        
        # Hold for one year (equivalent to Stata's expand logic)
        logger.info("Expanding to monthly frequency...")
        
        monthly_data = []
        for _, row in data.iterrows():
            for month_offset in range(12):
                new_row = row.copy()
                new_row['time_avail_m'] = row['time_avail_m'] + pd.DateOffset(months=month_offset)
                monthly_data.append(new_row)
        
        data = pd.DataFrame(monthly_data)
        logger.info(f"After expanding to monthly: {len(data)} records")
        
        logger.info("Successfully calculated sfe signal")
        
        # Clean up temporary file
        if temp_path.exists():
            temp_path.unlink()
            logger.info("Cleaned up temporary IBES file")
        
        # SAVE RESULTS
        logger.info("Saving sfe predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data.copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['sfe'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        # Convert time_avail_m to datetime if needed for year/month extraction
        if not pd.api.types.is_datetime64_any_dtype(output_data['time_avail_m']):
            output_data['time_avail_m'] = pd.to_datetime(output_data['time_avail_m'])
        
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "sfe.csv"
        csv_data = output_data[['permno', 'yyyymm', 'sfe']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved sfe predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed sfe predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct sfe predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    sfe()
