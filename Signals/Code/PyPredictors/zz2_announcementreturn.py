"""
ZZ2_AnnouncementReturn Predictor Implementation

This script implements the AnnouncementReturn predictor:
- AnnouncementReturn: Earnings announcement return

The script calculates abnormal returns around earnings announcement dates,
aggregating returns over announcement windows and filling in missing months
with recent announcement returns.
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def zz2_announcementreturn():
    """Main function to calculate AnnouncementReturn predictor."""
    
    # Define file paths
    base_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data")
    ccm_path = base_path / "Intermediate" / "CCMLinkingTable.csv"
    compustat_path = base_path / "Intermediate" / "m_QCompustat.csv"
    daily_crsp_path = base_path / "Intermediate" / "dailyCRSP.csv"
    daily_ff_path = base_path / "Intermediate" / "dailyFF.csv"
    temp_cw_path = base_path / "Temp" / "tempCW.csv"
    temp_ann_dates_path = base_path / "Temp" / "tempAnnDats.csv"
    output_path = base_path / "Predictors"
    
    # Ensure directories exist
    temp_cw_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.mkdir(parents=True, exist_ok=True)
    
    logger.info("Starting AnnouncementReturn predictor calculation")
    
    try:
        # Prepare crosswalk CRSP-CS
        logger.info("Preparing CRSP-Compustat crosswalk")
        ccm_data = pd.read_csv(ccm_path, usecols=['gvkey', 'permno', 'timeLinkStart_d', 'timeLinkEnd_d'])
        ccm_data.to_csv(temp_cw_path, index=False)
        
        # Prepare earnings announcement dates
        logger.info("Preparing earnings announcement dates")
        compustat_data = pd.read_csv(compustat_path, usecols=['gvkey', 'rdq'])
        compustat_data = compustat_data.dropna(subset=['rdq'])
        compustat_data = compustat_data.rename(columns={'rdq': 'time_ann_d'})
        compustat_data = compustat_data.drop_duplicates()
        compustat_data.to_csv(temp_ann_dates_path, index=False)
        
        # Load daily CRSP data
        logger.info("Loading daily CRSP data")
        crsp_data = pd.read_csv(daily_crsp_path, usecols=['permno', 'time_d', 'ret'])
        
        # Match announcement dates
        logger.info("Matching announcement dates")
        crsp_data = crsp_data.merge(ccm_data, on='permno', how='inner')
        
        # Convert dates to datetime
        crsp_data['time_d'] = pd.to_datetime(crsp_data['time_d'])
        crsp_data['timeLinkStart_d'] = pd.to_datetime(crsp_data['timeLinkStart_d'])
        crsp_data['timeLinkEnd_d'] = pd.to_datetime(crsp_data['timeLinkEnd_d'])
        
        # Use only if data date is within the validity period of the link
        crsp_data['temp'] = (crsp_data['timeLinkStart_d'] <= crsp_data['time_d']) & (crsp_data['time_d'] <= crsp_data['timeLinkEnd_d'])
        crsp_data = crsp_data[crsp_data['temp'] == True]
        crsp_data = crsp_data.drop(['temp', 'timeLinkStart_d', 'timeLinkEnd_d'], axis=1)
        
        # Rename time_d to time_ann_d for merging
        crsp_data = crsp_data.rename(columns={'time_d': 'time_ann_d'})
        crsp_data['gvkey'] = pd.to_numeric(crsp_data['gvkey'], errors='coerce')
        
        # Merge with announcement dates
        compustat_data['time_ann_d'] = pd.to_datetime(compustat_data['time_ann_d'])
        crsp_data = crsp_data.merge(compustat_data, on=['gvkey', 'time_ann_d'], how='inner')
        crsp_data['anndat'] = True
        crsp_data = crsp_data.drop('gvkey', axis=1)
        
        # Merge market return
        crsp_data = crsp_data.rename(columns={'time_ann_d': 'time_d'})
        ff_data = pd.read_csv(daily_ff_path, usecols=['time_d', 'mktrf', 'rf'])
        ff_data['time_d'] = pd.to_datetime(ff_data['time_d'])
        crsp_data = crsp_data.merge(ff_data, on='time_d', how='inner')
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating announcement returns")
        
        # Calculate abnormal returns
        crsp_data['AnnouncementReturn'] = crsp_data['ret'] - (crsp_data['mktrf'] + crsp_data['rf'])
        
        # Create time index for each permno
        crsp_data = crsp_data.sort_values(['permno', 'time_d'])
        crsp_data['time_temp'] = crsp_data.groupby('permno').cumcount() + 1
        
        # Create announcement windows
        logger.info("Creating announcement windows")
        crsp_data['time_ann_d'] = np.nan
        
        # Set time_ann_d for announcement dates and surrounding days
        crsp_data.loc[crsp_data['anndat'] == True, 'time_ann_d'] = crsp_data.loc[crsp_data['anndat'] == True, 'time_temp']
        
        # Forward shifts for days after announcement
        crsp_data['anndat_f1'] = crsp_data.groupby('permno')['anndat'].shift(-1)
        crsp_data['anndat_f2'] = crsp_data.groupby('permno')['anndat'].shift(-2)
        crsp_data.loc[crsp_data['anndat_f1'] == True, 'time_ann_d'] = crsp_data.loc[crsp_data['anndat_f1'] == True, 'time_temp'] + 1
        crsp_data.loc[crsp_data['anndat_f2'] == True, 'time_ann_d'] = crsp_data.loc[crsp_data['anndat_f2'] == True, 'time_temp'] + 2
        
        # Backward shifts for days before announcement
        crsp_data['anndat_l1'] = crsp_data.groupby('permno')['anndat'].shift(1)
        crsp_data.loc[crsp_data['anndat_l1'] == True, 'time_ann_d'] = crsp_data.loc[crsp_data['anndat_l1'] == True, 'time_temp'] - 1
        
        # Drop observations outside announcement windows
        crsp_data = crsp_data.dropna(subset=['time_ann_d'])
        
        # Aggregate returns over announcement windows
        logger.info("Aggregating returns over announcement windows")
        announcement_data = crsp_data.groupby(['permno', 'time_ann_d']).agg({
            'AnnouncementReturn': 'sum',
            'time_d': 'max'
        }).reset_index()
        
        # Convert daily date to monthly date
        # Convert time_d to datetime if needed for period conversion
        if not pd.api.types.is_datetime64_any_dtype(announcement_data['time_d']):
            announcement_data['time_d'] = pd.to_datetime(announcement_data['time_d'])
        
        announcement_data['time_avail_m'] = announcement_data['time_d'].dt.to_period('M').dt.to_timestamp()
        announcement_data = announcement_data[['permno', 'time_avail_m', 'AnnouncementReturn']]
        announcement_data = announcement_data.dropna(subset=['time_avail_m'])
        
        # Keep only the most recent announcement return for each permno-month
        announcement_data = announcement_data.sort_values(['permno', 'time_avail_m'])
        announcement_data = announcement_data.groupby(['permno', 'time_avail_m']).last().reset_index()
        
        # Fill in missing months with recent announcement returns
        logger.info("Filling in missing months")
        
        # Create complete time series for each permno
        all_months = pd.date_range(
            announcement_data['time_avail_m'].min(),
            announcement_data['time_avail_m'].max(),
            freq='ME'
        )
        
        permno_list = announcement_data['permno'].unique()
        complete_data = pd.DataFrame([
            {'permno': permno, 'time_avail_m': month}
            for permno in permno_list
            for month in all_months
        ])
        
        # Merge with announcement data
        complete_data = complete_data.merge(announcement_data, on=['permno', 'time_avail_m'], how='left')
        
        # Sort and fill missing values with recent announcement returns
        complete_data = complete_data.sort_values(['permno', 'time_avail_m'])
        
        # Forward fill up to 6 months
        for i in range(1, 7):
            complete_data[f'AnnouncementReturn_lag{i}'] = complete_data.groupby('permno')['AnnouncementReturn'].shift(i)
            complete_data['AnnouncementReturn'] = complete_data['AnnouncementReturn'].fillna(complete_data[f'AnnouncementReturn_lag{i}'])
        
        # Drop temporary columns and missing values
        lag_cols = [col for col in complete_data.columns if 'lag' in col]
        complete_data = complete_data.drop(lag_cols, axis=1)
        complete_data = complete_data.dropna(subset=['AnnouncementReturn'])
        
        # Prepare output data
        logger.info("Preparing output data")
        
        # For AnnouncementReturn
        announcementreturn_output = complete_data[['permno', 'time_avail_m', 'AnnouncementReturn']].copy()
        # Convert time_avail_m to datetime if needed for strftime
        if not pd.api.types.is_datetime64_any_dtype(announcementreturn_output['time_avail_m']):
            announcementreturn_output['time_avail_m'] = pd.to_datetime(announcementreturn_output['time_avail_m'])
        
        announcementreturn_output['yyyymm'] = announcementreturn_output['time_avail_m'].dt.strftime('%Y%m').astype(int)
        announcementreturn_output = announcementreturn_output[['permno', 'yyyymm', 'AnnouncementReturn']]
        
        # Save results
        logger.info("Saving results")
        
        # Save AnnouncementReturn
        announcementreturn_file = output_path / "AnnouncementReturn.csv"
        announcementreturn_output.to_csv(announcementreturn_file, index=False)
        logger.info(f"Saved AnnouncementReturn predictor to {announcementreturn_file}")
        logger.info(f"AnnouncementReturn: {len(announcementreturn_output)} observations")
        
        # Clean up temporary files
        if temp_cw_path.exists():
            temp_cw_path.unlink()
        if temp_ann_dates_path.exists():
            temp_ann_dates_path.unlink()
        
        logger.info("Successfully completed AnnouncementReturn predictor calculation")
        return True
        
    except Exception as e:
        logger.error(f"Error in AnnouncementReturn calculation: {str(e)}")
        raise

if __name__ == "__main__":
    main()
