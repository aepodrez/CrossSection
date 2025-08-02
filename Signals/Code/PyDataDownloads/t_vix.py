"""
Python equivalent of T_VIX.do
Generated from: T_VIX.do

Original Stata file: T_VIX.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime
import fredapi

logger = logging.getLogger(__name__)

def t_vix():
    """
    Python equivalent of T_VIX.do
    
    Downloads and processes VIX data from FRED
    """
    logger.info("Downloading VIX data from FRED...")
    
    try:
        # Use global FRED connection from master.py
        from master import fred
        if fred is None:
            logger.error("FRED connection not available. Please run master.py")
            return False
        
        # FRED series IDs from original Stata file
        # VIXCLS: VIX (CBOE Volatility Index)
        # VXOCLS: VXO (CBOE S&P 100 Volatility Index) - discontinued in 2021
        series_ids = ['VIXCLS', 'VXOCLS']
        
        logger.info(f"Downloading VIX data for series: {series_ids}")
        
        # Download data from FRED
        data_list = []
        for series_id in series_ids:
            try:
                series_data = fred.get_series(series_id)
                series_data = series_data.reset_index()
                series_data.columns = ['time_d', series_id]
                data_list.append(series_data)
                logger.info(f"Downloaded {len(series_data)} records for {series_id}")
            except Exception as e:
                logger.warning(f"Failed to download {series_id}: {e}")
                # Create empty DataFrame for missing series
                empty_df = pd.DataFrame(columns=['time_d', series_id])
                data_list.append(empty_df)
        
        # Merge the series data
        if len(data_list) == 2:
            data = data_list[0].merge(data_list[1], on='time_d', how='outer')
        else:
            data = data_list[0]
        
        logger.info(f"Combined data has {len(data)} records")
        
        # Create vix column (equivalent to Stata's "gen vix = VXOCLS")
        data['vix'] = data['VXOCLS']
        
        # Replace vix with VIXCLS where VXOCLS is missing and date >= 2021-09-23
        # Equivalent to Stata's "replace vix = VIXCLS if mi(VXOCLS) & daten >= dmy(23, 9, 2021)"
        cutoff_date = pd.to_datetime('2021-09-23')
        mask = (data['VXOCLS'].isna()) & (data['time_d'] >= cutoff_date)
        data.loc[mask, 'vix'] = data.loc[mask, 'VIXCLS']
        
        logger.info(f"Applied VIXCLS replacement for {mask.sum()} records after 2021-09-23")
        
        # Drop unnecessary columns (equivalent to Stata's "drop datestr VIXCLS VXOCLS")
        columns_to_drop = ['VIXCLS', 'VXOCLS']
        for col in columns_to_drop:
            if col in data.columns:
                data = data.drop(columns=[col])
        
        # Calculate dVIX (VIX change) - equivalent to Stata's lag calculation
        data = data.sort_values('time_d')
        data['dVIX'] = data['vix'].diff()
        
        logger.info(f"Calculated VIX changes (dVIX) for {len(data)} records")
        
        # Save to intermediate file
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/d_vix.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved VIX data to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/d_vix.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log some summary statistics
        logger.info("VIX data summary:")
        logger.info(f"  Total records: {len(data)}")
        logger.info(f"  Time range: {data['time_d'].min()} to {data['time_d'].max()}")
        
        # Check data availability
        if 'vix' in data.columns:
            non_missing_vix = data['vix'].notna().sum()
            missing_vix = data['vix'].isna().sum()
            logger.info(f"  VIX availability: {non_missing_vix} non-missing, {missing_vix} missing")
            
            # VIX summary statistics
            vix_data = data['vix'].dropna()
            if len(vix_data) > 0:
                mean_val = vix_data.mean()
                std_val = vix_data.std()
                min_val = vix_data.min()
                max_val = vix_data.max()
                logger.info(f"  VIX statistics: mean={mean_val:.2f}, std={std_val:.2f}, range=[{min_val:.2f}, {max_val:.2f}]")
                
                # VIX level analysis
                logger.info("  VIX level analysis:")
                logger.info(f"    Average VIX level: {mean_val:.2f}")
                logger.info(f"    VIX volatility: {std_val:.2f}")
                logger.info(f"    Historical range: {min_val:.2f} to {max_val:.2f}")
                
                # VIX regime analysis
                low_vol = (vix_data < 15).sum()
                normal_vol = ((vix_data >= 15) & (vix_data < 25)).sum()
                high_vol = ((vix_data >= 25) & (vix_data < 35)).sum()
                extreme_vol = (vix_data >= 35).sum()
                
                total_obs = len(vix_data)
                logger.info(f"    Low volatility (<15): {low_vol} ({low_vol/total_obs*100:.1f}%)")
                logger.info(f"    Normal volatility (15-25): {normal_vol} ({normal_vol/total_obs*100:.1f}%)")
                logger.info(f"    High volatility (25-35): {high_vol} ({high_vol/total_obs*100:.1f}%)")
                logger.info(f"    Extreme volatility (≥35): {extreme_vol} ({extreme_vol/total_obs*100:.1f}%)")
        
        # dVIX analysis
        if 'dVIX' in data.columns:
            non_missing_dvix = data['dVIX'].notna().sum()
            missing_dvix = data['dVIX'].isna().sum()
            logger.info(f"  dVIX availability: {non_missing_dvix} non-missing, {missing_dvix} missing")
            
            # dVIX summary statistics
            dvix_data = data['dVIX'].dropna()
            if len(dvix_data) > 0:
                mean_val = dvix_data.mean()
                std_val = dvix_data.std()
                min_val = dvix_data.min()
                max_val = dvix_data.max()
                logger.info(f"  dVIX statistics: mean={mean_val:.4f}, std={std_val:.4f}, range=[{min_val:.4f}, {max_val:.4f}]")
                
                # dVIX analysis
                logger.info("  dVIX analysis:")
                positive_changes = (dvix_data > 0).sum()
                negative_changes = (dvix_data < 0).sum()
                zero_changes = (dvix_data == 0).sum()
                
                total_obs = len(dvix_data)
                logger.info(f"    Positive changes: {positive_changes} ({positive_changes/total_obs*100:.1f}%)")
                logger.info(f"    Negative changes: {negative_changes} ({negative_changes/total_obs*100:.1f}%)")
                logger.info(f"    Zero changes: {zero_changes} ({zero_changes/total_obs*100:.1f}%)")
        
        # Daily frequency analysis
        logger.info("  Daily frequency analysis:")
        logger.info(f"    Total days: {len(data)}")
        logger.info(f"    Years covered: {(data['time_d'].max() - data['time_d'].min()).days / 365.25:.1f}")
        
        # Check for any gaps in daily data
        expected_days = pd.date_range(
            start=data['time_d'].min(),
            end=data['time_d'].max(),
            freq='D'
        )
        actual_days = data['time_d'].sort_values()
        missing_days = set(expected_days) - set(actual_days)
        
        if missing_days:
            logger.warning(f"    Missing trading days: {len(missing_days)}")
            logger.warning(f"    First few missing days: {sorted(list(missing_days))[:5]}")
        else:
            logger.info("    No missing trading days detected")
        
        # VIX interpretation
        logger.info("  VIX interpretation:")
        logger.info("    VIX: CBOE Volatility Index (Fear Gauge)")
        logger.info("    - Measures market volatility expectations")
        logger.info("    - Higher VIX = Higher market fear/uncertainty")
        logger.info("    - Lower VIX = Lower market fear/uncertainty")
        logger.info("    - dVIX: Daily change in VIX (volatility surprise)")
        logger.info("    - Used in volatility timing and risk management")
        
        logger.info("Successfully downloaded and processed VIX data")
        logger.info("Note: Daily VIX data for volatility analysis and risk management")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download VIX data: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the download function
    t_vix()
