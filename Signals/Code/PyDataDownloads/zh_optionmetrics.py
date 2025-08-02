"""
Python equivalent of ZH_OptionMetrics.do
Generated from: ZH_OptionMetrics.do

Original Stata file: ZH_OptionMetrics.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def zh_optionmetrics():
    """
    Python equivalent of ZH_OptionMetrics.do
    
    Processes multiple OptionMetrics datasets from pre-processed CSV files
    Note: Requires PrepScripts/master.sh to be run first to generate the input CSV files
    """
    logger.info("Processing OptionMetrics data...")
    
    try:
        # Define input file paths (equivalent to Stata's "$pathDataPrep/...")
        prep_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Code/PrepScripts")
        
        input_files = {
            'volume': prep_path / "OptionMetricsVolume.csv",
            'volsurf': prep_path / "OptionMetricsVolSurf.csv", 
            'xzz': prep_path / "OptionMetricsXZZ.csv",
            'bali_hovak': prep_path / "bali_hovak_imp_vol.csv"
        }
        
        # Check if all input files exist
        missing_files = [name for name, path in input_files.items() if not path.exists()]
        if missing_files:
            logger.error(f"Missing input files: {missing_files}")
            logger.error("Please run PrepScripts/master.sh first to generate the input CSV files")
            return False
        
        logger.info("All OptionMetrics input files found")
        
        # Process OptionMetrics Volume data
        logger.info("=== Processing OptionMetrics Volume Data ===")
        volume_data = process_optionmetrics_volume(input_files['volume'])
        if volume_data is None:
            return False
        
        # Process OptionMetrics Volatility Surface data
        logger.info("=== Processing OptionMetrics Volatility Surface Data ===")
        volsurf_data = process_optionmetrics_volsurf(input_files['volsurf'])
        if volsurf_data is None:
            return False
        
        # Process OptionMetrics XZZ data
        logger.info("=== Processing OptionMetrics XZZ Data ===")
        xzz_data = process_optionmetrics_xzz(input_files['xzz'])
        if xzz_data is None:
            return False
        
        # Process Bali-Hovakimian Implied Volatility data
        logger.info("=== Processing Bali-Hovakimian Implied Volatility Data ===")
        bh_data = process_optionmetrics_bh(input_files['bali_hovak'])
        if bh_data is None:
            return False
        
        logger.info("Successfully processed all OptionMetrics datasets")
        logger.info("Note: Multiple OptionMetrics datasets for options analysis")
        return True
        
    except Exception as e:
        logger.error(f"Failed to process OptionMetrics data: {e}")
        return False

def process_optionmetrics_volume(file_path):
    """Process OptionMetrics Volume data"""
    logger.info(f"Reading OptionMetrics Volume data from: {file_path}")
    
    try:
        # Read the CSV file (equivalent to Stata's "import delimited")
        data = pd.read_csv(file_path)
        logger.info(f"Successfully loaded {len(data)} records from OptionMetrics Volume data")
        
        # Display initial data info
        logger.info("Initial data info:")
        logger.info(f"  Columns: {list(data.columns)}")
        logger.info(f"  Shape: {data.shape}")
        
        # Create time_d from time_avail_m (equivalent to Stata's "gen time_d = date(time_avail_m,"YMD")")
        if 'time_avail_m' in data.columns:
            data['time_d'] = pd.to_datetime(data['time_avail_m'], format='%Y-%m-%d', errors='coerce')
            logger.info("Created time_d from time_avail_m")
        
        # Drop original time_avail_m (equivalent to Stata's "drop time_avail_m")
        if 'time_avail_m' in data.columns:
            data = data.drop(columns=['time_avail_m'])
            logger.info("Dropped original time_avail_m column")
        
        # Create new time_avail_m (equivalent to Stata's "gen time_avail_m = mofd(time_d)")
        if 'time_d' in data.columns:
            data['time_avail_m'] = data['time_d'].dt.to_period('M')
            logger.info("Created new time_avail_m from time_d")
        
        # Drop time_d (equivalent to Stata's "drop time_d")
        if 'time_d' in data.columns:
            data = data.drop(columns=['time_d'])
            logger.info("Dropped time_d column")
        
        # Compress data (equivalent to Stata's "compress")
        data = optimize_data_types(data)
        logger.info("Optimized data types for memory efficiency")
        
        # Save to intermediate file (equivalent to Stata's "save "$pathDataIntermediate/OptionMetricsVolume", replace")
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/OptionMetricsVolume.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved OptionMetrics Volume data to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/OptionMetricsVolume.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log summary statistics
        log_optionmetrics_summary(data, "OptionMetrics Volume")
        
        return data
        
    except Exception as e:
        logger.error(f"Failed to process OptionMetrics Volume data: {e}")
        return None

def process_optionmetrics_volsurf(file_path):
    """Process OptionMetrics Volatility Surface data"""
    logger.info(f"Reading OptionMetrics Volatility Surface data from: {file_path}")
    
    try:
        # Read the CSV file (equivalent to Stata's "import delimited")
        data = pd.read_csv(file_path)
        logger.info(f"Successfully loaded {len(data)} records from OptionMetrics Volatility Surface data")
        
        # Display initial data info
        logger.info("Initial data info:")
        logger.info(f"  Columns: {list(data.columns)}")
        logger.info(f"  Shape: {data.shape}")
        
        # Create time_d from time_avail_m (equivalent to Stata's "gen time_d = date(time_avail_m,"YMD")")
        if 'time_avail_m' in data.columns:
            data['time_d'] = pd.to_datetime(data['time_avail_m'], format='%Y-%m-%d', errors='coerce')
            logger.info("Created time_d from time_avail_m")
        
        # Drop original time_avail_m (equivalent to Stata's "drop time_avail_m")
        if 'time_avail_m' in data.columns:
            data = data.drop(columns=['time_avail_m'])
            logger.info("Dropped original time_avail_m column")
        
        # Create new time_avail_m (equivalent to Stata's "gen time_avail_m = mofd(time_d)")
        if 'time_d' in data.columns:
            data['time_d'] = pd.to_datetime(data['time_d'], format='%Y-%m-%d', errors='coerce')
            data['time_avail_m'] = data['time_d'].dt.to_period('M')
            logger.info("Created new time_avail_m from time_d")
        
        # Drop time_d (equivalent to Stata's "drop time_d")
        if 'time_d' in data.columns:
            data = data.drop(columns=['time_d'])
            logger.info("Dropped time_d column")
        
        # Reorder columns (equivalent to Stata's "order secid days delta cp_flag time_avail_m")
        desired_order = ['secid', 'days', 'delta', 'cp_flag', 'time_avail_m']
        available_columns = [col for col in desired_order if col in data.columns]
        other_columns = [col for col in data.columns if col not in desired_order]
        
        if available_columns:
            data = data[available_columns + other_columns]
            logger.info(f"Reordered columns: {available_columns}")
        
        # Save to intermediate file (equivalent to Stata's "save "$pathDataIntermediate/OptionMetricsVolSurf", replace")
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/OptionMetricsVolSurf.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved OptionMetrics Volatility Surface data to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/OptionMetricsVolSurf.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log summary statistics
        log_optionmetrics_summary(data, "OptionMetrics Volatility Surface")
        
        return data
        
    except Exception as e:
        logger.error(f"Failed to process OptionMetrics Volatility Surface data: {e}")
        return None

def process_optionmetrics_xzz(file_path):
    """Process OptionMetrics XZZ data"""
    logger.info(f"Reading OptionMetrics XZZ data from: {file_path}")
    
    try:
        # Read the CSV file (equivalent to Stata's "import delimited")
        data = pd.read_csv(file_path)
        logger.info(f"Successfully loaded {len(data)} records from OptionMetrics XZZ data")
        
        # Display initial data info
        logger.info("Initial data info:")
        logger.info(f"  Columns: {list(data.columns)}")
        logger.info(f"  Shape: {data.shape}")
        
        # Create time_d from time_avail_m (equivalent to Stata's "gen time_d = date(time_avail_m,"YMD")")
        if 'time_avail_m' in data.columns:
            data['time_d'] = pd.to_datetime(data['time_avail_m'], format='%Y-%m-%d', errors='coerce')
            logger.info("Created time_d from time_avail_m")
        
        # Drop original time_avail_m (equivalent to Stata's "drop time_avail_m")
        if 'time_avail_m' in data.columns:
            data = data.drop(columns=['time_avail_m'])
            logger.info("Dropped original time_avail_m column")
        
        # Create new time_avail_m (equivalent to Stata's "gen time_avail_m = mofd(time_d)")
        if 'time_d' in data.columns:
            data['time_avail_m'] = data['time_d'].dt.to_period('M')
            logger.info("Created new time_avail_m from time_d")
        
        # Drop time_d (equivalent to Stata's "drop time_d")
        if 'time_d' in data.columns:
            data = data.drop(columns=['time_d'])
            logger.info("Dropped time_d column")
        
        # Compress data (equivalent to Stata's "compress")
        data = optimize_data_types(data)
        logger.info("Optimized data types for memory efficiency")
        
        # Reorder columns (equivalent to Stata's "order secid time_avail_m")
        desired_order = ['secid', 'time_avail_m']
        available_columns = [col for col in desired_order if col in data.columns]
        other_columns = [col for col in data.columns if col not in desired_order]
        
        if available_columns:
            data = data[available_columns + other_columns]
            logger.info(f"Reordered columns: {available_columns}")
        
        # Save to intermediate file (equivalent to Stata's "save "$pathDataIntermediate/OptionMetricsVolSurf", replace")
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/OptionMetricsVolSurf.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved OptionMetrics XZZ data to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/OptionMetricsXZZ.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log summary statistics
        log_optionmetrics_summary(data, "OptionMetrics XZZ")
        
        return data
        
    except Exception as e:
        logger.error(f"Failed to process OptionMetrics XZZ data: {e}")
        return None

def process_optionmetrics_bh(file_path):
    """Process Bali-Hovakimian Implied Volatility data"""
    logger.info(f"Reading Bali-Hovakimian Implied Volatility data from: {file_path}")
    
    try:
        # Read the CSV file (equivalent to Stata's "import delimited")
        data = pd.read_csv(file_path)
        logger.info(f"Successfully loaded {len(data)} records from Bali-Hovakimian Implied Volatility data")
        
        # Display initial data info
        logger.info("Initial data info:")
        logger.info(f"  Columns: {list(data.columns)}")
        logger.info(f"  Shape: {data.shape}")
        
        # Create time_d from date (equivalent to Stata's "gen time_d = date(date,"YMD")")
        if 'date' in data.columns:
            data['time_d'] = pd.to_datetime(data['date'], format='%Y-%m-%d', errors='coerce')
            logger.info("Created time_d from date")
        
        # Drop original date (equivalent to Stata's "drop date")
        if 'date' in data.columns:
            data = data.drop(columns=['date'])
            logger.info("Dropped original date column")
        
        # Create time_avail_m (equivalent to Stata's "gen time_avail_m = mofd(time_d)")
        if 'time_d' in data.columns:
            data['time_avail_m'] = data['time_d'].dt.to_period('M')
            logger.info("Created time_avail_m from time_d")
        
        # Drop time_d (equivalent to Stata's "drop time_d")
        if 'time_d' in data.columns:
            data = data.drop(columns=['time_d'])
            logger.info("Dropped time_d column")
        
        # Drop missing secid (equivalent to Stata's "drop if mi(secid)")
        if 'secid' in data.columns:
            initial_count = len(data)
            data = data.dropna(subset=['secid'])
            dropped_count = initial_count - len(data)
            logger.info(f"Dropped {dropped_count} records with missing secid")
        
        # Compress data (equivalent to Stata's "compress")
        data = optimize_data_types(data)
        logger.info("Optimized data types for memory efficiency")
        
        # Reorder columns (equivalent to Stata's "order secid time_avail_m")
        desired_order = ['secid', 'time_avail_m']
        available_columns = [col for col in desired_order if col in data.columns]
        other_columns = [col for col in data.columns if col not in desired_order]
        
        if available_columns:
            data = data[available_columns + other_columns]
            logger.info(f"Reordered columns: {available_columns}")
        
        # Save to intermediate file (equivalent to Stata's "save "$pathDataIntermediate/OptionMetricsBH", replace")
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/OptionMetricsBH.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved Bali-Hovakimian Implied Volatility data to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/OptionMetricsBH.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log summary statistics
        log_optionmetrics_summary(data, "Bali-Hovakimian Implied Volatility")
        
        return data
        
    except Exception as e:
        logger.error(f"Failed to process Bali-Hovakimian Implied Volatility data: {e}")
        return None

def optimize_data_types(data):
    """Optimize data types for memory efficiency (equivalent to Stata's compress)"""
    for col in data.columns:
        if data[col].dtype == 'object':
            # Try to convert to numeric if possible
            try:
                data[col] = pd.to_numeric(data[col], errors='ignore')
            except:
                pass
    return data

def log_optionmetrics_summary(data, dataset_name):
    """Log comprehensive summary statistics for OptionMetrics data"""
    logger.info(f"{dataset_name} summary:")
    logger.info(f"  Total records: {len(data)}")
    
    # Time range analysis
    if 'time_avail_m' in data.columns:
        min_date = data['time_avail_m'].min()
        max_date = data['time_avail_m'].max()
        logger.info(f"  Time range: {min_date} to {max_date}")
        
        # Monthly frequency analysis
        monthly_counts = data['time_avail_m'].value_counts().sort_index()
        logger.info(f"  Monthly observations range: {monthly_counts.min()} to {monthly_counts.max()}")
        logger.info(f"  Average monthly observations: {monthly_counts.mean():.1f}")
    
    # Security analysis
    if 'secid' in data.columns:
        unique_securities = data['secid'].nunique()
        logger.info(f"  Unique securities (secid): {unique_securities}")
        
        # Securities with most observations
        security_counts = data['secid'].value_counts().head(10)
        logger.info(f"  Securities with most observations:")
        for secid, count in security_counts.items():
            logger.info(f"    {secid}: {count} observations")
    
    # Options-specific analysis
    if 'cp_flag' in data.columns:
        cp_counts = data['cp_flag'].value_counts()
        logger.info("  Call/Put flag distribution:")
        for flag, count in cp_counts.items():
            percentage = count / len(data) * 100
            flag_name = "Call" if flag == 'C' else "Put" if flag == 'P' else str(flag)
            logger.info(f"    {flag_name}: {count} ({percentage:.1f}%)")
    
    if 'delta' in data.columns:
        delta_data = data['delta'].dropna()
        if len(delta_data) > 0:
            logger.info("  Delta analysis:")
            logger.info(f"    Mean delta: {delta_data.mean():.3f}")
            logger.info(f"    Median delta: {delta_data.median():.3f}")
            logger.info(f"    Range: [{delta_data.min():.3f}, {delta_data.max():.3f}]")
    
    if 'days' in data.columns:
        days_data = data['days'].dropna()
        if len(days_data) > 0:
            logger.info("  Days to expiration analysis:")
            logger.info(f"    Mean days: {days_data.mean():.1f}")
            logger.info(f"    Median days: {days_data.median():.1f}")
            logger.info(f"    Range: [{days_data.min():.0f}, {days_data.max():.0f}]")
    
    # Data quality checks
    logger.info("  Data quality checks:")
    
    # Check for missing values
    missing_analysis = data.isnull().sum()
    logger.info("    Missing values per column:")
    for column, missing_count in missing_analysis.items():
        percentage = missing_count / len(data) * 100
        logger.info(f"      {column}: {missing_count} ({percentage:.1f}%)")
    
    # Check for duplicate records
    if 'secid' in data.columns and 'time_avail_m' in data.columns:
        duplicates = data.duplicated(subset=['secid', 'time_avail_m']).sum()
        logger.info(f"    Duplicate secid-time_avail_m combinations: {duplicates}")

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the processing function
    zh_optionmetrics()
