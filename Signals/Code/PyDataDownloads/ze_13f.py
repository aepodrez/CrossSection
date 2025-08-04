"""
Python equivalent of ZE_13F.do
Generated from: ZE_13F.do

Original Stata file: ZE_13F.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def ze_13f():
    """
    Python equivalent of ZE_13F.do
    
    Processes 13F institutional ownership data from pre-processed CSV file
    Note: Requires tr13f_download.sas to be run first to generate the input CSV
    """
    logger.info("Processing 13F institutional ownership data...")
    
    try:
        # Input file path (equivalent to Stata's "$pathDataPrep/tr_13f.csv")
        input_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Code/PrepScripts/tr_13f.csv")
        
        logger.info(f"Reading 13F institutional ownership data from: {input_path}")
        
        # Check if input file exists
        if not input_path.exists():
            logger.error(f"Input file not found: {input_path}")
            logger.error("Please run tr13f_download.sas first to generate the input CSV file")
            return False
        
        # Read the CSV file (equivalent to Stata's "import delimited")
        data = pd.read_csv(input_path)
        logger.info(f"Successfully loaded {len(data)} records from 13F institutional ownership data")
        
        # Display initial data info
        logger.info("Initial data info:")
        logger.info(f"  Columns: {list(data.columns)}")
        logger.info(f"  Shape: {data.shape}")
        logger.info(f"  Sample data:")
        logger.info(data.head().to_string())
        
        # Drop missing permno (equivalent to Stata's "drop if mi(permno)")
        if 'permno' in data.columns:
            initial_count = len(data)
            data = data.dropna(subset=['permno'])
            dropped_count = initial_count - len(data)
            logger.info(f"Dropped {dropped_count} records with missing permno")
        
        # Convert numeric columns (equivalent to Stata's "destring instown_perc maxinstown_perc numinstown, replace force")
        numeric_columns = ['instown_perc', 'maxinstown_perc', 'numinstown']
        for col in numeric_columns:
            if col in data.columns:
                data[col] = pd.to_numeric(data[col], errors='coerce')
                logger.info(f"Converted {col} to numeric")
        
        # Create time_d from rdate (equivalent to Stata's "gen time_d = date(rdate,"DMY")")
        if 'rdate' in data.columns:
            data['time_d'] = pd.to_datetime(data['rdate'], format='%d/%m/%Y', errors='coerce')
            logger.info("Created time_d from rdate")
        
        # Create time_avail_m (equivalent to Stata's "gen time_avail_m = mofd(time_d)")
        if 'time_d' in data.columns:
            data['time_avail_m'] = data['time_d'].dt.to_period('M')
            logger.info("Created time_avail_m from time_d")
        
        # Drop temporary columns (equivalent to Stata's "drop rdate time_d")
        columns_to_drop = ['rdate', 'time_d']
        data = data.drop(columns=[col for col in columns_to_drop if col in data.columns])
        logger.info(f"Dropped temporary columns: {[col for col in columns_to_drop if col in data.columns]}")
        
        # Fill in missing months (equivalent to Stata's xtset and tsfill)
        logger.info("Filling in missing months with forward fill...")
        
        # Group by permno and process each group
        processed_data = []
        
        for permno in data['permno'].unique():
            permno_data = data[data['permno'] == permno].copy()
            permno_data = permno_data.sort_values('time_avail_m')
            
            # Create complete time series for this permno
            min_date = permno_data['time_avail_m'].min()
            max_date = permno_data['time_avail_m'].max()
            
            # Generate complete monthly time series
            complete_dates = pd.date_range(
                start=min_date.to_timestamp(),
                end=max_date.to_timestamp(),
                freq='M'
            )
            complete_dates = [pd.Period(date, freq='M') for date in complete_dates]
            
            # Create complete time series dataframe
            complete_series = pd.DataFrame({
                'permno': permno,
                'time_avail_m': complete_dates
            })
            
            # Get all columns except permno and time_avail_m
            value_columns = [col for col in permno_data.columns if col not in ['permno', 'time_avail_m']]
            
            # Merge with original data to get values
            complete_series = complete_series.merge(
                permno_data[['time_avail_m'] + value_columns], 
                on='time_avail_m', 
                how='left'
            )
            
            # Forward fill missing values (equivalent to Stata's forward fill)
            complete_series = complete_series.sort_values('time_avail_m')
            for col in value_columns:
                complete_series['col'] = complete_series['col'].ffill()
            
            # Forward fill permno
            complete_series['permno'] = complete_series['permno'].ffill()
            
            processed_data.append(complete_series)
        
        # Combine all processed data
        data = pd.concat(processed_data, ignore_index=True)
        logger.info(f"After filling missing months: {len(data)} records")
        
        # Compress data (equivalent to Stata's "compress" - optimize memory usage)
        # In pandas, we can optimize by converting to appropriate dtypes
        for col in data.columns:
            if data[col].dtype == 'object':
                # Try to convert to numeric if possible
                try:
                    data[col] = pd.to_numeric(data[col], errors='ignore')
                except:
                    pass
        
        logger.info("Optimized data types for memory efficiency")
        
        # Save to intermediate file (equivalent to Stata's "save "$pathDataIntermediate/TR_13F", replace")
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/TR_13F.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved 13F institutional ownership data to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/TR_13F.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log comprehensive summary statistics
        logger.info("13F institutional ownership summary:")
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
        
        # Company analysis
        if 'permno' in data.columns:
            unique_companies = data['permno'].nunique()
            logger.info(f"  Unique companies (permno): {unique_companies}")
            
            # Companies with most observations
            company_counts = data['permno'].value_counts().head(10)
            logger.info(f"  Companies with most observations:")
            for permno, count in company_counts.items():
                logger.info(f"    {permno}: {count} observations")
        
        # Institutional ownership analysis
        if 'instown_perc' in data.columns:
            instown_data = data['instown_perc'].dropna()
            if len(instown_data) > 0:
                logger.info("  Institutional ownership percentage analysis:")
                logger.info(f"    Non-missing observations: {len(instown_data)}")
                logger.info(f"    Missing observations: {data['instown_perc'].isna().sum()}")
                
                # Ownership statistics
                mean_instown = instown_data.mean()
                std_instown = instown_data.std()
                min_instown = instown_data.min()
                max_instown = instown_data.max()
                median_instown = instown_data.median()
                
                logger.info(f"    Mean institutional ownership: {mean_instown:.2f}%")
                logger.info(f"    Median institutional ownership: {median_instown:.2f}%")
                logger.info(f"    Std institutional ownership: {std_instown:.2f}%")
                logger.info(f"    Range: [{min_instown:.2f}%, {max_instown:.2f}%]")
                
                # Ownership distribution analysis
                logger.info("    Institutional ownership distribution:")
                
                # Percentiles
                percentiles = [1, 5, 10, 25, 50, 75, 90, 95, 99]
                for p in percentiles:
                    value = instown_data.quantile(p/100)
                    logger.info(f"      {p}th percentile: {value:.2f}%")
                
                # Ownership concentration analysis
                logger.info("    Ownership concentration analysis:")
                low_instown = (instown_data <= 10).sum()  # ≤ 10%
                medium_instown = ((instown_data > 10) & (instown_data <= 50)).sum()  # 10% - 50%
                high_instown = (instown_data > 50).sum()  # > 50%
                
                total_obs = len(instown_data)
                logger.info(f"      Low institutional ownership (≤10%): {low_instown} ({low_instown/total_obs*100:.1f}%)")
                logger.info(f"      Medium institutional ownership (10%-50%): {medium_instown} ({medium_instown/total_obs*100:.1f}%)")
                logger.info(f"      High institutional ownership (>50%): {high_instown} ({high_instown/total_obs*100:.1f}%)")
        
        # Maximum institutional ownership analysis
        if 'maxinstown_perc' in data.columns:
            maxinstown_data = data['maxinstown_perc'].dropna()
            if len(maxinstown_data) > 0:
                logger.info("  Maximum institutional ownership analysis:")
                logger.info(f"    Non-missing observations: {len(maxinstown_data)}")
                
                # Max ownership statistics
                mean_maxinstown = maxinstown_data.mean()
                median_maxinstown = maxinstown_data.median()
                logger.info(f"    Mean max institutional ownership: {mean_maxinstown:.2f}%")
                logger.info(f"    Median max institutional ownership: {median_maxinstown:.2f}%")
        
        # Number of institutional owners analysis
        if 'numinstown' in data.columns:
            numinstown_data = data['numinstown'].dropna()
            if len(numinstown_data) > 0:
                logger.info("  Number of institutional owners analysis:")
                logger.info(f"    Non-missing observations: {len(numinstown_data)}")
                
                # Number of owners statistics
                mean_numinstown = numinstown_data.mean()
                median_numinstown = numinstown_data.median()
                min_numinstown = numinstown_data.min()
                max_numinstown = numinstown_data.max()
                
                logger.info(f"    Mean number of institutional owners: {mean_numinstown:.1f}")
                logger.info(f"    Median number of institutional owners: {median_numinstown:.1f}")
                logger.info(f"    Range: [{min_numinstown:.0f}, {max_numinstown:.0f}]")
                
                # Distribution of number of owners
                logger.info("    Distribution of number of institutional owners:")
                owner_ranges = [
                    (0, 5, "Very few (0-5)"),
                    (6, 20, "Few (6-20)"),
                    (21, 100, "Moderate (21-100)"),
                    (101, 500, "Many (101-500)"),
                    (501, float('inf'), "Very many (>500)")
                ]
                
                for min_owners, max_owners, label in owner_ranges:
                    if max_owners == float('inf'):
                        count = (numinstown_data >= min_owners).sum()
                    else:
                        count = ((numinstown_data >= min_owners) & (numinstown_data <= max_owners)).sum()
                    percentage = count / len(numinstown_data) * 100
                    logger.info(f"      {label}: {count} ({percentage:.1f}%)")
        
        # Data quality checks
        logger.info("  Data quality checks:")
        
        # Check for duplicate records
        if 'permno' in data.columns and 'time_avail_m' in data.columns:
            duplicates = data.duplicated(subset=['permno', 'time_avail_m']).sum()
            logger.info(f"    Duplicate permno-time_avail_m combinations: {duplicates}")
        
        # Check for extreme values in institutional ownership
        if 'instown_perc' in data.columns:
            instown_data = data['instown_perc'].dropna()
            if len(instown_data) > 0:
                # Check for values > 100% (impossible)
                impossible_values = (instown_data > 100).sum()
                logger.info(f"    Impossible ownership values (>100%): {impossible_values}")
                
                # Check for negative values
                negative_values = (instown_data < 0).sum()
                logger.info(f"    Negative ownership values: {negative_values}")
        
        # Data processing explanation
        logger.info("  Data processing explanation:")
        logger.info("    - Original data: 13F institutional ownership from SAS preprocessing")
        logger.info("    - Input file: tr_13f.csv (generated by tr13f_download.sas)")
        logger.info("    - Time processing: Report date converted to time_avail_m periods")
        logger.info("    - Missing months: Filled with forward fill interpolation")
        logger.info("    - Usage: Institutional ownership and breadth analysis")
        
        # 13F data explanation
        logger.info("  Form 13F explanation:")
        logger.info("    Form 13F Holdings:")
        logger.info("    - Quarterly institutional investment holdings")
        logger.info("    - Required for institutional investors > $100M")
        logger.info("    - Filed with SEC 45 days after quarter end")
        logger.info("    - Contains equity holdings and ownership percentages")
        logger.info("    - Used in institutional ownership research")
        
        # Institutional ownership applications
        logger.info("  Institutional ownership applications:")
        logger.info("    - Institutional ownership analysis")
        logger.info("    - Ownership concentration research")
        logger.info("    - Institutional trading patterns")
        logger.info("    - Corporate governance studies")
        logger.info("    - Market microstructure analysis")
        
        # Reporting lag note
        logger.info("  Reporting lag note:")
        logger.info("    - 13F data has quarterly reporting lag")
        logger.info("    - Data available ~45 days after quarter end")
        logger.info("    - Historical lag may have been different in 1980s")
        logger.info("    - Important for time series analysis")
        
        logger.info("Successfully processed 13F institutional ownership data")
        logger.info("Note: Monthly institutional ownership data for institutional analysis")
        return True
        
    except Exception as e:
        logger.error(f"Failed to process 13F institutional ownership data: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the processing function
    ze_13f()
