"""
Python equivalent of ZD_CorwinSchultz.do
Generated from: ZD_CorwinSchultz.do

Original Stata file: ZD_CorwinSchultz.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def zd_corwinschultz():
    """
    Python equivalent of ZD_CorwinSchultz.do
    
    Processes Corwin-Schultz bid-ask spread data from pre-processed CSV file
    Note: Requires Corwin_Schultz_Edit.sas to be run first to generate the input CSV
    """
    logger.info("Processing Corwin-Schultz bid-ask spread data...")
    
    try:
        # Input file path (equivalent to Stata's "$pathDataPrep/corwin_schultz_spread.csv")
        input_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Code/PrepScripts/corwin_schultz_spread.csv")
        
        logger.info(f"Reading Corwin-Schultz spread data from: {input_path}")
        
        # Check if input file exists
        if not input_path.exists():
            logger.error(f"Input file not found: {input_path}")
            logger.error("Please run Corwin_Schultz_Edit.sas first to generate the input CSV file")
            return False
        
        # Read the CSV file (equivalent to Stata's "import delimited")
        data = pd.read_csv(input_path)
        logger.info(f"Successfully loaded {len(data)} records from Corwin-Schultz spread data")
        
        # Display initial data info
        logger.info("Initial data info:")
        logger.info(f"  Columns: {list(data.columns)}")
        logger.info(f"  Shape: {data.shape}")
        logger.info(f"  Sample data:")
        logger.info(data.head().to_string())
        
        # Rename columns to match expected format
        if 'PERMNO' in data.columns:
            data = data.rename(columns={'PERMNO': 'permno'})
            logger.info("Renamed PERMNO to permno")
        
        # Convert month to string (equivalent to Stata's "tostring month, replace")
        if 'month' in data.columns:
            data['month'] = data['month'].astype(str)
            logger.info("Converted month column to string")
        
        # Extract year and month from month string (equivalent to Stata's "gen y = substr(month, 1,4)" and "gen m = substr(month, 5,2)")
        if 'month' in data.columns:
            # Extract year (first 4 characters)
            data['y'] = data['month'].str[:4]
            # Extract month (characters 5-6)
            data['m'] = data['month'].str[4:6]
            
            logger.info("Extracted year and month from month string")
            
            # Convert to numeric (equivalent to Stata's "destring y m, replace")
            data['y'] = pd.to_numeric(data['y'], errors='coerce')
            data['m'] = pd.to_numeric(data['m'], errors='coerce')
            
            logger.info("Converted year and month to numeric")
        
        # Create time_avail_m (equivalent to Stata's "gen time_avail_m = ym(y, m)")
        if 'y' in data.columns and 'm' in data.columns:
            # Create datetime and convert to period
            data['day'] = 1
            logger.info(f"Columns before datetime creation: {list(data.columns)}")
            logger.info(f"Sample y values: {data['y'].head()}")
            logger.info(f"Sample m values: {data['m'].head()}")
            # Rename columns to what pandas expects
            data_temp = data[['y', 'm', 'day']].copy()
            data_temp.columns = ['year', 'month', 'day']
            data['time_avail_m'] = pd.to_datetime(data_temp).dt.to_period('M')
            logger.info("Created time_avail_m column")
        
        # Drop temporary columns (equivalent to Stata's "drop y m month")
        columns_to_drop = ['y', 'm', 'month']
        data = data.drop(columns=[col for col in columns_to_drop if col in data.columns])
        logger.info(f"Dropped temporary columns: {[col for col in columns_to_drop if col in data.columns]}")
        
        # Drop missing permno (equivalent to Stata's "drop if mi(permno)")
        if 'permno' in data.columns:
            initial_count = len(data)
            data = data.dropna(subset=['permno'])
            dropped_count = initial_count - len(data)
            logger.info(f"Dropped {dropped_count} records with missing permno")
        
        # Rename hlspread to BidAskSpread (equivalent to Stata's "rename hlspread BidAskSpread")
        if 'hlspread' in data.columns:
            data = data.rename(columns={'hlspread': 'BidAskSpread'})
            logger.info("Renamed hlspread to BidAskSpread")
        
        # Note about price division (from Stata comment)
        logger.info("Note: hlspread already divides by price (in both Corwin's xlsx and sas code)")
        
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
        
        # Save to intermediate file (equivalent to Stata's "save "$pathDataIntermediate/BAspreadsCorwin", replace")
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/BAspreadsCorwin.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved Corwin-Schultz spread data to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/BAspreadsCorwin.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log comprehensive summary statistics
        logger.info("Corwin-Schultz bid-ask spread summary:")
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
        
        # Bid-ask spread analysis
        if 'BidAskSpread' in data.columns:
            spread_data = data['BidAskSpread'].dropna()
            if len(spread_data) > 0:
                logger.info("  Bid-ask spread analysis:")
                logger.info(f"    Non-missing observations: {len(spread_data)}")
                logger.info(f"    Missing observations: {data['BidAskSpread'].isna().sum()}")
                
                # Spread statistics
                mean_spread = spread_data.mean()
                std_spread = spread_data.std()
                min_spread = spread_data.min()
                max_spread = spread_data.max()
                median_spread = spread_data.median()
                
                logger.info(f"    Mean spread: {mean_spread:.6f}")
                logger.info(f"    Median spread: {median_spread:.6f}")
                logger.info(f"    Std spread: {std_spread:.6f}")
                logger.info(f"    Range: [{min_spread:.6f}, {max_spread:.6f}]")
                
                # Spread distribution analysis
                logger.info("    Spread distribution:")
                
                # Percentiles
                percentiles = [1, 5, 10, 25, 50, 75, 90, 95, 99]
                for p in percentiles:
                    value = spread_data.quantile(p/100)
                    logger.info(f"      {p}th percentile: {value:.6f}")
                
                # Spread quality analysis
                logger.info("    Spread quality analysis:")
                low_spread = (spread_data <= 0.001).sum()  # ≤ 0.1%
                medium_spread = ((spread_data > 0.001) & (spread_data <= 0.01)).sum()  # 0.1% - 1%
                high_spread = (spread_data > 0.01).sum()  # > 1%
                
                total_obs = len(spread_data)
                logger.info(f"      Low spread (≤0.1%): {low_spread} ({low_spread/total_obs*100:.1f}%)")
                logger.info(f"      Medium spread (0.1%-1%): {medium_spread} ({medium_spread/total_obs*100:.1f}%)")
                logger.info(f"      High spread (>1%): {high_spread} ({high_spread/total_obs*100:.1f}%)")
                
                # Zero and negative spreads
                zero_spreads = (spread_data == 0).sum()
                negative_spreads = (spread_data < 0).sum()
                logger.info(f"      Zero spreads: {zero_spreads} ({zero_spreads/total_obs*100:.1f}%)")
                logger.info(f"      Negative spreads: {negative_spreads} ({negative_spreads/total_obs*100:.1f}%)")
        
        # Data quality checks
        logger.info("  Data quality checks:")
        
        # Check for duplicate records
        if 'permno' in data.columns and 'time_avail_m' in data.columns:
            duplicates = data.duplicated(subset=['permno', 'time_avail_m']).sum()
            logger.info(f"    Duplicate permno-time_avail_m combinations: {duplicates}")
        
        # Check for extreme values
        if 'BidAskSpread' in data.columns:
            spread_data = data['BidAskSpread'].dropna()
            if len(spread_data) > 0:
                # Check for extreme outliers (beyond 3 standard deviations)
                mean_spread = spread_data.mean()
                std_spread = spread_data.std()
                extreme_outliers = ((spread_data < mean_spread - 3*std_spread) | 
                                   (spread_data > mean_spread + 3*std_spread)).sum()
                logger.info(f"    Extreme outliers (>3 std dev): {extreme_outliers} ({extreme_outliers/len(spread_data)*100:.1f}%)")
        
        # Data processing explanation
        logger.info("  Data processing explanation:")
        logger.info("    - Original data: Corwin-Schultz bid-ask spread from SAS preprocessing")
        logger.info("    - Input file: corwin_schultz_spread.csv (generated by Corwin_Schultz_Edit.sas)")
        logger.info("    - Time processing: Month string converted to time_avail_m periods")
        logger.info("    - Spread calculation: hlspread already normalized by price")
        logger.info("    - Usage: High-frequency bid-ask spread analysis")
        
        # Corwin-Schultz method explanation
        logger.info("  Corwin-Schultz method explanation:")
        logger.info("    Corwin-Schultz Bid-Ask Spread:")
        logger.info("    - High-frequency bid-ask spread estimator")
        logger.info("    - Uses high-low price ratio")
        logger.info("    - Accounts for bid-ask bounce")
        logger.info("    - More accurate than simple bid-ask spreads")
        logger.info("    - Based on Corwin and Schultz (2012)")
        logger.info("    - Used in market microstructure research")
        
        # Market microstructure applications
        logger.info("  Market microstructure applications:")
        logger.info("    - Liquidity measurement")
        logger.info("    - Market efficiency analysis")
        logger.info("    - Trading cost estimation")
        logger.info("    - Market quality assessment")
        logger.info("    - High-frequency trading analysis")
        
        logger.info("Successfully processed Corwin-Schultz bid-ask spread data")
        logger.info("Note: Monthly bid-ask spread data for market microstructure analysis")
        return True
        
    except Exception as e:
        logger.error(f"Failed to process Corwin-Schultz bid-ask spread data: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the processing function
    zd_corwinschultz()
