"""
Python equivalent of ZJ_InputOutputMomentum.do
Generated from: ZJ_InputOutputMomentum.do

Original Stata file: ZJ_InputOutputMomentum.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime
import subprocess
import sys
import os

logger = logging.getLogger(__name__)

def zj_inputoutputmomentum():
    """
    Python equivalent of ZJ_InputOutputMomentum.do
    
    Calls R script to process input-output momentum data and then processes the output
    Note: Requires R script ZJR_InputOutputMomentum.R to be available
    """
    logger.info("Processing input-output momentum data via R script...")
    
    try:
        # Define paths
        project_path = Path("/Users/alexpodrez/Documents/CrossSection")
        r_script_path = project_path / "Signals" / "Code" / "DataDownloads" / "ZJR_InputOutputMomentum.R"
        input_file_path = project_path / "Signals" / "Data" / "Intermediate" / "InputOutputMomentum.csv"
        
        logger.info(f"R script path: {r_script_path}")
        logger.info(f"Expected input path: {input_file_path}")
        
        # Check if R script exists
        if not r_script_path.exists():
            logger.error(f"R script not found: {r_script_path}")
            logger.error("Please ensure ZJR_InputOutputMomentum.R is available in the DataDownloads directory")
            return False
        
        logger.info("R script found, executing...")
        
        # Execute R script (equivalent to Stata's rscript/shell Rscript)
        try:
            # Determine OS and execute R script accordingly
            if sys.platform.startswith('win'):
                # Windows
                logger.info("Detected Windows OS")
                result = subprocess.run([
                    'rscript', 'using', str(r_script_path), 
                    'args', str(project_path)
                ], capture_output=True, text=True, cwd=str(project_path))
            else:
                # Unix/Linux/macOS
                logger.info("Detected Unix/Linux/macOS OS")
                result = subprocess.run([
                    'Rscript', str(r_script_path), str(project_path)
                ], capture_output=True, text=True, cwd=str(project_path))
            
            # Log R script output
            if result.stdout:
                logger.info("R script stdout:")
                for line in result.stdout.strip().split('\n'):
                    if line.strip():
                        logger.info(f"  {line}")
            
            if result.stderr:
                logger.warning("R script stderr:")
                for line in result.stderr.strip().split('\n'):
                    if line.strip():
                        logger.warning(f"  {line}")
            
            # Check return code
            if result.returncode != 0:
                logger.error(f"R script failed with return code: {result.returncode}")
                return False
            
            logger.info("R script executed successfully")
            
        except FileNotFoundError:
            logger.error("Rscript command not found. Please ensure R is installed and in PATH")
            return False
        except Exception as e:
            logger.error(f"Failed to execute R script: {e}")
            return False
        
        # Check if input file exists (equivalent to Stata's "use")
        logger.info("Checking for input file...")
        
        # Check for both .dta and .csv formats
        possible_input_files = [
            input_file_path,
            input_file_path.with_suffix('.dta'),
            project_path / "Signals" / "Data" / "Intermediate" / "InputOutputMomentum.dta"
        ]
        
        input_file = None
        for file_path in possible_input_files:
            if file_path.exists():
                input_file = file_path
                break
        
        if input_file is None:
            logger.error("Input file not found. Expected one of:")
            for file_path in possible_input_files:
                logger.error(f"  {file_path}")
            logger.error("R script may have failed to generate input file")
            return False
        
        logger.info(f"Input file found: {input_file}")
        
        # Load the data
        try:
            if input_file.suffix == '.csv':
                data = pd.read_csv(input_file)
            else:
                # For .dta files, we would need to use pandas with stata support
                # For now, assume it's been converted to CSV by the R script
                logger.warning("DTA file format not directly supported, expecting CSV output from R script")
                return False
            
            logger.info(f"Successfully loaded input-output momentum data: {len(data)} records")
            
        except Exception as e:
            logger.error(f"Failed to load input-output momentum data: {e}")
            return False
        
        # Display initial data info
        logger.info("Initial data info:")
        logger.info(f"  Columns: {list(data.columns)}")
        logger.info(f"  Shape: {data.shape}")
        logger.info(f"  Sample data:")
        logger.info(data.head().to_string())
        
        # Create time_avail_m (equivalent to Stata's "gen time_avail_m = ym(year_avail, month_avail)")
        if 'year_avail' in data.columns and 'month_avail' in data.columns:
            # Create datetime and convert to period
            data['time_avail_m'] = pd.to_datetime(data[['year_avail', 'month_avail']].assign(day=1)).dt.to_period('M')
            logger.info("Created time_avail_m from year_avail and month_avail")
        else:
            logger.warning("year_avail or month_avail columns not found")
        
        # Group by and aggregate (equivalent to Stata's "gcollapse (mean) retmatch portind, by(gvkey time_avail_m type)")
        if all(col in data.columns for col in ['gvkey', 'time_avail_m', 'type', 'retmatch', 'portind']):
            logger.info("Aggregating data by gvkey, time_avail_m, and type...")
            
            # Group by and calculate mean
            data = data.groupby(['gvkey', 'time_avail_m', 'type']).agg({
                'retmatch': 'mean',
                'portind': 'mean'
            }).reset_index()
            
            logger.info(f"After aggregation: {len(data)} records")
            
            # Check for duplicates
            duplicates = data.duplicated(subset=['gvkey', 'time_avail_m', 'type']).sum()
            if duplicates > 0:
                logger.warning(f"Found {duplicates} duplicate records after aggregation")
            else:
                logger.info("No duplicate records found after aggregation")
        else:
            logger.warning("Required columns for aggregation not found")
        
        # Reshape wide (equivalent to Stata's "reshape wide retmatch portind, i(gvkey time_avail_m) j(type) string")
        if all(col in data.columns for col in ['gvkey', 'time_avail_m', 'type', 'retmatch', 'portind']):
            logger.info("Reshaping data from long to wide format...")
            
            # Pivot the data to wide format
            data_wide = data.pivot_table(
                index=['gvkey', 'time_avail_m'],
                columns='type',
                values=['retmatch', 'portind'],
                aggfunc='first'  # Take first value if duplicates exist
            ).reset_index()
            
            # Flatten column names
            data_wide.columns = [f"{col[0]}_{col[1]}" if col[1] else col[0] for col in data_wide.columns]
            
            data = data_wide
            logger.info(f"After reshaping: {len(data)} records")
            logger.info(f"New columns: {list(data.columns)}")
        else:
            logger.warning("Required columns for reshaping not found")
        
        # Save to intermediate file (equivalent to Stata's "save "$pathDataIntermediate/InputOutputMomentumProcessed", replace")
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/InputOutputMomentumProcessed.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved processed input-output momentum data to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/InputOutputMomentumProcessed.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log comprehensive summary statistics
        log_inputoutput_summary(data)
        
        logger.info("Successfully processed input-output momentum data")
        logger.info("Note: Input-output momentum data processed via R script and Python")
        return True
        
    except Exception as e:
        logger.error(f"Failed to process input-output momentum data: {e}")
        return False

def log_inputoutput_summary(data):
    """Log comprehensive summary statistics for input-output momentum data"""
    logger.info("Input-output momentum data summary:")
    logger.info(f"  Total records: {len(data)}")
    
    # Display column information
    logger.info(f"  Columns: {list(data.columns)}")
    logger.info(f"  Shape: {data.shape}")
    
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
    if 'gvkey' in data.columns:
        unique_companies = data['gvkey'].nunique()
        logger.info(f"  Unique companies (gvkey): {unique_companies}")
        
        # Companies with most observations
        company_counts = data['gvkey'].value_counts().head(10)
        logger.info(f"  Companies with most observations:")
        for gvkey, count in company_counts.items():
            logger.info(f"    {gvkey}: {count} observations")
    
    # Return match analysis
    retmatch_columns = [col for col in data.columns if 'retmatch' in col.lower()]
    for col in retmatch_columns:
        retmatch_data = data[col].dropna()
        if len(retmatch_data) > 0:
            logger.info(f"  {col} analysis:")
            logger.info(f"    Non-missing observations: {len(retmatch_data)}")
            logger.info(f"    Missing observations: {data[col].isna().sum()}")
            
            # Return match statistics
            mean_retmatch = retmatch_data.mean()
            median_retmatch = retmatch_data.median()
            std_retmatch = retmatch_data.std()
            min_retmatch = retmatch_data.min()
            max_retmatch = retmatch_data.max()
            
            logger.info(f"    Mean return match: {mean_retmatch:.6f}")
            logger.info(f"    Median return match: {median_retmatch:.6f}")
            logger.info(f"    Std return match: {std_retmatch:.6f}")
            logger.info(f"    Range: [{min_retmatch:.6f}, {max_retmatch:.6f}]")
            
            # Return match distribution
            logger.info(f"    Return match distribution:")
            
            # Percentiles
            percentiles = [1, 5, 10, 25, 50, 75, 90, 95, 99]
            for p in percentiles:
                value = retmatch_data.quantile(p/100)
                logger.info(f"      {p}th percentile: {value:.6f}")
            
            # Return match quality analysis
            logger.info(f"    Return match quality analysis:")
            negative_returns = (retmatch_data < 0).sum()
            positive_returns = (retmatch_data > 0).sum()
            zero_returns = (retmatch_data == 0).sum()
            
            total_obs = len(retmatch_data)
            logger.info(f"      Negative returns: {negative_returns} ({negative_returns/total_obs*100:.1f}%)")
            logger.info(f"      Positive returns: {positive_returns} ({positive_returns/total_obs*100:.1f}%)")
            logger.info(f"      Zero returns: {zero_returns} ({zero_returns/total_obs*100:.1f}%)")
    
    # Portfolio indicator analysis
    portind_columns = [col for col in data.columns if 'portind' in col.lower()]
    for col in portind_columns:
        portind_data = data[col].dropna()
        if len(portind_data) > 0:
            logger.info(f"  {col} analysis:")
            logger.info(f"    Non-missing observations: {len(portind_data)}")
            logger.info(f"    Missing observations: {data[col].isna().sum()}")
            
            # Portfolio indicator statistics
            mean_portind = portind_data.mean()
            median_portind = portind_data.median()
            std_portind = portind_data.std()
            min_portind = portind_data.min()
            max_portind = portind_data.max()
            
            logger.info(f"    Mean portfolio indicator: {mean_portind:.6f}")
            logger.info(f"    Median portfolio indicator: {median_portind:.6f}")
            logger.info(f"    Std portfolio indicator: {std_portind:.6f}")
            logger.info(f"    Range: [{min_portind:.6f}, {max_portind:.6f}]")
            
            # Portfolio indicator distribution
            unique_values = portind_data.nunique()
            logger.info(f"    Unique portfolio indicator values: {unique_values}")
            
            # Show distribution of values
            value_counts = portind_data.value_counts().sort_index()
            logger.info(f"    Portfolio indicator value counts:")
            for value, count in value_counts.head(10).items():
                percentage = count / len(portind_data) * 100
                logger.info(f"      {value}: {count} ({percentage:.1f}%)")
    
    # Data quality checks
    logger.info("  Data quality checks:")
    
    # Check for missing values
    missing_analysis = data.isnull().sum()
    logger.info("    Missing values per column:")
    for column, missing_count in missing_analysis.items():
        percentage = missing_count / len(data) * 100
        logger.info(f"      {column}: {missing_count} ({percentage:.1f}%)")
    
    # Check for duplicate records
    if 'gvkey' in data.columns and 'time_avail_m' in data.columns:
        duplicates = data.duplicated(subset=['gvkey', 'time_avail_m']).sum()
        logger.info(f"    Duplicate gvkey-time_avail_m combinations: {duplicates}")
    
    # Check for extreme values
    numeric_columns = data.select_dtypes(include=[np.number]).columns
    for col in numeric_columns:
        if col in data.columns:
            col_data = data[col].dropna()
            if len(col_data) > 0:
                # Check for extreme outliers (beyond 3 standard deviations)
                mean_val = col_data.mean()
                std_val = col_data.std()
                extreme_outliers = ((col_data < mean_val - 3*std_val) | 
                                   (col_data > mean_val + 3*std_val)).sum()
                if extreme_outliers > 0:
                    logger.info(f"    Extreme outliers in {col} (>3 std dev): {extreme_outliers} ({extreme_outliers/len(col_data)*100:.1f}%)")
    
    # Data processing explanation
    logger.info("  Data processing explanation:")
    logger.info("    - Original data: Input-output momentum data from R script processing")
    logger.info("    - R script: ZJR_InputOutputMomentum.R processes raw input-output data")
    logger.info("    - Aggregation: Mean aggregation by gvkey, time_avail_m, and type")
    logger.info("    - Reshaping: Wide format with type-specific columns")
    logger.info("    - Usage: Input-output momentum analysis and research")
    
    # Input-output momentum explanation
    logger.info("  Input-output momentum explanation:")
    logger.info("    Input-Output Momentum (Menzly and Ozbas):")
    logger.info("    - Measures momentum in input-output relationships")
    logger.info("    - Based on supply chain and customer relationships")
    logger.info("    - Captures momentum effects in economic networks")
    logger.info("    - Used in momentum and network effects research")
    logger.info("    - Important for supply chain and customer momentum")
    
    # Input-output applications
    logger.info("  Input-output applications:")
    logger.info("    - Supply chain momentum analysis")
    logger.info("    - Customer momentum research")
    logger.info("    - Network effects analysis")
    logger.info("    - Economic relationship momentum")
    logger.info("    - Cross-firm momentum effects")

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the processing function
    zj_inputoutputmomentum()
