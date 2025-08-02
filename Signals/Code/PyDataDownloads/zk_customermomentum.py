"""
Python equivalent of ZK_CustomerMomentum.do
Generated from: ZK_CustomerMomentum.do

Original Stata file: ZK_CustomerMomentum.do
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

def zk_customermomentum():
    """
    Python equivalent of ZK_CustomerMomentum.do
    
    Calls R script to process customer segments data and then processes the output
    Note: Requires R script ZKR_CustomerSegments.R to be available
    """
    logger.info("Processing customer momentum data via R script...")
    
    try:
        # Define paths
        project_path = Path("/Users/alexpodrez/Documents/CrossSection")
        r_script_path = project_path / "Signals" / "Code" / "DataDownloads" / "ZKR_CustomerSegments.R"
        input_file_path = project_path / "Signals" / "Data" / "Intermediate" / "customerMom.csv"
        
        logger.info(f"R script path: {r_script_path}")
        logger.info(f"Expected input path: {input_file_path}")
        
        # Check if R script exists
        if not r_script_path.exists():
            logger.error(f"R script not found: {r_script_path}")
            logger.error("Please ensure ZKR_CustomerSegments.R is available in the DataDownloads directory")
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
        
        # Check if input file exists (equivalent to Stata's "import delimited")
        logger.info("Checking for input file...")
        
        if not input_file_path.exists():
            logger.error(f"Input file not found: {input_file_path}")
            logger.error("R script may have failed to generate input file")
            return False
        
        logger.info(f"Input file found: {input_file_path}")
        
        # Load the data (equivalent to Stata's "import delimited")
        try:
            data = pd.read_csv(input_file_path)
            logger.info(f"Successfully loaded customer momentum data: {len(data)} records")
            
        except Exception as e:
            logger.error(f"Failed to load customer momentum data: {e}")
            return False
        
        # Display initial data info
        logger.info("Initial data info:")
        logger.info(f"  Columns: {list(data.columns)}")
        logger.info(f"  Shape: {data.shape}")
        logger.info(f"  Sample data:")
        logger.info(data.head().to_string())
        
        # Create temp time_avail_m (equivalent to Stata's "gen temp = mofd(date(time_avail_m, "YMD"))")
        if 'time_avail_m' in data.columns:
            # Convert time_avail_m to datetime and then to monthly period
            data['temp'] = pd.to_datetime(data['time_avail_m'], format='%Y-%m-%d', errors='coerce').dt.to_period('M')
            logger.info("Created temp time_avail_m from time_avail_m")
        else:
            logger.warning("time_avail_m column not found")
        
        # Drop original time_avail_m (equivalent to Stata's "drop time_avail_m")
        if 'time_avail_m' in data.columns:
            data = data.drop(columns=['time_avail_m'])
            logger.info("Dropped original time_avail_m column")
        
        # Rename temp to time_avail_m (equivalent to Stata's "rename temp time_avail_m")
        if 'temp' in data.columns:
            data = data.rename(columns={'temp': 'time_avail_m'})
            logger.info("Renamed temp to time_avail_m")
        
        # Save to intermediate file (equivalent to Stata's "save "$pathDataIntermediate/customerMom", replace")
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/customerMom.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved customer momentum data to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/customerMom.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log comprehensive summary statistics
        log_customer_momentum_summary(data)
        
        logger.info("Successfully processed customer momentum data")
        logger.info("Note: Customer momentum data processed via R script and Python")
        return True
        
    except Exception as e:
        logger.error(f"Failed to process customer momentum data: {e}")
        return False

def log_customer_momentum_summary(data):
    """Log comprehensive summary statistics for customer momentum data"""
    logger.info("Customer momentum data summary:")
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
    company_columns = [col for col in data.columns if 'gvkey' in col.lower() or 'permno' in col.lower() or 'company' in col.lower()]
    for col in company_columns:
        unique_companies = data[col].nunique()
        logger.info(f"  Unique companies ({col}): {unique_companies}")
        
        # Companies with most observations
        company_counts = data[col].value_counts().head(10)
        logger.info(f"  Companies with most observations ({col}):")
        for company, count in company_counts.items():
            logger.info(f"    {company}: {count} observations")
    
    # Customer analysis
    customer_columns = [col for col in data.columns if 'customer' in col.lower() or 'client' in col.lower()]
    for col in customer_columns:
        if col in data.columns:
            unique_customers = data[col].nunique()
            logger.info(f"  Unique customers ({col}): {unique_customers}")
            
            # Customers with most observations
            customer_counts = data[col].value_counts().head(10)
            logger.info(f"  Customers with most observations ({col}):")
            for customer, count in customer_counts.items():
                logger.info(f"    {customer}: {count} observations")
    
    # Momentum analysis
    momentum_columns = [col for col in data.columns if 'momentum' in col.lower() or 'return' in col.lower() or 'ret' in col.lower()]
    for col in momentum_columns:
        momentum_data = data[col].dropna()
        if len(momentum_data) > 0:
            logger.info(f"  {col} analysis:")
            logger.info(f"    Non-missing observations: {len(momentum_data)}")
            logger.info(f"    Missing observations: {data[col].isna().sum()}")
            
            # Momentum statistics
            mean_momentum = momentum_data.mean()
            median_momentum = momentum_data.median()
            std_momentum = momentum_data.std()
            min_momentum = momentum_data.min()
            max_momentum = momentum_data.max()
            
            logger.info(f"    Mean momentum: {mean_momentum:.6f}")
            logger.info(f"    Median momentum: {median_momentum:.6f}")
            logger.info(f"    Std momentum: {std_momentum:.6f}")
            logger.info(f"    Range: [{min_momentum:.6f}, {max_momentum:.6f}]")
            
            # Momentum distribution
            logger.info(f"    Momentum distribution:")
            
            # Percentiles
            percentiles = [1, 5, 10, 25, 50, 75, 90, 95, 99]
            for p in percentiles:
                value = momentum_data.quantile(p/100)
                logger.info(f"      {p}th percentile: {value:.6f}")
            
            # Momentum quality analysis
            logger.info(f"    Momentum quality analysis:")
            negative_momentum = (momentum_data < 0).sum()
            positive_momentum = (momentum_data > 0).sum()
            zero_momentum = (momentum_data == 0).sum()
            
            total_obs = len(momentum_data)
            logger.info(f"      Negative momentum: {negative_momentum} ({negative_momentum/total_obs*100:.1f}%)")
            logger.info(f"      Positive momentum: {positive_momentum} ({positive_momentum/total_obs*100:.1f}%)")
            logger.info(f"      Zero momentum: {zero_momentum} ({zero_momentum/total_obs*100:.1f}%)")
    
    # Customer segment analysis
    segment_columns = [col for col in data.columns if 'segment' in col.lower() or 'category' in col.lower() or 'type' in col.lower()]
    for col in segment_columns:
        if col in data.columns:
            segment_data = data[col].dropna()
            if len(segment_data) > 0:
                logger.info(f"  {col} analysis:")
                logger.info(f"    Non-missing observations: {len(segment_data)}")
                logger.info(f"    Missing observations: {data[col].isna().sum()}")
                
                # Segment distribution
                segment_counts = segment_data.value_counts()
                logger.info(f"    Segment distribution:")
                for segment, count in segment_counts.items():
                    percentage = count / len(segment_data) * 100
                    logger.info(f"      {segment}: {count} ({percentage:.1f}%)")
    
    # Sales/revenue analysis
    sales_columns = [col for col in data.columns if 'sales' in col.lower() or 'revenue' in col.lower() or 'amount' in col.lower()]
    for col in sales_columns:
        sales_data = data[col].dropna()
        if len(sales_data) > 0:
            logger.info(f"  {col} analysis:")
            logger.info(f"    Non-missing observations: {len(sales_data)}")
            logger.info(f"    Missing observations: {data[col].isna().sum()}")
            
            # Sales statistics
            mean_sales = sales_data.mean()
            median_sales = sales_data.median()
            std_sales = sales_data.std()
            min_sales = sales_data.min()
            max_sales = sales_data.max()
            
            logger.info(f"    Mean sales: {mean_sales:.2f}")
            logger.info(f"    Median sales: {median_sales:.2f}")
            logger.info(f"    Std sales: {std_sales:.2f}")
            logger.info(f"    Range: [{min_sales:.2f}, {max_sales:.2f}]")
    
    # Data quality checks
    logger.info("  Data quality checks:")
    
    # Check for missing values
    missing_analysis = data.isnull().sum()
    logger.info("    Missing values per column:")
    for column, missing_count in missing_analysis.items():
        percentage = missing_count / len(data) * 100
        logger.info(f"      {column}: {missing_count} ({percentage:.1f}%)")
    
    # Check for duplicate records
    if len(data.columns) >= 2:
        # Try to identify key columns for duplicate checking
        key_columns = []
        for col in data.columns:
            if any(keyword in col.lower() for keyword in ['gvkey', 'permno', 'customer', 'time_avail_m']):
                key_columns.append(col)
        
        if key_columns:
            duplicates = data.duplicated(subset=key_columns).sum()
            logger.info(f"    Duplicate records based on {key_columns}: {duplicates}")
    
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
    logger.info("    - Original data: Customer momentum data from R script processing")
    logger.info("    - R script: ZKR_CustomerSegments.R processes customer segment data")
    logger.info("    - Time processing: time_avail_m converted to monthly periods")
    logger.info("    - Usage: Customer momentum and segment analysis")
    
    # Customer momentum explanation
    logger.info("  Customer momentum explanation:")
    logger.info("    Customer Momentum Data:")
    logger.info("    - Measures momentum in customer relationships")
    logger.info("    - Based on customer segment analysis")
    logger.info("    - Captures customer relationship momentum")
    logger.info("    - Used in customer analysis and relationship research")
    logger.info("    - Important for customer relationship management")
    
    # Customer applications
    logger.info("  Customer applications:")
    logger.info("    - Customer relationship analysis")
    logger.info("    - Customer segment momentum")
    logger.info("    - Customer relationship management")
    logger.info("    - Customer profitability analysis")
    logger.info("    - Customer retention research")

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the processing function
    zk_customermomentum()
