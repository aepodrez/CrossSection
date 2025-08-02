"""
Python equivalent of ZI_PatentCitations.do
Generated from: ZI_PatentCitations.do

Original Stata file: ZI_PatentCitations.do
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

def zi_patentcitations():
    """
    Python equivalent of ZI_PatentCitations.do
    
    Calls R script to process patent citation data and verifies output
    Note: Requires R script ZIR_Patents.R to be available
    """
    logger.info("Processing patent citation data via R script...")
    
    try:
        # Define paths
        project_path = Path("/Users/alexpodrez/Documents/CrossSection")
        r_script_path = project_path / "Signals" / "Code" / "DataDownloads" / "ZIR_Patents.R"
        output_file_path = project_path / "Signals" / "Data" / "Intermediate" / "PatentDataProcessed.csv"
        
        logger.info(f"R script path: {r_script_path}")
        logger.info(f"Expected output path: {output_file_path}")
        
        # Check if R script exists
        if not r_script_path.exists():
            logger.error(f"R script not found: {r_script_path}")
            logger.error("Please ensure ZIR_Patents.R is available in the DataDownloads directory")
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
        
        # Verify output file exists (equivalent to Stata's "confirm file")
        logger.info("Verifying output file...")
        
        # Check for both .dta and .csv formats (R might output either)
        possible_output_files = [
            output_file_path,
            output_file_path.with_suffix('.dta'),
            project_path / "Signals" / "Data" / "Intermediate" / "PatentDataProcessed.dta"
        ]
        
        output_file = None
        for file_path in possible_output_files:
            if file_path.exists():
                output_file = file_path
                break
        
        if output_file is None:
            logger.error("Output file not found. Expected one of:")
            for file_path in possible_output_files:
                logger.error(f"  {file_path}")
            logger.error("R script may have failed to generate output file")
            return False
        
        logger.info(f"Output file confirmed: {output_file}")
        
        # If output is CSV, load and analyze it
        if output_file.suffix == '.csv':
            try:
                data = pd.read_csv(output_file)
                logger.info(f"Successfully loaded patent data: {len(data)} records")
                
                # Log comprehensive summary statistics
                log_patent_summary(data)
                
                # Also save to main data directory for compatibility
                main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/PatentDataProcessed.csv")
                data.to_csv(main_output_path, index=False)
                logger.info(f"Saved to main data directory: {main_output_path}")
                
            except Exception as e:
                logger.error(f"Failed to load patent data: {e}")
                return False
        
        logger.info("Successfully processed patent citation data")
        logger.info("Note: Patent citation data processed via R script")
        return True
        
    except Exception as e:
        logger.error(f"Failed to process patent citation data: {e}")
        return False

def log_patent_summary(data):
    """Log comprehensive summary statistics for patent data"""
    logger.info("Patent citation data summary:")
    logger.info(f"  Total records: {len(data)}")
    
    # Display column information
    logger.info(f"  Columns: {list(data.columns)}")
    logger.info(f"  Shape: {data.shape}")
    
    # Time range analysis
    time_columns = [col for col in data.columns if 'date' in col.lower() or 'year' in col.lower() or 'time' in col.lower()]
    for col in time_columns:
        try:
            if data[col].dtype == 'object':
                # Try to convert to datetime
                data[col] = pd.to_datetime(data[col], errors='coerce')
            
            if pd.api.types.is_datetime64_any_dtype(data[col]):
                min_date = data[col].min()
                max_date = data[col].max()
                logger.info(f"  {col} range: {min_date} to {max_date}")
            elif pd.api.types.is_numeric_dtype(data[col]):
                min_val = data[col].min()
                max_val = data[col].max()
                logger.info(f"  {col} range: {min_val} to {max_val}")
        except Exception as e:
            logger.warning(f"  Could not analyze {col}: {e}")
    
    # Company/patent analysis
    id_columns = [col for col in data.columns if 'id' in col.lower() or 'permno' in col.lower() or 'gvkey' in col.lower()]
    for col in id_columns:
        unique_count = data[col].nunique()
        logger.info(f"  Unique {col}: {unique_count}")
        
        # Show distribution of most common values
        value_counts = data[col].value_counts().head(10)
        logger.info(f"  {col} with most records:")
        for value, count in value_counts.items():
            percentage = count / len(data) * 100
            logger.info(f"    {value}: {count} ({percentage:.1f}%)")
    
    # Patent-specific analysis
    if 'citations' in data.columns or 'citation' in data.columns:
        citation_col = 'citations' if 'citations' in data.columns else 'citation'
        citation_data = data[citation_col].dropna()
        if len(citation_data) > 0:
            logger.info(f"  Patent citation analysis:")
            logger.info(f"    Non-missing citations: {len(citation_data)}")
            logger.info(f"    Missing citations: {data[citation_col].isna().sum()}")
            
            # Citation statistics
            mean_citations = citation_data.mean()
            median_citations = citation_data.median()
            std_citations = citation_data.std()
            min_citations = citation_data.min()
            max_citations = citation_data.max()
            
            logger.info(f"    Mean citations: {mean_citations:.2f}")
            logger.info(f"    Median citations: {median_citations:.2f}")
            logger.info(f"    Std citations: {std_citations:.2f}")
            logger.info(f"    Range: [{min_citations:.0f}, {max_citations:.0f}]")
            
            # Citation distribution
            logger.info(f"    Citation distribution:")
            citation_ranges = [
                (0, 0, "No citations"),
                (1, 5, "Low citations (1-5)"),
                (6, 20, "Medium citations (6-20)"),
                (21, 100, "High citations (21-100)"),
                (101, float('inf'), "Very high citations (>100)")
            ]
            
            for min_cite, max_cite, label in citation_ranges:
                if max_cite == float('inf'):
                    count = (citation_data >= min_cite).sum()
                else:
                    count = ((citation_data >= min_cite) & (citation_data <= max_cite)).sum()
                percentage = count / len(citation_data) * 100
                logger.info(f"      {label}: {count} ({percentage:.1f}%)")
    
    # Patent count analysis
    if 'patents' in data.columns or 'patent' in data.columns:
        patent_col = 'patents' if 'patents' in data.columns else 'patent'
        patent_data = data[patent_col].dropna()
        if len(patent_data) > 0:
            logger.info(f"  Patent count analysis:")
            logger.info(f"    Non-missing patent counts: {len(patent_data)}")
            
            # Patent count statistics
            mean_patents = patent_data.mean()
            median_patents = patent_data.median()
            logger.info(f"    Mean patents: {mean_patents:.2f}")
            logger.info(f"    Median patents: {median_patents:.2f}")
    
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
            if any(keyword in col.lower() for keyword in ['id', 'permno', 'gvkey', 'patent']):
                key_columns.append(col)
        
        if key_columns:
            duplicates = data.duplicated(subset=key_columns).sum()
            logger.info(f"    Duplicate records based on {key_columns}: {duplicates}")
    
    # Data processing explanation
    logger.info("  Data processing explanation:")
    logger.info("    - Original data: Patent citation data from R script processing")
    logger.info("    - R script: ZIR_Patents.R processes raw patent data")
    logger.info("    - Output: Processed patent citation data")
    logger.info("    - Usage: Patent analysis and innovation research")
    
    # Patent research explanation
    logger.info("  Patent research explanation:")
    logger.info("    Patent Citation Data:")
    logger.info("    - Measures innovation and knowledge spillovers")
    logger.info("    - Patent citations indicate knowledge flow")
    logger.info("    - Used in innovation and R&D research")
    logger.info("    - Important for technology and innovation studies")
    logger.info("    - Based on USPTO patent database")
    
    # Patent applications
    logger.info("  Patent applications:")
    logger.info("    - Innovation measurement")
    logger.info("    - R&D effectiveness analysis")
    logger.info("    - Knowledge spillover research")
    logger.info("    - Technology transfer studies")
    logger.info("    - Corporate innovation analysis")

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the processing function
    zi_patentcitations()
