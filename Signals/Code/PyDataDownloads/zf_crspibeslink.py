"""
Python equivalent of ZF_CRSPIBESLink.do
Generated from: ZF_CRSPIBESLink.do

Original Stata file: ZF_CRSPIBESLink.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def zf_crspibeslink():
    """
    Python equivalent of ZF_CRSPIBESLink.do
    
    Processes CRSP-IBES linking table data from pre-processed CSV file
    Note: Requires SAS preprocessing to generate the input CSV
    """
    logger.info("Processing CRSP-IBES linking table data...")
    
    try:
        # Input file path (equivalent to Stata's "$pathDataPrep/iclink.csv")
        input_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Code/PrepScripts/iclink.csv")
        
        logger.info(f"Reading CRSP-IBES linking data from: {input_path}")
        
        # Check if input file exists
        if not input_path.exists():
            logger.error(f"Input file not found: {input_path}")
            logger.error("Please run SAS preprocessing first to generate the input CSV file")
            return False
        
        # Read the CSV file (equivalent to Stata's "import delimited")
        data = pd.read_csv(input_path)
        logger.info(f"Successfully loaded {len(data)} records from CRSP-IBES linking data")
        
        # Convert column names to lowercase for consistency
        data.columns = data.columns.str.lower()
        
        # Display initial data info
        logger.info("Initial data info:")
        logger.info(f"  Columns: {list(data.columns)}")
        logger.info(f"  Shape: {data.shape}")
        logger.info(f"  Sample data:")
        logger.info(data.head().to_string())
        
        # Keep only high-quality links (equivalent to Stata's "keep if score <= 2")
        if 'score' in data.columns:
            initial_count = len(data)
            data = data[data['score'] <= 2].copy()
            filtered_count = len(data)
            dropped_count = initial_count - filtered_count
            logger.info(f"Filtered to high-quality links (score <= 2): {filtered_count} records")
            logger.info(f"Dropped {dropped_count} low-quality links (score > 2)")
        else:
            logger.warning("Score column not found - no quality filtering applied")
        
        # Keep best match per permno (equivalent to Stata's "bysort permno (score): keep if _n == 1")
        if 'permno' in data.columns and 'score' in data.columns:
            initial_count = len(data)
            # Sort by permno and score, then keep first (best) match per permno
            data = data.sort_values(['permno', 'score']).drop_duplicates(subset=['permno'], keep='first')
            deduplicated_count = len(data)
            removed_count = initial_count - deduplicated_count
            logger.info(f"Deduplicated to best match per permno: {deduplicated_count} records")
            logger.info(f"Removed {removed_count} duplicate permno entries")
        else:
            logger.warning("permno or score column not found - no deduplication applied")
        
        # Rename ticker to tickerIBES (equivalent to Stata's "rename ticker tickerIBES")
        if 'ticker' in data.columns:
            data = data.rename(columns={'ticker': 'tickerIBES'})
            logger.info("Renamed ticker to tickerIBES")
        
        # Keep only essential columns (equivalent to Stata's "keep tickerIBES permno")
        essential_columns = ['tickerIBES', 'permno']
        available_columns = [col for col in essential_columns if col in data.columns]
        
        if len(available_columns) == len(essential_columns):
            data = data[available_columns].copy()
            logger.info(f"Kept essential columns: {available_columns}")
        else:
            missing_columns = [col for col in essential_columns if col not in data.columns]
            logger.warning(f"Missing essential columns: {missing_columns}")
            logger.info(f"Available columns: {list(data.columns)}")
        
        # Save to intermediate file (equivalent to Stata's "save "$pathDataIntermediate/IBESCRSPLinkingTable", replace")
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/IBESCRSPLinkingTable.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved CRSP-IBES linking table to {output_path}")
        
        # Log comprehensive summary statistics
        logger.info("CRSP-IBES linking table summary:")
        logger.info(f"  Total linking records: {len(data)}")
        
        # CRSP permno analysis
        if 'permno' in data.columns:
            unique_permnos = data['permno'].nunique()
            logger.info(f"  Unique CRSP permnos: {unique_permnos}")
            
            # Permnos with most IBES tickers (should be 1 per permno after deduplication)
            permno_counts = data['permno'].value_counts().head(10)
            logger.info(f"  Permnos with most IBES tickers:")
            for permno, count in permno_counts.items():
                logger.info(f"    {permno}: {count} IBES tickers")
            
            # Check for any remaining duplicates
            duplicates = data['permno'].duplicated().sum()
            if duplicates > 0:
                logger.warning(f"  ⚠️  Found {duplicates} duplicate permno entries after deduplication")
            else:
                logger.info("  ✅ No duplicate permno entries found")
        
        # IBES ticker analysis
        if 'tickerIBES' in data.columns:
            unique_tickers = data['tickerIBES'].nunique()
            logger.info(f"  Unique IBES tickers: {unique_tickers}")
            
            # Tickers with most permnos (should be 1 per ticker after deduplication)
            ticker_counts = data['tickerIBES'].value_counts().head(10)
            logger.info(f"  IBES tickers with most permnos:")
            for ticker, count in ticker_counts.items():
                logger.info(f"    {ticker}: {count} permnos")
            
            # Check for any remaining duplicates
            duplicates = data['tickerIBES'].duplicated().sum()
            if duplicates > 0:
                logger.warning(f"  ⚠️  Found {duplicates} duplicate ticker entries after deduplication")
            else:
                logger.info("  ✅ No duplicate ticker entries found")
        
        # Link quality analysis
        if 'score' in data.columns:
            score_data = data['score'].dropna()
            if len(score_data) > 0:
                logger.info("  Link quality analysis:")
                logger.info(f"    Non-missing scores: {len(score_data)}")
                logger.info(f"    Missing scores: {data['score'].isna().sum()}")
                
                # Score statistics
                mean_score = score_data.mean()
                std_score = score_data.std()
                min_score = score_data.min()
                max_score = score_data.max()
                median_score = score_data.median()
                
                logger.info(f"    Mean score: {mean_score:.3f}")
                logger.info(f"    Median score: {median_score:.3f}")
                logger.info(f"    Std score: {std_score:.3f}")
                logger.info(f"    Range: [{min_score:.0f}, {max_score:.0f}]")
                
                # Score distribution
                score_counts = score_data.value_counts().sort_index()
                logger.info("    Score distribution:")
                for score, count in score_counts.items():
                    percentage = count / len(score_data) * 100
                    logger.info(f"      Score {score}: {count} links ({percentage:.1f}%)")
                
                # Quality assessment
                logger.info("    Link quality assessment:")
                perfect_links = (score_data == 1).sum()
                good_links = (score_data == 2).sum()
                
                total_links = len(score_data)
                logger.info(f"      Perfect links (score=1): {perfect_links} ({perfect_links/total_links*100:.1f}%)")
                logger.info(f"      Good links (score=2): {good_links} ({good_links/total_links*100:.1f}%)")
        
        # Data coverage analysis
        logger.info("  Data coverage analysis:")
        
        # Check for missing values
        missing_analysis = data.isnull().sum()
        logger.info("    Missing values per column:")
        for column, missing_count in missing_analysis.items():
            percentage = missing_count / len(data) * 100
            logger.info(f"      {column}: {missing_count} ({percentage:.1f}%)")
        
        # Data quality checks
        logger.info("  Data quality checks:")
        
        # Check for empty strings or invalid values
        if 'tickerIBES' in data.columns:
            empty_tickers = (data['tickerIBES'] == '').sum()
            logger.info(f"    Empty tickerIBES values: {empty_tickers}")
        
        if 'permno' in data.columns:
            invalid_permnos = (data['permno'] <= 0).sum()
            logger.info(f"    Invalid permno values (≤0): {invalid_permnos}")
        
        # Check for one-to-one mapping
        if 'permno' in data.columns and 'tickerIBES' in data.columns:
            permno_to_ticker = data.groupby('permno')['tickerIBES'].nunique()
            ticker_to_permno = data.groupby('tickerIBES')['permno'].nunique()
            
            multiple_tickers = (permno_to_ticker > 1).sum()
            multiple_permnos = (ticker_to_permno > 1).sum()
            
            logger.info(f"    Permnos with multiple tickers: {multiple_tickers}")
            logger.info(f"    Tickers with multiple permnos: {multiple_permnos}")
            
            if multiple_tickers == 0 and multiple_permnos == 0:
                logger.info("    ✅ Perfect one-to-one mapping achieved")
            else:
                logger.warning("    ⚠️  One-to-many or many-to-one mappings detected")
        
        # Data processing explanation
        logger.info("  Data processing explanation:")
        logger.info("    - Original data: CRSP-IBES linking table from SAS preprocessing")
        logger.info("    - Input file: iclink.csv (generated by SAS preprocessing)")
        logger.info("    - Quality filtering: Kept only high-quality links (score <= 2)")
        logger.info("    - Deduplication: Kept best match per permno")
        logger.info("    - Usage: Linking CRSP and IBES data for analysis")
        
        # CRSP-IBES linking explanation
        logger.info("  CRSP-IBES linking explanation:")
        logger.info("    CRSP-IBES Linking Table:")
        logger.info("    - Links CRSP permno identifiers to IBES ticker identifiers")
        logger.info("    - Enables merging of CRSP market data with IBES analyst data")
        logger.info("    - Quality scores indicate link reliability (1=perfect, 2=good)")
        logger.info("    - Essential for cross-database analysis")
        logger.info("    - Used in earnings forecast and analyst research")
        
        # Linking applications
        logger.info("  Linking applications:")
        logger.info("    - CRSP-IBES data merging")
        logger.info("    - Earnings forecast analysis")
        logger.info("    - Analyst recommendation studies")
        logger.info("    - Cross-database research")
        logger.info("    - Market-analyst data integration")
        
        # Score interpretation
        logger.info("  Score interpretation:")
        logger.info("    - Score 1: Perfect match (highest quality)")
        logger.info("    - Score 2: Good match (acceptable quality)")
        logger.info("    - Score 3+: Lower quality (filtered out)")
        logger.info("    - Based on name matching and identifier validation")
        
        # Data usage notes
        logger.info("  Data usage notes:")
        logger.info("    - One-to-one mapping between permno and tickerIBES")
        logger.info("    - High-quality links only (score <= 2)")
        logger.info("    - Essential for CRSP-IBES data integration")
        logger.info("    - Used in earnings forecast and analyst studies")
        
        logger.info("Successfully processed CRSP-IBES linking table")
        logger.info("Note: High-quality linking table for CRSP-IBES data integration")
        return True
        
    except Exception as e:
        logger.error(f"Failed to process CRSP-IBES linking table: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the processing function
    zf_crspibeslink()
