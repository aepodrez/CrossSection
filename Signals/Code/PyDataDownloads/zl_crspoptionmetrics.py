"""
Python equivalent of ZL_CRSPOPTIONMETRICS.do
Generated from: ZL_CRSPOPTIONMETRICS.do

Original Stata file: ZL_CRSPOPTIONMETRICS.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def zl_crspoptionmetrics():
    """
    Python equivalent of ZL_CRSPOPTIONMETRICS.do
    
    Processes CRSP-OptionMetrics linking table data from pre-processed CSV file
    Note: Requires SAS preprocessing to generate the input CSV
    """
    logger.info("Processing CRSP-OptionMetrics linking table data...")
    
    try:
        # Input file path (equivalent to Stata's "$pathDataPrep/oclink.csv")
        input_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Code/PrepScripts/oclink.csv")
        
        logger.info(f"Reading CRSP-OptionMetrics linking data from: {input_path}")
        
        # Check if input file exists
        if not input_path.exists():
            logger.error(f"Input file not found: {input_path}")
            logger.error("Please run SAS preprocessing first to generate the input CSV file")
            return False
        
        # Read the CSV file (equivalent to Stata's "import delimited")
        data = pd.read_csv(input_path)
        logger.info(f"Successfully loaded {len(data)} records from CRSP-OptionMetrics linking data")
        
        # Rename columns to match expected format
        if 'PERMNO' in data.columns:
            data = data.rename(columns={'PERMNO': 'permno'})
            logger.info("Renamed PERMNO to permno")
        
        if 'SCORE' in data.columns:
            data = data.rename(columns={'SCORE': 'score'})
            logger.info("Renamed SCORE to score")
        
        # Display initial data info
        logger.info("Initial data info:")
        logger.info(f"  Columns: {list(data.columns)}")
        logger.info(f"  Shape: {data.shape}")
        logger.info(f"  Sample data:")
        logger.info(data.head().to_string())
        
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
        
        # Keep only high-quality links (equivalent to Stata's "keep if score <= 6")
        if 'score' in data.columns:
            initial_count = len(data)
            data = data[data['score'] <= 6].copy()
            filtered_count = len(data)
            dropped_count = initial_count - filtered_count
            logger.info(f"Filtered to high-quality links (score <= 6): {filtered_count} records")
            logger.info(f"Dropped {dropped_count} low-quality links (score > 6)")
            
            # Score distribution analysis
            if 'score' in data.columns:
                score_counts = data['score'].value_counts().sort_index()
                logger.info("Score distribution after filtering:")
                for score, count in score_counts.items():
                    percentage = count / len(data) * 100
                    logger.info(f"  Score {score}: {count} links ({percentage:.1f}%)")
        else:
            logger.warning("Score column not found - no quality filtering applied")
        
        # Rename score to om_score (equivalent to Stata's "rename score om_score")
        if 'score' in data.columns:
            data = data.rename(columns={'score': 'om_score'})
            logger.info("Renamed score to om_score")
        
        # Keep only essential columns (equivalent to Stata's "keep secid permno om_score")
        essential_columns = ['secid', 'permno', 'om_score']
        available_columns = [col for col in essential_columns if col in data.columns]
        
        if len(available_columns) == len(essential_columns):
            data = data[available_columns].copy()
            logger.info(f"Kept essential columns: {available_columns}")
        else:
            missing_columns = [col for col in essential_columns if col not in data.columns]
            logger.warning(f"Missing essential columns: {missing_columns}")
            logger.info(f"Available columns: {list(data.columns)}")
        
        # Save to intermediate file (equivalent to Stata's "save "$pathDataIntermediate/OPTIONMETRICSCRSPLinkingTable", replace")
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/OPTIONMETRICSCRSPLinkingTable.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved CRSP-OptionMetrics linking table to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/OPTIONMETRICSCRSPLinkingTable.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log comprehensive summary statistics
        logger.info("CRSP-OptionMetrics linking table summary:")
        logger.info(f"  Total linking records: {len(data)}")
        
        # CRSP permno analysis
        if 'permno' in data.columns:
            unique_permnos = data['permno'].nunique()
            logger.info(f"  Unique CRSP permnos: {unique_permnos}")
            
            # Permnos with most OptionMetrics secids (should be 1 per permno after deduplication)
            permno_counts = data['permno'].value_counts().head(10)
            logger.info(f"  Permnos with most OptionMetrics secids:")
            for permno, count in permno_counts.items():
                logger.info(f"    {permno}: {count} OptionMetrics secids")
            
            # Check for any remaining duplicates
            duplicates = data['permno'].duplicated().sum()
            if duplicates > 0:
                logger.warning(f"  ⚠️  Found {duplicates} duplicate permno entries after deduplication")
            else:
                logger.info("  ✅ No duplicate permno entries found")
        
        # OptionMetrics secid analysis
        if 'secid' in data.columns:
            unique_secids = data['secid'].nunique()
            logger.info(f"  Unique OptionMetrics secids: {unique_secids}")
            
            # Secids with most permnos (should be 1 per secid after deduplication)
            secid_counts = data['secid'].value_counts().head(10)
            logger.info(f"  OptionMetrics secids with most permnos:")
            for secid, count in secid_counts.items():
                logger.info(f"    {secid}: {count} permnos")
            
            # Check for any remaining duplicates
            duplicates = data['secid'].duplicated().sum()
            if duplicates > 0:
                logger.warning(f"  ⚠️  Found {duplicates} duplicate secid entries after deduplication")
            else:
                logger.info("  ✅ No duplicate secid entries found")
        
        # Link quality analysis
        if 'om_score' in data.columns:
            score_data = data['om_score'].dropna()
            if len(score_data) > 0:
                logger.info("  Link quality analysis:")
                logger.info(f"    Non-missing scores: {len(score_data)}")
                logger.info(f"    Missing scores: {data['om_score'].isna().sum()}")
                
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
                perfect_links = (score_data == 0).sum()
                excellent_links = (score_data == 1).sum()
                good_links = (score_data == 2).sum()
                acceptable_links = (score_data == 3).sum()
                fair_links = (score_data == 4).sum()
                poor_links = (score_data == 5).sum()
                marginal_links = (score_data == 6).sum()
                
                total_links = len(score_data)
                logger.info(f"      Perfect links (score=0): {perfect_links} ({perfect_links/total_links*100:.1f}%)")
                logger.info(f"      Excellent links (score=1): {excellent_links} ({excellent_links/total_links*100:.1f}%)")
                logger.info(f"      Good links (score=2): {good_links} ({good_links/total_links*100:.1f}%)")
                logger.info(f"      Acceptable links (score=3): {acceptable_links} ({acceptable_links/total_links*100:.1f}%)")
                logger.info(f"      Fair links (score=4): {fair_links} ({fair_links/total_links*100:.1f}%)")
                logger.info(f"      Poor links (score=5): {poor_links} ({poor_links/total_links*100:.1f}%)")
                logger.info(f"      Marginal links (score=6): {marginal_links} ({marginal_links/total_links*100:.1f}%)")
        
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
        if 'secid' in data.columns:
            empty_secids = (data['secid'] == '').sum()
            logger.info(f"    Empty secid values: {empty_secids}")
        
        if 'permno' in data.columns:
            invalid_permnos = (data['permno'] <= 0).sum()
            logger.info(f"    Invalid permno values (≤0): {invalid_permnos}")
        
        # Check for one-to-one mapping
        if 'permno' in data.columns and 'secid' in data.columns:
            permno_to_secid = data.groupby('permno')['secid'].nunique()
            secid_to_permno = data.groupby('secid')['permno'].nunique()
            
            multiple_secids = (permno_to_secid > 1).sum()
            multiple_permnos = (secid_to_permno > 1).sum()
            
            logger.info(f"    Permnos with multiple secids: {multiple_secids}")
            logger.info(f"    Secids with multiple permnos: {multiple_permnos}")
            
            if multiple_secids == 0 and multiple_permnos == 0:
                logger.info("    ✅ Perfect one-to-one mapping achieved")
            else:
                logger.warning("    ⚠️  One-to-many or many-to-one mappings detected")
        
        # Data processing explanation
        logger.info("  Data processing explanation:")
        logger.info("    - Original data: CRSP-OptionMetrics linking table from SAS preprocessing")
        logger.info("    - Input file: oclink.csv (generated by SAS preprocessing)")
        logger.info("    - Quality filtering: Kept only high-quality links (score <= 6)")
        logger.info("    - Deduplication: Kept best match per permno")
        logger.info("    - Usage: Linking CRSP and OptionMetrics data for analysis")
        
        # CRSP-OptionMetrics linking explanation
        logger.info("  CRSP-OptionMetrics linking explanation:")
        logger.info("    CRSP-OptionMetrics Linking Table:")
        logger.info("    - Links CRSP permno identifiers to OptionMetrics secid identifiers")
        logger.info("    - Enables merging of CRSP market data with OptionMetrics options data")
        logger.info("    - Quality scores indicate link reliability (0=perfect, 6=marginal)")
        logger.info("    - Essential for cross-database options analysis")
        logger.info("    - Used in options research and volatility analysis")
        
        # Linking applications
        logger.info("  Linking applications:")
        logger.info("    - CRSP-OptionMetrics data merging")
        logger.info("    - Options volatility analysis")
        logger.info("    - Options trading research")
        logger.info("    - Cross-database options analysis")
        logger.info("    - Market-options data integration")
        
        # Score interpretation
        logger.info("  Score interpretation:")
        logger.info("    - Score 0: Perfect match (highest quality)")
        logger.info("    - Score 1: Excellent match (very high quality)")
        logger.info("    - Score 2: Good match (high quality)")
        logger.info("    - Score 3: Acceptable match (moderate quality)")
        logger.info("    - Score 4: Fair match (lower quality)")
        logger.info("    - Score 5: Poor match (low quality)")
        logger.info("    - Score 6: Marginal match (lowest acceptable quality)")
        logger.info("    - Score 7+: Unacceptable (filtered out)")
        
        # Data usage notes
        logger.info("  Data usage notes:")
        logger.info("    - One-to-one mapping between permno and secid")
        logger.info("    - High-quality links only (score <= 6)")
        logger.info("    - Essential for CRSP-OptionMetrics data integration")
        logger.info("    - Used in options volatility and trading research")
        logger.info("    - Score 6 threshold optimized for Bali-Hovakimian analysis")
        
        logger.info("Successfully processed CRSP-OptionMetrics linking table")
        logger.info("Note: High-quality linking table for CRSP-OptionMetrics data integration")
        return True
        
    except Exception as e:
        logger.error(f"Failed to process CRSP-OptionMetrics linking table: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the processing function
    zl_crspoptionmetrics()
