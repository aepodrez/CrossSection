"""
Python equivalent of X2_CIQCreditRatings.do
Generated from: X2_CIQCreditRatings.do

Original Stata file: X2_CIQCreditRatings.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def x2_ciqcreditratings():
    """
    Python equivalent of X2_CIQCreditRatings.do
    
    Downloads and processes CIQ credit ratings from WRDS
    """
    logger.info("Downloading CIQ credit ratings from WRDS...")
    
    try:
        # Use global WRDS connection from master.py
        from master import wrds_conn
        if wrds_conn is None:
            logger.error("WRDS connection not available. Please run master.py")
            return False
        conn = wrds_conn
        
        # Download entity ratings
        logger.info("Downloading entity ratings...")
        entity_query = """
        SELECT DISTINCT 
            b.gvkey, b.ticker, a.ratingdate, a.ratingtime, 
            a.ratingactionword, a.currentratingsymbol, b.entity_id
        FROM ciq.wrds_erating a
        LEFT JOIN ciq.ratings_ids b
        ON a.entity_id = b.entity_id
        WHERE b.gvkey IS NOT NULL
        AND a.ratingdate >= '1970-01-01'
        """
        
        entity_data = conn.raw_sql(entity_query, date_cols=['ratingdate'])
        entity_data['instrument_id'] = ""
        entity_data['security_id'] = ""
        logger.info(f"Downloaded {len(entity_data)} entity rating records")
        
        # Download instrument ratings
        logger.info("Downloading instrument ratings...")
        instrument_query = """
        SELECT DISTINCT 
            b.gvkey, b.ticker, a.ratingdate, a.ratingtime, 
            a.ratingactionword, a.currentratingsymbol, b.instrument_id
        FROM ciq.wrds_irating a
        LEFT JOIN ciq.ratings_ids b
        ON a.instrument_id = b.instrument_id
        WHERE b.gvkey IS NOT NULL
        AND a.ratingdate >= '1970-01-01'
        """
        
        instrument_data = conn.raw_sql(instrument_query, date_cols=['ratingdate'])
        instrument_data['entity_id'] = ""
        instrument_data['security_id'] = ""
        logger.info(f"Downloaded {len(instrument_data)} instrument rating records")
        
        # Download security ratings
        logger.info("Downloading security ratings...")
        security_query = """
        SELECT DISTINCT 
            b.gvkey, b.ticker, a.ratingdate, a.ratingtime, 
            a.ratingactionword, a.currentratingsymbol, b.security_id
        FROM ciq.wrds_srating a
        LEFT JOIN ciq.ratings_ids b
        ON a.security_id = b.security_id
        WHERE b.gvkey IS NOT NULL
        AND a.ratingdate >= '1970-01-01'
        """
        
        security_data = conn.raw_sql(security_query, date_cols=['ratingdate'])
        security_data['entity_id'] = ""
        security_data['instrument_id'] = ""
        logger.info(f"Downloaded {len(security_data)} security rating records")
        
        # Append all datasets
        logger.info("Appending entity, instrument, and security ratings...")
        combined_data = pd.concat([entity_data, instrument_data, security_data], ignore_index=True)
        logger.info(f"Combined data has {len(combined_data)} records")
        
        # Drop "Not Rated" actions
        if 'ratingactionword' in combined_data.columns:
            combined_data = combined_data[combined_data['ratingactionword'] != 'Not Rated']
            logger.info(f"After dropping 'Not Rated' actions: {len(combined_data)} records")
        
        # Rank the sources (entity > instrument > security)
        combined_data['source'] = 3  # Default to security (lowest priority)
        combined_data.loc[combined_data['entity_id'] != '', 'source'] = 1  # Entity (highest priority)
        combined_data.loc[combined_data['instrument_id'] != '', 'source'] = 2  # Instrument (medium priority)
        
        # For each time, keep the best source
        combined_data = combined_data.sort_values(['gvkey', 'ratingdate', 'ratingtime', 'source'])
        combined_data = combined_data.drop_duplicates(subset=['gvkey', 'ratingdate', 'ratingtime'], keep='first')
        logger.info(f"After keeping best source per gvkey-date-time: {len(combined_data)} records")
        
        # Create time_avail_m (month of ratingdate)
        combined_data['time_avail_m'] = combined_data['ratingdate'].dt.to_period('M')
        
        # Remove duplicates by keeping last rating time each year-month
        combined_data = combined_data.sort_values(['gvkey', 'time_avail_m', 'ratingdate', 'ratingtime'], ascending=[True, True, False, False])
        combined_data = combined_data.drop_duplicates(subset=['gvkey', 'time_avail_m'], keep='first')
        logger.info(f"After keeping last rating per gvkey-month: {len(combined_data)} records")
        
        # Convert gvkey to numeric
        combined_data['gvkey'] = pd.to_numeric(combined_data['gvkey'], errors='coerce')
        
        # Keep only essential columns
        final_columns = ['gvkey', 'ticker', 'ratingdate', 'ratingtime', 'ratingactionword', 'currentratingsymbol', 'time_avail_m']
        final_data = combined_data[final_columns].copy()
        
        # Save to intermediate file
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_CIQ_creditratings.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        final_data.to_csv(output_path, index=False)
        logger.info(f"Saved CIQ credit ratings to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/m_CIQ_creditratings.csv")
        final_data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log some summary statistics
        logger.info("CIQ credit ratings summary:")
        logger.info(f"  Total records: {len(final_data)}")
        logger.info(f"  Time range: {final_data['time_avail_m'].min()} to {final_data['time_avail_m'].max()}")
        
        # Check data availability
        if 'currentratingsymbol' in final_data.columns:
            non_missing = final_data['currentratingsymbol'].notna().sum()
            missing = final_data['currentratingsymbol'].isna().sum()
            logger.info(f"  Credit rating availability: {non_missing} non-missing, {missing} missing")
            
            # Credit rating distribution analysis
            rating_data = final_data['currentratingsymbol'].dropna()
            if len(rating_data) > 0:
                logger.info("  Credit rating distribution:")
                
                # Count unique ratings
                unique_ratings = rating_data.nunique()
                logger.info(f"    Unique rating symbols: {unique_ratings}")
                
                # Show top 10 most common ratings
                rating_counts = rating_data.value_counts().head(10)
                logger.info("    Top 10 most common ratings:")
                for rating, count in rating_counts.items():
                    percentage = count / len(rating_data) * 100
                    logger.info(f"      {rating}: {count} ({percentage:.1f}%)")
                
                # Rating action analysis
                if 'ratingactionword' in final_data.columns:
                    action_data = final_data['ratingactionword'].dropna()
                    if len(action_data) > 0:
                        logger.info("  Rating action analysis:")
                        action_counts = action_data.value_counts().head(10)
                        logger.info("    Top 10 rating actions:")
                        for action, count in action_counts.items():
                            percentage = count / len(action_data) * 100
                            logger.info(f"      {action}: {count} ({percentage:.1f}%)")
        
        # Monthly frequency analysis
        logger.info("  Monthly frequency analysis:")
        logger.info(f"    Total months: {len(final_data)}")
        logger.info(f"    Years covered: {final_data['time_avail_m'].dt.year.max() - final_data['time_avail_m'].dt.year.min() + 1}")
        
        # Check for any gaps in monthly data
        expected_months = pd.date_range(
            start=final_data['time_avail_m'].min().to_timestamp(),
            end=final_data['time_avail_m'].max().to_timestamp(),
            freq='M'
        )
        actual_months = final_data['time_avail_m'].dt.to_timestamp().sort_values()
        missing_months = set(expected_months) - set(actual_months)
        
        if missing_months:
            logger.warning(f"    Missing months: {len(missing_months)}")
            logger.warning(f"    First few missing months: {sorted(list(missing_months))[:5]}")
        else:
            logger.info("    No missing months detected")
        
        # Company analysis
        if 'gvkey' in final_data.columns:
            unique_companies = final_data['gvkey'].nunique()
            logger.info(f"  Unique companies: {unique_companies}")
            
            # Companies with most rating records
            company_counts = final_data['gvkey'].value_counts().head(10)
            logger.info(f"  Companies with most rating records:")
            for gvkey, count in company_counts.items():
                logger.info(f"    Company {gvkey}: {count} rating records")
        
        # Data processing explanation
        logger.info("  Data processing explanation:")
        logger.info("    - Original data: CIQ credit ratings from WRDS")
        logger.info("    - Sources: Entity, instrument, and security ratings")
        logger.info("    - Priority: Entity > Instrument > Security")
        logger.info("    - Deduplication: Best source per gvkey-date-time")
        logger.info("    - Time aggregation: Monthly frequency with last rating per month")
        logger.info("    - Usage: Alternative credit rating data source")
        
        # CIQ credit rating interpretation
        logger.info("  CIQ credit rating interpretation:")
        logger.info("    CIQ Credit Ratings:")
        logger.info("    - Alternative to S&P credit ratings")
        logger.info("    - Multiple rating sources: Entity, instrument, security")
        logger.info("    - Entity ratings: Company-level credit ratings")
        logger.info("    - Instrument ratings: Specific debt instrument ratings")
        logger.info("    - Security ratings: Specific security ratings")
        logger.info("    - Used as alternative credit rating data source")
        
        logger.info("Successfully downloaded and processed CIQ credit ratings")
        logger.info("Note: Monthly CIQ credit ratings for alternative credit risk analysis")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download CIQ credit ratings: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the download function
    x2_ciqcreditratings()
