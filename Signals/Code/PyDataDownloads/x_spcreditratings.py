"""
Python equivalent of X_SPCreditRatings.do
Generated from: X_SPCreditRatings.do

Original Stata file: X_SPCreditRatings.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def x_spcreditratings():
    """
    Python equivalent of X_SPCreditRatings.do
    
    Downloads and processes S&P credit ratings from WRDS
    """
    logger.info("Downloading S&P credit ratings from WRDS...")
    
    try:
        # Use global WRDS connection from master.py
        from master import wrds_conn
        if wrds_conn is None:
            logger.error("WRDS connection not available. Please run master.py")
            return False
        conn = wrds_conn
        
        # SQL query from original Stata file
        query = """
        SELECT 
            gvkey, datadate, splticrm
        FROM comp.adsprate
        """
        
        # Execute query
        data = conn.raw_sql(query, date_cols=['datadate'])
        logger.info(f"Downloaded {len(data)} S&P credit rating records")
        
        # Create time_avail_m (month of datadate) - equivalent to Stata's "gen time_avail_m = mofd(datadate)"
        data['time_avail_m'] = data['datadate'].dt.to_period('M')
        
        # Drop datadate column
        data = data.drop(columns=['datadate'])
        
        # Create numerical rating mapping - equivalent to Stata's replace statements
        rating_mapping = {
            'D': 1,
            'C': 2,
            'CC': 3,
            'CCC-': 4,
            'CCC': 5,
            'CCC+': 6,
            'B-': 7,
            'B': 8,
            'B+': 9,
            'BB-': 10,
            'BB': 11,
            'BB+': 12,
            'BBB-': 13,
            'BBB': 14,
            'BBB+': 15,
            'A-': 16,
            'A': 17,
            'A+': 18,
            'AA-': 19,
            'AA': 20,
            'AA+': 21,
            'AAA': 22
        }
        
        # Create numerical rating column
        data['credrat'] = data['splticrm'].map(rating_mapping)
        
        # Fill missing values with 0 (equivalent to Stata's "gen credrat = 0")
        data['credrat'] = data['credrat'].fillna(0)
        
        # Drop splticrm column (equivalent to Stata's "drop sp")
        data = data.drop(columns=['splticrm'])
        
        # Convert gvkey to numeric (equivalent to Stata's "destring gvkey, replace")
        data['gvkey'] = pd.to_numeric(data['gvkey'], errors='coerce')
        
        # Save to intermediate file
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_SP_creditratings.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved S&P credit ratings to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/m_SP_creditratings.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log some summary statistics
        logger.info("S&P credit ratings summary:")
        logger.info(f"  Total records: {len(data)}")
        logger.info(f"  Time range: {data['time_avail_m'].min()} to {data['time_avail_m'].max()}")
        
        # Check data availability
        if 'credrat' in data.columns:
            non_missing = data['credrat'].notna().sum()
            missing = data['credrat'].isna().sum()
            logger.info(f"  Credit rating availability: {non_missing} non-missing, {missing} missing")
            
            # Credit rating summary statistics
            credrat_data = data['credrat'].dropna()
            if len(credrat_data) > 0:
                mean_val = credrat_data.mean()
                std_val = credrat_data.std()
                min_val = credrat_data.min()
                max_val = credrat_data.max()
                logger.info(f"  Credit rating statistics: mean={mean_val:.2f}, std={std_val:.2f}, range=[{min_val:.0f}, {max_val:.0f}]")
                
                # Credit rating distribution analysis
                logger.info("  Credit rating distribution:")
                
                # Define rating categories
                rating_categories = {
                    'Investment Grade (AAA-BBB)': (13, 22),
                    'Speculative Grade (BB-B)': (8, 12),
                    'Highly Speculative (CCC-C)': (2, 7),
                    'Default (D)': (1, 1),
                    'No Rating (0)': (0, 0)
                }
                
                for category, (min_rating, max_rating) in rating_categories.items():
                    count = ((credrat_data >= min_rating) & (credrat_data <= max_rating)).sum()
                    percentage = count / len(credrat_data) * 100
                    logger.info(f"    {category}: {count} ({percentage:.1f}%)")
                
                # Rating level analysis
                logger.info("  Rating level analysis:")
                rating_levels = {
                    22: 'AAA', 21: 'AA+', 20: 'AA', 19: 'AA-',
                    18: 'A+', 17: 'A', 16: 'A-',
                    15: 'BBB+', 14: 'BBB', 13: 'BBB-',
                    12: 'BB+', 11: 'BB', 10: 'BB-',
                    9: 'B+', 8: 'B', 7: 'B-',
                    6: 'CCC+', 5: 'CCC', 4: 'CCC-',
                    3: 'CC', 2: 'C', 1: 'D', 0: 'No Rating'
                }
                
                # Show top 10 most common ratings
                rating_counts = credrat_data.value_counts().head(10)
                logger.info("    Top 10 most common ratings:")
                for rating_num, count in rating_counts.items():
                    rating_name = rating_levels.get(rating_num, f'Rating {rating_num}')
                    percentage = count / len(credrat_data) * 100
                    logger.info(f"      {rating_name} ({rating_num}): {count} ({percentage:.1f}%)")
        
        # Monthly frequency analysis
        logger.info("  Monthly frequency analysis:")
        logger.info(f"    Total months: {len(data)}")
        logger.info(f"    Years covered: {data['time_avail_m'].dt.year.max() - data['time_avail_m'].dt.year.min() + 1}")
        
        # Check for any gaps in monthly data
        expected_months = pd.date_range(
            start=data['time_avail_m'].min().to_timestamp(),
            end=data['time_avail_m'].max().to_timestamp(),
            freq='M'
        )
        actual_months = data['time_avail_m'].dt.to_timestamp().sort_values()
        missing_months = set(expected_months) - set(actual_months)
        
        if missing_months:
            logger.warning(f"    Missing months: {len(missing_months)}")
            logger.warning(f"    First few missing months: {sorted(list(missing_months))[:5]}")
        else:
            logger.info("    No missing months detected")
        
        # Company analysis
        if 'gvkey' in data.columns:
            unique_companies = data['gvkey'].nunique()
            logger.info(f"  Unique companies: {unique_companies}")
            
            # Companies with most rating changes
            rating_changes = data.groupby('gvkey')['credrat'].nunique().sort_values(ascending=False)
            logger.info(f"  Companies with most rating changes:")
            for i, (gvkey, changes) in enumerate(rating_changes.head(5).items()):
                logger.info(f"    Company {gvkey}: {changes} rating changes")
        
        # Data processing explanation
        logger.info("  Data processing explanation:")
        logger.info("    - Original data: S&P credit ratings from Compustat")
        logger.info("    - Rating conversion: Text ratings converted to numerical scale (1-22)")
        logger.info("    - Time aggregation: Monthly frequency with datadate conversion")
        logger.info("    - Usage: Credit risk analysis and credit rating factor models")
        
        # Credit rating interpretation
        logger.info("  Credit rating interpretation:")
        logger.info("    S&P Credit Ratings:")
        logger.info("    - AAA to BBB-: Investment grade (lower default risk)")
        logger.info("    - BB+ to B-: Speculative grade (higher default risk)")
        logger.info("    - CCC+ to C: Highly speculative (very high default risk)")
        logger.info("    - D: Default")
        logger.info("    - 0: No rating available")
        logger.info("    - Higher numerical values = Better credit quality")
        
        logger.info("Successfully downloaded and processed S&P credit ratings")
        logger.info("Note: Monthly S&P credit ratings for credit risk analysis")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download S&P credit ratings: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the download function
    x_spcreditratings()