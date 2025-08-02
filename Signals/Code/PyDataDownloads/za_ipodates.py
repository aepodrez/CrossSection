"""
Python equivalent of ZA_IPODates.do
Generated from: ZA_IPODates.do

Original Stata file: ZA_IPODates.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime
import requests
import tempfile
import os

logger = logging.getLogger(__name__)

def za_ipodates():
    """
    Python equivalent of ZA_IPODates.do
    
    Downloads and processes IPO dates from Jay Ritter's website
    """
    logger.info("Downloading IPO dates from Jay Ritter's website...")
    
    try:
        # URL from original Stata file (most recent version)
        url = "https://site.warrington.ufl.edu/ritter/files/IPO-age.xlsx"
        
        logger.info(f"Downloading IPO data from: {url}")
        
        # Download the Excel file
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()  # Raise an exception for bad status codes
            
            # Create a temporary file to store the downloaded data
            with tempfile.NamedTemporaryFile(mode='wb', suffix='.xlsx', delete=False) as temp_file:
                temp_file.write(response.content)
                temp_file_path = temp_file.name
            
            # Read the Excel file
            data = pd.read_excel(temp_file_path)
            
            # Clean up temporary file
            os.unlink(temp_file_path)
            
            logger.info(f"Successfully downloaded {len(data)} IPO records")
            
        except requests.RequestException as e:
            logger.error(f"Failed to download IPO data: {e}")
            return False
        
        # Rename columns to match original Stata logic
        # Note: Column names may vary between versions, so we'll handle multiple possibilities
        column_mapping = {}
        
        # Handle Founding column
        if 'Founding' in data.columns:
            data = data.rename(columns={'Founding': 'FoundingYear'})
            logger.info("Renamed 'Founding' to 'FoundingYear'")
        
        # Handle OfferDate column (variable name changes between versions)
        if 'Offerdate' in data.columns:
            data = data.rename(columns={'Offerdate': 'OfferDate'})
            logger.info("Renamed 'Offerdate' to 'OfferDate'")
        elif 'offerdate' in data.columns:
            data = data.rename(columns={'offerdate': 'OfferDate'})
            logger.info("Renamed 'offerdate' to 'OfferDate'")
        
        # Handle permno column (variable name changes between versions)
        if 'CRSPpermanentID' in data.columns:
            data = data.rename(columns={'CRSPpermanentID': 'permno'})
            logger.info("Renamed 'CRSPpermanentID' to 'permno'")
        elif 'CRSPperm' in data.columns:
            data = data.rename(columns={'CRSPperm': 'permno'})
            logger.info("Renamed 'CRSPperm' to 'permno'")
        
        # Convert permno to numeric (equivalent to Stata's "destring permno, replace")
        if 'permno' in data.columns:
            data['permno'] = pd.to_numeric(data['permno'], errors='coerce')
            logger.info("Converted permno to numeric")
        
        # Convert OfferDate to IPOdate (month of offer date)
        if 'OfferDate' in data.columns:
            # Convert to string first (equivalent to Stata's "tostring OfferDate, gen(temp)")
            data['temp'] = data['OfferDate'].astype(str)
            
            # Convert to date (equivalent to Stata's "gen temp2 = date(temp, "YMD")")
            data['temp2'] = pd.to_datetime(data['temp'], errors='coerce')
            
            # Convert to month (equivalent to Stata's "gen IPOdate = mofd(temp2)")
            data['IPOdate'] = data['temp2'].dt.to_period('M')
            
            # Drop temporary columns
            data = data.drop(columns=['temp', 'temp2'])
            
            logger.info("Converted OfferDate to IPOdate (monthly)")
        
        # Keep only essential columns
        keep_columns = ['permno', 'FoundingYear', 'IPOdate']
        data = data[keep_columns].copy()
        
        # Drop missing or invalid permno values (equivalent to Stata's "drop if mi(permno) | permno == 999 | permno <= 0")
        data = data.dropna(subset=['permno'])
        data = data[data['permno'] != 999]
        data = data[data['permno'] > 0]
        logger.info(f"After dropping invalid permno values: {len(data)} records")
        
        # Keep only first observation per permno (equivalent to Stata's "bys permno: keep if _n == 1")
        data = data.drop_duplicates(subset=['permno'], keep='first')
        logger.info(f"After keeping first observation per permno: {len(data)} records")
        
        # Replace negative FoundingYear with missing (equivalent to Stata's "replace FoundingYear = . if FoundingYear < 0")
        if 'FoundingYear' in data.columns:
            data.loc[data['FoundingYear'] < 0, 'FoundingYear'] = np.nan
            logger.info("Replaced negative FoundingYear values with missing")
        
        # Save to intermediate file
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/IPODates.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved IPO dates to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/IPODates.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log some summary statistics
        logger.info("IPO dates summary:")
        logger.info(f"  Total records: {len(data)}")
        
        # Time range analysis
        if 'IPOdate' in data.columns:
            min_date = data['IPOdate'].min()
            max_date = data['IPOdate'].max()
            logger.info(f"  IPO date range: {min_date} to {max_date}")
            
            # IPO date distribution analysis
            logger.info("  IPO date distribution:")
            
            # Count IPOs by decade
            data['decade'] = data['IPOdate'].dt.year // 10 * 10
            decade_counts = data['decade'].value_counts().sort_index()
            logger.info("    IPOs by decade:")
            for decade, count in decade_counts.items():
                percentage = count / len(data) * 100
                logger.info(f"      {decade}s: {count} ({percentage:.1f}%)")
            
            # Recent IPO analysis
            recent_data = data[data['IPOdate'].dt.year >= 2010]
            logger.info(f"    Recent IPOs (2010+): {len(recent_data)} ({len(recent_data)/len(data)*100:.1f}%)")
        
        # Founding year analysis
        if 'FoundingYear' in data.columns:
            founding_data = data['FoundingYear'].dropna()
            if len(founding_data) > 0:
                logger.info("  Founding year analysis:")
                logger.info(f"    Records with founding year: {len(founding_data)} ({len(founding_data)/len(data)*100:.1f}%)")
                logger.info(f"    Founding year range: {founding_data.min():.0f} to {founding_data.max():.0f}")
                
                # Company age at IPO analysis
                data_with_age = data.dropna(subset=['FoundingYear', 'IPOdate'])
                if len(data_with_age) > 0:
                    data_with_age['age_at_ipo'] = data_with_age['IPOdate'].dt.year - data_with_age['FoundingYear']
                    age_stats = data_with_age['age_at_ipo'].describe()
                    logger.info(f"    Average age at IPO: {age_stats['mean']:.1f} years")
                    logger.info(f"    Median age at IPO: {age_stats['50%']:.1f} years")
                    logger.info(f"    Age range: {age_stats['min']:.0f} to {age_stats['max']:.0f} years")
                    
                    # Age distribution
                    young_companies = (data_with_age['age_at_ipo'] <= 5).sum()
                    medium_companies = ((data_with_age['age_at_ipo'] > 5) & (data_with_age['age_at_ipo'] <= 15)).sum()
                    old_companies = (data_with_age['age_at_ipo'] > 15).sum()
                    
                    total_with_age = len(data_with_age)
                    logger.info(f"    Young companies (≤5 years): {young_companies} ({young_companies/total_with_age*100:.1f}%)")
                    logger.info(f"    Medium companies (6-15 years): {medium_companies} ({medium_companies/total_with_age*100:.1f}%)")
                    logger.info(f"    Old companies (>15 years): {old_companies} ({old_companies/total_with_age*100:.1f}%)")
        
        # Permno analysis
        if 'permno' in data.columns:
            logger.info("  Permno analysis:")
            logger.info(f"    Unique companies: {data['permno'].nunique()}")
            logger.info(f"    Permno range: {data['permno'].min():.0f} to {data['permno'].max():.0f}")
        
        # Data quality analysis
        logger.info("  Data quality analysis:")
        missing_permno = data['permno'].isna().sum()
        missing_founding = data['FoundingYear'].isna().sum()
        missing_ipo = data['IPOdate'].isna().sum()
        
        logger.info(f"    Missing permno: {missing_permno}")
        logger.info(f"    Missing founding year: {missing_founding} ({missing_founding/len(data)*100:.1f}%)")
        logger.info(f"    Missing IPO date: {missing_ipo}")
        
        # Data processing explanation
        logger.info("  Data processing explanation:")
        logger.info("    - Original data: IPO dates from Jay Ritter's website")
        logger.info("    - Source: University of Florida IPO database")
        logger.info("    - Date conversion: Offer date converted to monthly IPO date")
        logger.info("    - Data cleaning: Invalid permno values removed")
        logger.info("    - Deduplication: First observation per permno kept")
        logger.info("    - Usage: IPO analysis and company age calculations")
        
        # IPO data interpretation
        logger.info("  IPO data interpretation:")
        logger.info("    IPO Dates Dataset:")
        logger.info("    - Comprehensive IPO database from Jay Ritter")
        logger.info("    - Includes founding dates and IPO dates")
        logger.info("    - Used for IPO analysis and company age studies")
        logger.info("    - Covers US IPOs from 1975 onwards")
        logger.info("    - Includes CRSP permno for linking to market data")
        
        logger.info("Successfully downloaded and processed IPO dates")
        logger.info("Note: IPO dates and founding years for IPO analysis and company age studies")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download IPO dates: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the download function
    za_ipodates()
