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
        
        # Download the Excel file with fallback mechanism (equivalent to Stata's capture block)
        data = None
        temp_file_path = None
        
        try:
            # First attempt: direct download and import
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            # Create a temporary file to store the downloaded data
            with tempfile.NamedTemporaryFile(mode='wb', suffix='.xlsx', delete=False) as temp_file:
                temp_file.write(response.content)
                temp_file_path = temp_file.name
            
            # Read the Excel file
            data = pd.read_excel(temp_file_path)
            logger.info(f"Successfully downloaded {len(data)} IPO records")
            
        except Exception as e:
            logger.warning(f"Direct download failed: {e}")
            
            # Second attempt: download to intermediate directory (equivalent to Stata's shell wget)
            try:
                intermediate_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate")
                intermediate_dir.mkdir(parents=True, exist_ok=True)
                temp_file_path = intermediate_dir / "deleteme.xlsx"
                
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                
                with open(temp_file_path, 'wb') as f:
                    f.write(response.content)
                
                data = pd.read_excel(temp_file_path)
                logger.info(f"Successfully downloaded via intermediate file: {len(data)} IPO records")
                
            except Exception as e2:
                logger.error(f"Both download methods failed: {e2}")
                return False
        
        # Clean up temporary file
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
            except:
                pass
        
        if data is None or len(data) == 0:
            logger.error("No data downloaded")
            return False
        
        # Rename columns to match original Stata logic exactly
        # rename Founding FoundingYear
        if 'Founding' in data.columns:
            data = data.rename(columns={'Founding': 'FoundingYear'})
            logger.info("Renamed 'Founding' to 'FoundingYear'")
        
        # rename offerdate OfferDate (variable name change in 2024 version)
        if 'offerdate' in data.columns:
            data = data.rename(columns={'offerdate': 'OfferDate'})
            logger.info("Renamed 'offerdate' to 'OfferDate'")
        elif 'Offerdate' in data.columns:
            data = data.rename(columns={'Offerdate': 'OfferDate'})
            logger.info("Renamed 'Offerdate' to 'OfferDate'")
        elif 'offer date' in data.columns:
            data = data.rename(columns={'offer date': 'OfferDate'})
            logger.info("Renamed 'offer date' to 'OfferDate'")
        
        # rename CRSPperm permno (var name change in 2024 version)
        if 'CRSPperm' in data.columns:
            data = data.rename(columns={'CRSPperm': 'permno'})
            logger.info("Renamed 'CRSPperm' to 'permno'")
        elif 'CRSPpermanentID' in data.columns:
            data = data.rename(columns={'CRSPpermanentID': 'permno'})
            logger.info("Renamed 'CRSPpermanentID' to 'permno'")
        elif 'CRSP Perm' in data.columns:
            data = data.rename(columns={'CRSP Perm': 'permno'})
            logger.info("Renamed 'CRSP Perm' to 'permno'")
        
        # destring permno, replace
        if 'permno' in data.columns:
            data['permno'] = pd.to_numeric(data['permno'], errors='coerce')
            logger.info("Converted permno to numeric")
        
        # Convert OfferDate to IPOdate (month of offer date)
        if 'OfferDate' in data.columns:
            # tostring OfferDate, gen(temp)
            data['temp'] = data['OfferDate'].astype(str)
            
            # gen temp2 = date(temp, "YMD")
            data['temp2'] = pd.to_datetime(data['temp'], errors='coerce')
            
            # gen IPOdate = mofd(temp2)
            data['IPOdate'] = data['temp2'].dt.to_period('M')
            
            # Drop temporary columns
            data = data.drop(columns=['temp', 'temp2'])
            
            logger.info("Converted OfferDate to IPOdate (monthly)")
        
        # Keep only essential columns (equivalent to Stata's keep command)
        keep_columns = []
        if 'permno' in data.columns:
            keep_columns.append('permno')
        if 'FoundingYear' in data.columns:
            keep_columns.append('FoundingYear')
        if 'IPOdate' in data.columns:
            keep_columns.append('IPOdate')
        
        if not keep_columns:
            logger.error("None of the required columns (permno, FoundingYear, IPOdate) found in the data")
            logger.error(f"Available columns: {list(data.columns)}")
            return False
        
        data = data[keep_columns].copy()
        
        # Drop missing or invalid permno values (equivalent to Stata's drop if mi(permno) | permno == 999 | permno <= 0)
        if 'permno' in data.columns:
            data = data.dropna(subset=['permno'])
            data = data[data['permno'] != 999]
            data = data[data['permno'] > 0]
            logger.info(f"After dropping invalid permno values: {len(data)} records")
            
            # Keep only first observation per permno (equivalent to Stata's bys permno: keep if _n == 1)
            data = data.drop_duplicates(subset=['permno'], keep='first')
            logger.info(f"After keeping first observation per permno: {len(data)} records")
        else:
            logger.error("permno column not found in data")
            return False
        
        # Replace negative FoundingYear with missing (equivalent to Stata's replace FoundingYear = . if FoundingYear < 0)
        if 'FoundingYear' in data.columns:
            data.loc[data['FoundingYear'] < 0, 'FoundingYear'] = np.nan
            logger.info("Replaced negative FoundingYear values with missing")
        
        # Save to intermediate file (equivalent to Stata's save command)
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/IPODates.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved IPO dates to {output_path}")
        
        # Log summary statistics
        logger.info("IPO dates summary:")
        logger.info(f"  Total records: {len(data)}")
        
        # Time range analysis
        if 'IPOdate' in data.columns:
            min_date = data['IPOdate'].min()
            max_date = data['IPOdate'].max()
            logger.info(f"  IPO date range: {min_date} to {max_date}")
        
        # Founding year analysis
        if 'FoundingYear' in data.columns:
            founding_data = data['FoundingYear'].dropna()
            if len(founding_data) > 0:
                logger.info(f"  Records with founding year: {len(founding_data)} ({len(founding_data)/len(data)*100:.1f}%)")
                
                # Company age at IPO analysis (fix the pandas warning)
                data_with_age = data.dropna(subset=['FoundingYear', 'IPOdate']).copy()
                if len(data_with_age) > 0:
                    data_with_age['age_at_ipo'] = data_with_age['IPOdate'].dt.year - data_with_age['FoundingYear']
                    age_stats = data_with_age['age_at_ipo'].describe()
                    logger.info(f"  Average age at IPO: {age_stats['mean']:.1f} years")
                    logger.info(f"  Median age at IPO: {age_stats['50%']:.1f} years")
        
        # Permno analysis
        if 'permno' in data.columns:
            logger.info(f"  Unique companies: {data['permno'].nunique()}")
        
        logger.info("Successfully downloaded and processed IPO dates")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download IPO dates: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the download function
    za_ipodates()
