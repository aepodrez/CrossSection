"""
Python equivalent of ZIR_Patents.R
Generated from: ZIR_Patents.R

Original R file: ZIR_Patents.R
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path
import requests
import tempfile
import zipfile
import os
from datetime import datetime

logger = logging.getLogger(__name__)

def zi_patentcitations():
    """
    Python equivalent of ZIR_Patents.R
    
    Downloads and processes patent citation data from NBER
    Creates PatentDataProcessed.dta with gvkey, year, npat, ncitscale
    """
    logger.info("Processing patent citation data from NBER...")
    
    try:
        # Use the global PROJECT_PATH from master.py
        from master import PROJECT_PATH
        arg1 = PROJECT_PATH
        
        logger.info(f"Using project path: {arg1}")
        
        # Create intermediate directory
        intermediate_dir = Path(arg1) / "Signals" / "Data" / "Intermediate"
        intermediate_dir.mkdir(parents=True, exist_ok=True)
        
        # Download method (equivalent to R's dlmethod logic)
        dlmethod = 'auto'
        import platform
        if platform.system() == "Linux":
            dlmethod = 'wget'
        
        # Download 1: dynass.dta.zip
        logger.info("Downloading dynass.dta.zip...")
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.zip')
        tmp.close()
        
        try:
            response = requests.get("http://www.nber.org/~jbessen/dynass.dta.zip", timeout=30)
            response.raise_for_status()
            
            with open(tmp.name, 'wb') as f:
                f.write(response.content)
            
            # Extract dynass.dta from zip
            with zipfile.ZipFile(tmp.name, 'r') as zip_ref:
                zip_ref.extract('dynass.dta', path=tempfile.gettempdir())
            
            # Read the Stata file
            dynass_path = Path(tempfile.gettempdir()) / 'dynass.dta'
            dynass = pd.read_stata(dynass_path)
            logger.info(f"Downloaded dynass.dta: {len(dynass)} records")
            
        except Exception as e:
            logger.error(f"Failed to download dynass.dta.zip: {e}")
            return False
        finally:
            if os.path.exists(tmp.name):
                os.unlink(tmp.name)
            if os.path.exists(dynass_path):
                os.unlink(dynass_path)
        
        # Download 2: cite76_06.dta.zip
        logger.info("Downloading cite76_06.dta.zip...")
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.zip')
        tmp.close()
        
        try:
            response = requests.get("http://www.nber.org/~jbessen/cite76_06.dta.zip", timeout=30)
            response.raise_for_status()
            
            with open(tmp.name, 'wb') as f:
                f.write(response.content)
            
            # Extract cite76_06.dta from zip
            with zipfile.ZipFile(tmp.name, 'r') as zip_ref:
                zip_ref.extract('cite76_06.dta', path=tempfile.gettempdir())
            
            # Read the Stata file
            cite_path = Path(tempfile.gettempdir()) / 'cite76_06.dta'
            cite76_06 = pd.read_stata(cite_path)
            logger.info(f"Downloaded cite76_06.dta: {len(cite76_06)} records")
            
        except Exception as e:
            logger.error(f"Failed to download cite76_06.dta.zip: {e}")
            return False
        finally:
            if os.path.exists(tmp.name):
                os.unlink(tmp.name)
            if os.path.exists(cite_path):
                os.unlink(cite_path)
        
        # Download 3: pat76_06_assg.dta.zip
        logger.info("Downloading pat76_06_assg.dta.zip...")
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.zip')
        tmp.close()
        
        try:
            response = requests.get("http://www.nber.org/~jbessen/pat76_06_assg.dta.zip", timeout=30)
            response.raise_for_status()
            
            with open(tmp.name, 'wb') as f:
                f.write(response.content)
            
            # Extract pat76_06_assg.dta from zip
            with zipfile.ZipFile(tmp.name, 'r') as zip_ref:
                zip_ref.extract('pat76_06_assg.dta', path=tempfile.gettempdir())
            
            # Read the Stata file
            pat_path = Path(tempfile.gettempdir()) / 'pat76_06_assg.dta'
            pat76_06_assg = pd.read_stata(pat_path)
            logger.info(f"Downloaded pat76_06_assg.dta: {len(pat76_06_assg)} records")
            
        except Exception as e:
            logger.error(f"Failed to download pat76_06_assg.dta.zip: {e}")
            return False
        finally:
            if os.path.exists(tmp.name):
                os.unlink(tmp.name)
            if os.path.exists(pat_path):
                os.unlink(pat_path)
        
        # Number of patents (equivalent to R's df_npat logic)
        logger.info("Processing number of patents...")
        df_npat = pat76_06_assg.groupby(['pdpass', 'gyear']).size().reset_index(name='npat')
        
        # Match gvkey identifier
        df_npat = df_npat.merge(dynass, on='pdpass', how='left')
        
        # Add empty gvkey column
        df_npat['gvkey'] = ""
        
        # Sort correct gvkey depending on time (equivalent to R's case_when logic)
        conditions = [
            (df_npat['gyear'] >= df_npat['begyr1']) & (df_npat['gyear'] <= df_npat['endyr1']),
            (df_npat['gyear'] >= df_npat['begyr2']) & (df_npat['gyear'] <= df_npat['endyr2']),
            (df_npat['gyear'] >= df_npat['begyr3']) & (df_npat['gyear'] <= df_npat['endyr3']),
            (df_npat['gyear'] >= df_npat['begyr4']) & (df_npat['gyear'] <= df_npat['endyr4']),
            (df_npat['gyear'] >= df_npat['begyr5']) & (df_npat['gyear'] <= df_npat['endyr5'])
        ]
        choices = [df_npat['gvkey1'], df_npat['gvkey2'], df_npat['gvkey3'], df_npat['gvkey4'], df_npat['gvkey5']]
        df_npat['gvkey'] = np.select(conditions, choices, default="")
        
        # Convert types and filter
        df_npat['year'] = pd.to_numeric(df_npat['gyear'], errors='coerce')
        df_npat['gvkey'] = pd.to_numeric(df_npat['gvkey'], errors='coerce')
        df_npat = df_npat[df_npat['gvkey'] != ""]
        df_npat = df_npat[['pdpass', 'gvkey', 'year', 'npat']].drop_duplicates()
        
        # Number of patent citations (equivalent to R's df_cite logic)
        logger.info("Processing patent citations...")
        df_cite_match = pat76_06_assg[['patent', 'pdpass', 'gyear', 'cat', 'subcat']].copy()
        df_cite_match = df_cite_match[df_cite_match['pdpass'] != ""]
        
        # Match twice to get citing and cited patent info
        df_cite = cite76_06.merge(df_cite_match, left_on='cited', right_on='patent', how='left', suffixes=('.x', ''))
        df_cite = df_cite.merge(df_cite_match, left_on='citing', right_on='patent', how='left', suffixes=('.x', '.y'))
        
        # Filter valid records
        df_cite = df_cite[
            (df_cite['pdpass.x'] != "") & 
            (df_cite['pdpass.y'] != "") & 
            (df_cite['gyear.x'] != "") & 
            (df_cite['gyear.y'] != "")
        ]
        
        # Calculate year difference and filter
        df_cite['gdiff'] = df_cite['gyear.y'] - df_cite['gyear.x']
        df_cite = df_cite[df_cite['gdiff'] <= 5]  # Only consider citations in first 5 years
        
        # Drop ncites7606 column if it exists
        if 'ncites7606' in df_cite.columns:
            df_cite = df_cite.drop('ncites7606', axis=1)
        
        # Create citation scale (equivalent to R's df_scale logic)
        df_scale = df_npat.merge(
            df_cite, 
            left_on=['pdpass', 'year'], 
            right_on=['pdpass.x', 'gyear.y'], 
            how='left'
        )
        
        df_scale = df_scale[df_scale['cited'] != ""]
        df_scale = df_scale[['pdpass', 'gvkey', 'year', 'gyear.x', 'subcat.x']]
        
        # Number of citations
        df_scale = df_scale.groupby(['pdpass', 'gvkey', 'year', 'gyear.x', 'subcat.x']).size().reset_index(name='ncites')
        
        # Scale by average cites in same subcategory
        df_scale['citscale'] = df_scale.groupby(['year', 'gyear.x', 'subcat.x'])['ncites'].transform(
            lambda x: x / x.mean() if x.mean() > 0 else 0
        )
        
        # Sum by gvkey and year
        df_scale = df_scale.groupby(['gvkey', 'year'])['citscale'].sum().reset_index(name='ncitscale')
        
        # Merge number of patents and (scaled) number of citations
        df_patents = df_npat.groupby(['gvkey', 'year'])['npat'].sum().reset_index()
        df_patents = df_patents.merge(df_scale, on=['gvkey', 'year'], how='left')
        
        # Expand to balanced panel (equivalent to R's expand logic)
        logger.info("Creating balanced panel...")
        min_year = df_patents['year'].min()
        max_year = df_patents['year'].max()
        
        # Create all combinations of gvkey and year
        all_gvkeys = sorted(df_patents['gvkey'].unique())
        all_years = range(int(min_year), int(max_year) + 1)
        
        # Create balanced panel
        balanced_panel = pd.MultiIndex.from_product([all_gvkeys, all_years], names=['gvkey', 'year']).to_frame(index=False)
        
        # Merge with actual data
        df_patents = balanced_panel.merge(df_patents, on=['gvkey', 'year'], how='left')
        
        # Fill missing values with 0
        df_patents['npat'] = df_patents['npat'].fillna(0)
        df_patents['ncitscale'] = df_patents['ncitscale'].fillna(0)
        
        # Save to CSV format in the main Data directory (as expected by master script)
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/PatentDataProcessed.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df_patents.to_csv(output_path, index=False)
        
        # Also save to intermediate directory for compatibility
        intermediate_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/PatentDataProcessed.dta")
        intermediate_path.parent.mkdir(parents=True, exist_ok=True)
        df_patents.to_stata(intermediate_path, write_index=False)
        
        logger.info(f"Saved PatentDataProcessed.csv to {output_path}")
        logger.info(f"Also saved PatentDataProcessed.dta to {intermediate_path}")
        logger.info(f"Final dataset: {len(df_patents)} records")
        logger.info(f"Unique companies: {df_patents['gvkey'].nunique()}")
        logger.info(f"Year range: {df_patents['year'].min()} to {df_patents['year'].max()}")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to process patent citation data: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the function
    zi_patentcitations()
