"""
Python equivalent of ZB_PIN.do
Generated from: ZB_PIN.do

Original Stata file: ZB_PIN.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime
import requests
import tempfile
import os
import zipfile

logger = logging.getLogger(__name__)

def zb_pin():
    """
    Python equivalent of ZB_PIN.do
    
    Downloads and processes PIN (Probability of Informed Trading) data from Dropbox
    """
    logger.info("Downloading PIN (Probability of Informed Trading) data from Dropbox...")
    
    try:
        # URL from original Stata file
        url = "https://www.dropbox.com/s/45b42e89gaafg0n/cpie_data.zip?dl=1"
        
        logger.info(f"Downloading PIN data from: {url}")
        
        # Download the ZIP file
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()  # Raise an exception for bad status codes
            
            # Create a temporary file to store the downloaded ZIP
            with tempfile.NamedTemporaryFile(mode='wb', suffix='.zip', delete=False) as temp_file:
                temp_file.write(response.content)
                temp_file_path = temp_file.name
            
            # Extract the ZIP file
            extract_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate")
            extract_path.mkdir(parents=True, exist_ok=True)
            
            with zipfile.ZipFile(temp_file_path, 'r') as zip_ref:
                zip_ref.extractall(extract_path)
            
            # Clean up temporary ZIP file
            os.unlink(temp_file_path)
            
            logger.info("Successfully downloaded and extracted PIN data")
            
        except requests.RequestException as e:
            logger.error(f"Failed to download PIN data: {e}")
            return False
        except zipfile.BadZipFile as e:
            logger.error(f"Failed to extract ZIP file: {e}")
            return False
        
        # Read the PIN yearly data
        pin_yearly_path = extract_path / "pin_yearly.csv"
        if not pin_yearly_path.exists():
            logger.error(f"PIN yearly file not found: {pin_yearly_path}")
            return False
        
        data = pd.read_csv(pin_yearly_path)
        logger.info(f"Loaded {len(data)} PIN yearly records")
        
        # Clean up extracted files (equivalent to Stata's rm commands)
        files_to_remove = [
            "cpie_data.zip",
            "owr_yearly.csv", 
            "pin_yearly.csv",
            "cpie_daily.csv",
            "gpin_yearly.csv",
            "dy_yearly.csv"
        ]
        
        for file_name in files_to_remove:
            file_path = extract_path / file_name
            if file_path.exists():
                file_path.unlink()
                logger.info(f"Removed {file_name}")
        
        # Generate yearly PIN measure from Easley et al
        # The data should already contain the PIN measure, so we'll just verify it exists
        if 'pin' not in data.columns:
            logger.warning("PIN column not found in data. Available columns: " + ", ".join(data.columns))
            # If PIN column doesn't exist, we'll assume it's the main measure in the data
            # This might need adjustment based on actual column names
        
        # Convert to monthly (equivalent to Stata's expand 12)
        logger.info("Converting yearly data to monthly frequency...")
        
        # Create monthly expansion
        monthly_data = []
        for _, row in data.iterrows():
            year = row['year']
            permno = row['permno']
            
            # Expand each yearly observation to 12 monthly observations
            for month in range(1, 13):
                monthly_row = row.copy()
                monthly_row['month'] = month
                monthly_row['modate'] = pd.Period(year=year, month=month, freq='ME')
                monthly_row['time_avail_m'] = monthly_row['modate'] + 11  # Add 11 months lag
                monthly_data.append(monthly_row)
        
        data = pd.DataFrame(monthly_data)
        logger.info(f"Expanded to {len(data)} monthly observations")
        
        # Sort by permno and year (equivalent to Stata's "sort permno year")
        data = data.sort_values(['permno', 'year', 'month'])
        
        # Keep only essential columns
        essential_columns = ['permno', 'year', 'month', 'modate', 'time_avail_m']
        
        # Add PIN column if it exists, or use the main measure column
        if 'pin' in data.columns:
            essential_columns.append('pin')
        else:
            # Find the main measure column (likely the PIN measure)
            measure_columns = [col for col in data.columns if col not in essential_columns]
            if measure_columns:
                essential_columns.extend(measure_columns[:1])  # Add the first measure column
                logger.info(f"Using {measure_columns[0]} as PIN measure")
        
        data = data[essential_columns].copy()
        
        # Save to intermediate file
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/pin_monthly.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved PIN monthly data to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/pin_monthly.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        # Log some summary statistics
        logger.info("PIN (Probability of Informed Trading) summary:")
        logger.info(f"  Total records: {len(data)}")
        
        # Time range analysis
        if 'time_avail_m' in data.columns:
            min_date = data['time_avail_m'].min()
            max_date = data['time_avail_m'].max()
            logger.info(f"  Time range: {min_date} to {max_date}")
            
            # Year range analysis
            if 'year' in data.columns:
                min_year = data['year'].min()
                max_year = data['year'].max()
                logger.info(f"  Year range: {min_year} to {max_year}")
        
        # Company analysis
        if 'permno' in data.columns:
            unique_companies = data['permno'].nunique()
            logger.info(f"  Unique companies: {unique_companies}")
            
            # Companies with most observations
            company_counts = data['permno'].value_counts().head(10)
            logger.info(f"  Companies with most observations:")
            for permno, count in company_counts.items():
                logger.info(f"    Company {permno}: {count} observations")
        
        # PIN measure analysis
        pin_column = None
        for col in data.columns:
            if col.lower() in ['pin', 'probability', 'informed', 'trading'] or 'pin' in col.lower():
                pin_column = col
                break
        
        if pin_column:
            pin_data = data[pin_column].dropna()
            if len(pin_data) > 0:
                logger.info(f"  PIN measure analysis ({pin_column}):")
                logger.info(f"    Non-missing observations: {len(pin_data)}")
                logger.info(f"    Missing observations: {data[pin_column].isna().sum()}")
                
                # PIN statistics
                mean_val = pin_data.mean()
                std_val = pin_data.std()
                min_val = pin_data.min()
                max_val = pin_data.max()
                logger.info(f"    Mean: {mean_val:.6f}")
                logger.info(f"    Std: {std_val:.6f}")
                logger.info(f"    Range: [{min_val:.6f}, {max_val:.6f}]")
                
                # PIN distribution analysis
                logger.info("    PIN distribution:")
                low_pin = (pin_data < 0.1).sum()
                medium_pin = ((pin_data >= 0.1) & (pin_data < 0.2)).sum()
                high_pin = (pin_data >= 0.2).sum()
                
                total_obs = len(pin_data)
                logger.info(f"      Low PIN (<0.1): {low_pin} ({low_pin/total_obs*100:.1f}%)")
                logger.info(f"      Medium PIN (0.1-0.2): {medium_pin} ({medium_pin/total_obs*100:.1f}%)")
                logger.info(f"      High PIN (≥0.2): {high_pin} ({high_pin/total_obs*100:.1f}%)")
        
        # Monthly frequency analysis
        logger.info("  Monthly frequency analysis:")
        logger.info(f"    Total monthly observations: {len(data)}")
        
        # Check for any gaps in monthly data
        if 'time_avail_m' in data.columns:
                    expected_months = pd.date_range(
            start=data['time_avail_m'].min().to_timestamp(),
            end=data['time_avail_m'].max().to_timestamp(),
            freq='ME'
        )
            actual_months = data['time_avail_m'].dt.to_timestamp().sort_values()
            missing_months = set(expected_months) - set(actual_months)
            
            if missing_months:
                logger.warning(f"    Missing months: {len(missing_months)}")
                logger.warning(f"    First few missing months: {sorted(list(missing_months))[:5]}")
            else:
                logger.info("    No missing months detected")
        
        # Data processing explanation
        logger.info("  Data processing explanation:")
        logger.info("    - Original data: PIN data from Easley et al.")
        logger.info("    - Source: Dropbox hosted CPIE data")
        logger.info("    - Frequency conversion: Yearly to monthly expansion")
        logger.info("    - Time lag: 11-month availability lag applied")
        logger.info("    - Usage: Probability of informed trading analysis")
        
        # PIN interpretation
        logger.info("  PIN interpretation:")
        logger.info("    PIN (Probability of Informed Trading):")
        logger.info("    - Measures probability of informed trading")
        logger.info("    - Higher PIN = Higher probability of informed trading")
        logger.info("    - Lower PIN = Lower probability of informed trading")
        logger.info("    - Used in market microstructure analysis")
        logger.info("    - Based on Easley et al. (1996) model")
        
        logger.info("Successfully downloaded and processed PIN data")
        logger.info("Note: Monthly PIN data for probability of informed trading analysis")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download PIN data: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the download function
    zb_pin()
