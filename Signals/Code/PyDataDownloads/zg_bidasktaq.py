"""
Python equivalent of ZG_BidaskTAQ.do
Generated from: ZG_BidaskTAQ.do

Original Stata file: ZG_BidaskTAQ.do
"""

import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def sanitize_filename(filename):
    """Sanitize filename for logging"""
    return str(filename).replace('.do', '').replace('_', ' ').title()

def zg_bidasktaq():
    """
    Python equivalent of ZG_BidaskTAQ.do
    
    Processes bid-ask spread data from TAQ
    """
    logger.info("Processing bid-ask spread data from TAQ...")
    
    try:
        # Load the prepared data file
        input_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Prep/wrds_iid_monthly.csv")
        
        # Check if the file exists
        if not input_path.exists():
            logger.error(f"Input file not found: {input_path}")
            return False
        
        # Load the data
        data = pd.read_csv(input_path)
        logger.info(f"Loaded {len(data)} records from {input_path}")
        
        # Convert yearm to string and extract year and month
        data['yearm'] = data['yearm'].astype(str)
        data['y'] = data['yearm'].str[:4]
        data['m'] = data['yearm'].str[4:6]
        
        # Convert to numeric
        data['y'] = pd.to_numeric(data['y'], errors='coerce')
        data['m'] = pd.to_numeric(data['m'], errors='coerce')
        
        # Create time_avail_m (equivalent to Stata's "gen time_avail_m = ym(y, m)")
        # Create datetime by combining year, month, and day=1
        data['time_avail_m'] = pd.to_datetime({
            'year': data['y'],
            'month': data['m'],
            'day': 1
        })
        data['time_avail_m'] = data['time_avail_m'].dt.to_period('M')
        
        # Create hf_spread (equivalent to Stata's "gen hf_spread = espread_pct_mean")
        # Note: column name is espread_pct_Mean in the new file
        data['hf_spread'] = data['espread_pct_Mean']
        
        # For now, we'll use symbol as permno since we don't have the mapping
        # In a full implementation, we would need to merge with a symbol-permno mapping
        data['permno'] = data['symbol']
        
        # Keep only required columns (equivalent to Stata's "keep permno time_avail_m hf_spread")
        data = data[['permno', 'time_avail_m', 'hf_spread']]
        
        # Save to intermediate file
        output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/hf_spread.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_path, index=False)
        logger.info(f"Saved bid-ask spread data to {output_path}")
        
        # Also save to main data directory for compatibility
        main_output_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/hf_spread.csv")
        data.to_csv(main_output_path, index=False)
        logger.info(f"Saved to main data directory: {main_output_path}")
        
        logger.info("Successfully processed bid-ask spread data")
        return True
        
    except Exception as e:
        logger.error(f"Failed to process bid-ask spread data: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the download function
    zg_bidasktaq()
