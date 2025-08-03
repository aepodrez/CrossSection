"""
Master file for CrossSection signal construction
Python equivalent of master.do

This code is set up to run with the following path structure:
+---Signals
+---/Code          (contains scripts that call other scripts and SAS/R files)
|   /Data          (contains all data download scripts)
|   /Logs          (contains log files created during running scripts) 

Optional inputs:
    /Data/Prep/
        iclink.csv        
        OptionMetrics.csv
        tr_13f.csv
        corwin_schultz_spread.csv        

These are created by code in Signals/PrepScripts/. They are required for producing 
the signals that use IBES (iclink.csv), OptionMetrics, and Thomson-Reuter's 
13f data, but master.py will still produce the CRSP-Compustat signals if you do not have them.
"""

import os
import sys
import subprocess
import logging
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
import wrds
from fredapi import Fred

# ============================================================
# SET PROJECT PATH AND CONNECTION SETTINGS HERE !
# ============================================================
PROJECT_PATH = "/Users/alexpodrez/Documents/CrossSection/"  # required, should point to project root
WRDS_CONNECTION = "wrds-stata"  # required, see readme
RSCRIPT_PATH = "/usr/local/bin/Rscript"  # optional, used for like 3 signals
FRED_API_KEY = "d9b5ebeddbd909b857b4129528a130b6"  # FRED API key

# Validate paths
if not PROJECT_PATH or not WRDS_CONNECTION:
    print("ERROR: Relevant paths have not all been set")
    print("Please set PROJECT_PATH and WRDS_CONNECTION in master.py")
    sys.exit(999)

if not os.path.exists(PROJECT_PATH):
    print(f"ERROR: Project path does not exist: {PROJECT_PATH}")
    sys.exit(999)

# Set up FRED API
fred = Fred(api_key=FRED_API_KEY)

# Set up logging
log_dir = Path(PROJECT_PATH) / "Signals" / "Logs"
log_dir.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / "master.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Global WRDS connection (will be initialized in main)
wrds_conn = None

# Import PyDataDownloads functions
try:
    from Signals.Code.PyDataDownloads import DOWNLOAD_FUNCTIONS
    PYDATADOWNLOADS_AVAILABLE = True
    logger.info("✅ PyDataDownloads package imported successfully")
except ImportError as e:
    logger.warning(f"PyDataDownloads not available: {e}")
    PYDATADOWNLOADS_AVAILABLE = False

# Import PyPredictors functions
try:
    from Signals.Code.PyPredictors import PREDICTOR_FUNCTIONS
    PYPREDICTORS_AVAILABLE = True
    logger.info("✅ PyPredictors package imported successfully")
except (ImportError, FileNotFoundError) as e:
    logger.warning(f"PyPredictors not available: {e}")
    logger.warning("This is expected if data files don't exist yet. Will be available after data download.")
    PYPREDICTORS_AVAILABLE = False

# ============================================================
# Configuration and Settings
# ============================================================

def check_fred_access():
    """Check FRED API access (optional)"""
    try:
        # Test FRED API connection
        test_data = fred.get_series('GDP', limit=1)
        logger.info("✅ FRED API access working!")
        return True
    except Exception as e:
        logger.warning(f"❌ FRED API not available: {e}")
        logger.warning("This is optional - you'll miss about 6 predictors")
        return False

def check_predictor_availability():
    """Check if predictor packages are available"""
    if not PYPREDICTORS_AVAILABLE:
        logger.warning("⚠️  PyPredictors package not available!")
        logger.warning("This is expected if data files don't exist yet.")
        logger.warning("Predictors will be available after data download is complete.")
        return False
    else:
        logger.info(f"✅ PyPredictors package available with {len(PREDICTOR_FUNCTIONS)} functions")
        return True

def create_directories():
    """Create necessary directories"""
    dirs_to_create = [
        Path(PROJECT_PATH) / "Signals" / "Data",
        Path(PROJECT_PATH) / "Signals" / "Logs",
        Path(PROJECT_PATH) / "Signals" / "Code" / "Data",
        Path(PROJECT_PATH) / "Signals" / "Code" / "Data" / "Prep"
    ]
    
    for dir_path in dirs_to_create:
        dir_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created directory: {dir_path}")

def check_required_data_files():
    """Check if required data files exist for predictor construction"""
    required_files = [
        Path(PROJECT_PATH) / "Signals" / "Code" / "Data" / "crspm.csv",
        Path(PROJECT_PATH) / "Signals" / "Code" / "Data" / "compustat.csv",
        Path(PROJECT_PATH) / "Signals" / "Code" / "Data" / "ibes.csv"
    ]
    
    missing_files = []
    for file_path in required_files:
        if not file_path.exists():
            missing_files.append(str(file_path))
    
    if missing_files:
        logger.warning("⚠️  Some required data files are missing:")
        for file_path in missing_files:
            logger.warning(f"   - {file_path}")
        logger.warning("This may cause some predictors to fail. Make sure to run data downloads first.")
        return False
    else:
        logger.info("✅ All required data files found")
        return True

# ============================================================
# Data Download Functions
# ============================================================

def download_data():
    """Download all data using PyDataDownloads functions"""
    logger.info("=== STEP 1: Downloading All Data ===")
    
    if not PYDATADOWNLOADS_AVAILABLE:
        logger.error("PyDataDownloads not available. Please run create_pydatadownloads.py first.")
        return False
    
    # Initialize global WRDS connection
    global wrds_conn
    try:
        logger.info("Connecting to WRDS...")
        wrds_conn = wrds.Connection(wrds_username=input("Enter your WRDS username: "))
        logger.info("✅ WRDS connection established")
    except Exception as e:
        logger.error(f"Failed to connect to WRDS: {e}")
        return False
    
    # Track success/failure for each download
    download_results = []
    
    for func in DOWNLOAD_FUNCTIONS:
        try:
            logger.info(f"Executing: {func.__name__}")
            start_time = datetime.now()
            
            # Execute the download function with WRDS connection
            success = func(wrds_conn)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Record results
            result = {
                'function': func.__name__,
                'success': success,
                'duration_seconds': duration,
                'timestamp': start_time.strftime('%Y-%m-%d %H:%M:%S')
            }
            download_results.append(result)
            
            if success:
                logger.info(f"✅ {func.__name__} completed successfully in {duration:.1f} seconds")
            else:
                logger.error(f"❌ {func.__name__} failed after {duration:.1f} seconds")
                
        except Exception as e:
            logger.error(f"❌ {func.__name__} failed with exception: {e}")
            download_results.append({
                'function': func.__name__,
                'success': False,
                'duration_seconds': 0,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'error': str(e)
            })
    
    # Summary report
    successful_downloads = [r for r in download_results if r['success']]
    failed_downloads = [r for r in download_results if not r['success']]
    
    logger.info(f"📊 Download Summary:")
    logger.info(f"   ✅ Successful: {len(successful_downloads)}/{len(download_results)}")
    logger.info(f"   ❌ Failed: {len(failed_downloads)}/{len(download_results)}")
    
    if failed_downloads:
        logger.warning("Failed downloads:")
        for result in failed_downloads:
            logger.warning(f"   - {result['function']}: {result.get('error', 'Unknown error')}")
    
    # Save download results to log file
    results_df = pd.DataFrame(download_results)
    log_path = Path(PROJECT_PATH) / "Signals" / "Logs" / "download_results.csv"
    results_df.to_csv(log_path, index=False)
    logger.info(f"📝 Download results saved to: {log_path}")
    
    return len(failed_downloads) == 0  # Return True if all downloads succeeded

# ============================================================
# Signal Construction Functions
# ============================================================

def construct_predictor_signals():
    """Construct all predictor signals using PyPredictors functions"""
    logger.info("=== STEP 2: Constructing All Predictor Signals ===")
    
    if not PYPREDICTORS_AVAILABLE:
        logger.error("PyPredictors not available. Please run create_pypredictors.py first.")
        return False
    
    # Track success/failure for each predictor
    predictor_results = []
    total_predictors = len(PREDICTOR_FUNCTIONS)
    
    logger.info(f"Starting construction of {total_predictors} predictor signals...")
    
    for i, func in enumerate(PREDICTOR_FUNCTIONS, 1):
        logger.info(f"Progress: {i}/{total_predictors} - {func.__name__}")
        try:
            logger.info(f"Executing: {func.__name__}")
            start_time = datetime.now()
            
            # Execute the predictor function
            success = func()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Record results
            result = {
                'function': func.__name__,
                'success': success,
                'duration_seconds': duration,
                'timestamp': start_time.strftime('%Y-%m-%d %H:%M:%S')
            }
            predictor_results.append(result)
            
            if success:
                logger.info(f"✅ {func.__name__} completed successfully in {duration:.1f} seconds")
            else:
                logger.error(f"❌ {func.__name__} failed after {duration:.1f} seconds")
                
        except Exception as e:
            logger.error(f"❌ {func.__name__} failed with exception: {e}")
            predictor_results.append({
                'function': func.__name__,
                'success': False,
                'duration_seconds': 0,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'error': str(e)
            })
    
    # Summary report
    successful_predictors = [r for r in predictor_results if r['success']]
    failed_predictors = [r for r in predictor_results if not r['success']]
    
    logger.info(f"📊 Predictor Construction Summary:")
    logger.info(f"   ✅ Successful: {len(successful_predictors)}/{len(predictor_results)}")
    logger.info(f"   ❌ Failed: {len(failed_predictors)}/{len(predictor_results)}")
    
    if failed_predictors:
        logger.warning("Failed predictors:")
        for result in failed_predictors:
            logger.warning(f"   - {result['function']}: {result.get('error', 'Unknown error')}")
    
    # Save predictor results to log file
    results_df = pd.DataFrame(predictor_results)
    log_path = Path(PROJECT_PATH) / "Signals" / "Logs" / "predictor_results.csv"
    results_df.to_csv(log_path, index=False)
    logger.info(f"📝 Predictor results saved to: {log_path}")
    
    # Calculate total time
    total_time = sum(r['duration_seconds'] for r in predictor_results)
    avg_time = total_time / len(predictor_results) if predictor_results else 0
    
    logger.info(f"⏱️  Total time: {total_time/60:.1f} minutes")
    logger.info(f"⏱️  Average time per predictor: {avg_time:.1f} seconds")
    
    return len(failed_predictors) == 0  # Return True if all predictors succeeded

def construct_placebo_signals():
    """Construct all placebo signals"""
    logger.info("Constructing placebo signals...")
    pass

    """Construct quarterly Cash Flow signal"""
    logger.info("Constructing quarterly Cash Flow signal...")
    # Placeholder for CF quarterly signal construction
    pass

# ============================================================
# Utility Functions
# ============================================================

def save_signal(signal_data, signal_name, signal_type="predictor"):
    """Save signal data to CSV"""
    output_dir = Path(PROJECT_PATH) / "Signals" / "Data"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    filename = f"{signal_name}.csv"
    filepath = output_dir / filename
    
    signal_data.to_csv(filepath, index=False)
    logger.info(f"Saved {signal_type} signal: {filepath}")

def run_r_script(script_path):
    """Run R script if R is available"""
    if os.path.exists(RSCRIPT_PATH):
        try:
            result = subprocess.run([RSCRIPT_PATH, script_path], 
                                  capture_output=True, text=True)
            if result.returncode == 0:
                logger.info(f"Successfully ran R script: {script_path}")
            else:
                logger.error(f"R script failed: {result.stderr}")
        except Exception as e:
            logger.error(f"Error running R script {script_path}: {e}")
    else:
        logger.warning(f"R not available, skipping script: {script_path}")

# ============================================================
# Main Execution
# ============================================================

def main():
    """Main execution function"""
    logger.info("Starting CrossSection signal construction...")
    
    # Create directories
    create_directories()
    
    # Check FRED access
    check_fred_access()
    
    # Check predictor availability (may not be available initially)
    check_predictor_availability()
    
    # Download data
    logger.info("=== STEP 1: Downloading Data ===")
    download_data()
    
    # Check required data files
    check_required_data_files()
    
    # Re-check predictor availability after data download
    if not PYPREDICTORS_AVAILABLE:
        logger.info("Re-checking PyPredictors availability after data download...")
        try:
            from Signals.Code.PyPredictors import PREDICTOR_FUNCTIONS
            # Update global variables
            globals()['PYPREDICTORS_AVAILABLE'] = True
            globals()['PREDICTOR_FUNCTIONS'] = PREDICTOR_FUNCTIONS
            logger.info(f"✅ PyPredictors package now available with {len(PREDICTOR_FUNCTIONS)} functions")
        except (ImportError, FileNotFoundError) as e:
            logger.error(f"❌ PyPredictors still not available after data download: {e}")
            logger.error("Please check that all required data files were downloaded successfully.")
            return
    
    # Construct predictors
    logger.info("=== STEP 2: Creating Predictors ===")
    construct_predictor_signals()

    logger.info("Done downloading and constructing predictors")
    exit()
    
    # Construct placebos
    logger.info("=== STEP 3: Creating Placebos ===")
    construct_placebo_signals()
    
    logger.info("CrossSection signal construction complete!")

if __name__ == "__main__":
    main() 