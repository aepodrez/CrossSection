"""
ABOUTME: Executes all AP (Alternative Provider) data download scripts in DataDownloads/ directory sequentially
ABOUTME: Tracks execution results, timeouts, and errors with detailed logging
ABOUTME: Only runs scripts that start with "AP_"

Inputs:
  - All AP_*.py files in DataDownloads/ directory
  - Optional preprocessed files in ../pyData/Prep/

Outputs:
  - Various data files in ../pyData/ (location depends on individual scripts)
  - ../Logs/AP_01_DownloadDataFlags.csv (execution tracking)
  - ../Logs/AP_01_DownloadData_console.txt (detailed console output)

Usage:
  python AP_01_DownloadData.py
"""

import os
import sys
import time
import subprocess
import threading
import pandas as pd
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from config import SCRIPT_TIMEOUT_MINUTES

# Load environment variables
load_dotenv()

def setup_logging():
    """Initialize error tracking and console logging"""
    log_dir = Path("../Logs")
    log_dir.mkdir(exist_ok=True)
    
    # Create error tracking DataFrame for execution monitoring
    error_log = pd.DataFrame(columns=['DataFile', 'DataTime', 'ReturnCode', 'Message'])
    
    # Initialize console log list for detailed txt output
    console_log = []
    console_log.append(f"AP Data Download Log - Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    console_log.append("=" * 80)
    
    return error_log, console_log

def find_download_scripts():
    """Locate all AP_*.py download scripts to execute"""
    downloads_dir = Path("DataDownloads")
    
    if not downloads_dir.exists():
        print(f"ERROR: {downloads_dir} directory not found")
        return []
    
    # Find all AP_*.py files, excluding __pycache__ and system files
    py_files = []
    for file in downloads_dir.glob("AP_*.py"):
        if not file.name.startswith("__"):
            py_files.append(file.name)
    
    # Sort alphabetically for consistent execution order
    py_files.sort()
    
    print(f"Found {len(py_files)} AP download scripts:")
    for file in py_files:
        print(f"  - {file}")
    
    return py_files

def execute_script(script_name, error_log, console_log):
    """Run individual download script with timeout and error tracking"""
    start_msg = f"\n🔄 Starting: {script_name}"
    separator = "=" * 60
    
    print(start_msg)
    print(separator)
    
    # Add to console log
    console_log.append(f"\n{start_msg}")
    console_log.append(separator)
    
    start_time = time.time()
    return_code = 0
    script_output = []
    process = None
    timer = None
    timed_out = False
    
    def timeout_handler():
        """Handle timeout by terminating the process"""
        nonlocal timed_out
        timed_out = True
        if process and process.poll() is None:
            process.terminate()
            # Give it 2 seconds to terminate gracefully, then kill
            threading.Timer(2.0, lambda: process.kill() if process.poll() is None else None).start()
    
    try:
        # Execute the script in the DataDownloads directory
        script_path = Path("DataDownloads") / script_name
        
        # Use Popen for real-time output streaming and capture
        process = subprocess.Popen(
            [sys.executable, str(script_path)],
            cwd=".",
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        # Set up configurable timeout (convert minutes to seconds)
        # Special timeout for long-running scripts: 5 hours (300 minutes)
        if script_name in ["AP_CRSPAcquisitions.py", "AP_CompustatQuarterly.py", "AP_InstitutionalHoldings13F.py"]:
            timeout_minutes = 300  # 5 hours
        else:
            timeout_minutes = SCRIPT_TIMEOUT_MINUTES
        timeout_seconds = timeout_minutes * 60
        timer = threading.Timer(timeout_seconds, timeout_handler)
        timer.start()
        
        # Stream output in real-time and capture for logging
        for line in process.stdout:
            if timed_out:
                break
            print(line, end='', flush=True)
            script_output.append(line.rstrip())
        
        # Cancel timer if process completed normally
        if timer:
            timer.cancel()
        
        # Wait for completion and check return code
        process.wait()
        
        if timed_out:
            return_code = -9  # SIGKILL return code
            # Use the actual timeout that was set for this script
            actual_timeout = 300 if script_name == "AP_CRSPAcquisitions.py" else SCRIPT_TIMEOUT_MINUTES
            timeout_msg = f"⏱️ TIMEOUT in {script_name}: Script exceeded {actual_timeout} minutes"
            print(separator)
            print(timeout_msg)
            
            # Add timeout details to console log
            console_log.extend(script_output)
            console_log.append(separator)
            console_log.append(timeout_msg)
            console_log.append(f"Script was terminated due to {actual_timeout}-minute timeout")
            
        elif process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, [sys.executable, str(script_path)])
        else:
            success_msg = f"✅ Completed: {script_name}"
            print(separator)
            print(success_msg)
            
            # Add success to console log
            console_log.extend(script_output)
            console_log.append(separator)
            console_log.append(success_msg)
        
    except subprocess.CalledProcessError as e:
        return_code = e.returncode
        error_msg = f"❌ ERROR in {script_name}: Return code {e.returncode}"
        print(separator)
        print(error_msg)
        
        # Add error details to console log
        console_log.extend(script_output)
        console_log.append(separator)
        console_log.append(error_msg)
        console_log.append(f"Error details: Script failed with return code {e.returncode}")
    
    except Exception as e:
        return_code = 1
        error_msg = f"💥 UNEXPECTED ERROR in {script_name}: {e}"
        print(separator)
        print(error_msg)
        
        # Add exception details to console log
        console_log.extend(script_output)
        console_log.append(separator)
        console_log.append(error_msg)
        console_log.append(f"Exception details: {str(e)}")
        console_log.append(f"Exception type: {type(e).__name__}")
    
    finally:
        # Clean up timer and process
        if timer:
            timer.cancel()
        if process and process.poll() is None:
            process.terminate()
    
    # Calculate execution time
    execution_time = time.time() - start_time
    time_msg = f"Execution time: {execution_time:.2f} seconds"
    console_log.append(time_msg)
    
    # Log results (equivalent to Stata's error tracking)
    if timed_out:
        message = "Processing timeout"
    elif return_code == 0:
        message = "Processing successful"
    else:
        message = "Processing error"
    
    new_row = pd.DataFrame({
        'DataFile': [script_name],
        'DataTime': [execution_time],
        'ReturnCode': [return_code],
        'Message': [message]
    })
    
    error_log = pd.concat([error_log, new_row], ignore_index=True)
    
    return error_log, return_code, console_log

def save_error_log(error_log, console_log):
    """Write execution logs to CSV and text files"""
    csv_path = Path("../Logs/AP_01_DownloadDataFlags.csv")
    txt_path = Path("../Logs/AP_01_DownloadData_console.txt")
    
    # Save to CSV
    error_log.to_csv(csv_path, index=False)
    
    # Save detailed console output to txt file
    with open(txt_path, 'w') as f:
        f.write('\n'.join(console_log))
        f.write(f"\n\nLog completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
  
    print(f"\nLog files saved:")
    print(f"  CSV: {csv_path}")
    print(f"  TXT: {txt_path}")

def check_environment():
    """Verify required environment variables are set in .env file"""
    print("\n" + "=" * 60)
    print("Checking Environment Variables (.env file)")
    print("=" * 60)
    
    # Required environment variables by script
    required_vars = {
        "FRED_API_KEY": {
            "required_by": ["AP_VIX.py", "AP_GNPDeflator.py", "AP_TreasuryBill3M.py", "AP_QFactorModel.py"],
            "description": "FRED API key for economic data",
            "get_key_url": "https://fred.stlouisfed.org/docs/api/api_key.html"
        },
        "REFINITIV_APP_KEY": {
            "required_by": ["AP_IBESEPSAdjusted.py", "AP_IBESEPSUnadjusted.py"],
            "description": "Refinitiv Platform App Key",
            "get_key_url": None
        },
        "REFINITIV_USERNAME": {
            "required_by": ["AP_IBESEPSAdjusted.py", "AP_IBESEPSUnadjusted.py"],
            "description": "Refinitiv Platform Username",
            "get_key_url": None
        },
        "REFINITIV_PASSWORD": {
            "required_by": ["AP_IBESEPSAdjusted.py", "AP_IBESEPSUnadjusted.py"],
            "description": "Refinitiv Platform Password",
            "get_key_url": None
        },
        "BEA_API_KEY": {
            "required_by": ["AP_BEAInputOutput.py"],
            "description": "BEA API key for input-output tables",
            "get_key_url": "https://apps.bea.gov/API/signup/"
        }
    }
    
    # Optional environment variables (with defaults)
    optional_vars = {
        "SEC_EMAIL": {
            "required_by": ["AP_BuildFFPortfolios.py"],
            "description": "Email for SEC EDGAR identity (defaults provided)",
            "default": "your_email@example.com"
        },
        "EDGAR_IDENTITY": {
            "required_by": ["AP_CompustatAnnual.py", "AP_QFactorModel.py"],
            "description": "EDGAR identity string (defaults provided)",
            "default": "Your Name your.email@example.com"
        },
        "IBES_BATCH_SIZE": {
            "required_by": ["AP_IBESEPSAdjusted.py", "AP_IBESEPSUnadjusted.py"],
            "description": "Batch size for IBES requests (default: 20)",
            "default": "20"
        },
        "RD_HTTP_TIMEOUT": {
            "required_by": ["AP_IBESEPSAdjusted.py", "AP_IBESEPSUnadjusted.py"],
            "description": "HTTP timeout for Refinitiv requests (default: 60)",
            "default": "60"
        }
    }
    
    missing_required = []
    missing_optional = []
    
    # Check required variables
    for var_name, var_info in required_vars.items():
        value = os.getenv(var_name)
        if not value or value.strip() == "":
            missing_required.append((var_name, var_info))
            print(f"❌ {var_name}: NOT SET")
            print(f"   Required by: {', '.join(var_info['required_by'])}")
            print(f"   Description: {var_info['description']}")
            if var_info['get_key_url']:
                print(f"   Get key: {var_info['get_key_url']}")
        else:
            print(f"✓ {var_name}: SET (length: {len(value)})")
    
    # Check optional variables
    for var_name, var_info in optional_vars.items():
        value = os.getenv(var_name)
        if not value or value.strip() == "":
            missing_optional.append((var_name, var_info))
            print(f"⚠️  {var_name}: NOT SET (will use default: {var_info['default']})")
        else:
            print(f"✓ {var_name}: SET")
    
    # Summary
    print("\n" + "-" * 60)
    if missing_required:
        print(f"❌ {len(missing_required)} REQUIRED environment variable(s) missing!")
        print("\nPlease add the following to your .env file:")
        for var_name, var_info in missing_required:
            print(f"  {var_name}=your_value_here")
            if var_info['get_key_url']:
                print(f"    # Get key at: {var_info['get_key_url']}")
        print("\n⚠️  Scripts that require these variables will fail.")
        return False
    else:
        print("✓ All required environment variables are set!")
        if missing_optional:
            print(f"⚠️  Note: {len(missing_optional)} optional variable(s) using defaults")
        return True

def check_optional_files():
    """Verify availability of optional preprocessed data files"""
    print("Checking for optional preprocessed files...")
    
    optional_files = [
        "../pyData/Prep/OptionMetrics.csv", 
        "../pyData/Prep/tr_13f.csv",
        "../pyData/Prep/corwin_schultz_spread.csv"
    ]
    
    missing_files = []
    for file_path in optional_files:
        if not Path(file_path).exists():
            missing_files.append(file_path)
    
    if missing_files:
        print("WARNING: Some optional files are missing:")
        for file_path in missing_files:
            print(f"  - {file_path}")
        print("Some signals that depend on these files cannot be generated.")
        print("These files are created by code in ../PrepScripts/")
    else:
        print("✓ All optional preprocessed files found")

def main():
    """Execute all AP download scripts with comprehensive logging"""
    print("=" * 60)
    print("AP Data Download Script - Alternative Provider Downloads")
    print("=" * 60)
    
    # Check environment variables first
    env_ok = check_environment()
    if not env_ok:
        print("\n" + "=" * 60)
        print("⚠️  WARNING: Missing required environment variables!")
        print("Some scripts may fail. Continue anyway? (y/n)")
        print("=" * 60)
        # For automated runs, continue but warn
        print("Continuing with execution (scripts will fail if vars missing)...")
    
    # Check for optional files
    check_optional_files()
    
    # Setup logging
    error_log, console_log = setup_logging()
    
    # Find all AP download scripts
    download_scripts = find_download_scripts()
    
    if not download_scripts:
        print("No AP download scripts found in DataDownloads/")
        return
    
    # Execute each script sequentially with error tracking
    print(f"\nExecuting {len(download_scripts)} AP download scripts...")
    
    failed_scripts = []
    
    for script in download_scripts:
        error_log, return_code, console_log = execute_script(script, error_log, console_log)
        
        if return_code != 0:
            failed_scripts.append(script)
        
        # Save execution log after each script for monitoring
        save_error_log(error_log, console_log)
    
    # Generate final execution summary and report
    print("\n" + "=" * 60)
    print("AP DOWNLOAD SUMMARY")
    print("=" * 60)
    
    total_scripts = len(download_scripts)
    successful_scripts = total_scripts - len(failed_scripts)
    
    print(f"Total scripts: {total_scripts}")
    print(f"Successful: {successful_scripts}")
    print(f"Failed: {len(failed_scripts)}")
    
    if failed_scripts:
        print("\nThe following AP download scripts did not complete successfully:")
        for script in failed_scripts:
            print(f"  ✗ {script}")
        print(f"\nCheck {Path('../Logs/AP_01_DownloadDataFlags.csv').absolute()} for details")
    else:
        print("\n✓ All AP download scripts completed successfully!")
    
    print("=" * 60)

if __name__ == "__main__":
    main()

