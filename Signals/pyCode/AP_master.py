"""
ABOUTME: Master orchestrator for AP (Alternative Provider) data processing pipeline
ABOUTME: Executes AP data downloads (AP_01_DownloadData.py) - download stage only for now
Inputs: Environment variables from .env file (FRED_API_KEY, etc.), existing folder structure
Outputs: Complete AP pipeline execution with data in pyData/ folders
How to run: python AP_master.py (from pyCode/ directory)
"""

import os
import subprocess
import sys
from pathlib import Path
from dotenv import load_dotenv


def check_environment():
    """Verify required environment variables are available"""
    print("Checking environment setup...")
    
    # Load environment variables
    load_dotenv()
    
    # Check FRED API key (required for AP scripts)
    fred_key = os.getenv("FRED_API_KEY")
    
    if not fred_key:
        print("WARNING: FRED_API_KEY not set in .env file")
        print("Some AP scripts may fail without FRED API key")
        print("Get a free key from: https://fred.stlouisfed.org/docs/api/api_key.html")
    else:
        print("✓ FRED_API_KEY found")
    
    return True


def check_folder_structure():
    """Verify required data and log folders exist"""
    print("Checking folder structure...")
    
    required_folders = [
        "../pyData",
        "../pyData/Intermediate",
        "../pyData/Predictors",
        "../pyData/Placebos",
        "../pyData/temp",
        "../pyData/Prep",
        "../Logs"
    ]
    
    missing_folders = []
    for folder in required_folders:
        if not Path(folder).exists():
            missing_folders.append(folder)
    
    if missing_folders:
        print(f"Folder(s) missing and will be created: {missing_folders}")
        return False
    
    print("✓ Folder structure verified")
    return True


def run_settings():
    """Create missing folders and verify directory structure"""
    print("Running folder setup and verification...")
    
    if not check_folder_structure():
        print("Creating missing folders...")
        create_folder_structure()
    
    print("✓ Setup complete")


def create_folder_structure():
    """Create the folder structure matching ../Code/settings.do"""
    folders = [
        "../pyData",
        "../pyData/temp",
        "../pyData/Prep",
        "../pyData/Intermediate",
        "../pyData/Predictors",
        "../pyData/Placebos",
        "../Logs",
    ]
    
    for folder in folders:
        Path(folder).mkdir(parents=True, exist_ok=True)
        print(f"Created: {folder}")


def main():
    """Execute AP data processing pipeline - download stage only"""
    print("=" * 60)
    print("AP Data Processing Pipeline - Master Orchestrator")
    print("=" * 60)
    
    # Check if we're in the right directory (should end with pyCode)
    current_dir = Path.cwd()
    if current_dir.name != "pyCode":
        print("ERROR: Please run this script from the pyCode directory")
        print(f"Current directory: {current_dir}")
        sys.exit(1)
    
    # Verify environment setup
    check_environment()
    
    # Create required folders and verify structure
    run_settings()
    
    # Enable CSV output for data files
    os.environ["SAVE_CSV"] = "1"
    
    print("\nRunning AP data download stage...")
    
    # Execute AP data download scripts sequentially
    print("\n1. Running AP data downloads...")
    try:
        # Use subprocess.run without capture_output for real-time streaming
        result = subprocess.run([sys.executable, "-u", "AP_01_DownloadData.py"],
                              check=True)
        print("✓ AP data downloads completed")
    except subprocess.CalledProcessError as e:
        print(f"ERROR in AP data downloads: {e}")
        sys.exit(1)
    
    print("\n" + "=" * 60)
    print("AP Master script completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()

