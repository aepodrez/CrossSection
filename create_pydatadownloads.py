#!/usr/bin/env python3
"""
Script to create Python files in PyDataDownloads directory
based on existing Stata .do files in DataDownloads directory.
"""

import os
import shutil
import re
from pathlib import Path

def sanitize_filename(filename):
    """Convert filename to valid Python module name"""
    # Remove .do extension
    name = filename.replace('.do', '')
    
    # Convert to lowercase
    name = name.lower()
    
    # Replace hyphens with underscores
    name = name.replace('-', '_')
    
    # Replace any other invalid characters with underscores
    name = re.sub(r'[^a-z0-9_]', '_', name)
    
    # Remove leading/trailing underscores
    name = name.strip('_')
    
    return name

def create_pydatadownloads():
    """Create PyDataDownloads directory and generate Python files"""
    
    # Define paths
    project_path = Path("/Users/alexpodrez/Documents/CrossSection")
    data_downloads_path = project_path / "Signals" / "Code" / "DataDownloads"
    py_data_downloads_path = project_path / "Signals" / "Code" / "PyDataDownloads"
    
    # Create PyDataDownloads directory if it doesn't exist
    py_data_downloads_path.mkdir(parents=True, exist_ok=True)
    print(f"✅ Created directory: {py_data_downloads_path}")
    
    # Get all .do files from DataDownloads and sort them
    do_files = sorted(data_downloads_path.glob("*.do"), key=lambda x: x.name.lower())
    print(f"📁 Found {len(do_files)} .do files in DataDownloads")
    
    # Create Python files for each .do file
    created_files = []
    for do_file in do_files:
        # Create valid Python filename
        py_filename = sanitize_filename(do_file.name) + ".py"
        py_file_path = py_data_downloads_path / py_filename
        
        # Create Python file with basic template
        with open(py_file_path, 'w') as f:
            f.write(f'''"""
Python equivalent of {do_file.name}
Generated from: {do_file.name}

Original Stata file: {do_file.name}
"""

import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def {sanitize_filename(do_file.name)}():
    """
    Python equivalent of {do_file.name}
    
    TODO: Implement the data download logic from the original Stata file
    """
    logger.info(f"Downloading data for {{sanitize_filename(do_file.name)}}...")
    
    try:
        # TODO: Implement the actual data download logic here
        # This should replicate the functionality of {do_file.name}
        
        logger.info(f"Successfully downloaded data for {{sanitize_filename(do_file.name)}}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download data for {{sanitize_filename(do_file.name)}}: {{e}}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the download function
    {sanitize_filename(do_file.name)}()
''')
        
        created_files.append(py_filename)
        print(f"📝 Created: {py_filename}")
    
    # Create an __init__.py file for the package
    init_file_path = py_data_downloads_path / "__init__.py"
    with open(init_file_path, 'w') as f:
        f.write(f'''"""
PyDataDownloads package
Contains Python equivalents of Stata .do files from DataDownloads directory.

Generated files: {len(created_files)}
"""

# Import all download functions
''')
        
        # Add imports for each function
        for py_file in created_files:
            function_name = py_file.replace('.py', '')
            f.write(f"from .{function_name} import {function_name}\n")
        
        f.write(f'''

# List of all available download functions
DOWNLOAD_FUNCTIONS = [
''')
        
        for py_file in created_files:
            function_name = py_file.replace('.py', '')
            f.write(f"    {function_name},\n")
        
        f.write("]\n")
    
    print(f"📦 Created __init__.py with {len(created_files)} function imports")
    
    # Summary
    print(f"\n🎉 Successfully created {len(created_files)} Python files:")
    print(f"   📁 Directory: {py_data_downloads_path}")
    print(f"   📝 Files: {', '.join(created_files[:5])}{'...' if len(created_files) > 5 else ''}")
    print(f"   📦 Package: __init__.py with function imports")
    
    return created_files

if __name__ == "__main__":
    created_files = create_pydatadownloads() 