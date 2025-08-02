#!/usr/bin/env python3
"""
Script to create Python stub files for all predictor .do files
Similar to create_pydatadownloads.py but for predictors
"""

import os
from pathlib import Path
import re

def sanitize_filename(filename):
    """
    Convert a Stata .do filename to a valid Python module/function name
    """
    # Remove .do extension
    name = filename.replace('.do', '')
    
    # Convert to lowercase
    name = name.lower()
    
    # Replace hyphens and other special characters with underscores
    name = re.sub(r'[^a-z0-9_]', '_', name)
    
    # Remove leading/trailing underscores
    name = name.strip('_')
    
    # Ensure it starts with a letter
    if name and name[0].isdigit():
        name = 'p_' + name
    
    return name

def create_pypredictors():
    """
    Create Python stub files for all predictor .do files
    """
    # Paths
    predictors_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Code/Predictors")
    py_predictors_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Code/PyPredictors")
    
    # Create PyPredictors directory if it doesn't exist
    py_predictors_path.mkdir(parents=True, exist_ok=True)
    
    # Get all .do files from Predictors directory
    do_files = sorted([f for f in predictors_path.glob("*.do")])
    
    print(f"📁 Found {len(do_files)} predictor .do files")
    print(f"📁 Creating Python files in: {py_predictors_path}")
    
    # List to store all function names for __init__.py
    function_names = []
    
    # Create Python file for each .do file
    for do_file in do_files:
        python_filename = sanitize_filename(do_file.name) + ".py"
        python_filepath = py_predictors_path / python_filename
        
        print(f"🔄 Creating {python_filename} from {do_file.name}...")
        
        # Create the Python file content
        python_content = f'''"""
Python equivalent of {do_file.name}
Generated from: {do_file.name}

Original Stata file: {do_file.name}
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def {sanitize_filename(do_file.name)}():
    """
    Python equivalent of {do_file.name}
    
    TODO: Implement the predictor construction logic from the original Stata file
    """
    logger.info("Constructing predictor signal: {sanitize_filename(do_file.name)}...")
    
    try:
        # TODO: Implement the actual predictor construction logic here
        # This should replicate the functionality of {do_file.name}
        
        # Example structure:
        # 1. Load required data files
        # 2. Apply predictor-specific calculations
        # 3. Create the predictor signal
        # 4. Save the predictor signal
        
        logger.info(f"Successfully constructed predictor: {sanitize_filename(do_file.name)}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct predictor {sanitize_filename(do_file.name)}: {{e}}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    {sanitize_filename(do_file.name)}()
'''
        
        # Write the Python file
        with open(python_filepath, 'w') as f:
            f.write(python_content)
        
        # Add function name to list
        function_names.append(sanitize_filename(do_file.name))
        
        print(f"✅ Created {python_filename}")
    
    # Create __init__.py file
    init_content = f'''"""
PyPredictors package
Generated from Predictors .do files
"""

# Import all predictor functions
'''
    
    # Add imports for all functions
    for func_name in function_names:
        init_content += f"from .{func_name} import {func_name}\n"
    
    # Add PREDICTOR_FUNCTIONS list
    init_content += f"\n# List of all predictor functions\nPREDICTOR_FUNCTIONS = [\n"
    for func_name in function_names:
        init_content += f"    {func_name},\n"
    init_content += "]\n"
    
    # Add function count
    init_content += f"\n# Total number of predictor functions\nPREDICTOR_COUNT = {len(function_names)}\n"
    
    # Add summary
    init_content += f'''
# Summary
__all__ = [
    "PREDICTOR_FUNCTIONS",
    "PREDICTOR_COUNT",
] + [
    "{'", "'.join(function_names)}"
]

print(f"📊 PyPredictors package loaded with {{PREDICTOR_COUNT}} predictor functions")
'''
    
    # Write __init__.py file
    init_filepath = py_predictors_path / "__init__.py"
    with open(init_filepath, 'w') as f:
        f.write(init_content)
    
    print(f"✅ Created __init__.py with {len(function_names)} predictor functions")
    
    # Summary
    print(f"\n🎉 Successfully created PyPredictors package!")
    print(f"📁 Directory: {py_predictors_path}")
    print(f"📄 Files created: {len(do_files)} Python files + __init__.py")
    print(f"🔧 Functions: {len(function_names)} predictor functions")
    
    # Show first few function names as examples
    print(f"\n📋 Example predictor functions:")
    for i, func_name in enumerate(function_names[:10]):
        print(f"   {i+1:2d}. {func_name}")
    if len(function_names) > 10:
        print(f"   ... and {len(function_names) - 10} more")
    
    print(f"\n💡 Usage:")
    print(f"   from Signals.Code.PyPredictors import PREDICTOR_FUNCTIONS")
    print(f"   for func in PREDICTOR_FUNCTIONS:")
    print(f"       func()  # Construct the predictor signal")

if __name__ == "__main__":
    create_pypredictors() 