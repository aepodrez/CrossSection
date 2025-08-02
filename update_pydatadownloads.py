#!/usr/bin/env python3
"""
Script to update all PyDataDownloads functions to use global WRDS connection
"""

import os
from pathlib import Path

def update_pydatadownloads():
    """Update all PyDataDownloads functions to use global WRDS connection"""
    
    py_data_downloads_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Code/PyDataDownloads")
    
    # Get all Python files (excluding __init__.py)
    py_files = [f for f in py_data_downloads_path.glob("*.py") if f.name != "__init__.py"]
    
    print(f"📁 Found {len(py_files)} Python files to update")
    
    for py_file in py_files:
        print(f"🔄 Updating {py_file.name}...")
        
        # Read the current file
        with open(py_file, 'r') as f:
            content = f.read()
        
        # Replace the WRDS connection code
        old_connection_code = '''        # Connect to WRDS
        conn = wrds.Connection(wrds_username=input("Enter your WRDS username: "))'''
        
        new_connection_code = '''        # Use global WRDS connection from master.py
        from master import wrds_conn
        if wrds_conn is None:
            logger.error("WRDS connection not available. Please run master.py")
            return False
        conn = wrds_conn'''
        
        # Replace the connection code
        if old_connection_code in content:
            content = content.replace(old_connection_code, new_connection_code)
            
            # Write the updated content back
            with open(py_file, 'w') as f:
                f.write(content)
            
            print(f"✅ Updated {py_file.name}")
        else:
            print(f"⚠️  No connection code found in {py_file.name}")
    
    print(f"\n🎉 Updated {len(py_files)} files to use global WRDS connection")

if __name__ == "__main__":
    update_pydatadownloads() 