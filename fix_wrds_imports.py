#!/usr/bin/env python3
"""
Script to fix WRDS import issues in PyDataDownloads functions
"""

import os
import re
from pathlib import Path

def fix_wrds_imports_in_file(file_path):
    """Fix WRDS imports in a single file"""
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Find the function name
    func_match = re.search(r'def (\w+)\(\):', content)
    if not func_match:
        print(f"Could not find function definition in {file_path}")
        return False
    
    func_name = func_match.group(1)
    
    # Check if this file has the problematic import
    if 'from master import wrds_conn' not in content:
        print(f"No WRDS import found in {file_path}")
        return False
    
    # Update function signature
    content = re.sub(
        rf'def {func_name}\(\):',
        rf'def {func_name}(wrds_conn=None):',
        content
    )
    
    # Replace the import and connection check
    old_pattern = r'# Use global WRDS connection from master\.py\s+from master import wrds_conn\s+if wrds_conn is None:\s+logger\.error\("WRDS connection not available\. Please run master\.py"\)\s+return False\s+conn = wrds_conn'
    new_pattern = '''# Check if WRDS connection is provided
        if wrds_conn is None:
            logger.error("WRDS connection not available. Please run master.py")
            return False
        conn = wrds_conn'''
    
    content = re.sub(old_pattern, new_pattern, content, flags=re.MULTILINE)
    
    # Write back to file
    with open(file_path, 'w') as f:
        f.write(content)
    
    print(f"Fixed {file_path}")
    return True

def main():
    """Main function to fix all WRDS imports"""
    downloads_dir = Path("Signals/Code/PyDataDownloads")
    
    if not downloads_dir.exists():
        print(f"Directory not found: {downloads_dir}")
        return
    
    # List of files that need fixing (from grep search)
    files_to_fix = [
        "q_marketreturns.py",
        "n_ibes_unadjustedactuals.py", 
        "k_crspacquisitions.py",
        "m_ibes_recommendations.py",
        "p_monthly_fama_french.py",
        "c_compustatquarterly.py",
        "g_compustatshortinterest.py",
        "e_compustatbusinesssegments.py",
        "h_crspdistributions.py",
        "d_compustatpensions.py",
        "f_compustatcustomersegments.py",
        "l2_ibes_eps_adj.py",
        "i2_crspmonthlyraw.py",
        "i_crspmonthly.py",
        "j_crspdaily.py",
        "r_monthlyliquidityfactor.py",
        "l_ibes_eps_unadj.py",
        "x_spcreditratings.py",
        "x2_ciqcreditratings.py",
        "o_daily_fama_french.py"
    ]
    
    fixed_count = 0
    for filename in files_to_fix:
        file_path = downloads_dir / filename
        if file_path.exists():
            if fix_wrds_imports_in_file(file_path):
                fixed_count += 1
        else:
            print(f"File not found: {file_path}")
    
    print(f"\nFixed {fixed_count} files")

if __name__ == "__main__":
    main() 