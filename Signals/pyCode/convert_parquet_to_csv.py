#!/usr/bin/env python3
"""
Convert all parquet files in the Intermediate folder to CSV files.
Only converts files that don't already have CSV versions.
Uses pyarrow to read parquet and writes CSV directly.
"""

import pyarrow.parquet as pq
import csv
from pathlib import Path

# Define paths
intermediate_dir = Path(__file__).parent.parent / "pyData" / "Intermediate"

# Find all parquet files
parquet_files = list(intermediate_dir.glob("*.parquet"))

if not parquet_files:
    print("No parquet files found in Intermediate folder.")
else:
    print(f"Found {len(parquet_files)} parquet file(s):")
    
    converted_count = 0
    skipped_count = 0
    
    for parquet_file in parquet_files:
        # Check if CSV version already exists
        csv_file = parquet_file.with_suffix('.csv')
        
        if csv_file.exists():
            print(f"\n⏭️  Skipping {parquet_file.name} (CSV already exists: {csv_file.name})")
            skipped_count += 1
            continue
        
        print(f"\nConverting: {parquet_file.name}")
        
        try:
            # Read parquet file
            table = pq.read_table(parquet_file)
            
            # Get column names
            column_names = table.column_names
            
            # Write to CSV
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                
                # Write header
                writer.writerow(column_names)
                
                # Write rows
                for i in range(table.num_rows):
                    row = [str(table[col][i].as_py()) if table[col][i] is not None else '' 
                           for col in column_names]
                    writer.writerow(row)
            
            print(f"  ✓ Successfully converted to: {csv_file.name}")
            print(f"    Rows: {table.num_rows:,}, Columns: {len(column_names)}")
            converted_count += 1
            
        except Exception as e:
            print(f"  ✗ Error converting {parquet_file.name}: {str(e)}")
            import traceback
            traceback.print_exc()
    
    print(f"\n{'='*60}")
    print(f"✓ Conversion complete!")
    print(f"  Converted: {converted_count} file(s)")
    print(f"  Skipped (CSV exists): {skipped_count} file(s)")
    print(f"  Total parquet files: {len(parquet_files)}")


