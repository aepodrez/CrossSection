#!/usr/bin/env python3
import requests
import pandas as pd
import tempfile
import os

# Download the Excel file
url = "https://site.warrington.ufl.edu/ritter/files/IPO-age.xlsx"
print(f"Downloading from: {url}")

response = requests.get(url, timeout=30)
response.raise_for_status()

# Create a temporary file to store the downloaded data
with tempfile.NamedTemporaryFile(mode='wb', suffix='.xlsx', delete=False) as temp_file:
    temp_file.write(response.content)
    temp_file_path = temp_file.name

print(f"Saved to: {temp_file_path}")

# Read the Excel file
data = pd.read_excel(temp_file_path)

# Clean up temporary file
os.unlink(temp_file_path)

print(f"Data shape: {data.shape}")
print(f"Columns: {list(data.columns)}")
print("\nFirst 5 rows:")
print(data.head().to_string())

print("\nColumn info:")
print(data.info()) 