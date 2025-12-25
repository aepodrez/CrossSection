#!/bin/bash
# Download OptionMetrics raw tables from WRDS PostgreSQL
# Filters match the R scripts to reduce file sizes
# Run on WRDS cluster: bash download_optionmetrics_tables.sh

set +e  # Don't exit on error

echo "=========================================="
echo "OptionMetrics Table Download Script"
echo "Started at: $(date)"
echo "=========================================="

# Create output directories
BASE_DIR="$HOME/temp_prep/OptionMetricsRaw"
mkdir -p "$BASE_DIR/opprcd"
mkdir -p "$BASE_DIR/vsurfd"
mkdir -p "$BASE_DIR/secprd"

echo "Output directory: $BASE_DIR"
echo ""

# Database connection
DB_HOST="wrds-pgdata.wharton.upenn.edu"
DB_PORT="9737"
DB_NAME="wrds"
DB_USER="${PGUSER:-$USER}"

# Check for password
if [ -z "$PGPASSWORD" ]; then
    echo "ERROR: PGPASSWORD environment variable not set!"
    echo "Run: export PGPASSWORD='your_password'"
    exit 1
fi
echo "Using database user: $DB_USER"

# Function to download opprcd (Option Prices) - filtered
download_opprcd() {
    local year=$1
    local output_file="$BASE_DIR/opprcd/opprcd${year}.csv"
    
    echo "[$(date +%H:%M:%S)] Downloading opprcd${year}..."
    
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -w -c "\copy (SELECT secid, optionid, date, exdate, volume FROM optionm.opprcd${year} WHERE cp_flag != 'NaN' AND volume > 0 AND exdate - date >= 5 AND exdate - date <= 47) TO '$output_file' CSV HEADER;" 2>&1 | grep -v "COPY" || true
    
    if [ -f "$output_file" ] && [ -s "$output_file" ]; then
        local size=$(du -h "$output_file" | cut -f1)
        echo "  ✓ Saved: $output_file ($size)"
    else
        echo "  ✗ Failed to download opprcd${year}"
    fi
}

# Function to download vsurfd (Volatility Surface) - filtered
download_vsurfd() {
    local year=$1
    local output_file="$BASE_DIR/vsurfd/vsurfd${year}.csv"
    
    echo "[$(date +%H:%M:%S)] Downloading vsurfd${year}..."
    
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -w -c "\copy (SELECT secid, date, days, cp_flag, delta, impl_volatility FROM optionm.vsurfd${year} WHERE impl_volatility != 'NaN' AND abs(delta) = 50 AND days IN (30, 91)) TO '$output_file' CSV HEADER;" 2>&1 | grep -v "COPY" || true
    
    if [ -f "$output_file" ] && [ -s "$output_file" ]; then
        local size=$(du -h "$output_file" | cut -f1)
        echo "  ✓ Saved: $output_file ($size)"
    else
        echo "  ✗ Failed to download vsurfd${year}"
    fi
}

# Function to download secprd (Security Prices) - filtered
download_secprd() {
    local year=$1
    local output_file="$BASE_DIR/secprd/secprd${year}.csv"
    
    echo "[$(date +%H:%M:%S)] Downloading secprd${year}..."
    
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -w -c "\copy (SELECT secid, date, close, volume FROM optionm.secprd${year} WHERE volume > 0) TO '$output_file' CSV HEADER;" 2>&1 | grep -v "COPY" || true
    
    if [ -f "$output_file" ] && [ -s "$output_file" ]; then
        local size=$(du -h "$output_file" | cut -f1)
        echo "  ✓ Saved: $output_file ($size)"
    else
        echo "  ✗ Failed to download secprd${year}"
    fi
}

# Download securd (Security Reference - single table)
download_securd() {
    local output_file="$BASE_DIR/securd.csv"
    
    echo "[$(date +%H:%M:%S)] Downloading securd..."
    
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -w -c "\copy (SELECT ticker, secid FROM optionm.securd) TO '$output_file' CSV HEADER;" 2>&1 | grep -v "COPY" || true
    
    if [ -f "$output_file" ] && [ -s "$output_file" ]; then
        local size=$(du -h "$output_file" | cut -f1)
        echo "  ✓ Saved: $output_file ($size)"
    else
        echo "  ✗ Failed to download securd"
    fi
}

# Main download loop
echo "Starting downloads..."
echo ""

# Download yearly tables (2019-2025 only - earlier years already downloaded)
for year in {2019..2025}; do
    echo "--- Processing year $year ---"
    download_opprcd "$year"
    download_vsurfd "$year"
    download_secprd "$year"
    echo ""
done

# Download securd (single table)
download_securd

echo ""
echo "=========================================="
echo "Download Summary"
echo "=========================================="
echo "opprcd files: $(ls -1 $BASE_DIR/opprcd/*.csv 2>/dev/null | wc -l)"
echo "vsurfd files: $(ls -1 $BASE_DIR/vsurfd/*.csv 2>/dev/null | wc -l)"
echo "secprd files: $(ls -1 $BASE_DIR/secprd/*.csv 2>/dev/null | wc -l)"
echo "securd file: $([ -f $BASE_DIR/securd.csv ] && echo "1" || echo "0")"
echo ""
echo "Total size:"
du -sh "$BASE_DIR" 2>/dev/null || echo "Could not calculate size"
echo ""
echo "Completed at: $(date)"
echo "=========================================="
echo ""
echo "Files are ready in: $BASE_DIR"
echo "You can now download them using scp from your local machine:"
echo "  scp -r $USER@wrds-cloud.wharton.upenn.edu:$BASE_DIR/* /local/path/"
