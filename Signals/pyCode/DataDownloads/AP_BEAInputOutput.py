# ABOUTME: Live BEA Input-Output tables via BEA API (replaces static AllTablesSUP.zip download)
# ABOUTME: Fetches latest Supply Table and Supply-Use Framework tables from the InputOutput dataset
# ABOUTME: Outputs parquet + CSV files in ../pyData/Intermediate with "AP_" prefix
"""
Usage:
    python AP_BEAInputOutput.py   # run from Signals/pyCode

Inputs:
    - Environment variable BEA_API_KEY (required)

Outputs (written to ../pyData/Intermediate):
    - AP_BEA_Supply_Table.parquet / .csv
    - AP_BEA_SupplyUse_Framework.parquet / .csv

Notes:
    - Uses BEA API dataset InputOutput with method=GetParameterValues to discover TableIDs,
      then method=GetData for the selected tables.
    - Picks the latest TableID containing the keywords below to stay aligned with
      the original static files:
          Supply Table: ["supply table"]
          Supply-Use Framework: ["supply-use framework", "supply use framework", "use table"]
    - If BEA changes table names, update the keyword lists accordingly.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import List

import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

BASE_URL = "https://apps.bea.gov/api/data"
DATASET = "InputOutput"
OUTPUT_DIR = Path("../pyData/Intermediate")
PRE1997_URLS = [
    (
        "https://apps.bea.gov/industry/xls/io-annual/IOMake_Before_Redefinitions_1963-1996_Summary.xlsx",
        "AP_IOMake_Before_Redefinitions_1963-1996_Summary.xlsx",
    ),
    (
        "https://apps.bea.gov/industry/xls/io-annual/IOUse_Before_Redefinitions_PRO_1963-1996_Summary.xlsx",
        "AP_IOUse_Before_Redefinitions_PRO_1963-1996_Summary.xlsx",
    ),
]

SUPPLY_KEYWORDS = ["domestic supply of commodities", "supply of commodities"]
SUPPLYUSE_KEYWORDS = [
    "use of commodities by industries",
    "use table",
    "supply-use framework",
    "supply use framework",
]


def log(msg: str) -> None:
    print(msg, flush=True)


def get_api_key() -> str:
    key = os.getenv("BEA_API_KEY")
    if not key:
        raise ValueError("BEA_API_KEY not set. Add it to your environment or .env file.")
    return key


def call_bea(params: dict) -> dict:
    resp = requests.get(BASE_URL, params=params, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"BEA API HTTP {resp.status_code}: {resp.text[:200]}")
    try:
        payload = resp.json()
    except Exception as e:
        raise RuntimeError(f"Failed to parse BEA API response: {e}") from e
    if "BEAAPI" not in payload:
        raise RuntimeError(f"Unexpected BEA API response: {payload}")
    bea_response = payload["BEAAPI"]

    # Check for API errors in the response (Results can be dict or list)
    results = bea_response.get("Results")
    if isinstance(results, dict) and "Error" in results:
        error = results["Error"]
        error_code = error.get("APIErrorCode", "Unknown")
        error_desc = error.get("APIErrorDescription", "Unknown error")
        raise RuntimeError(f"BEA API Error {error_code}: {error_desc}")
    if isinstance(results, list):
        for item in results:
            if isinstance(item, dict) and "Error" in item:
                error = item["Error"]
                error_code = error.get("APIErrorCode", "Unknown")
                error_desc = error.get("APIErrorDescription", "Unknown error")
                raise RuntimeError(f"BEA API Error {error_code}: {error_desc}")

    return bea_response


def fetch_table_metadata(api_key: str) -> pd.DataFrame:
    params = {
        "UserID": api_key,
        "method": "GetParameterValues",
        "datasetname": DATASET,
        "ParameterName": "TableID",
    }
    bea = call_bea(params)
    results = bea.get("Results", {})
    
    # Check if ParamValue exists in results
    if "ParamValue" not in results:
        raise RuntimeError(f"No ParamValue in BEA response. Results keys: {list(results.keys())}")
    
    rows = results.get("ParamValue", [])
    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("No TableID metadata returned from BEA.")
    df["TableID"] = pd.to_numeric(df["Key"], errors="coerce")
    df["TableName"] = df["Desc"].astype(str)
    df = df.dropna(subset=["TableID"])
    return df[["TableID", "TableName"]]


def select_table_id(meta: pd.DataFrame, keywords: List[str]) -> int:
    kw_lower = [k.lower() for k in keywords]

    def match(name: str) -> bool:
        name_lower = name.lower()
        return any(k in name_lower for k in kw_lower)

    filtered = meta[meta["TableName"].map(match)]
    if filtered.empty:
        available = "; ".join(f"{row.TableID}: {row.TableName}" for row in meta.itertuples())
        raise RuntimeError(
            f"No table found matching keywords: {keywords}. "
            f"Available tables: {available}"
        )

    # Choose the highest TableID (newest) among matches
    table_id = int(filtered["TableID"].max())
    table_name = filtered.loc[filtered["TableID"].idxmax(), "TableName"]
    log(f"Selected TableID {table_id} -> {table_name}")
    return table_id


def fetch_table(api_key: str, table_id: int) -> pd.DataFrame:
    params = {
        "UserID": api_key,
        "method": "GetData",
        "datasetname": DATASET,
        "TableID": table_id,
        "Year": "ALL",
        "ResultFormat": "JSON",
    }
    bea = call_bea(params)
    results = bea.get("Results", [])
    data = []
    if isinstance(results, dict):
        data = results.get("Data", [])
    elif isinstance(results, list):
        for item in results:
            if isinstance(item, dict) and "Data" in item:
                data.extend(item["Data"])
    df = pd.DataFrame(data)
    if df.empty:
        raise RuntimeError(f"BEA returned no data for TableID {table_id}.")
    # Normalize types
    if "TableID" in df.columns:
        df["TableID"] = pd.to_numeric(df["TableID"], errors="coerce")
    if "Year" in df.columns:
        # Some BEA responses include 'Most Recent'. Keep years that are numeric or 'XREF'.
        df = df[df["Year"].astype(str).str.isnumeric()]
        df["Year"] = df["Year"].astype(int)
    return df


def save_excel_from_long(df: pd.DataFrame, base_name: str) -> None:
    """Pivot BEA long-form IO table to Excel with one sheet per year (mimics static files)."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    end_year = df["Year"].max()
    filename = f"{base_name}_1997-{end_year}_Summary.xlsx"
    path = OUTPUT_DIR / filename

    # Ensure numeric values and consistent ordering
    df = df.copy()
    df["DataValue"] = pd.to_numeric(df["DataValue"], errors="coerce")
    df = df[df["Year"].notna()]

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for year in sorted(df["Year"].unique()):
            sub = df[df["Year"] == year]
            if sub.empty:
                continue
            # Pivot to matrix with IO codes
            pivot = sub.pivot_table(
                index="RowCode", columns="ColCode", values="DataValue", aggfunc="first"
            ).sort_index(axis=1)
            row_descr = (
                sub.drop_duplicates("RowCode")
                .set_index("RowCode")["RowDescr"]
                .reindex(pivot.index)
            )
            pivot.insert(0, "Description", row_descr)
            pivot.insert(0, "IOCode", pivot.index)
            # Write with blank top rows so readxl(skip=5) still works
            pivot.to_excel(writer, sheet_name=str(year), startrow=5, index=False)

    log(f"Saved Excel -> {path.name}")
    return path


def save_outputs(df: pd.DataFrame, stem: str) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    parquet_path = OUTPUT_DIR / f"{stem}.parquet"
    csv_path = OUTPUT_DIR / f"{stem}.csv"
    df.to_parquet(parquet_path, index=False)
    df.to_csv(csv_path, index=False)
    log(f"Saved {len(df):,} rows -> {parquet_path.name} and {csv_path.name}")


def main() -> None:
    log("=" * 70)
    log("📊 AP_BEAInputOutput.py - Live BEA Supply/Use via API")
    log("=" * 70)

    api_key = get_api_key()
    meta = fetch_table_metadata(api_key)
    log(f"Loaded {len(meta):,} table metadata rows from BEA InputOutput dataset")

    supply_id = select_table_id(meta, SUPPLY_KEYWORDS)
    supplyuse_id = select_table_id(meta, SUPPLYUSE_KEYWORDS)

    log("\nDownloading Supply Table...")
    supply_df = fetch_table(api_key, supply_id)
    save_outputs(supply_df, "AP_BEA_Supply_Table")
    save_excel_from_long(supply_df, "AP_Supply_Tables")

    log("\nDownloading Supply-Use Framework...")
    supplyuse_df = fetch_table(api_key, supplyuse_id)
    save_outputs(supplyuse_df, "AP_BEA_SupplyUse_Framework")
    save_excel_from_long(supplyuse_df, "AP_Supply-Use_Framework")

    # Pre-1997 static tables (same files expected by IO momentum R script)
    log("\nDownloading pre-1997 Supply/Use Excel tables...")
    for url, filename in PRE1997_URLS:
        dest = OUTPUT_DIR / filename
        resp = requests.get(url, stream=True, timeout=300)
        resp.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        log(f"Saved {filename}")

    log("\n✅ AP_BEAInputOutput complete")
    log("=" * 70)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"❌ {e}")
        sys.exit(1)
