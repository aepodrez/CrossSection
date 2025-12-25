# ABOUTME: Builds the AP version of SignalMasterTable using AP CRSP + AP Compustat.
# ABOUTME: Produces a 17-column table matching the original SignalMasterTable schema.
import sys
from pathlib import Path

import numpy as np
import pandas as pd


OUTPUT_PATH = Path("../pyData/Intermediate/AP_SignalMasterTable.parquet")
AP_MONTHLY_PATH = Path("../pyData/Intermediate/AP_monthlyCRSP.parquet")
AP_COMP_PATH = Path("../pyData/Intermediate/AP_m_aCompustat.parquet")
MAP_CANDIDATES = [
    Path("../pyData/Intermediate/AP_ticker_to_permno_monthly.csv"),
    Path("../pyData/Intermediate/AP_ticker_to_permno.csv"),
]


def load_ticker_map() -> pd.DataFrame:
    """Load ticker -> (permno, permco) mapping."""
    for path in MAP_CANDIDATES:
        if path.exists():
            df = pd.read_csv(path)
            if not {"ticker", "permno", "permco"}.issubset(df.columns):
                raise ValueError(f"Mapping {path} missing required columns")
            df["ticker"] = df["ticker"].str.upper()
            return df[["ticker", "permno", "permco"]]
    raise FileNotFoundError("No AP ticker->permno mapping found in expected paths.")


def load_ap_monthly() -> pd.DataFrame:
    """Load and filter AP monthly CRSP-like data."""
    if not AP_MONTHLY_PATH.exists():
        raise FileNotFoundError(f"Missing AP monthly CRSP at {AP_MONTHLY_PATH}")

    df = pd.read_parquet(AP_MONTHLY_PATH)
    df["ticker"] = df["ticker"].str.upper()

    # Filter to common stocks on major exchanges
    df = df[(df["shrcd"].isin([10, 11, 12])) & (df["exchcd"].isin([1, 2, 3]))].copy()

    # Type cleanup
    df["NYSE"] = (df["exchcd"] == 1).astype("int8")
    df["exchcd"] = df["exchcd"].astype("int8")
    df["shrcd"] = df["shrcd"].astype("int8")
    df["sicCRSP"] = df["sicCRSP"].astype("Int16")
    df["permco"] = df["permco"].astype("Int64")
    df["permno"] = df["permno"].astype("Int64")

    return df


def load_ap_comp(map_df: pd.DataFrame) -> pd.DataFrame:
    """Load AP Compustat monthly and attach permno/permco via ticker mapping."""
    if not AP_COMP_PATH.exists():
        print(f"⚠️  AP Compustat monthly missing at {AP_COMP_PATH}; filling gvkey/sicCS with blanks")
        return pd.DataFrame(columns=["permno", "time_avail_m", "gvkey", "sicCS"])

    comp = pd.read_parquet(AP_COMP_PATH)
    comp["ticker"] = comp["ticker"].str.upper()
    comp = comp.merge(map_df, on="ticker", how="left")

    comp_out = comp[["permno", "time_avail_m"]].copy()
    # Use cik as surrogate gvkey when available; otherwise fallback to permno
    comp_out["gvkey"] = comp.get("cik")
    comp_out["gvkey"] = comp_out["gvkey"].fillna(comp_out["permno"])
    comp_out["sicCS"] = ""

    comp_out["permno"] = comp_out["permno"].astype("Int64")
    comp_out["gvkey"] = comp_out["gvkey"].astype("float64")

    return comp_out


def build_master():
    print("🔄 Building AP_SignalMasterTable...")

    ticker_map = load_ticker_map()
    print(f"✓ Loaded ticker map: {len(ticker_map):,} tickers")

    monthly = load_ap_monthly()
    print(f"✓ Loaded AP_monthlyCRSP: {len(monthly):,} rows")

    comp = load_ap_comp(ticker_map)
    if not comp.empty:
        print(f"✓ Loaded AP_m_aCompustat: {len(comp):,} rows")
    else:
        print("⚠️  No Compustat data merged (file missing or empty)")

    df = monthly.merge(comp, on=["permno", "time_avail_m"], how="left")

    # Optional/empty identifiers to match schema
    # Add IBES ticker (if available)
    print("Checking for AP IBES-CRSP linking table...")
    
    AP_IBESCRSPLink_path = Path("../pyData/Intermediate/AP_IBESCRSPLinkingTable.parquet")
    if AP_IBESCRSPLink_path.exists():
        print("Adding AP IBES-CRSP link...")
        ibes_link = pd.read_parquet(
            AP_IBESCRSPLink_path,
            columns=["permno", "time_avail_m", "tickerIBES", "score"],
        )
        df = df.merge(ibes_link, on=["permno", "time_avail_m"], how="left")
        
        # Standardize IBES ticker string format (handle None -> empty string)
        if "tickerIBES" in df.columns:
            df["tickerIBES"] = df["tickerIBES"].fillna("")
        
        if "score" in df.columns:
            df["score"] = pd.to_numeric(df["score"], errors="coerce").astype("Int16")
        
        print(f"After IBES link merge: {df.shape[0]} rows, {df.shape[1]} columns")
    else:
        print("Not adding AP IBES-CRSP link. Some signals cannot be generated.")
        df["tickerIBES"] = ""
        df["score"] = pd.Series(pd.NA, index=df.index, dtype="Int16")
    
    df["secid"] = np.nan
    df["gvkey"] = df["gvkey"].fillna(df["permno"]).astype("float64")
    df["sicCS"] = df["sicCS"].fillna("")

    # Column order to match original SignalMasterTable
    cols = [
        "permno",
        "permco",
        "time_avail_m",
        "mve_c",
        "mve_permco",
        "ticker",
        "exchcd",
        "shrcd",
        "prc",
        "ret",
        "sicCRSP",
        "gvkey",
        "sicCS",
        "NYSE",
        "tickerIBES",
        "score",
        "secid",
    ]

    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in master build: {missing}")

    df = df[cols]

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT_PATH, index=False)

    print(f"✅ Saved {OUTPUT_PATH} with shape {df.shape}")
    print(df.head())


if __name__ == "__main__":
    try:
        build_master()
    except Exception as e:
        print(f"❌ Failed to build AP_SignalMasterTable: {e}")
        sys.exit(1)
