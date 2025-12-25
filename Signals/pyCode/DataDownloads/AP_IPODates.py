# ABOUTME: Builds AP_IPODates by combining Ritter IPO-age spreadsheet with live IPO calendars
# ABOUTME: Injects provisional live IPOs (Nasdaq/NYSE) into the historical file and saves parquet/CSV
"""
Inputs:
  - Ritter Excel: https://site.warrington.ufl.edu/ritter/files/IPO-age.xlsx
  - Live IPO calendars: Nasdaq JSON + NYSE HTML table (best-effort; failures are logged)
  - Optional ticker->permno maps:
      ../pyData/Intermediate/AP_ticker_to_permno_monthly.csv
      ../pyData/Intermediate/AP_ticker_to_permno.csv

Outputs:
  - ../pyData/Intermediate/AP_IPODates.parquet
  - (optional) ../pyData/Intermediate/AP_IPODates.csv when SAVE_CSV=1

Notes:
  - Live IPO rows are marked provisional=True and may lack FoundingYear
  - If a ticker-to-permno map is available, permno is attached; otherwise permno stays missing
  - This mirrors the original IPODates.py schema plus extra metadata columns:
      permno, IPOdate, FoundingYear, ticker, company_name, exchange, source, provisional
"""

import os
from pathlib import Path
from datetime import datetime

import pandas as pd
import requests

OUTPUT_PARQUET = Path("../pyData/Intermediate/AP_IPODates.parquet")
OUTPUT_CSV = Path("../pyData/Intermediate/AP_IPODates.csv")
RITTER_URL = "https://site.warrington.ufl.edu/ritter/files/IPO-age.xlsx"

NASDAQ_IPO_URL = "https://api.nasdaq.com/api/ipo/calendar"
NYSE_IPO_URL = "https://www.nyse.com/ipo-center"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json,text/html",
    "Referer": "https://www.nasdaq.com/",
}

MAP_CANDIDATES = [
    Path("../pyData/Intermediate/AP_ticker_to_permno_monthly.csv"),
    Path("../pyData/Intermediate/AP_ticker_to_permno.csv"),
]


def load_ticker_map() -> pd.DataFrame:
    """Load ticker -> permno mapping when available."""
    for path in MAP_CANDIDATES:
        if path.exists():
            df = pd.read_csv(path)
            if "ticker" not in df.columns:
                continue
            df["ticker"] = df["ticker"].str.upper()
            keep_cols = ["ticker", "permno"]
            if "permco" in df.columns:
                keep_cols.append("permco")
            return df[keep_cols]
    print("⚠️  No AP ticker->permno map found; live IPOs will have missing permno")
    return pd.DataFrame(columns=["ticker", "permno"])


def load_ritter_base() -> pd.DataFrame:
    """Download and standardize Ritter IPO-age Excel."""
    print("📥 Downloading Ritter IPO-age spreadsheet...")
    resp = requests.get(RITTER_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    tmp_xlsx = Path("../pyData/Intermediate/temp_AP_IPO.xlsx")
    tmp_xlsx.write_bytes(resp.content)

    ipo = pd.read_excel(tmp_xlsx)
    tmp_xlsx.unlink(missing_ok=True)

    ipo = ipo.rename(columns={"Founding": "FoundingYear", "offer date": "OfferDate", "CRSP Perm": "permno"})
    ipo["permno"] = pd.to_numeric(ipo.get("permno"), errors="coerce")
    ipo["OfferDate"] = pd.to_datetime(ipo.get("OfferDate"), format="%Y%m%d", errors="coerce")
    ipo["IPOdate"] = ipo["OfferDate"].dt.to_period("M").dt.to_timestamp()
    ipo.loc[ipo.get("FoundingYear", pd.Series(dtype=float)) < 0, "FoundingYear"] = pd.NA

    ipo = ipo.dropna(subset=["permno"])
    ipo = ipo[ipo["permno"] > 0]
    ipo = ipo.drop_duplicates(subset=["permno"], keep="first")

    ipo = ipo.assign(
        ticker="",
        company_name="",
        exchange="",
        source="ritter_excel",
        provisional=False,
    )

    keep_cols = [
        "permno",
        "IPOdate",
        "FoundingYear",
        "ticker",
        "company_name",
        "exchange",
        "source",
        "provisional",
    ]
    return ipo[keep_cols]


def fetch_nasdaq_ipos() -> pd.DataFrame:
    """Best-effort fetch from Nasdaq calendar JSON."""
    resp = requests.get(NASDAQ_IPO_URL, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    rows = data.get("data", {}).get("rows", []) or []
    out = []
    for row in rows:
        out.append(
            {
                "company_name": row.get("companyName", ""),
                "ticker": (row.get("symbol") or "").upper(),
                "ipo_date": pd.to_datetime(row.get("offerDate"), errors="coerce"),
                "exchange": "NASDAQ",
                "source": "nasdaq_calendar",
                "provisional": True,
            }
        )
    return pd.DataFrame(out)


def fetch_nyse_ipos() -> pd.DataFrame:
    """Best-effort fetch from NYSE IPO HTML table."""
    tables = pd.read_html(NYSE_IPO_URL)
    if not tables:
        return pd.DataFrame(columns=["company_name", "ticker", "ipo_date", "exchange", "source", "provisional"])
    df = tables[0].rename(
        columns={"Company": "company_name", "Symbol": "ticker", "Pricing Date": "ipo_date"}
    )
    df["exchange"] = "NYSE"
    df["source"] = "nyse_calendar"
    df["provisional"] = True
    return df[["company_name", "ticker", "ipo_date", "exchange", "source", "provisional"]]


def load_live_ipos() -> pd.DataFrame:
    """Collect live IPOs from available calendars."""
    dfs = []
    for fetcher in (fetch_nasdaq_ipos, fetch_nyse_ipos):
        try:
            dfs.append(fetcher())
        except Exception as exc:  # pragma: no cover
            print(f"⚠️  Live IPO fetch failed: {exc}")
    if not dfs:
        return pd.DataFrame(columns=["company_name", "ticker", "ipo_date", "exchange", "source", "provisional"])
    live = pd.concat(dfs, ignore_index=True)
    live["ticker"] = live["ticker"].fillna("").str.upper()
    live["ipo_date"] = pd.to_datetime(live["ipo_date"], errors="coerce")
    live = live.dropna(subset=["ticker", "ipo_date"])
    live["IPOdate"] = live["ipo_date"].dt.to_period("M").dt.to_timestamp()
    live = live.drop(columns=["ipo_date"])
    live["FoundingYear"] = pd.NA  # unknown in live feeds
    live["permno"] = pd.NA
    cols = [
        "permno",
        "IPOdate",
        "FoundingYear",
        "ticker",
        "company_name",
        "exchange",
        "source",
        "provisional",
    ]
    return live[cols]


def attach_permno(df: pd.DataFrame, ticker_map: pd.DataFrame) -> pd.DataFrame:
    """Attach permno when ticker map is available."""
    if ticker_map.empty or "ticker" not in df.columns:
        return df
    if "permno" in df.columns:
        df = df.drop(columns=["permno"])
    out = df.merge(ticker_map[["ticker", "permno"]], on="ticker", how="left")
    return out


def build_ap_ipodates():
    print("=" * 70)
    print("🚀 Building AP_IPODates")
    print("=" * 70)

    ticker_map = load_ticker_map()

    # Base Ritter data
    base = load_ritter_base()
    print(f"✓ Ritter IPO rows: {len(base):,}")

    # Live IPO data (provisional)
    live = load_live_ipos()
    if not live.empty:
        live = attach_permno(live, ticker_map)
        print(f"✓ Live IPO rows: {len(live):,}")
    else:
        print("⚠️  No live IPO rows fetched")

    combined = pd.concat([base, live], ignore_index=True)

    # Deduplicate: prefer non-provisional then earliest IPOdate
    combined = combined.sort_values(["provisional", "IPOdate"]).drop_duplicates(
        subset=["permno", "ticker", "IPOdate"], keep="first"
    )

    # Fill missing string columns with blanks for CRSP-like compatibility
    for col in ["ticker", "company_name", "exchange", "source"]:
        if col in combined.columns:
            combined[col] = combined[col].fillna("")

    combined["IPOdate"] = pd.to_datetime(combined["IPOdate"])
    combined["FoundingYear"] = pd.to_numeric(combined["FoundingYear"], errors="coerce")

    OUTPUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(OUTPUT_PARQUET, index=False)
    print(f"✅ Saved {OUTPUT_PARQUET} ({len(combined):,} rows)")

    if os.getenv("SAVE_CSV", "0") == "1":
        combined.to_csv(OUTPUT_CSV, index=False)
        print(f"✅ Saved {OUTPUT_CSV}")

    print("Date range:", combined["IPOdate"].min(), "to", combined["IPOdate"].max())
    recent = combined[combined["provisional"] == True]  # noqa: E712
    print(f"Provisional live IPO rows: {len(recent):,}")


if __name__ == "__main__":
    try:
        build_ap_ipodates()
    except Exception as exc:  # pragma: no cover
        print(f"❌ Failed to build AP_IPODates: {exc}")
        raise
