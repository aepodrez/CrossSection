#!/usr/bin/env python3
"""
APId_Maintainer.py

Continuously maintains internal identifier mappings:

- AP_firm_id  <->  CIK (firm-level)
- AP_sec_id   <->  FIGI + ticker + exchange (security-level)
- Time-varying ticker_history: (ticker, exchange, AP_sec_id, from_d, thru_d)

Data sources:
- SEC company_tickers.json (free)
- OpenFIGI API (optional; for FIGI lookup by ticker+exchange)

Requires:
- Python 3.9+
- pip install requests pandas python-dotenv

Environment variables:
- EDGAR_IDENTITY    : Email or app string for SEC User-Agent
- OPENFIGI_API_KEY  : (optional) API key for OpenFIGI mapping
"""

import argparse
import datetime as dt
import json
import os
import sqlite3
import time
from typing import Optional, Dict, Any, List

import pandas as pd
import requests
from dotenv import load_dotenv

# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "..", "pyData", "Reference", "ap_master.db")

SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"  # SEC master (CIK, ticker, exchange)
OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"

# How often to refresh data in continuous mode (seconds)
SYNC_INTERVAL_SECONDS = 6 * 60 * 60  # 6 hours

OPEN_END_DATE = "9999-12-31"

# Simple mapping from SEC "exchange" field to MIC (you can extend this)
EXCHANGE_TO_MIC = {
    "Nasdaq": "XNAS",
    "NASDAQ": "XNAS",
    "New York Stock Exchange": "XNYS",
    "NYSE": "XNYS",
    "NYSE American": "XASE",
    "NYSE Arca": "ARCX",
    "Cboe BZX": "BATS",
}


# -----------------------------------------------------------------------------
# ENV + SESSION
# -----------------------------------------------------------------------------

load_dotenv()

EDGAR_IDENTITY = os.getenv("EDGAR_IDENTITY", "your-email@example.com APIdMaintainer/1.0")
OPENFIGI_API_KEY = os.getenv("OPENFIGI_API_KEY", None)

SEC_HEADERS = {
    "User-Agent": EDGAR_IDENTITY,
    "Accept-Encoding": "gzip, deflate",
    "Host": "www.sec.gov",
}

OPENFIGI_HEADERS = {
    "Content-Type": "application/json",
}
if OPENFIGI_API_KEY:
    OPENFIGI_HEADERS["X-OPENFIGI-APIKEY"] = OPENFIGI_API_KEY


# -----------------------------------------------------------------------------
# DB INIT
# -----------------------------------------------------------------------------

def ensure_directories():
    db_dir = os.path.dirname(DB_PATH)
    os.makedirs(db_dir, exist_ok=True)


def get_conn() -> sqlite3.Connection:
    ensure_directories()
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db():
    """Create tables if they don't exist."""
    conn = get_conn()
    cur = conn.cursor()

    # firm_master: one row per firm/entity
    cur.execute("""
    CREATE TABLE IF NOT EXISTS firm_master (
        ap_firm_id     INTEGER PRIMARY KEY AUTOINCREMENT,
        cik_str        TEXT UNIQUE NOT NULL,
        cik_int        INTEGER NOT NULL,
        primary_name   TEXT,
        country        TEXT,
        sic            TEXT,
        first_seen_d   TEXT,
        last_seen_d    TEXT
    );
    """)

    # security_master: one row per security (share class / line of stock)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS security_master (
        ap_sec_id        INTEGER PRIMARY KEY AUTOINCREMENT,
        ap_firm_id       INTEGER NOT NULL,
        figi             TEXT UNIQUE,
        ticker_current   TEXT,
        exchange_current TEXT,
        mic              TEXT,
        security_type    TEXT,
        first_trade_d    TEXT,
        last_trade_d     TEXT,
        FOREIGN KEY(ap_firm_id) REFERENCES firm_master(ap_firm_id)
    );
    """)

    # ticker_history: time-varying mapping ticker+exchange -> ap_sec_id
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ticker_history (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        ap_sec_id   INTEGER NOT NULL,
        ticker      TEXT NOT NULL,
        exchange    TEXT,
        from_d      TEXT NOT NULL,
        thru_d      TEXT NOT NULL,
        is_primary  INTEGER NOT NULL DEFAULT 1,
        source      TEXT NOT NULL,
        FOREIGN KEY(ap_sec_id) REFERENCES security_master(ap_sec_id)
    );
    """)

    # Helpful indexes
    cur.execute("CREATE INDEX IF NOT EXISTS idx_firm_cik ON firm_master(cik_str);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sec_firm ON security_master(ap_firm_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ticker_hist_tkr ON ticker_history(ticker, exchange);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ticker_hist_open ON ticker_history(thru_d);")

    conn.commit()
    conn.close()


# -----------------------------------------------------------------------------
# SEC TICKERS
# -----------------------------------------------------------------------------

def fetch_sec_tickers() -> pd.DataFrame:
    """
    Fetch SEC company_tickers.json and return as DataFrame with columns:
    cik_str, cik_int, ticker, exchange, name
    """
    print("[SEC] Downloading company_tickers.json ...", flush=True)
    resp = requests.get(SEC_TICKERS_URL, headers=SEC_HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # SEC gives a dict keyed by index, each value with {cik, ticker, title, exchange}
    rows = []
    for _, v in data.items():
        cik_int = int(v["cik"])
        cik_str = f"{cik_int:010d}"
        rows.append({
            "cik_str": cik_str,
            "cik_int": cik_int,
            "ticker": v["ticker"].upper().strip(),
            "exchange": v.get("exchange", "").strip(),
            "name": v.get("title", "").strip(),
        })

    df = pd.DataFrame(rows)
    print(f"[SEC] Loaded {len(df)} rows from SEC ticker file.", flush=True)
    return df


# -----------------------------------------------------------------------------
# OPENFIGI
# -----------------------------------------------------------------------------

def map_figi_batch(rows: List[Dict[str, Any]]) -> Dict[str, Optional[str]]:
    """
    Map a batch of (ticker, exchange) to FIGI using OpenFIGI.

    rows: list of dicts with keys: ticker, exchange
    Returns: dict key=(ticker,exchange) -> figi or None
    """
    if not OPENFIGI_API_KEY:
        # FIGI mapping not configured; return None for all
        return { (r["ticker"], r["exchange"]): None for r in rows }

    # Prepare OpenFIGI request body
    # We use 'ticker' + 'exchCode' (approximate; you can refine with MIC)
    payload = []
    for r in rows:
        exch = r["exchange"]
        mic = EXCHANGE_TO_MIC.get(exch, None)
        entry = {
            "idType": "TICKER",
            "idValue": r["ticker"],
        }
        if mic:
            entry["micCode"] = mic
        payload.append(entry)

    print(f"[FIGI] Requesting FIGI for {len(payload)} tickers ...", flush=True)
    resp = requests.post(OPENFIGI_URL, headers=OPENFIGI_HEADERS, data=json.dumps(payload), timeout=30)
    resp.raise_for_status()
    results = resp.json()

    out: Dict[str, Optional[str]] = {}
    for r, res in zip(rows, results):
        key = (r["ticker"], r["exchange"])
        if res is None or "data" not in res or len(res["data"]) == 0:
            out[key] = None
        else:
            # Take the first mapping; you may want smarter selection later
            out[key] = res["data"][0].get("figi")
    return out


# -----------------------------------------------------------------------------
# DB HELPERS
# -----------------------------------------------------------------------------

def upsert_firm(conn: sqlite3.Connection, cik_str: str, cik_int: int, name: str) -> int:
    """Ensure firm_master row exists for this CIK; return ap_firm_id."""
    today = dt.date.today().isoformat()
    cur = conn.cursor()

    cur.execute("SELECT ap_firm_id, first_seen_d FROM firm_master WHERE cik_str = ?", (cik_str,))
    row = cur.fetchone()
    if row:
        ap_firm_id, first_seen_d = row
        # Update last_seen_d and name
        cur.execute("""
            UPDATE firm_master
            SET primary_name = COALESCE(?, primary_name),
                last_seen_d  = ?
            WHERE ap_firm_id = ?;
        """, (name or None, today, ap_firm_id))
        conn.commit()
        return ap_firm_id

    # Insert new firm
    cur.execute("""
        INSERT INTO firm_master (cik_str, cik_int, primary_name, first_seen_d, last_seen_d)
        VALUES (?, ?, ?, ?, ?);
    """, (cik_str, cik_int, name or None, today, today))
    conn.commit()
    return cur.lastrowid


def get_or_create_security(
    conn: sqlite3.Connection,
    ap_firm_id: int,
    ticker: str,
    exchange: str,
    figi: Optional[str],
) -> int:
    """
    Find or create a security_master row.

    Priority:
    - If figi is available: match on figi
    - Else: match on (ap_firm_id, ticker_current, exchange_current)

    Returns ap_sec_id.
    """
    cur = conn.cursor()

    if figi:
        cur.execute("SELECT ap_sec_id FROM security_master WHERE figi = ?", (figi,))
        row = cur.fetchone()
        if row:
            ap_sec_id = row[0]
            # Update ticker_current/exchange_current (latest snapshot)
            cur.execute("""
                UPDATE security_master
                SET ticker_current = ?, exchange_current = ?
                WHERE ap_sec_id = ?;
            """, (ticker, exchange, ap_sec_id))
            conn.commit()
            return ap_sec_id

    # Fallback: look by firm + ticker + exchange
    cur.execute("""
        SELECT ap_sec_id FROM security_master
        WHERE ap_firm_id = ? AND ticker_current = ? AND exchange_current = ?;
    """, (ap_firm_id, ticker, exchange))
    row = cur.fetchone()
    if row:
        return row[0]

    # Create new security
    cur.execute("""
        INSERT INTO security_master (ap_firm_id, figi, ticker_current, exchange_current, mic, security_type)
        VALUES (?, ?, ?, ?, ?, ?);
    """, (ap_firm_id, figi, ticker, exchange, EXCHANGE_TO_MIC.get(exchange, None), "CommonStock"))
    conn.commit()
    return cur.lastrowid


def ensure_ticker_history_open(
    conn: sqlite3.Connection,
    ap_sec_id: int,
    ticker: str,
    exchange: str,
    source: str = "SEC"
):
    """
    Ensure there is an open (thru_d = OPEN_END_DATE) ticker_history row
    for this AP_sec_id + ticker + exchange. If none, insert a new one
    with from_d = today.
    """
    today = dt.date.today().isoformat()
    cur = conn.cursor()

    cur.execute("""
        SELECT id FROM ticker_history
        WHERE ap_sec_id = ?
          AND ticker = ?
          AND IFNULL(exchange, '') = IFNULL(?, '')
          AND thru_d = ?;
    """, (ap_sec_id, ticker, exchange, OPEN_END_DATE))
    row = cur.fetchone()
    if row:
        return  # already open

    # Insert a new open interval
    cur.execute("""
        INSERT INTO ticker_history (ap_sec_id, ticker, exchange, from_d, thru_d, is_primary, source)
        VALUES (?, ?, ?, ?, ?, 1, ?);
    """, (ap_sec_id, ticker, exchange, today, OPEN_END_DATE, source))
    conn.commit()


def close_ticker_history_rows_not_in_sec_snapshot(conn: sqlite3.Connection, current_snapshot: pd.DataFrame):
    """
    For any open ticker_history rows with source='SEC' whose (cik_str,ticker,exchange)
    combination is no longer in the current SEC ticker snapshot, set thru_d = today - 1.

    This handles tickers that disappeared from SEC file (e.g., delisted or renamed).
    """
    today = dt.date.today()
    thru_d_close = (today - dt.timedelta(days=1)).isoformat()

    # Build set of current SEC (cik_str, ticker, exchange)
    if current_snapshot.empty:
        active = set()
    else:
        active = set(
            (row.cik_str, row.ticker, row.exchange)
            for row in current_snapshot.itertuples(index=False)
        )

    cur = conn.cursor()

    # Fetch open ticker_history rows with source='SEC'
    cur.execute("""
        SELECT th.id, th.ap_sec_id, th.ticker, th.exchange, fm.cik_str
        FROM ticker_history AS th
        JOIN security_master AS sm ON th.ap_sec_id = sm.ap_sec_id
        JOIN firm_master    AS fm ON sm.ap_firm_id = fm.ap_firm_id
        WHERE th.source = 'SEC'
          AND th.thru_d = ?;
    """, (OPEN_END_DATE,))
    rows = cur.fetchall()

    to_close_ids = []
    for row in rows:
        th_id, _, ticker, exchange, cik_str = row
        key = (cik_str, ticker, exchange if exchange is not None else "")
        if key not in active:
            to_close_ids.append(th_id)

    if not to_close_ids:
        print("[TICKER] No old SEC tickers to close.", flush=True)
        return

    print(f"[TICKER] Closing {len(to_close_ids)} outdated SEC ticker_history rows ...", flush=True)
    cur.executemany(
        "UPDATE ticker_history SET thru_d = ? WHERE id = ?;",
        [(thru_d_close, th_id) for th_id in to_close_ids]
    )
    conn.commit()


# -----------------------------------------------------------------------------
# SYNC LOGIC
# -----------------------------------------------------------------------------

def sync_once():
    """
    One maintenance cycle:
    - Fetch SEC ticker snapshot
    - Map (ticker,exchange) -> FIGI (optional)
    - Upsert into firm_master, security_master
    - Maintain ticker_history open intervals
    - Close ticker_history rows for tickers no longer in SEC snapshot
    """
    print("=" * 80, flush=True)
    print(f"🧩 AP_id sync cycle started @ {dt.datetime.now()}", flush=True)
    print("=" * 80, flush=True)

    sec_df = fetch_sec_tickers()

    # Optionally enrich with FIGI
    # Here we do it in batches to respect API limits
    all_rows = sec_df[["ticker", "exchange"]].drop_duplicates().to_dict(orient="records")
    figi_map: Dict[tuple, Optional[str]] = {}
    batch_size = 50
    for i in range(0, len(all_rows), batch_size):
        batch = all_rows[i: i + batch_size]
        figi_map.update(map_figi_batch(batch))

    conn = get_conn()

    try:
        # Upsert firms and securities
        for row in sec_df.itertuples(index=False):
            cik_str = row.cik_str
            cik_int = row.cik_int
            ticker = row.ticker
            exchange = row.exchange
            name = row.name

            # 1) Firm
            ap_firm_id = upsert_firm(conn, cik_str, cik_int, name)

            # 2) FIGI lookup
            figi = figi_map.get((ticker, exchange), None)

            # 3) Security
            ap_sec_id = get_or_create_security(conn, ap_firm_id, ticker, exchange, figi)

            # 4) Ticker history (keep open)
            ensure_ticker_history_open(conn, ap_sec_id, ticker, exchange, source="SEC")

        # 5) Close outdated SEC tickers
        close_ticker_history_rows_not_in_sec_snapshot(conn, sec_df)

    finally:
        conn.close()

    print("=" * 80, flush=True)
    print(f"✅ AP_id sync cycle completed @ {dt.datetime.now()}", flush=True)
    print("=" * 80, flush=True)


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Maintain AP_id / CIK / FIGI / ticker mappings continuously or once.")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single sync cycle and exit (no continuous loop).",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=SYNC_INTERVAL_SECONDS,
        help=f"Sync interval in seconds (default {SYNC_INTERVAL_SECONDS}).",
    )
    args = parser.parse_args()

    init_db()

    if args.once:
        sync_once()
        return

    # Continuous mode
    while True:
        sync_once()
        print(f"Sleeping for {args.interval_seconds} seconds ...", flush=True)
        time.sleep(args.interval_seconds)


if __name__ == "__main__":
    main()
