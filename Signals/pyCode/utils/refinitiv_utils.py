import os
import time
from typing import Iterable, List, Optional

import pandas as pd
import refinitiv.data as rd
from refinitiv.data import _configure as rd_config
from refinitiv.data.discovery import SymbolTypes, convert_symbols


def convert_tickers_to_rics(
    tickers: Iterable[str],
    batch_size: Optional[int] = None,
    max_retries: Optional[int] = None,
    retry_backoff: Optional[int] = None,
) -> List[str]:
    """
    Convert ticker symbols to Refinitiv RIC format using symbology conversion.
    Includes basic batching and retry logic to mitigate transient failures.
    """
    tickers = list(tickers or [])
    print(f"\nConverting {len(tickers)} tickers to RIC format...")

    if not tickers:
        print("⚠️  No tickers provided.")
        return []

    batch_size = batch_size or int(os.getenv("IBES_BATCH_SIZE", "10"))
    max_retries = 0 if max_retries is None else max_retries
    retry_backoff = retry_backoff or int(os.getenv("RD_RETRY_BACKOFF", "2"))

    resolved_rics: List[str] = []
    unresolved: List[str] = []
    ticker_symbol_type = getattr(SymbolTypes, "TICKER_SYMBOL", getattr(SymbolTypes, "TICKER", None))
    ric_symbol_type = getattr(SymbolTypes, "RIC", None)

    if ric_symbol_type is None:
        print("⚠️  Refinitiv symbology SymbolTypes.RIC not available. Using .O fallback for all tickers.")
        fallback_rics = [f"{str(t).upper()}.O" for t in tickers]
        print(f"✓ Converted to {len(fallback_rics)} RICs (fallback)")
        return fallback_rics

    printed_columns = False

    for i in range(0, len(tickers), batch_size):
        batch = [t for t in tickers[i : i + batch_size] if isinstance(t, str) and t]
        if not batch:
            continue

        print(
            f"Resolving batch {i//batch_size + 1}/{(len(tickers)-1)//batch_size + 1} "
            f"({len(batch)} tickers)...",
            end=" ",
        )

        df = None
        for attempt in range(max_retries + 1):
            try:
                df = convert_symbols(
                    symbols=[t.upper() for t in batch],
                    from_symbol_type=ticker_symbol_type or "_AllUnique",
                    to_symbol_types=(ric_symbol_type,),
                )
                break
            except Exception as e:
                if attempt < max_retries:
                    sleep_time = retry_backoff * (attempt + 1)
                    print(f"Error: {e} | retrying in {sleep_time}s", end=" ")
                    time.sleep(sleep_time)
                else:
                    print(f"Error: {e}")

        mapping = {}
        if df is not None and len(df) > 0:
            ric_cols = [c for c in df.columns if "ric" in str(c).lower()]
            input_cols = [
                c for c in df.columns if "input" in str(c).lower() or "request" in str(c).lower() or "ticker" in str(c).lower()
            ]

            if not printed_columns:
                print(f"Columns returned from conversion: {list(df.columns)}")
                printed_columns = True

            if ric_cols:
                ric_col = ric_cols[0]
                input_col = input_cols[0] if input_cols else None

                if input_col:
                    for _, row in df.iterrows():
                        input_symbol = row.get(input_col)
                        ric_value = next(
                            (row[c] for c in ric_cols if c in row and pd.notna(row[c])),
                            None,
                        )
                        if pd.notna(input_symbol) and pd.notna(ric_value):
                            mapping[str(input_symbol).upper()] = str(ric_value)
                elif len(df) == len(batch):
                    for original, ric_value in zip(batch, df[ric_col].tolist()):
                        if pd.notna(ric_value):
                            mapping[original.upper()] = str(ric_value)
            else:
                print("  ⚠️  No RIC column in conversion response; using fallback for this batch.")

            print(f"Resolved {len(mapping)} tickers")
        else:
            print("No data returned from Refinitiv")

        for ticker in batch:
            ric = mapping.get(ticker.upper())
            if ric:
                resolved_rics.append(ric)
            else:
                fallback = f"{ticker.upper()}.O"
                unresolved.append(ticker)
                print(f"\n  ⚠️  Could not resolve '{ticker}'. Using fallback {fallback}")
                resolved_rics.append(fallback)

    if unresolved:
        unique_unresolved = sorted(set(unresolved))
        preview = unique_unresolved[:10]
        more_flag = "..." if len(unique_unresolved) > 10 else ""
        print(f"⚠️  {len(unresolved)} tickers could not be resolved directly: {preview}{more_flag}")

    print(f"✓ Converted to {len(resolved_rics)} RICs")
    return resolved_rics


def is_token_error(error: Exception) -> bool:
    msg = str(error).lower()
    return "token expired" in msg or "invalid_grant" in msg


def initialize_refinitiv_platform_session(http_timeout: Optional[int] = None) -> None:
    """
    Initialize Refinitiv Data session using the Platform (RDP).
    Expects REFINITIV_APP_KEY, REFINITIV_USERNAME, REFINITIV_PASSWORD in env.
    """
    app_key = os.getenv("REFINITIV_APP_KEY")
    username = os.getenv("REFINITIV_USERNAME")
    password = os.getenv("REFINITIV_PASSWORD")

    if not app_key or not username or not password:
        raise Exception("REFINITIV_APP_KEY, REFINITIV_USERNAME, and REFINITIV_PASSWORD must be set in your .env file.")

    session_name = "rdp"
    session_path = f"sessions.platform.{session_name}"

    rd_config.set_param(f"{session_path}.app-key", app_key, auto_create=True)
    rd_config.set_param(f"{session_path}.username", username, auto_create=True)
    rd_config.set_param(f"{session_path}.password", password, auto_create=True)
    rd_config.set_param("sessions.default", f"platform.{session_name}", auto_create=True)

    if http_timeout is None:
        http_timeout = int(os.getenv("RD_HTTP_TIMEOUT", "60"))
    rd_config.set_param("http.request-timeout", http_timeout, auto_create=True)

    try:
        rd.open_session(f"platform.{session_name}")
        test_df = rd.get_data(universe=["AAPL.O"], fields=["TR.CompanyName"])
        if test_df is None:
            raise Exception("Platform session opened but test request returned no data.")
        print("Connected to Refinitiv Platform session")
    except Exception as e:
        try:
            rd.close_session()
        except Exception:
            pass
        raise Exception(f"Could not connect to Refinitiv Platform with APP_KEY. {e}")


def refresh_refinitiv_platform_session(http_timeout: Optional[int] = None) -> None:
    """
    Close and reopen the Refinitiv session. Useful after token expiry errors.
    """
    try:
        rd.close_session()
    except Exception:
        pass
    initialize_refinitiv_platform_session(http_timeout=http_timeout)
