from __future__ import annotations

import time
from typing import Any, Mapping, Optional, Sequence

import pandas as pd
import refinitiv.data as rd

from utils.refinitiv_utils import is_token_error, refresh_refinitiv_platform_session


def rd_get_data_with_refresh(
    *,
    universe: Sequence[str],
    fields: Sequence[str],
    parameters: Mapping[str, Any],
    max_retries: int,
    retry_backoff: int,
    http_timeout: Optional[int] = None,
    print_errors: bool = True,
    error_prefix: str = "Error",
    print_inline: bool = True,
) -> pd.DataFrame:
    """
    Call `rd.get_data()` with simple retry + token-refresh handling.

    Returns an empty DataFrame on failure (after retries) to match the existing
    download-script pattern of continuing past failed batches.
    """
    df = None

    for attempt in range(max_retries + 1):
        try:
            df = rd.get_data(universe=list(universe), fields=list(fields), parameters=dict(parameters))
            break
        except Exception as e:
            if attempt < max_retries:
                sleep_time = retry_backoff * (attempt + 1)
                if print_errors:
                    end = " " if print_inline else "\n"
                    print(
                        f"{error_prefix} (attempt {attempt+1}/{max_retries+1}): {e} | retrying in {sleep_time}s",
                        end=end,
                    )
                if is_token_error(e):
                    if print_errors:
                        end = " " if print_inline else "\n"
                        print("Attempting session refresh due to token error...", end=end)
                    refresh_refinitiv_platform_session(http_timeout=http_timeout)
                time.sleep(sleep_time)
            else:
                if print_errors:
                    print(f"{error_prefix}: {e}")

    if df is None:
        return pd.DataFrame()

    return df
