# ABOUTME: Tail risk beta using AP (yfinance-based) CRSP proxies.
# ABOUTME: Mirrors BetaTailRisk.py but reads AP_dailyCRSP/AP_monthlyCRSP and
# ABOUTME: saves AP_BetaTailRisk.csv.

"""
AP_BetaTailRisk.py

Usage:
    Run from [Repo-Root]/Signals/pyCode/
    python3 Predictors/AP_BetaTailRisk.py

Inputs (built from yfinance via AP_CRSPDaily.py and AP_CRSPMonthly.py):
    - AP_dailyCRSP.parquet: Daily CRSP-like data [permno, time_d, ret]
    - AP_monthlyCRSP.parquet: Monthly CRSP-like data [permno, time_avail_m, ret, shrcd]

Outputs:
    - AP_TailRisk.parquet: Monthly tail risk factor
    - AP_BetaTailRisk.csv: [permno, yyyymm, AP_BetaTailRisk]
"""

import os
import sys
from pathlib import Path

try:
    import polars as pl
    import polars_ols as pls  # Registers .least_squares namespace
except ImportError:
    print("❌ Required packages missing. Please install: pip install polars polars-ols")
    sys.exit(1)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from utils.save_standardized import save_predictor


PREDICTOR_NAME = "AP_BetaTailRisk"
SCRIPT_DIR = Path(__file__).resolve().parent
# Data lives under Signals/pyData/, which is a sibling of pyCode
DATA_DIR = SCRIPT_DIR.parent.parent / "pyData" / "Intermediate"
DAILY_PATH = DATA_DIR / "AP_dailyCRSP.parquet"
MONTHLY_PATH = DATA_DIR / "AP_monthlyCRSP.parquet"
TAILRISK_PATH = DATA_DIR / "AP_TailRisk.parquet"


def check_inputs():
    missing = [p for p in [DAILY_PATH, MONTHLY_PATH] if not p.exists()]
    if missing:
        missing_str = ", ".join(str(p) for p in missing)
        raise FileNotFoundError(
            f"Missing required AP data: {missing_str}. "
            "Run AP_CRSPDaily.py and AP_CRSPMonthly.py first (these pull from yfinance)."
        )


def main():
    print("=" * 80)
    print("🏗️  AP_BetaTailRisk.py")
    print("Generating Tail Risk Beta predictor using AP (yfinance) data")
    print("=" * 80)

    check_inputs()

    # PART 1: CREATE TAIL RISK FACTOR FROM DAILY DATA
    print("📊 Part 1: Creating monthly tail risk factor from AP daily returns...")
    print(f"Loading {DAILY_PATH.name}...")
    daily_crsp = pl.read_parquet(str(DAILY_PATH)).select(["permno", "time_d", "ret"])
    print(f"Loaded AP daily CRSP-like data: {len(daily_crsp):,} daily observations")

    # Convert daily dates to monthly (drop timezone to avoid join mismatch)
    daily_crsp = daily_crsp.with_columns(
        [pl.col("time_d").dt.truncate("1mo").dt.date().alias("time_avail_m")]
    )

    print("Calculating 5th percentile returns by month...")
    monthly_p5 = daily_crsp.group_by("time_avail_m").agg(
        [pl.col("ret").quantile(0.05, interpolation="lower").alias("retp5")]
    )
    print(f"Generated monthly 5th percentiles for {len(monthly_p5):,} months")

    # Merge back to daily data
    daily_with_p5 = daily_crsp.join(monthly_p5, on="time_avail_m", how="inner")

    print("Filtering to tail observations (bottom 5%) and calculating tail excess returns...")
    tail_data = daily_with_p5.filter(pl.col("ret") <= pl.col("retp5"))
    tail_data = tail_data.with_columns(
        [(pl.col("ret") / pl.col("retp5")).log().alias("tailex")]
    )

    print(f"Filtered to {len(tail_data):,} tail observations")

    monthly_tailrisk = (
        tail_data.group_by("time_avail_m")
        .agg([pl.col("tailex").mean().alias("tailex")])
        .with_columns(pl.col("time_avail_m").dt.date().alias("time_avail_m"))
        .sort("time_avail_m")
    )
    print(f"Generated monthly tail risk factor for {len(monthly_tailrisk):,} months")

    # Save intermediate tail risk factor
    monthly_tailrisk.write_parquet(str(TAILRISK_PATH))
    print(f"Saved {TAILRISK_PATH.name}")

    # PART 2: BETA REGRESSION WITH MONTHLY DATA
    print("📊 Part 2: Computing tail risk betas from AP monthly returns...")
    print(f"Loading {MONTHLY_PATH.name}...")
    monthly_crsp = (
        pl.read_parquet(str(MONTHLY_PATH))
        .select(["permno", "time_avail_m", "ret", "shrcd"])
        .with_columns(pl.col("time_avail_m").dt.date().alias("time_avail_m"))
    )
    print(f"Loaded AP monthly CRSP-like data: {len(monthly_crsp):,} monthly observations")

    print("Merging with tail risk factor...")
    df = monthly_crsp.join(monthly_tailrisk, on="time_avail_m", how="left").sort(
        ["permno", "time_avail_m"]
    )
    print(f"After merging: {len(df):,} observations")

    # Convert time_avail_m to integer for window-based regression
    df = df.with_columns(
        [
            (
                (pl.col("time_avail_m").dt.year() - 1960) * 12
                + (pl.col("time_avail_m").dt.month() - 1)
            ).alias("time_avail_m_int")
        ]
    )

    total_months = df["time_avail_m"].n_unique()
    window_size = 120
    min_periods = 72
    if total_months < window_size:
        # Fall back when AP history is short (yfinance defaults to ~2 years in AP downloads)
        window_size = total_months
        min_periods = max(12, int(window_size * 0.6))
        print(
            f"⚠️  Only {total_months} months available; "
            f"using window_size={window_size}, min_periods={min_periods} instead of 120/72."
        )

    print(
        f"Computing rolling {window_size}-month tail risk betas for {df['permno'].n_unique():,} unique permnos..."
    )
    print(
        f"Rolling {window_size}-month regression windows with minimum {min_periods} observations per permno"
    )

    df = df.sort(["permno", "time_avail_m_int"])

    df_with_beta = df.with_columns(
        pl.col("ret")
        .least_squares.rolling_ols(
            pl.col("tailex"),
            window_size=window_size,
            min_periods=min_periods,
            mode="coefficients",
            add_intercept=True,
            null_policy="drop",
        )
        .over("permno")
        .alias("coef")
    ).with_columns(
        [
            pl.col("coef").struct.field("const").alias("b_const"),
            pl.col("coef").struct.field("tailex").alias("b_tailex"),
        ]
    )

    df_with_beta = df_with_beta.with_columns(
        pl.col("b_tailex").alias(PREDICTOR_NAME)
    )

    # Apply filters: remove missing betas and keep common stocks (shrcd <= 11)
    df_final = df_with_beta.filter(
        pl.col(PREDICTOR_NAME).is_not_null() & (pl.col("shrcd") <= 11)
    ).select(["permno", "time_avail_m", PREDICTOR_NAME])

    print(f"Generated {PREDICTOR_NAME} values: {len(df_final):,} observations")

    if len(df_final) > 0:
        df_final_pd = df_final.to_pandas()

        print(f"{PREDICTOR_NAME} summary stats:")
        print(f"  Mean: {df_final_pd[PREDICTOR_NAME].mean():.4f}")
        print(f"  Std: {df_final_pd[PREDICTOR_NAME].std():.4f}")
        print(f"  Min: {df_final_pd[PREDICTOR_NAME].min():.4f}")
        print(f"  Max: {df_final_pd[PREDICTOR_NAME].max():.4f}")

        print(f"💾 Saving {PREDICTOR_NAME} predictor...")
        save_predictor(df_final_pd, PREDICTOR_NAME)
        print(f"✅ {PREDICTOR_NAME}.csv saved successfully")
    else:
        print(f"⚠️ No {PREDICTOR_NAME} values generated - check data and parameters")

    print("=" * 80)
    print("✅ AP_BetaTailRisk.py Complete")
    print("Tail risk beta predictor generated using AP yfinance-based data")
    print("=" * 80)


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as e:
        print(f"❌ {e}")
        sys.exit(1)
