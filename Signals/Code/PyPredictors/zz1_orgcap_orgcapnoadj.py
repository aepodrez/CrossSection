"""
OrgCap & OrgCapNoAdj  –  Python port of the 2021-AC Stata routine
---------------------------------------------------------------

Steps replicated 1-for-1:

1.  Load SignalMasterTable, Compustat monthly file, and quarterly GNP deflator
2.  Keep December fiscal-year-end observations, drop 6000–6999 SICs
3.  Fill missing XSGA with 0, deflate by GNP price index
4.  Build the Eisfeldt-Papanikolaou stock:
        OrgCapNoAdj_t = 4·XSGA_t   (first 12 obs)
                     = 0.85·OrgCapNoAdj_{t-12} + XSGA_t   (age > 12)
   then scale by AT and set 0 → NaN
5.  Winsorise OrgCapNoAdj by month at 1st/99th pctls
6.  Map SIC to Fama-French 17 industries and z-score within (month × FF17)
7.  Save two CSVs:
        Data/Predictors/OrgCap.csv
        Data/Placebos/OrgCapNoAdj.csv
"""

from pathlib import Path
import pandas as pd
import numpy as np
import logging

# ---------------------------------------------------------------------  logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s"
)
log = logging.getLogger(__name__)

# ----------------------------------------------------------------  FF-17 mapping
def sic_to_ff17(sic: float) -> float:
    """Vectorised SIC→FF17 mapper (returns NaN if not in a bucket)."""
    if np.isnan(sic):
        return np.nan
    sic = int(sic)
    if 100 <= sic <= 999:
        return 1  # Agriculture
    if 1000 <= sic <= 1299 or 1400 <= sic <= 1499:
        return 2  # Mining
    if 1300 <= sic <= 1399:
        return 3  # Oil & gas
    if 1500 <= sic <= 1799:
        return 4  # Construction
    if 2000 <= sic <= 2399:
        return 5  # Food, textiles
    if 2400 <= sic <= 2799:
        return 6  # Paper, print, etc.
    if 2800 <= sic <= 3099:
        return 7  # Chemicals
    if 3100 <= sic <= 3199:
        return 8  # Rubber
    if 3200 <= sic <= 3569:
        return 9  # Machinery
    if 3570 <= sic <= 3999:
        return 10  # Electronics, Instr.
    if 4000 <= sic <= 4799:
        return 11  # Transp.
    if 4800 <= sic <= 4899:
        return 12  # Telecom
    if 4900 <= sic <= 4999:
        return 13  # Utilities
    if 5000 <= sic <= 5999:
        return 14  # Wholesale, Retail
    if 6000 <= sic <= 6999:
        return 15  # **Financials**   (we will drop later)
    if 7000 <= sic <= 8999:
        return 16  # Services
    if sic >= 9100:
        return 17  # Public admin & other
    return np.nan


# --------------------------------------------------------  paths & input files
BASE = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data")
INT   = BASE / "Intermediate"
OUT_P = BASE / "Placebos"
OUT_S = BASE / "Predictors"
OUT_P.mkdir(parents=True, exist_ok=True)
OUT_S.mkdir(parents=True, exist_ok=True)

def zz1_orgcap_orgcapnoadj():
    """
    Python equivalent of ZZ1_OrgCap_OrgCapNoAdj.do
    
    Constructs the OrgCap and OrgCapNoAdj predictor signals.
    """
    log.info("Constructing predictor signals: OrgCap and OrgCapNoAdj...")
    
    try:
        # ------------------------------------------------------------------  load data
        log.info("Loading SignalMasterTable …")
        master = pd.read_csv(
            INT / "SignalMasterTable.csv",
            usecols=[
                "permno",
                "time_avail_m",
                "sicCRSP",
                "shrcd",
                "exchcd",
            ],
        )

        log.info("Loading Compustat monthly file …")
        comp = pd.read_csv(
            INT / "m_aCompustat.csv",
            usecols=["permno", "time_avail_m", "xsga", "at", "datadate", "sic"],
        )

        log.info("Loading GNP deflator …")
        gnpdefl = pd.read_csv(
            INT / "GNPdefl.csv",  # columns: time_avail_m, gnpdefl
        )

        # -----------------------------------------------------------  merge & filters
        df = (
            master.merge(comp, on=["permno", "time_avail_m"], how="inner")
            .merge(gnpdefl, on="time_avail_m", how="left")
        )

        # SIC numeric
        df["sic"] = pd.to_numeric(df["sic"], errors="coerce")

        # Keep December FYE & non-financial SICs
        log.info("Applying December-FYE + SIC filters …")
        df["datadate"] = pd.to_datetime(df["datadate"])
        good = (
            (df["datadate"].dt.month == 12)
            & ((df["sic"] < 6000) | (df["sic"] >= 7000))
            & (~df["sic"].isna())
        )
        df = df.loc[good].copy()

        # -----------------------------------------------------  organisational capital
        df = df.sort_values(["permno", "time_avail_m"])
        df["xsga"] = df["xsga"].fillna(0) / df["gnpdefl"]  # deflate

        # build age index (1,2,3,… by permno)
        df["age"] = df.groupby("permno").cumcount() + 1

        # recursive stock
        log.info("Building OrgCapNoAdj stock …")
        df["OrgCapNoAdj"] = np.nan

        def compute_stock(sub):
            sub = sub.sort_values("time_avail_m").copy()
            xsga = sub["xsga"].values
            stock = np.empty_like(xsga)
            for i in range(len(xsga)):
                if i < 12:
                    stock[i] = 4 * xsga[i]
                else:
                    stock[i] = 0.85 * stock[i - 12] + xsga[i]
            sub["OrgCapNoAdj"] = stock / sub["at"].values
            return sub

        df = df.groupby("permno", group_keys=False).apply(compute_stock)
        df["OrgCapNoAdj"].replace(0, np.nan, inplace=True)

        # --------------------------------------------  winsorise by month (1-99 pct)
        log.info("Winsorising by month …")
        def winsorise(x):
            low, high = np.nanpercentile(x, [1, 99])
            return np.clip(x, low, high)

        df["OrgCapNoAdjtemp"] = (
            df.groupby("time_avail_m")["OrgCapNoAdj"].transform(winsorise)
        )

        # ----------------------------------------------------------  FF-17 industry
        log.info("Assigning FF-17 industries …")
        df["tempFF17"] = df["sicCRSP"].apply(sic_to_ff17)
        df = df[df["tempFF17"].notna()].copy()

        # -------------------------  z-score within (month × industry) to get OrgCap
        log.info("Computing industry-adjusted z-scores …")
        g = df.groupby(["tempFF17", "time_avail_m"])["OrgCapNoAdjtemp"]
        df["OrgCap"] = (df["OrgCapNoAdjtemp"] - g.transform("mean")) / g.transform("std")

        # --------------------------------------------------------  prepare & save out
        def _prepare(sub_df, colname):
            out = sub_df[["permno", "time_avail_m", colname]].dropna().copy()
            out["yyyymm"] = (
                pd.to_datetime(out["time_avail_m"]).dt.year * 100
                + pd.to_datetime(out["time_avail_m"]).dt.month
            )
            return out[["permno", "yyyymm", colname]]

        log.info("Saving OrgCapNoAdj (placebo) …")
        _prepare(df, "OrgCapNoAdj").to_csv(OUT_P / "OrgCapNoAdj.csv", index=False)

        log.info("Saving OrgCap (predictor) …")
        _prepare(df, "OrgCap").to_csv(OUT_S / "OrgCap.csv", index=False)

        log.info("OrgCap pipeline complete — files written")
        return True
        
    except Exception as e:
        log.error(f"Failed to construct OrgCap and OrgCapNoAdj predictors: {e}")
        return False

if __name__ == "__main__":
    # Run the predictor construction function
    zz1_orgcap_orgcapnoadj()