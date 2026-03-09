"""
extend_europe_prices.py
=======================
Extends data/europe_day_ahead_prices_GBP.csv with new dates from the EMBER CSVs
in data/new_day_ahead_prices/.

- Reads the 7 new EMBER country files (tab-separated, dd/mm/yyyy HH:MM UTC dates, EUR/MWhe)
- Downloads ECB EUR→GBP daily exchange rates for the required period via the ECB API
- Converts prices to GBP/MWhe
- Appends ONLY rows with timestamps after the last row already in the existing CSV
- Saves back to data/europe_day_ahead_prices_GBP.csv  (original rows untouched)

Run from the GBPower root directory:
    python extend_europe_prices.py
"""

import io
import sys
import requests
import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).parent
EXISTING_CSV = ROOT / "data" / "europe_day_ahead_prices_GBP.csv"
NEW_DATA_DIR = ROOT / "data" / "new_day_ahead_prices"

# Map from output column name → filename in new_day_ahead_prices/
COUNTRY_FILES = {
    "Norway":      "Norway.csv",
    "Belgium":     "Belgium.csv",
    "France":      "France.csv",
    "Germany":     "Germany.csv",
    "Netherlands": "Netherlands.csv",
    "Ireland":     "Irland.csv",   # intentional typo in the downloaded file
    "Denmark":     "Denmark.csv",
}


# ---------------------------------------------------------------------------
# 1. Load existing CSV and find the last timestamp
# ---------------------------------------------------------------------------
print(f"Loading existing CSV: {EXISTING_CSV}")
existing = pd.read_csv(EXISTING_CSV, index_col=0, parse_dates=True)
last_existing_ts = existing.index.max()
print(f"  Last timestamp in existing file: {last_existing_ts}")


# ---------------------------------------------------------------------------
# 2. Fetch ECB EUR/GBP daily exchange rates for the needed period
#    ECB series EXR.D.GBP.EUR.SP00.A  = GBP per 1 EUR
# ---------------------------------------------------------------------------
start_date = last_existing_ts.strftime("%Y-%m-%d")
end_date   = "2025-12-31"   # fetch a little beyond 2024 to be safe

ecb_url = (
    "https://data-api.ecb.europa.eu/service/data/EXR/D.GBP.EUR.SP00.A"
    f"?format=csvdata&startPeriod={start_date}&endPeriod={end_date}"
)
print(f"\nFetching ECB exchange rates ({start_date} → {end_date}) …")
resp = requests.get(ecb_url, timeout=30)
resp.raise_for_status()

ecb_df = pd.read_csv(io.StringIO(resp.text))
# The ECB csvdata format has columns TIME_PERIOD and OBS_VALUE
ecb_df = ecb_df[["TIME_PERIOD", "OBS_VALUE"]].copy()
ecb_df["TIME_PERIOD"] = pd.to_datetime(ecb_df["TIME_PERIOD"])
ecb_df = ecb_df.set_index("TIME_PERIOD")["OBS_VALUE"]
# Forward-fill weekends / bank holidays
ecb_df = (
    ecb_df.reindex(pd.date_range(ecb_df.index.min(), ecb_df.index.max(), freq="D"))
    .interpolate()
)
print(f"  Exchange rates fetched: {ecb_df.index.min().date()} → {ecb_df.index.max().date()}")


# ---------------------------------------------------------------------------
# 3. Read, convert and filter each new EMBER country CSV
# ---------------------------------------------------------------------------
def load_country(col_name: str, filename: str) -> pd.Series:
    filepath = NEW_DATA_DIR / filename
    df = pd.read_csv(
        filepath,
        sep="\t",
        parse_dates=["Datetime (UTC)"],
        dayfirst=True,       # dates are dd/mm/yyyy HH:MM
        dtype={"Price (EUR/MWhe)": float},
    )
    df = df[["Datetime (UTC)", "Price (EUR/MWhe)"]].copy()
    df = df.dropna(subset=["Datetime (UTC)", "Price (EUR/MWhe)"])

    # Keep only rows strictly after the last existing timestamp
    df = df[df["Datetime (UTC)"] > last_existing_ts]

    if df.empty:
        print(f"  {col_name}: no new rows (all data already covered)")
        return pd.Series(dtype=float, name=col_name)

    # Look up daily exchange rate and convert
    df["date"] = df["Datetime (UTC)"].dt.normalize()
    df["rate"] = df["date"].map(ecb_df)

    missing_rates = df["rate"].isna().sum()
    if missing_rates > 0:
        print(f"  WARNING: {col_name} has {missing_rates} rows with no exchange rate → interpolating")
        df["rate"] = df["rate"].interpolate()

    df[col_name] = df["Price (EUR/MWhe)"] * df["rate"]
    s = df.set_index("Datetime (UTC)")[col_name]
    print(f"  {col_name}: {len(s)} new hourly rows ({s.index.min()} → {s.index.max()})")
    return s


print("\nProcessing new country files …")
new_series = []
for col_name, filename in COUNTRY_FILES.items():
    s = load_country(col_name, filename)
    if not s.empty:
        new_series.append(s)

if not new_series:
    print("\nNothing to append — existing CSV is already up to date.")
    sys.exit(0)


# ---------------------------------------------------------------------------
# 4. Combine new country series into a DataFrame
# ---------------------------------------------------------------------------
new_data = pd.concat(new_series, axis=1)
new_data.index.name = "Datetime (UTC)"

# Ensure column order matches existing file exactly
new_data = new_data.reindex(columns=existing.columns)

missing_cols = new_data.columns[new_data.isna().all()].tolist()
if missing_cols:
    print(f"\nWARNING: these columns are entirely NaN in the new data: {missing_cols}")

print(f"\nNew rows to append: {len(new_data)}  ({new_data.index.min()} → {new_data.index.max()})")


# ---------------------------------------------------------------------------
# 5. Append and save
# ---------------------------------------------------------------------------
combined = pd.concat([existing, new_data])
combined = combined[~combined.index.duplicated(keep="first")]  # safety: no duplicate rows
combined.sort_index(inplace=True)

print(f"Combined CSV will have {len(combined)} rows  ({combined.index.min()} → {combined.index.max()})")

# Write back — same format as original
combined.to_csv(EXISTING_CSV)
print(f"\nSaved to {EXISTING_CSV}  ✓")
