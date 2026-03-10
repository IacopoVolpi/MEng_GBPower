"""
Rerun ONLY the IV balancing market clearing for every day in 2024.

This script bypasses Snakemake entirely — it directly imports the clearing
functions from IV_clear_balancing_market.py and runs them for each day.
No upstream rules (build_base, solve_network, etc.) are touched.

The only change vs the original run: DRAXX-5 and DRAXX-6 are excluded from
bids/offers before clearing (see EXCLUDED_BMUS below).

Usage:
    python rerun_iv_clearing_only.py              # full year
    python rerun_iv_clearing_only.py 2024-06-01   # resume from a specific date
"""

import sys
import logging
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

# ── Configuration ──────────────────────────────────────────────────────────
START_DATE = sys.argv[1] if len(sys.argv) > 1 else "2024-01-01"
END_DATE = "2024-12-31"
ROOT = Path(__file__).parent.resolve()
RESULTS_DIR = ROOT / "results"
DATA_DIR = ROOT / "data" / "base"
PRERUN_DIR = ROOT / "data" / "prerun"
LOG_FILE = "rerun_iv_clearing_only.log"

# BMUs to exclude from the clearing algorithm
EXCLUDED_BMUS = ['DRAXX-5', 'DRAXX-6']
# ───────────────────────────────────────────────────────────────────────────

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# Import clearing functions (add scripts/ to path)
sys.path.insert(0, str(ROOT / "scripts"))
from IV_clear_balancing_market import (
    create_unit_lookup,
    annotate_bids_offers,
    run_balancing_market_clearing,
)

# ── Load BMU classification once (shared across all days) ──────────────────
log.info("Loading BMU classification...")
bmu_classification = pd.read_csv(PRERUN_DIR / "bmu_constraint_classification.csv", index_col=0)
log.info(f"Loaded {len(bmu_classification)} BMUs")

log.info("Building unit lookup tables...")
unit_lookup = create_unit_lookup(bmu_classification)
log.info(f"Classified {len(unit_lookup['zone'])} units")

# ── Iterate over days ──────────────────────────────────────────────────────
days = pd.date_range(START_DATE, END_DATE, freq="D").strftime("%Y-%m-%d")

log.info("=" * 70)
log.info(f"Rerunning IV clearing ONLY: {START_DATE} → {END_DATE}  ({len(days)} days)")
log.info(f"Excluded BMUs: {EXCLUDED_BMUS}")
log.info("=" * 70)

successes = []
failures = []
skipped = []

for i, day in enumerate(days, 1):
    t0 = datetime.now()
    day_dir = RESULTS_DIR / day
    data_day = DATA_DIR / day

    # ── Check all inputs exist ──────────────────────────────────────────
    zone_vol_path = day_dir / "IV_dispatch_changes_by_zone_and_SP_flex.csv"
    sub_bids_path = data_day / "submitted_bids.csv"
    sub_offers_path = data_day / "submitted_offers.csv"

    missing = [p for p in [zone_vol_path, sub_bids_path, sub_offers_path] if not p.exists()]
    if missing:
        log.warning(f"[{i}/{len(days)}]  {day}  SKIP — missing inputs: {[str(p.name) for p in missing]}")
        skipped.append(day)
        continue

    try:
        # ── Step 1: Load zone volumes ───────────────────────────────────
        zone_volumes = pd.read_csv(zone_vol_path, parse_dates=["timestamp"])
        zone_volumes["timestamp"] = zone_volumes["timestamp"].dt.tz_localize(None)
        zone_volumes = zone_volumes.set_index(["timestamp", "zone"])

        # ── Step 2: Load & preprocess bids and offers ───────────────────
        bids_raw = pd.read_csv(sub_bids_path, parse_dates=["timestamp"])
        offers_raw = pd.read_csv(sub_offers_path, parse_dates=["timestamp"])

        bids_df = bids_raw[["timestamp", "NationalGridBmUnit", "PairId", "Bid", "LevelFrom"]].copy()
        bids_df.columns = ["timestamp", "unit_id", "pair_id", "price", "volume_mw"]

        offers_df = offers_raw[["timestamp", "NationalGridBmUnit", "PairId", "Offer", "LevelFrom"]].copy()
        offers_df.columns = ["timestamp", "unit_id", "pair_id", "price", "volume_mw"]

        bids_df["timestamp"] = bids_df["timestamp"].dt.tz_localize(None)
        offers_df["timestamp"] = offers_df["timestamp"].dt.tz_localize(None)

        # Absolute bid volumes
        bids_df["volume_mw"] = bids_df["volume_mw"].abs()

        # Filter to classified BMUs only
        bids_df = bids_df[bids_df["unit_id"].isin(bmu_classification.index)]
        offers_df = offers_df[offers_df["unit_id"].isin(bmu_classification.index)]

        # ── Step 2.6: Exclude problematic BMUs ──────────────────────────
        bids_df = bids_df[~bids_df["unit_id"].isin(EXCLUDED_BMUS)]
        offers_df = offers_df[~offers_df["unit_id"].isin(EXCLUDED_BMUS)]

        # Ensure float64
        bids_df["volume_mw"] = bids_df["volume_mw"].astype("float64")
        offers_df["volume_mw"] = offers_df["volume_mw"].astype("float64")

        # ── Step 3: Annotate with zone/type ─────────────────────────────
        bids_df, offers_df = annotate_bids_offers(bids_df, offers_df, unit_lookup)

        bids_df["volume_mw"] = bids_df["volume_mw"].astype("float64")
        offers_df["volume_mw"] = offers_df["volume_mw"].astype("float64")

        # ── Step 4: Run clearing ────────────────────────────────────────
        settlement_summary, accepted_actions, uncleared_summary = run_balancing_market_clearing(
            zone_volumes, bids_df, offers_df, unit_lookup
        )

        # ── Step 5: Save outputs ────────────────────────────────────────
        settlement_summary.to_csv(day_dir / "IV_clearing_settlement_summary_flex.csv", index=False)

        if not accepted_actions.empty:
            accepted_actions = accepted_actions[
                ["timestamp", "zone", "unit_id", "carrier_type", "action_type",
                 "price_per_mwh", "cost_gbp", "volume_mwh"]
            ]
            accepted_actions["cost_gbp"] = accepted_actions["cost_gbp"].round(2)
            accepted_actions["volume_mwh"] = accepted_actions["volume_mwh"].round(2)
        accepted_actions.to_csv(day_dir / "IV_clearing_accepted_actions_flex.csv", index=False)

        uncleared_summary.to_csv(day_dir / "IV_clearing_uncleared_summary_flex.csv", index=False)

        elapsed = datetime.now() - t0
        n_actions = len(accepted_actions)
        vol = accepted_actions["volume_mwh"].sum() if not accepted_actions.empty else 0
        log.info(f"[{i}/{len(days)}]  {day}  OK  ({n_actions} actions, {vol:.0f} MWh, {elapsed})")
        successes.append(day)

    except Exception as e:
        elapsed = datetime.now() - t0
        log.error(f"[{i}/{len(days)}]  {day}  FAILED  ({e}, {elapsed})")
        failures.append(day)

# ── Summary ────────────────────────────────────────────────────────────────
log.info("=" * 70)
log.info(f"Finished.  Successes: {len(successes)}  |  Failures: {len(failures)}  |  Skipped: {len(skipped)}")
if failures:
    log.info(f"Failed days: {failures}")
if skipped:
    log.info(f"Skipped days (missing inputs): {skipped}")
log.info("=" * 70)
