"""
Run the GBPower model for every day in 2024.

Usage:
    python run_full_year_2024.py              # full year
    python run_full_year_2024.py 2024-05-06   # resume from a specific date

This iterates over every day from the start date to 2024-12-31 and calls
snakemake to produce the final IV_clearing_accepted_actions_flex.csv target.

Progress and errors are logged to run_full_year_2024.log so you can review
what happened overnight.
"""

import os
import sys
import logging
from datetime import datetime

import pandas as pd

# ── Configuration ──────────────────────────────────────────────────────────
START_DATE = sys.argv[1] if len(sys.argv) > 1 else "2024-01-01"
END_DATE = "2024-12-31"
TARGET_TEMPLATE = "results/{day}/IV_clearing_accepted_actions_flex.csv"
SNAKEMAKE_CMD = (
    'snakemake -c all --rerun-triggers mtime --configfile config.yaml -- "{target}"'
)
LOG_FILE = "run_full_year_2024.log"
# ───────────────────────────────────────────────────────────────────────────

# Set up logging to both file and console
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

days = pd.date_range(START_DATE, END_DATE, freq="D").strftime("%Y-%m-%d")

log.info(f"Starting full-year run: {START_DATE} → {END_DATE}  ({len(days)} days)")
log.info(f"Target template: {TARGET_TEMPLATE}")
log.info("=" * 70)

successes = []
failures = []

for i, day in enumerate(days, 1):
    target = TARGET_TEMPLATE.format(day=day)
    cmd = SNAKEMAKE_CMD.format(target=target)

    log.info(f"[{i}/{len(days)}]  Running {day}  ...")
    t0 = datetime.now()

    exit_code = os.system(cmd)

    elapsed = datetime.now() - t0
    if exit_code == 0:
        log.info(f"[{i}/{len(days)}]  {day}  ✓  OK  ({elapsed})")
        successes.append(day)
    else:
        log.error(f"[{i}/{len(days)}]  {day}  ✗  FAILED (exit {exit_code}, {elapsed})")
        failures.append(day)

log.info("=" * 70)
log.info(f"Finished.  Successes: {len(successes)}  |  Failures: {len(failures)}")
if failures:
    log.info(f"Failed days: {failures}")
log.info("=" * 70)
