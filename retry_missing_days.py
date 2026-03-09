"""
Retry the specific days that failed in the full 2024 run.

Usage:
    python retry_missing_days.py
"""

import os
import sys
import logging
from datetime import datetime

MISSING_DAYS = [
    "2024-09-24",
    "2024-09-25",
    "2024-09-26",
]

TARGET_TEMPLATE = "results/{day}/IV_clearing_accepted_actions_flex.csv"
SNAKEMAKE_CMD = (
    'snakemake -c all --rerun-triggers mtime --rerun-incomplete --configfile config.yaml -- "{target}"'
)
LOG_FILE = "run_full_year_2024.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

log.info(f"Retrying {len(MISSING_DAYS)} missing days")
log.info("=" * 70)

successes = []
failures = []

for i, day in enumerate(MISSING_DAYS, 1):
    target = TARGET_TEMPLATE.format(day=day)
    cmd = SNAKEMAKE_CMD.format(target=target)

    log.info(f"[{i}/{len(MISSING_DAYS)}]  Running {day}  ...")
    t0 = datetime.now()

    exit_code = os.system(cmd)

    elapsed = datetime.now() - t0
    if exit_code == 0:
        log.info(f"[{i}/{len(MISSING_DAYS)}]  {day}  ✓  OK  ({elapsed})")
        successes.append(day)
    else:
        log.error(f"[{i}/{len(MISSING_DAYS)}]  {day}  ✗  FAILED (exit {exit_code}, {elapsed})")
        failures.append(day)

log.info("=" * 70)
log.info(f"Done.  {len(successes)} succeeded,  {len(failures)} failed.")
if failures:
    log.error(f"Still failing: {failures}")
