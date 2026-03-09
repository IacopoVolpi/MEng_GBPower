"""
Morning-after check: audit every day in 2024 and report exactly which
files were produced and which are missing.

Usage:
    python check_results_2024.py

Prints a detailed report to the console AND saves it to
    check_results_2024_report.txt

The report covers:
  1. Data retrieval  (data/base/{day}/*)
  2. Intermediate files  (networks, simplified, clustered)
  3. Solved networks
  4. Post-processing outputs  (revenues, dispatch, system costs, frontend)
  5. Final IV outputs
  6. Per-day summary table  (also saved as check_results_2024_summary.csv)
"""

import os
import sys
from pathlib import Path
from datetime import datetime

import pandas as pd

# ── Configuration ──────────────────────────────────────────────────────────
START_DATE = "2024-01-01"
END_DATE = "2024-12-31"
BASE_DIR = Path(__file__).resolve().parent          # = C:\GBPower
RESULTS = BASE_DIR / "results"
DATA_BASE = BASE_DIR / "data" / "base"
FRONTEND = BASE_DIR / "frontend"
REPORT_FILE = BASE_DIR / "check_results_2024_report.txt"
CSV_FILE = BASE_DIR / "check_results_2024_summary.csv"
# ───────────────────────────────────────────────────────────────────────────

days = pd.date_range(START_DATE, END_DATE, freq="D").strftime("%Y-%m-%d").tolist()

# ── Define every expected file per day ─────────────────────────────────────
# Keys are human-readable column names; values are path templates with {day}
EXPECTED_FILES = {
    # --- Data retrieval (build_base) ---
    "base/settlement_period_register": "data/base/{day}/settlement_period_register.csv",
    "base/boundary_flow_constraints": "data/base/{day}/boundary_flow_constraints.csv",
    "base/physical_notifications": "data/base/{day}/physical_notifications.csv",
    "base/maximum_export_limits": "data/base/{day}/maximum_export_limits.csv",
    "base/offers": "data/base/{day}/offers.csv",
    "base/bids": "data/base/{day}/bids.csv",
    "base/nemo_powerflow": "data/base/{day}/nemo_powerflow.csv",
    "base/day_ahead_prices": "data/base/{day}/day_ahead_prices.csv",
    "base/europe_day_ahead_prices": "data/base/{day}/europe_day_ahead_prices.csv",
    "base/europe_generation": "data/base/{day}/europe_generation.csv",
    # --- Submitted bids/offers ---
    "base/submitted_bids": "data/base/{day}/submitted_bids.csv",
    "base/submitted_offers": "data/base/{day}/submitted_offers.csv",
    # --- Networks (add_electricity) ---
    "network_flex": "results/{day}/network_flex.nc",
    # --- Simplified ---
    "network_flex_s": "results/{day}/network_flex_s.nc",
    # --- Clustered ---
    "network_flex_s_nodal": "results/{day}/network_flex_s_nodal.nc",
    "network_flex_s_zonal": "results/{day}/network_flex_s_zonal.nc",
    "network_flex_s_national": "results/{day}/network_flex_s_national.nc",
    # --- Solved ---
    "solved_nodal": "results/{day}/network_flex_s_nodal_solved.nc",
    "solved_national": "results/{day}/network_flex_s_national_solved.nc",
    "solved_national_redisp": "results/{day}/network_flex_s_national_solved_redispatch.nc",
    "solved_zonal": "results/{day}/network_flex_s_zonal_solved.nc",
    "solved_zonal_redisp": "results/{day}/network_flex_s_zonal_solved_redispatch.nc",
    # --- Post-processing: revenues & dispatch ---
    "bmu_revenues_nodal": "results/{day}/bmu_revenues_flex_nodal.csv",
    "bmu_revenues_zonal": "results/{day}/bmu_revenues_flex_zonal.csv",
    "bmu_revenues_national": "results/{day}/bmu_revenues_flex_national.csv",
    "bmu_dispatch_nodal": "results/{day}/bmu_dispatch_flex_nodal.csv",
    "bmu_dispatch_zonal": "results/{day}/bmu_dispatch_flex_zonal.csv",
    "bmu_dispatch_national": "results/{day}/bmu_dispatch_flex_national.csv",
    "bmu_rev_detail_national": "results/{day}/bmu_revenues_detailed_flex_national.csv",
    "bmu_rev_detail_nodal": "results/{day}/bmu_revenues_detailed_flex_nodal.csv",
    "bmu_rev_detail_zonal": "results/{day}/bmu_revenues_detailed_flex_zonal.csv",
    "gb_total_load": "results/{day}/gb_total_load_flex.csv",
    # --- System cost ---
    "marginal_prices": "results/{day}/marginal_prices_flex.csv",
    "system_cost_summary": "results/{day}/system_cost_summary_flex.csv",
    # --- Frontend ---
    "frontend/revenues": "frontend/{day}/revenues_flex.csv",
    "frontend/dispatch": "frontend/{day}/dispatch_flex.csv",
    "frontend/dispatch_intercon": "frontend/{day}/dispatch_flex_flex_intercon.csv",
    "frontend/marginal_costs": "frontend/{day}/marginal_costs_flex.csv",
    "frontend/thermal_dispatch": "frontend/{day}/thermal_dispatch_flex.csv",
    # --- IV outputs ---
    "IV_dispatch_changes_zone_SP": "results/{day}/IV_dispatch_changes_by_zone_and_SP_flex.csv",
    "IV_dispatch_changes_zone": "results/{day}/IV_dispatch_changes_by_zone_flex.csv",
    "IV_dispatch_changes_zone_type": "results/{day}/IV_dispatch_changes_by_zone_and_type_flex.csv",
    "IV_settlement_summary": "results/{day}/IV_clearing_settlement_summary_flex.csv",
    "IV_accepted_actions": "results/{day}/IV_clearing_accepted_actions_flex.csv",
    "IV_uncleared_summary": "results/{day}/IV_clearing_uncleared_summary_flex.csv",
}

# ── Categorise files into pipeline stages ──────────────────────────────────
STAGES = {
    "1_data_retrieval": [k for k in EXPECTED_FILES if k.startswith("base/")],
    "2_network_build": [k for k in EXPECTED_FILES if k.startswith("network_flex") and "solved" not in k],
    "3_solve": [k for k in EXPECTED_FILES if "solved" in k],
    "4_postprocess": [
        k for k in EXPECTED_FILES
        if k.startswith("bmu_") or k in ("gb_total_load", "marginal_prices", "system_cost_summary")
    ],
    "5_frontend": [k for k in EXPECTED_FILES if k.startswith("frontend/")],
    "6_IV_outputs": [k for k in EXPECTED_FILES if k.startswith("IV_")],
}


def check_file(template: str, day: str) -> bool:
    return (BASE_DIR / template.format(day=day)).is_file()


# ── Build the full audit matrix ────────────────────────────────────────────
print("Scanning files for all 366 days of 2024 ...")
records = []
for day in days:
    row = {"day": day}
    for col, tmpl in EXPECTED_FILES.items():
        row[col] = check_file(tmpl, day)
    records.append(row)

df = pd.DataFrame(records).set_index("day")

# ── Generate report ────────────────────────────────────────────────────────
lines = []


def out(text=""):
    lines.append(text)
    print(text)


out("=" * 80)
out(f"  GBPower 2024 simulation results audit – {datetime.now():%Y-%m-%d %H:%M}")
out("=" * 80)
out()

# Overall summary
total_days = len(days)
fully_complete = df.all(axis=1).sum()
out(f"Days fully complete (all files present):  {fully_complete} / {total_days}")
out(f"Days with at least one file:              {df.any(axis=1).sum()} / {total_days}")
out(f"Days with zero files:                     {(~df.any(axis=1)).sum()} / {total_days}")
out()

# Per-stage summary
out("-" * 80)
out("STAGE COMPLETION SUMMARY")
out("-" * 80)
for stage_name, cols in STAGES.items():
    stage_df = df[cols]
    stage_complete = stage_df.all(axis=1).sum()
    stage_partial = (stage_df.any(axis=1) & ~stage_df.all(axis=1)).sum()
    stage_missing = (~stage_df.any(axis=1)).sum()
    out(f"  {stage_name:<25s}  complete: {stage_complete:>3d}  partial: {stage_partial:>3d}  missing: {stage_missing:>3d}")
out()

# Per-file summary
out("-" * 80)
out("PER-FILE AVAILABILITY (across all days)")
out("-" * 80)
for col in EXPECTED_FILES:
    count = df[col].sum()
    pct = 100.0 * count / total_days
    out(f"  {col:<40s}  {int(count):>3d}/{total_days}  ({pct:5.1f}%)")
out()

# Days where the final target is missing
out("-" * 80)
out("DAYS MISSING FINAL TARGET (IV_accepted_actions)")
out("-" * 80)
missing_final = df.index[~df["IV_accepted_actions"]].tolist()
if missing_final:
    # Group into contiguous ranges for readability
    ranges = []
    start = missing_final[0]
    prev = missing_final[0]
    for d in missing_final[1:]:
        if pd.Timestamp(d) - pd.Timestamp(prev) > pd.Timedelta("1D"):
            ranges.append((start, prev))
            start = d
        prev = d
    ranges.append((start, prev))
    for s, e in ranges:
        if s == e:
            out(f"  {s}")
        else:
            out(f"  {s}  →  {e}")
    out(f"  Total missing: {len(missing_final)} days")
else:
    out("  None – all days have the final target!")
out()

# Days where data retrieval succeeded but solve failed
out("-" * 80)
out("DAYS WHERE DATA RETRIEVAL SUCCEEDED BUT SOLVE FAILED")
out("-" * 80)
data_ok = df[[c for c in STAGES["1_data_retrieval"]]].all(axis=1)
solve_ok = df[[c for c in STAGES["3_solve"]]].all(axis=1)
data_but_no_solve = data_ok & ~solve_ok
bad_days = df.index[data_but_no_solve].tolist()
if bad_days:
    for d in bad_days:
        out(f"  {d}")
    out(f"  Total: {len(bad_days)} days")
else:
    out("  None")
out()

# Early-exit / partial failure detail for last 10 failed days
out("-" * 80)
out("FAILURE DETAIL (first 20 days missing final target)")
out("-" * 80)
for d in missing_final[:20]:
    out(f"\n  {d}:")
    for col, tmpl in EXPECTED_FILES.items():
        status = "✓" if check_file(tmpl, d) else "✗"
        out(f"    {status}  {col}")
out()

# ── Check the run log if it exists ─────────────────────────────────────────
run_log = BASE_DIR / "run_full_year_2024.log"
if run_log.is_file():
    out("-" * 80)
    out("ERRORS FROM run_full_year_2024.log")
    out("-" * 80)
    with open(run_log, "r", encoding="utf-8") as f:
        for line in f:
            if "FAILED" in line or "ERROR" in line or "Error" in line:
                out(f"  {line.rstrip()}")
    out()

# ── Save outputs ───────────────────────────────────────────────────────────
df.to_csv(CSV_FILE)
out(f"Summary CSV saved to: {CSV_FILE}")

with open(REPORT_FILE, "w", encoding="utf-8") as f:
    f.write("\n".join(lines))
out(f"Full report saved to: {REPORT_FILE}")

out()
out("Done.")
