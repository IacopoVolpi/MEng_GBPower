### IV Balancing Market Clearing Extension for GBPower

An extension to the **[GBPower](README.md)** electricity market model that adds an explicit **Balancing Mechanism (BM) clearing simulation** for Great Britain. 
#### How It Works

The IV pipeline sits on top of GBPower's existing wholesale and redispatch simulations:

```
Wholesale Solution (day-ahead market clearing)
        |
Redispatch Solution (with transmission constraints)
        |
IV_distribute_balancing_volume
   -> Dispatch changes per zone per settlement period
        |
IV_clear_balancing_market
   -> Merit-order clearing of submitted bids/offers
        |
Results: accepted actions, settlement summaries, uncleared volumes
```

**Geographic zones.** GB is divided into 6 zones based on transmission constraint boundaries (SSE-SP, SCOTEX, SSHARN, FLOWSTH, SEIMP). Each Balancing Mechanism Unit (BMU) is classified into a zone. Clearing proceeds north-to-south (Red -> Orange -> Green -> Blue -> Purple -> Yellow), consuming bids/offers in merit order.

**Merit-order clearing.** For each settlement period and zone:
- **Upward balancing (turn-up):** offers are accepted in ascending price order (cheapest first).
- **Downward balancing (turn-down):** bids are accepted in descending price order (highest first).

Accepted actions are removed from the pool before the next zone clears.

---

#### IV Scripts

All IV scripts live in `scripts/`:

| Script | Purpose |
|--------|---------|
| `IV_classify_bmu_constraints.py` | **Pre-processing.** Classifies BMUs into geographic zones based on their coordinates relative to transmission constraint boundaries. One-time run; outputs are shipped with the repo. |
| `IV_build_all_bm_actions.py` | **Data fetch.** Downloads all submitted bids and offers from the Elexon BMRS API for a given day. |
| `IV_distribute_balancing_volume.py` | **Volume calculation.** Compares wholesale vs. redispatch dispatch to compute required flex (up/down) per zone per settlement period. |
| `IV_clear_balancing_market.py` | **Market clearing.** Main algorithm: accepts bids/offers in merit order to satisfy each zone's balancing requirement. Outputs settlement summaries and accepted actions. |

**Diagnostic / validation scripts** (not part of the pipeline, run standalone):

| Script | Purpose |
|--------|---------|
| `IV_test_clearing_bids.py` | Traces where submitted bids are filtered out during clearing. |
| `IV_test_early_exits.py` | Counts and categorises periods where clearing cannot proceed (no bids, no volume). |
| `IV_test_bm_acceptance_results.py` | Compares clearing results against real market accepted actions. |
| `IV_test_clearing_market.py` | End-to-end step-by-step validation of all clearing functions. |

---

#### Data Inputs and Outputs

**Pre-run data** (shipped with the repo, in `data/prerun/`):
- `bmu_constraint_classification.csv` -- BMU-to-zone mapping.
- `balancing_constraint_boundaries.yaml` -- Fitted boundary parameters for zone classification.

**Per-day inputs** (in `data/base/{day}/`):
- `submitted_bids.csv`, `submitted_offers.csv` -- All submitted bids/offers from Elexon API.
- `bids.csv`, `offers.csv` -- Real market accepted bids/offers (used for validation only).

**Per-day outputs** (in `results/{day}/`):
- `IV_dispatch_changes_by_zone_and_SP_flex.csv` -- Required flex per zone per settlement period.
- `IV_dispatch_changes_by_zone_flex.csv` -- Aggregated flex by zone.
- `IV_dispatch_changes_by_zone_and_type_flex.csv` -- Aggregated flex by zone and carrier type.
- `IV_clearing_settlement_summary_flex.csv` -- Settlement summary with required, cleared, and uncleared volumes per zone.
- `IV_clearing_accepted_actions_flex.csv` -- Every accepted bid/offer with unit, zone, carrier, price, volume, and cost.
- `IV_clearing_uncleared_summary_flex.csv` -- Periods flagged with insufficient bids/offers.

---

#### Installation

Follow the base GBPower installation from the [main README](README.md):

```bash
mamba env create -f envs/environment.yaml
conda activate gbpower
```

Create `scripts/_tokens.py` with your ENTSO-E API key:
```python
ENTSOE_API_KEY = '...'
```

---

#### Running the IV Pipeline

**Option 1: Via Snakemake (recommended for single days)**

Run the full pipeline for a specific day (wholesale -> redispatch -> IV clearing):

```bash
snakemake -call --configfile config.yaml -- results/2024-03-21/IV_clearing_accepted_actions_flex.csv
```

**Option 2: Full year via helper script**

Run the full pipeline for every day in 2024:

```bash
python run_full_year_2024.py                # Start from 2024-01-01
python run_full_year_2024.py 2024-06-01     # Resume from a specific date
```

Progress is logged to `run_full_year_2024.log`.

**Option 3: Re-run IV clearing only (skip network solving)**

If the wholesale and redispatch networks are already solved, re-run just the clearing step:

```bash
python rerun_iv_clearing_only.py                # Full year from 2024-01-01
python rerun_iv_clearing_only.py 2024-06-01     # Resume from a specific date
```

This imports the clearing functions directly and bypasses Snakemake, making it much faster for iterating on the clearing algorithm.

**Option 4: Retry specific failed days**

Edit the list of days in `retry_missing_days.py` and run:

```bash
python retry_missing_days.py
```

---

#### Validating Results

**Check coverage for 2024:**

```bash
python check_results_2024.py
```

This loads all daily clearing outputs and reports aggregate statistics (total volume, cost, acceptance rates).

**Compare against real market outcomes:**

```bash
python scripts/IV_test_bm_acceptance_results.py
```

Prints a detailed comparison of your clearing results vs. real accepted bids/offers from Elexon, including volume alignment, unit matching, cost breakdown by technology, and zone-level analysis.

**Step-by-step clearing inspection:**

```bash
python scripts/IV_test_clearing_market.py
```

Tests each clearing function individually and prints intermediate results for debugging.

---

#### Analysis Notebooks

All IV notebooks live in `notebooks/` and produce figures prefixed with `IV_fig_`:

| Notebook | What it shows |
|----------|---------------|
| `IV_plot_transmissions_constraints.ipynb` | Geographic map of the 6 zones and transmission constraint boundaries. |
| `IV_plot_balancing_volumes.ipynb` | Time series of required balancing volumes (flex up/down) by zone. |
| `IV_volume_comparison.ipynb` | Model clearing volumes vs. real market volumes, with unit-level comparison and acceptance rates. |
| `IV_cost_comparison.ipynb` | Clearing cost comparison: model vs. real market, broken down by technology and zone. |
| `IV_cost_per_volume.ipynb` | Unit cost analysis (GBP/MWh) over time, by zone and technology. |
| `IV_flexibility_analysis.ipynb` | Deep dive into flexibility provision: zone-level profiles, intra-day patterns, seasonal trends. |
| `IV_technology_mix.ipynb` | Technology composition of cleared actions: carrier participation rates, market share, and cost breakdown. |

Run the notebooks after the pipeline has produced results for the desired date range.

---

#### Snakemake Rules

The IV rules are defined in:
- `rules/prerun_rules.smk` -- `IV_classify_bmu_constraints` (one-time pre-processing).
- `rules/postprocess.smk` -- `IV_distribute_balancing_volume` and `IV_clear_balancing_market` (per-day post-processing).

These integrate into the existing GBPower Snakemake DAG: after the wholesale and redispatch networks are solved, the IV rules produce the clearing outputs automatically.

---

#### Technical Notes

- **Timestamp handling:** All timestamps are timezone-naive UTC to match PyPSA conventions. The clearing scripts strip timezone info from Elexon data.
- **Volume units:** PyPSA dispatch values are in MW (1-hour basis). Settlement periods are 30 minutes, so dispatch is multiplied by 0.5 to convert to MWh.
- **Excluded BMUs:** DRAXX-5 and DRAXX-6 (biomass units with GBP 0 bids) are excluded from clearing to avoid distorting results.
- **Uncleared volumes:** When available bids/offers are insufficient to meet a zone's requirement, the shortfall is flagged in the uncleared summary. 
---

#### Project Structure

```
GBPower/
  |-- scripts/
  |     |-- IV_classify_bmu_constraints.py    # BMU zone classification
  |     |-- IV_build_all_bm_actions.py        # Elexon API data fetch
  |     |-- IV_distribute_balancing_volume.py  # Zone volume calculation
  |     |-- IV_clear_balancing_market.py       # Merit-order clearing
  |     |-- IV_test_*.py                       # Diagnostic/validation scripts
  |
  |-- rules/
  |     |-- prerun_rules.smk                   # IV_classify_bmu_constraints rule
  |     |-- postprocess.smk                    # IV_distribute + IV_clear rules
  |
  |-- notebooks/
  |     |-- IV_*.ipynb                         # Analysis and visualisation notebooks
  |
  |-- data/
  |     |-- prerun/
  |     |     |-- bmu_constraint_classification.csv
  |     |     |-- balancing_constraint_boundaries.yaml
  |     |-- base/{day}/
  |           |-- submitted_bids.csv
  |           |-- submitted_offers.csv
  |
  |-- results/{day}/
  |     |-- IV_dispatch_changes_by_zone_*.csv
  |     |-- IV_clearing_settlement_summary_flex.csv
  |     |-- IV_clearing_accepted_actions_flex.csv
  |     |-- IV_clearing_uncleared_summary_flex.csv
  |
  |-- run_full_year_2024.py                    # Full-year runner
  |-- rerun_iv_clearing_only.py                # Clearing-only re-runner
  |-- retry_missing_days.py                    # Retry failed days
  |-- check_results_2024.py                    # Results validation
```
