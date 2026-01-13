# Quick Reference: Snakefile Rules at a Glance

## The 4 Phases of the Workflow

### PHASE 1: PRERUN (Static Data Setup) 
**Status**: Commented out (already done, results in repo)
```
Rule Name                    Output File                          Purpose
─────────────────────────────────────────────────────────────────────
prepare_bmus                prepared_bmus.csv                     Unit locations & metadata
build_bus_regions           regions_onshore.geojson              Geographic regions for network
                            regions_offshore.geojson
build_load_weights          load_weights.csv                     Demand allocation to regions
build_zonal_layout          zonal_layout.geojson                 6-zone market design definition
build_cfds                  cfd_strike_prices.csv                Subsidy levels for renewable units
build_flow_constraints      flow_constraints_{year}.csv          Transmission limits
```

---

### PHASE 2: RETRIEVE (Fetch Real Data for a Day)
**Status**: Runs automatically when you specify a date
```
Rule Name          Input                    Output (data/base/{day}/)     Purpose
──────────────────────────────────────────────────────────────────────
build_base         APIs (Elexon,            settlement_period_register   Fetch historical data
                   ENTSO-E, NESO)           physical_notifications       from grid operators
                                            day_ahead_prices
                                            offers / bids
                                            europe_day_ahead_prices
                                            nemo_powerflow
                                            boundary_flow_constraints
```

**Example**: `build_base` for 2024-03-21 downloads everything the GB grid recorded that day

---

### PHASE 3: RUN (Build & Solve Networks)
**Status**: Runs automatically once retrieve is complete

```
STEP 1: add_electricity
  Input:  network template + real data from PHASE 2 + subsidy data from PHASE 1
  Output: network_flex.nc (PyPSA network with all generators, loads, costs)
  ───────────────────────────────────────────────────────────────────────
  Purpose: Build the electricity network for the day
           - Create generator objects (one per BMU)
           - Set their marginal costs (merit order)
           - Add loads
           - Add transmission constraints

STEP 2: simplify_network
  Input:  network_flex.nc
  Output: network_flex_s.nc (fewer nodes, ~300 → ~100)
  ───────────────────────────────────────────────────────────────────────
  Purpose: Reduce computation time without losing physics
           - Merge nearby nodes
           - Simplify transmission network

STEP 3: cluster_network (3 parallel versions)
  Input:  network_flex_s.nc + zonal definitions
  Output: network_flex_s_national.nc   (1 node - current GB)
          network_flex_s_zonal.nc      (6 zones - proposed design)
          network_flex_s_nodal.nc      (300 nodes - theoretical max)
  ───────────────────────────────────────────────────────────────────────
  Purpose: Create different market structure versions
           - National: Whole GB as single market
           - Zonal: 6 independent price zones
           - Nodal: Each substation a separate node

STEP 4: solve_network (optimizes all 3 layouts)
  Input:  All 3 clustered networks + real bid/offer data
  Output: For each layout (national/zonal/nodal):
          - network_flex_s_{layout}_solved.nc         (wholesale market solution)
          - network_flex_s_{layout}_solved_redispatch.nc  (balancing market solution)
  
  Total: 6 solved networks
  ───────────────────────────────────────────────────────────────────────
  Purpose: Run optimization in 2 rounds:
           ROUND 1 (Wholesale): Assume all generators bid at cost
           ROUND 2 (Balancing): Apply real-world strategic bids/offers
           Calculate prices and dispatch for each scenario
```

---

### PHASE 4: POSTPROCESS (Extract Results)
**Status**: Runs automatically once networks are solved

```
Rule Name                 Output Files                       Purpose
──────────────────────────────────────────────────────────
summarize_bmu_revenues    bmu_revenues_{ic}_{layout}.csv    Revenue breakdown by unit
                          bmu_dispatch_{ic}_{layout}.csv    Dispatch schedule by unit
                          bmu_revenues_detailed_*.csv       Revenue by source (wholesale
                                                            + balancing + subsidies)

summarize_system_cost     system_cost_summary_{ic}.csv      Total GB costs:
                          marginal_prices_{ic}.csv          - Wholesale costs
                                                            - Balancing costs
                                                            - Subsidy costs
                                                            - Congestion rents

summarize_frontend_data   frontend/{day}/revenues_*.csv     Cleaned data for:
                          frontend/{day}/dispatch_*.csv     - Visualization
                          frontend/{day}/marginal_costs_*.csv - Notebooks
                          etc.                              - Web frontend
```

---

## File Dependencies Simplified

```
PRERUN (one-time setup)
  ├─ prepared_bmus.csv ──────┐
  ├─ regions_*.geojson ──────┐
  ├─ zonal_layout.geojson ───┤
  ├─ cfd_strike_prices.csv ──┤
  └─ flow_constraints.csv ───┘
                              │
                              ▼
                    add_electricity
                              │
                              ▼
                    simplify_network
                              │
                              ▼
                    cluster_network (×3)
                              │
                              ▼
                    solve_network
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
       summarize_    summarize_       summarize_
       bmu_revenues  system_cost      frontend_data

RETRIEVE (daily)
  ├─ physical_notifications.csv ┐
  ├─ day_ahead_prices.csv       ├─→ add_electricity
  ├─ offers.csv                 │
  ├─ bids.csv                   ┘
  └─ europe_*.csv

RESULT: results/{date}/system_cost_summary_flex.csv
```

---

## Quick Command Reference

**Run a specific day:**
```bash
snakemake -call --configfile config.yaml -- results/2024-03-21/system_cost_summary_flex.csv
```

**What happens:**
1. Snakemake looks at this path: `results/2024-03-21/system_cost_summary_flex.csv`
2. Extracts: date=2024-03-21, ic=flex
3. Works backward through the rules:
   - To make system_cost_summary → need all solved networks
   - To make solved networks → need clustered networks
   - To make clustered networks → need simplified networks
   - To make simplified network → need add_electricity output
   - To make that → need to retrieve data for 2024-03-21
4. Runs all necessary rules in order

**See what would run without actually running:**
```bash
snakemake --dry-run --configfile config.yaml -- results/2024-03-21/system_cost_summary_flex.csv
```

**Run multiple days (example for 2024 March):**
```python
import os
import pandas as pd

template = 'snakemake -call --configfile config.yaml -- results/{}/system_cost_summary_flex.csv'

for day in pd.date_range('2024-03-01', '2024-03-31', freq='d').strftime('%Y-%m-%d'):
    os.system(template.format(day))
```

---

## Key Insights

### Wildcards
- `{day}` = Date like 2024-03-21
- `{layout}` = national, zonal, or nodal
- `{ic}` = flex (static mode unused)

### Three Market Designs
- **National**: 1 price zone (entire GB)
- **Zonal**: 6 price zones
- **Nodal**: 300 price zones (one per substation)

### Two Optimization Rounds
- **Wholesale**: Optimal dispatch (generators bid at cost)
- **Balancing**: Real dispatch (generators bid strategically)

### Two Output Paths
- **results/**: All technical data (networks, detailed costs)
- **frontend/**: Simplified data for visualization

---

## Common Errors & Solutions

| Error | Likely Cause | Solution |
|-------|-------------|----------|
| `No output file` | Missing retrieve data | Data needs to be fetched from APIs |
| `Memory error` | Too many days at once | Run fewer days in parallel |
| `Solver fails` | Network infeasible | Check flow constraints are realistic |
| `Missing input` | Prerun scripts not run | Run prerun rules (uncomment outputs) |

---

## Next Steps

Now that you understand the SNAKEFILE structure:

1. **Run the example day**: 
   ```bash
   snakemake -call --configfile config.yaml -- results/2024-03-21/system_cost_summary_flex.csv
   ```
   Watch it go through each phase and understand real execution

2. **Dive into specific scripts**:
   - `scripts/build_base.py` → Understand data retrieval
   - `scripts/add_electricity.py` → Understand network building
   - `scripts/solve_network.py` → Understand optimization
   - `scripts/summarize_system_cost.py` → Understand result processing

3. **Explore a solved network**:
   ```python
   import pypsa
   n = pypsa.Network('results/2024-03-21/network_flex_s_national_solved.nc')
   print(n)  # See what's in it
   ```
