"""
Diagnostic script to trace where bids are disappearing
"""

import pandas as pd
import pypsa
from pathlib import Path
from IV_clear_balancing_market import (
    calculate_zone_volumes_per_period,
    create_unit_lookup,
)

print("\n" + "="*100)
print("DIAGNOSTIC: TRACING BID DATA LOSS")
print("="*100)

# Load networks
network_path = Path('results/2024-03-21/network_flex_s_national_solved_redispatch.nc')
network_wholesale_path = Path('results/2024-03-21/network_flex_s_national_solved.nc')
bmu_class_path = Path('data/prerun/bmu_constraint_classification.csv')
bids_path = Path('data/base/2024-03-21/submitted_bids.csv')

n_redispatch = pypsa.Network(str(network_path))
n_wholesale = pypsa.Network(str(network_wholesale_path))
bmu_classification = pd.read_csv(bmu_class_path, index_col=0)
bids_raw = pd.read_csv(bids_path, parse_dates=['timestamp'])

print("\n[STEP 1] Raw bids from CSV")
print(f"  Total rows: {len(bids_raw)}")
print(f"  Columns: {bids_raw.columns.tolist()}")
print(f"  Timestamp range: {bids_raw['timestamp'].min()} to {bids_raw['timestamp'].max()}")
print(f"  Unique timestamps: {bids_raw['timestamp'].nunique()}")
print(f"  Unique units: {bids_raw['NationalGridBmUnit'].nunique()}")
print(f"\n  First 5 rows:")
print(bids_raw[['timestamp', 'NationalGridBmUnit', 'Bid', 'LevelFrom']].head())

# Process step by step
print("\n[STEP 2] Rename columns")
bids_df = bids_raw[['timestamp', 'NationalGridBmUnit', 'PairId', 'Bid', 'LevelFrom']].copy()
bids_df.columns = ['timestamp', 'unit_id', 'pair_id', 'price', 'volume_mw']
bids_df['volume_mw'] = bids_df['volume_mw'].abs()

# Remove timezone info to match PyPSA network timestamps
bids_df['timestamp'] = bids_df['timestamp'].dt.tz_localize(None)

print(f"  Rows: {len(bids_df)}")

print("\n[STEP 3] Get network units")
network_units = set(n_redispatch.generators.index) | set(n_redispatch.storage_units.index)
print(f"  Network has {len(network_units)} units")
print(f"  Sample network units: {list(network_units)[:5]}")
print(f"  Sample bid units: {bids_df['unit_id'].unique()[:5]}")

print("\n[STEP 4] Filter to network units")
bids_before = len(bids_df)
bids_df = bids_df[bids_df['unit_id'].isin(network_units)].copy()
bids_after = len(bids_df)
print(f"  Before filter: {bids_before}")
print(f"  After filter: {bids_after}")
print(f"  Removed: {bids_before - bids_after} ({(bids_before - bids_after)/bids_before*100:.1f}%)")

if bids_after > 0:
    print(f"\n  Remaining bids per timestamp (sample):")
    bid_counts = bids_df.groupby('timestamp').size()
    print(bid_counts.head(10))
    print(f"  Min bids per timestamp: {bid_counts.min()}")
    print(f"  Max bids per timestamp: {bid_counts.max()}")
    print(f"  Mean bids per timestamp: {bid_counts.mean():.1f}")

print("\n[STEP 5] Add zone classification")
unit_lookup = create_unit_lookup(n_redispatch, bmu_classification)
bids_df['zone'] = bids_df['unit_id'].map(unit_lookup['zone'])
bids_df['carrier_type'] = bids_df['unit_id'].map(unit_lookup['type'])

print(f"  Rows: {len(bids_df)}")
print(f"  Zone distribution:")
print(bids_df['zone'].value_counts().sort_index())

print("\n[STEP 6] Check bids per zone per timestamp")
zone_order = ['red', 'orange', 'green', 'blue', 'purple', 'yellow']
timestamps = bids_df['timestamp'].unique()
print(f"  Unique timestamps in bids: {len(timestamps)}")

print("\n  Checking first timestamp in detail:")
first_ts = sorted(timestamps)[0]
bids_at_ts = bids_df[bids_df['timestamp'] == first_ts]
print(f"  Timestamp: {first_ts}")
print(f"  Total bids at this timestamp: {len(bids_at_ts)}")
print(f"  Bids per zone:")
for zone in zone_order:
    zone_bids = bids_at_ts[bids_at_ts['zone'] == zone]
    print(f"    {zone:15s}: {len(zone_bids):4d} bids")

print("\n" + "="*100)
print("DIAGNOSTIC COMPLETE")
print("="*100 + "\n")