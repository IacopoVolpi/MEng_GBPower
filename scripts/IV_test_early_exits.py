"""
Test script to count EARLY EXIT occurrences during downward balancing
across all settlement periods of a given day.
"""

import pandas as pd
import pypsa
from pathlib import Path
from IV_clear_balancing_market import (
    calculate_zone_volumes_per_period,
    create_unit_lookup,
    annotate_bids_offers,
)

print("\n" + "="*100)
print("TESTING: EARLY EXIT OCCURRENCES IN DOWNWARD BALANCING")
print("="*100)

# ===== LOAD DATA =====
print("\n[STEP 1] Loading data...")

network_path = Path('results/2024-03-21/network_flex_s_national_solved_redispatch.nc')
network_wholesale_path = Path('results/2024-03-21/network_flex_s_national_solved.nc')
bmu_class_path = Path('data/prerun/bmu_constraint_classification.csv')
bids_path = Path('data/base/2024-03-21/submitted_bids.csv')
offers_path = Path('data/base/2024-03-21/submitted_offers.csv')

n_redispatch = pypsa.Network(str(network_path))
n_wholesale = pypsa.Network(str(network_wholesale_path))
bmu_classification = pd.read_csv(bmu_class_path, index_col=0)

bids_raw = pd.read_csv(bids_path, parse_dates=['timestamp'])
offers_raw = pd.read_csv(offers_path, parse_dates=['timestamp'])

# Process bids/offers
bids_df = bids_raw[['timestamp', 'NationalGridBmUnit', 'PairId', 'Bid', 'LevelFrom']].copy()
bids_df.columns = ['timestamp', 'unit_id', 'pair_id', 'price', 'volume_mw']
bids_df['volume_mw'] = bids_df['volume_mw'].abs()

# Remove timezone info to match PyPSA network timestamps
bids_df['timestamp'] = bids_df['timestamp'].dt.tz_localize(None)

network_units = set(n_redispatch.generators.index) | set(n_redispatch.storage_units.index)
bids_df = bids_df[bids_df['unit_id'].isin(network_units)].copy()

# Annotate
unit_lookup = create_unit_lookup(n_redispatch, bmu_classification)
bids_df['zone'] = bids_df['unit_id'].map(unit_lookup['zone'])
bids_df['carrier_type'] = bids_df['unit_id'].map(unit_lookup['type'])

print(f"  ✓ Loaded network, BMU classification, bids: {len(bids_df)} rows")

# ===== CALCULATE ZONE VOLUMES =====
print("\n[STEP 2] Calculating required zone volumes...")
zone_volumes = calculate_zone_volumes_per_period(n_wholesale, n_redispatch, bmu_classification)
print(f"  ✓ Calculated {len(zone_volumes)} zone-period combinations")

# ===== COUNT EARLY EXITS =====
print("\n[STEP 3] Testing EARLY EXIT conditions for each zone-period...")
print("-" * 100)

zone_names_map = {
    'red': 'North of SSE-SP',
    'orange': 'SSE-SP to SCOTEX',
    'green': 'SCOTEX to SSHARN',
    'blue': 'SSHARN to FLOWSTH',
    'purple': 'FLOWSTH to SEIMP',
    'yellow': 'South of all constraints',
    'unknown': 'Not classified',
}

zone_order = ['red', 'orange', 'green', 'blue', 'purple', 'yellow', 'unknown']
timestamps = zone_volumes.index.get_level_values('timestamp').unique()

early_exit_count = 0
flex_down_count = 0
no_bids_count = 0
no_volume_count = 0

# Track per zone and timestamp
early_exit_details = []

for ts in timestamps:
    available_bids = bids_df[bids_df['timestamp'] == ts].copy()
    
    for zone in zone_order:
        try:
            zone_vol = zone_volumes.loc[(ts, zone)]
            flex_down_required = zone_vol['flex_down_mwh']
        except KeyError:
            flex_down_required = 0.0
        
        # Check if this zone requires downward balancing
        if flex_down_required > 0.1:
            flex_down_count += 1
            
            # Filter bids to this zone
            zone_bids = available_bids[available_bids['zone'] == zone]
            
            # Check for EARLY EXIT conditions
            if zone_bids.empty:
                early_exit_count += 1
                no_bids_count += 1
                early_exit_details.append({
                    'timestamp': ts,
                    'zone': zone,
                    'zone_name': zone_names_map.get(zone, 'Unknown'),
                    'flex_down_required_mwh': flex_down_required,
                    'available_bids': len(zone_bids),
                    'reason': 'NO BIDS',
                })
            elif flex_down_required <= 0:
                early_exit_count += 1
                no_volume_count += 1
                early_exit_details.append({
                    'timestamp': ts,
                    'zone': zone,
                    'zone_name': zone_names_map.get(zone, 'Unknown'),
                    'flex_down_required_mwh': flex_down_required,
                    'available_bids': len(zone_bids),
                    'reason': 'NO VOLUME REQUIRED',
                })

# ===== DISPLAY RESULTS =====
print("\n[RESULTS SUMMARY]")
print("-" * 100)
print(f"  Total settlement periods: {len(timestamps)}")
print(f"  Total zones: {len(zone_order)}")
print(f"  Total zone-period combinations: {len(timestamps) * len(zone_order)}")
print(f"\n  Downward balancing required: {flex_down_count} zone-periods")
print(f"  EARLY EXIT occurrences: {early_exit_count}")
print(f"    - Due to NO BIDS available: {no_bids_count}")
print(f"    - Due to NO VOLUME REQUIRED: {no_volume_count}")
print(f"\n  Early exit rate (of flex_down periods): {(early_exit_count/flex_down_count*100):.1f}% if flex_down_count > 0 else 0")

# ===== DETAILED BREAKDOWN BY ZONE =====
print("\n[EARLY EXITS BY ZONE]")
print("-" * 100)

if early_exit_details:
    early_exit_df = pd.DataFrame(early_exit_details)
    
    for zone in zone_order:
        zone_exits = early_exit_df[early_exit_df['zone'] == zone]
        if not zone_exits.empty:
            total = len(zone_exits)
            no_bids = (zone_exits['reason'] == 'NO BIDS').sum()
            no_vol = (zone_exits['reason'] == 'NO VOLUME REQUIRED').sum()
            
            print(f"\n  {zone:15s} ({zone_names_map.get(zone, 'Unknown')})")
            print(f"    Total early exits: {total}")
            print(f"      - No bids: {no_bids}")
            print(f"      - No volume: {no_vol}")
            
            # Show first 3 occurrences
            print(f"    First occurrences:")
            for idx, row in zone_exits.head(3).iterrows():
                print(f"      {row['timestamp']} | Vol needed: {row['flex_down_required_mwh']:10.2f} MWh | Bids: {row['available_bids']:3d} | Reason: {row['reason']}")

# ===== SAVE DETAILED REPORT =====
if early_exit_details:
    early_exit_df = pd.DataFrame(early_exit_details)
    early_exit_df = early_exit_df.sort_values(['timestamp', 'zone'])
    early_exit_df.to_csv('early_exit_report.csv', index=False)
    print(f"\n[REPORT SAVED]")
    print(f"  Early exit details saved to: early_exit_report.csv")
    print(f"  Total rows: {len(early_exit_df)}")

print("\n" + "="*100)
print("TEST COMPLETE")
print("="*100 + "\n")