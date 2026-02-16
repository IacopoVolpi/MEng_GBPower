"""
Visual debugging script to inspect outputs of each function 
in IV_clear_balancing_market.py step by step
"""

import pandas as pd
import numpy as np
import pypsa
from pathlib import Path
from IV_clear_balancing_market import (
    get_unit_type,
    classify_unit_to_zone,
    calculate_zone_volumes_per_period,
    create_unit_lookup,
    annotate_bids_offers,
    clear_zone_downward,
    clear_zone_upward,
)

print("\n" + "="*100)
print("VISUAL DEBUGGING: IV_clear_balancing_market.py")
print("="*100)

# ===== LOAD DATA =====
print("\n[STEP 1] Loading data...")

# Load network
network_path = Path('results/2024-03-21/network_flex_s_national_solved_redispatch.nc')
print(f"  Loading network: {network_path}")
n_redispatch = pypsa.Network(str(network_path))
print(f"  ✓ Loaded: {len(n_redispatch.generators)} generators, {len(n_redispatch.storage_units)} storage units")

# Load wholesale network for comparison
network_wholesale_path = Path('results/2024-03-21/network_flex_s_national_solved.nc')
n_wholesale = pypsa.Network(str(network_wholesale_path))
print(f"  ✓ Loaded wholesale: {len(n_wholesale.generators)} generators")

# Load BMU classification
bmu_class_path = Path('data/prerun/bmu_constraint_classification.csv')
bmu_classification = pd.read_csv(bmu_class_path, index_col=0)
print(f"  ✓ Loaded BMU classification: {len(bmu_classification)} BMUs")

# Load bids/offers
bids_path = Path('data/base/2024-03-21/submitted_bids.csv')
offers_path = Path('data/base/2024-03-21/submitted_offers.csv')
bids_raw = pd.read_csv(bids_path, parse_dates=['timestamp'])
offers_raw = pd.read_csv(offers_path, parse_dates=['timestamp'])
print(f"  ✓ Loaded bids: {len(bids_raw)}, offers: {len(offers_raw)}")

# ===== TEST 1: get_unit_type() =====
print("\n[TEST 1] get_unit_type()")
print("-" * 100)

print("  Testing with 5 generators:")
for gen_id in list(n_redispatch.generators.index)[:5]:
    carrier = get_unit_type(gen_id, n_redispatch)
    print(f"    {gen_id:40s} -> {carrier}")

print("  Testing with 3 storage units (if any):")
if len(n_redispatch.storage_units) > 0:
    for storage_id in list(n_redispatch.storage_units.index)[:3]:
        carrier = get_unit_type(storage_id, n_redispatch)
        print(f"    {storage_id:40s} -> {carrier}")
else:
    print("    (No storage units)")

print("  Testing with non-existent unit:")
result = get_unit_type('FAKE_UNIT_XYZ', n_redispatch)
print(f"    {'FAKE_UNIT_XYZ':40s} -> {result}")

# ===== TEST 2: classify_unit_to_zone() =====
print("\n[TEST 2] classify_unit_to_zone()")
print("-" * 100)

print("  Classification results for first 10 BMUs:")
for bmu_id in list(bmu_classification.index)[:10]:
    zone = classify_unit_to_zone(bmu_id, bmu_classification)
    print(f"    {bmu_id:40s} -> {zone}")

# Count zones
zone_counts = {}
for bmu_id in bmu_classification.index:
    zone = classify_unit_to_zone(bmu_id, bmu_classification)
    zone_counts[zone] = zone_counts.get(zone, 0) + 1

print("\n  Zone distribution across all BMUs:")
for zone in sorted(zone_counts.keys()):
    count = zone_counts[zone]
    pct = (count / len(bmu_classification)) * 100
    print(f"    {zone:15s}: {count:4d} BMUs ({pct:5.1f}%)")

# ===== TEST 3: create_unit_lookup() =====
print("\n[TEST 3] create_unit_lookup()")
print("-" * 100)

print("  Creating lookup tables...")
unit_lookup = create_unit_lookup(n_redispatch, bmu_classification)
print(f"  ✓ Zone lookup: {len(unit_lookup['zone'])} units")
print(f"  ✓ Type lookup: {len(unit_lookup['type'])} units")

print("\n  Sample lookups (first 5 generators):")
for gen_id in list(n_redispatch.generators.index)[:5]:
    zone = unit_lookup['zone'].get(gen_id, 'N/A')
    gen_type = unit_lookup['type'].get(gen_id, 'N/A')
    print(f"    {gen_id:40s} | Zone: {zone:10s} | Type: {gen_type}")

# ===== TEST 4: annotate_bids_offers() =====
print("\n[TEST 4] annotate_bids_offers()")
print("-" * 100)

# Rename columns to match expected format
bids_df = bids_raw[['timestamp', 'NationalGridBmUnit', 'PairId', 'Bid', 'LevelFrom']].copy()
bids_df.columns = ['timestamp', 'unit_id', 'pair_id', 'price', 'volume_mw']
bids_df['volume_mw'] = bids_df['volume_mw'].abs()

offers_df = offers_raw[['timestamp', 'NationalGridBmUnit', 'PairId', 'Offer', 'LevelFrom']].copy()
offers_df.columns = ['timestamp', 'unit_id', 'pair_id', 'price', 'volume_mw']

print(f"  Input bids: {len(bids_df)} rows")
print(f"  Input offers: {len(offers_df)} rows")

# Filter to network units
network_units = set(n_redispatch.generators.index) | set(n_redispatch.storage_units.index)
bids_df = bids_df[bids_df['unit_id'].isin(network_units)].copy()
offers_df = offers_df[offers_df['unit_id'].isin(network_units)].copy()
print(f"  After filtering to network units:")
print(f"    Bids: {len(bids_df)}")
print(f"    Offers: {len(offers_df)}")

# Annotate
bids_ann, offers_ann = annotate_bids_offers(bids_df, offers_df, unit_lookup)

print(f"\n  After annotation (first 5 bids):")
print(bids_ann[['timestamp', 'unit_id', 'zone', 'carrier_type', 'price', 'volume_mw']].head().to_string())

print(f"\n  Bids per zone:")
zone_bid_counts = bids_ann['zone'].value_counts().sort_index()
for zone, count in zone_bid_counts.items():
    print(f"    {zone:15s}: {count:5d} bids")

print(f"\n  Offers per zone:")
zone_offer_counts = offers_ann['zone'].value_counts().sort_index()
for zone, count in zone_offer_counts.items():
    print(f"    {zone:15s}: {count:5d} offers")

# ===== TEST 5: calculate_zone_volumes_per_period() =====
print("\n[TEST 5] calculate_zone_volumes_per_period()")
print("-" * 100)

print("  Calculating zone volumes...")
zone_volumes = calculate_zone_volumes_per_period(n_wholesale, n_redispatch, bmu_classification)
print(f"  ✓ Calculated {len(zone_volumes)} zone-period combinations")

print("\n  Zone volumes (first 10 rows) - in MWh:")
print("  " + "-" * 90)
display_df = zone_volumes.head(10).copy()
display_df['flex_up_mwh'] = display_df['flex_up_mwh'].round(2)
display_df['flex_down_mwh'] = display_df['flex_down_mwh'].round(2)
print(display_df.to_string())
print("  " + "-" * 90)

print("\n  Zone volume SUMMARY (total per zone) - in MWh:")
print("  " + "-" * 90)
zone_vol_summary = zone_volumes.groupby('zone')[['flex_up_mwh', 'flex_down_mwh']].sum()
zone_vol_summary['flex_up_mwh'] = zone_vol_summary['flex_up_mwh'].round(2)
zone_vol_summary['flex_down_mwh'] = zone_vol_summary['flex_down_mwh'].round(2)

# Add zone names for clarity
zone_names_map = {
    'red': 'North of SSE-SP',
    'orange': 'SSE-SP to SCOTEX',
    'green': 'SCOTEX to SSHARN',
    'blue': 'SSHARN to FLOWSTH',
    'purple': 'FLOWSTH to SEIMP',
    'yellow': 'South of all constraints',
    'unknown': 'Not classified',
}

print(f"  {'Zone':15s} {'Description':30s} {'Flex Up (MWh)':20s} {'Flex Down (MWh)':20s}")
print("  " + "-" * 90)
for zone in ['red', 'orange', 'green', 'blue', 'purple', 'yellow']:
    if zone in zone_vol_summary.index:
        flex_up = zone_vol_summary.loc[zone, 'flex_up_mwh']
        flex_down = zone_vol_summary.loc[zone, 'flex_down_mwh']
        name = zone_names_map[zone]
        print(f"  {zone:15s} {name:30s} {flex_up:20,.2f} {flex_down:20,.2f}")

print("  " + "-" * 90)

# ===== TEST 6: clear_zone_downward() =====
print("\n[TEST 6] clear_zone_downward()")
print("-" * 100)

# Create sample bids for testing
sample_bids = pd.DataFrame({
    'unit_id': ['GEN_A', 'GEN_B', 'GEN_C'],
    'price': [45.0, 55.0, 35.0],  # Different prices
    'volume_mw': [100.0, 150.0, 80.0],
    'pair_id': [1, 2, 3],
    'carrier_type': ['gas', 'coal', 'wind'],
    'zone': ['red', 'red', 'red'],
})

print(f"  Sample bids (sorted by price descending):")
print(sample_bids.sort_values('price', ascending=False)[['unit_id', 'price', 'volume_mw']].to_string())

result = clear_zone_downward(
    required_volume=100.0,
    available_bids=sample_bids.copy(),
    zone='red',
    settlement_timestamp=pd.Timestamp('2024-03-21 00:00', tz='UTC')
)

print(f"\n  Result (required: 100 MWh):")
print(f"    Cleared: {result['cleared_volume']:.2f} MWh")
print(f"    Uncleared: {result['uncleared_volume']:.2f} MWh")
print(f"    Total cost: £{result['total_cost']:.2f}")
print(f"    Actions accepted: {len(result['accepted_actions'])}")

if result['accepted_actions']:
    print(f"\n  Accepted actions:")
    for action in result['accepted_actions']:
        print(f"    {action['unit_id']:15s} | {action['volume_mwh']:8.2f} MWh @ £{action['price_per_mwh']:.2f}/MWh")

# ===== TEST 7: clear_zone_upward() =====
print("\n[TEST 7] clear_zone_upward()")
print("-" * 100)

# Create sample offers for testing
sample_offers = pd.DataFrame({
    'unit_id': ['GEN_X', 'GEN_Y', 'GEN_Z'],
    'price': [25.0, 15.0, 35.0],  # Different prices
    'volume_mw': [120.0, 90.0, 110.0],
    'pair_id': [10, 11, 12],
    'carrier_type': ['wind', 'gas', 'hydro'],
    'zone': ['yellow', 'yellow', 'yellow'],
})

print(f"  Sample offers (sorted by price ascending):")
print(sample_offers.sort_values('price', ascending=True)[['unit_id', 'price', 'volume_mw']].to_string())

result = clear_zone_upward(
    required_volume=100.0,
    available_offers=sample_offers.copy(),
    zone='yellow',
    settlement_timestamp=pd.Timestamp('2024-03-21 00:00', tz='UTC')
)

print(f"\n  Result (required: 100 MWh):")
print(f"    Cleared: {result['cleared_volume']:.2f} MWh")
print(f"    Uncleared: {result['uncleared_volume']:.2f} MWh")
print(f"    Total cost: £{result['total_cost']:.2f}")
print(f"    Actions accepted: {len(result['accepted_actions'])}")

if result['accepted_actions']:
    print(f"\n  Accepted actions:")
    for action in result['accepted_actions']:
        print(f"    {action['unit_id']:15s} | {action['volume_mwh']:8.2f} MWh @ £{action['price_per_mwh']:.2f}/MWh")

# ===== TEST 8: run_balancing_market_clearing() - FULL END-TO-END =====
print("\n[TEST 8] run_balancing_market_clearing() - FULL END-TO-END TEST")
print("="*100)

print("\n[STEP 8.1] Setup: Prepare bids, offers, and zone volumes with timezone normalization")
print("-" * 100)

# Reload and prepare bids/offers with timezone normalization
bids_raw = pd.read_csv(bids_path, parse_dates=['timestamp'])
offers_raw = pd.read_csv(offers_path, parse_dates=['timestamp'])

bids_df = bids_raw[['timestamp', 'NationalGridBmUnit', 'PairId', 'Bid', 'LevelFrom']].copy()
bids_df.columns = ['timestamp', 'unit_id', 'pair_id', 'price', 'volume_mw']
bids_df['volume_mw'] = bids_df['volume_mw'].abs()

offers_df = offers_raw[['timestamp', 'NationalGridBmUnit', 'PairId', 'Offer', 'LevelFrom']].copy()
offers_df.columns = ['timestamp', 'unit_id', 'pair_id', 'price', 'volume_mw']

# CRITICAL: Remove timezone to match PyPSA network timestamps
bids_df['timestamp'] = bids_df['timestamp'].dt.tz_localize(None)
offers_df['timestamp'] = offers_df['timestamp'].dt.tz_localize(None)

print(f"  Bids after timezone normalization:")
print(f"    Total rows: {len(bids_df)}")
print(f"    Timestamp sample: {bids_df['timestamp'].iloc[0]}")
print(f"    Timestamp type: {type(bids_df['timestamp'].iloc[0])}")

# Filter to network units
network_units = set(n_redispatch.generators.index) | set(n_redispatch.storage_units.index)
bids_df = bids_df[bids_df['unit_id'].isin(network_units)].copy()
offers_df = offers_df[offers_df['unit_id'].isin(network_units)].copy()

print(f"  After filtering to network units: {len(bids_df)} bids, {len(offers_df)} offers")

# Annotate with zones
bids_df, offers_df = annotate_bids_offers(bids_df, offers_df, unit_lookup)

print(f"  After annotation:")
print(f"    Bids distribution: {dict(bids_df['zone'].value_counts())}")
print(f"    Offers distribution: {dict(offers_df['zone'].value_counts())}")

# Calculate zone volumes
zone_volumes = calculate_zone_volumes_per_period(n_wholesale, n_redispatch, bmu_classification)
print(f"  Zone volumes calculated: {len(zone_volumes)} zone-period combinations")

print("\n[STEP 8.2] Verify timestamp compatibility")
print("-" * 100)

print(f"  Zone volumes index (first 5):")
for idx, val in zone_volumes.head().iterrows():
    ts, zone = idx
    print(f"    {ts} ({type(ts).__name__}) | Zone: {zone} | FlexDown: {val['flex_down_mwh']:.2f} MWh")

print(f"\n  Bids timestamps (unique count: {bids_df['timestamp'].nunique()}):")
for ts in sorted(bids_df['timestamp'].unique())[:3]:
    count = len(bids_df[bids_df['timestamp'] == ts])
    print(f"    {ts} ({type(ts).__name__}) | {count} bids")

print(f"\n  Zone volumes timestamps (unique count: {zone_volumes.index.get_level_values('timestamp').nunique()}):")
for ts in sorted(zone_volumes.index.get_level_values('timestamp').unique())[:3]:
    count = len(zone_volumes.xs(ts, level='timestamp'))
    print(f"    {ts} ({type(ts).__name__}) | {count} zones")

# Test manual match
first_ts_zones = zone_volumes.index.get_level_values('timestamp').unique()[0]
first_ts_bids = bids_df['timestamp'].unique()[0]
print(f"\n  First timestamp match test:")
print(f"    Zone volumes TS: {first_ts_zones} == Bids TS: {first_ts_bids}")
print(f"    Timestamps equal: {first_ts_zones == first_ts_bids}")
print(f"    Bids at this timestamp: {len(bids_df[bids_df['timestamp'] == first_ts_zones])}")

print("\n[STEP 8.3] Run clearing algorithm with detailed logging")
print("-" * 100)

from IV_clear_balancing_market import run_balancing_market_clearing

settlement_summary, accepted_actions, uncleared_summary = run_balancing_market_clearing(
    zone_volumes, bids_df, offers_df, unit_lookup
)

print(f"\n  Clearing complete!")
print(f"    Settlement results rows: {len(settlement_summary)}")
print(f"    Accepted actions: {len(accepted_actions)}")
print(f"    Uncleared summary: {len(uncleared_summary)}")

print("\n[STEP 8.4] Settlement Summary Analysis")
print("-" * 100)

print("\n  Overall statistics:")
print(f"    Total periods: {settlement_summary['timestamp'].nunique()}")
print(f"    Total zones: {settlement_summary['zone'].nunique()}")
print(f"    Total volume required: {settlement_summary['required_volume_mwh'].sum():.2f} MWh")
print(f"    Total volume cleared: {settlement_summary['cleared_volume_mwh'].sum():.2f} MWh")
print(f"    Total uncleared: {settlement_summary['uncleared_volume_mwh'].sum():.2f} MWh")
print(f"    Clearing efficiency: {settlement_summary['cleared_volume_mwh'].sum()/max(settlement_summary['required_volume_mwh'].sum(), 1)*100:.1f}%")

print("\n  By direction:")
for direction in ['up', 'down', 'none']:
    subset = settlement_summary[settlement_summary['direction'] == direction]
    if len(subset) > 0:
        print(f"    {direction:5s} | Count: {len(subset):3d} | Required: {subset['required_volume_mwh'].sum():10.2f} | Cleared: {subset['cleared_volume_mwh'].sum():10.2f} | Uncleared: {subset['uncleared_volume_mwh'].sum():10.2f}")

print("\n  By zone (showing downward balancing):")
downward = settlement_summary[settlement_summary['direction'] == 'down']
for zone in sorted(downward['zone'].unique()):
    subset = downward[downward['zone'] == zone]
    print(f"    {zone:15s} | Periods: {len(subset):2d} | Required: {subset['required_volume_mwh'].sum():10.2f} | Cleared: {subset['cleared_volume_mwh'].sum():10.2f} | Uncleared: {subset['uncleared_volume_mwh'].sum():10.2f}")

print("\n[STEP 8.5] Sample Settlement Results (first 15 rows)")
print("-" * 100)

display_cols = ['timestamp', 'zone', 'direction', 'required_volume_mwh', 'cleared_volume_mwh', 'uncleared_volume_mwh']
print(settlement_summary[display_cols].head(15).to_string())

print("\n[STEP 8.6] Accepted Actions Analysis")
print("-" * 100)

if len(accepted_actions) > 0:
    print(f"  Total actions: {len(accepted_actions)}")
    print(f"  Total volume accepted: {accepted_actions['volume_mwh'].sum():.2f} MWh")
    print(f"  Total cost: £{accepted_actions['cost_gbp'].sum():,.2f}")
    
    print(f"\n  By action type:")
    for action_type in ['bid', 'offer']:
        subset = accepted_actions[accepted_actions['action_type'] == action_type]
        if len(subset) > 0:
            print(f"    {action_type:10s} | Count: {len(subset):5d} | Volume: {subset['volume_mwh'].sum():10.2f} MWh | Cost: £{subset['cost_gbp'].sum():12,.2f}")
    
    print(f"\n  By zone:")
    for zone in sorted(accepted_actions['zone'].unique()):
        subset = accepted_actions[accepted_actions['zone'] == zone]
        print(f"    {zone:15s} | Count: {len(subset):5d} | Volume: {subset['volume_mwh'].sum():10.2f} MWh | Cost: £{subset['cost_gbp'].sum():12,.2f}")
    
    print(f"\n  Sample accepted actions (first 10):")
    display_cols = ['timestamp', 'zone', 'unit_id', 'action_type', 'volume_mwh', 'price_per_mwh', 'cost_gbp']
    print(accepted_actions[display_cols].head(10).to_string())
else:
    print("  ⚠️  NO ACTIONS ACCEPTED!")

print("\n[STEP 8.7] Uncleared Volume Analysis")
print("-" * 100)

if len(uncleared_summary) > 0:
    print(f"  Total uncleared records: {len(uncleared_summary)}")
    print(f"  Total uncleared volume: {uncleared_summary['uncleared_volume_mwh'].sum():.2f} MWh")
    
    print(f"\n  By direction:")
    for direction in ['up', 'down']:
        subset = uncleared_summary[uncleared_summary['direction'] == direction]
        if len(subset) > 0:
            print(f"    {direction:5s} | Count: {len(subset):3d} | Volume: {subset['uncleared_volume_mwh'].sum():10.2f} MWh")
    
    print(f"\n  By zone (top 5):")
    zone_uncleared = uncleared_summary.groupby('zone')['uncleared_volume_mwh'].sum().sort_values(ascending=False)
    for zone, volume in zone_uncleared.head(5).items():
        print(f"    {zone:15s} | {volume:10.2f} MWh")
    
    print(f"\n  First 10 uncleared volumes:")
    display_cols = ['timestamp', 'zone', 'direction', 'uncleared_volume_mwh']
    print(uncleared_summary[display_cols].head(10).to_string())
else:
    print("  ✓ All volumes cleared successfully!")

print("\n[STEP 8.8] Diagnostic: Why are bids not being cleared?")
print("-" * 100)

# Manually test one zone-timestamp
test_ts = sorted(zone_volumes.index.get_level_values('timestamp').unique())[0]
test_zone = 'red'

print(f"  Testing specific case: {test_ts} in zone {test_zone}")

# Get zone volume
try:
    zone_vol = zone_volumes.loc[(test_ts, test_zone)]
    flex_down_required = zone_vol['flex_down_mwh']
    print(f"    Required flex_down: {flex_down_required:.2f} MWh")
except KeyError:
    flex_down_required = 0.0
    print(f"    No flex_down required for this zone-period")

# Get bids for this timestamp
bids_at_ts = bids_df[bids_df['timestamp'] == test_ts]
print(f"    Total bids at this timestamp: {len(bids_at_ts)}")

# Get bids for this zone
zone_bids = bids_at_ts[bids_at_ts['zone'] == test_zone]
print(f"    Bids in {test_zone} zone: {len(zone_bids)}")

if len(zone_bids) > 0:
    print(f"    Zone bids detail:")
    print(f"      Min price: £{zone_bids['price'].min():.2f}/MWh")
    print(f"      Max price: £{zone_bids['price'].max():.2f}/MWh")
    print(f"      Total volume: {zone_bids['volume_mw'].sum():.2f} MW")
    print(f"      Sample units: {zone_bids['unit_id'].unique()[:3].tolist()}")
else:
    print(f"    ⚠️  NO BIDS IN {test_zone} ZONE!")
    print(f"    Zone distribution in bids_at_ts: {dict(bids_at_ts['zone'].value_counts())}")

print("\n" + "="*100)
print("END-TO-END TEST COMPLETE")
print("="*100 + "\n")