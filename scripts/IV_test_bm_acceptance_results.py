# SPDX-FileCopyrightText: 2024-2025 The PyPSA Authors
# SPDX-License-Identifier: MIT

"""
IV_test_bm_acceptance_results.py
================================
Compares your clearing algorithm's accepted actions with the REAL market accepted 
bids/offers to validate how well your algorithm reproduces actual clearing behavior.

This script:
1. Loads your clearing algorithm output (IV_clearing_accepted_actions_flex.csv)
2. Loads the REAL accepted bids/offers (bids.csv and offers.csv - System Operator accepted only)
3. Compares how closely your algorithm matches reality
4. Analyzes differences by zone, unit, and time period
5. Identifies where algorithm diverges from real market
"""

import logging
logger = logging.getLogger(__name__)

import pandas as pd
import numpy as np
from pathlib import Path

# Test date
TEST_DATE = '2024-03-21'
DATA_DIR = Path(f'data/base/{TEST_DATE}')
RESULTS_DIR = Path(f'results/{TEST_DATE}')

print("="*80)
print(f"REAL MARKET VALIDATION TEST")
print(f"Comparing: Your Clearing Algorithm vs Real Market Accepted Actions")
print(f"Date: {TEST_DATE}")
print("="*80)

# ============================================================================
# LOAD YOUR ALGORITHM RESULTS
# ============================================================================
print(f"\n[STEP 1] Loading YOUR clearing algorithm results...")
your_actions = pd.read_csv(
    RESULTS_DIR / 'IV_clearing_accepted_actions_flex.csv',
    parse_dates=['timestamp']
)
print(f"  Loaded {len(your_actions)} accepted actions from your algorithm")
print(f"  Columns: {your_actions.columns.tolist()}")

your_bids = your_actions[your_actions['action_type'] == 'bid'].copy()
your_offers = your_actions[your_actions['action_type'] == 'offer'].copy()
print(f"  Your algorithm accepted: {len(your_bids)} bids + {len(your_offers)} offers")

# ============================================================================
# LOAD REAL MARKET DATA (System Operator accepted)
# ============================================================================
print(f"\n[STEP 2] Loading REAL market accepted bids/offers...")

# Load real accepted bids (wide format: timestamp x units)
real_bids_wide = pd.read_csv(DATA_DIR / 'bids.csv', index_col=[0, 1])
real_offers_wide = pd.read_csv(DATA_DIR / 'offers.csv', index_col=[0, 1])

print(f"  Real bids data (wide): {real_bids_wide.shape[0]} rows × {real_bids_wide.shape[1]} units")
print(f"  Real offers data (wide): {real_offers_wide.shape[0]} rows × {real_offers_wide.shape[1]} units")

# Convert to long format for easier comparison
def wide_to_long(df_wide, action_type):
    """Convert wide format (timestamp x units) to long format"""
    results = []
    
    for (timestamp, metric) in df_wide.index:
        if metric == 'vol':  # Skip price rows for now, we'll match them after
            for unit_id in df_wide.columns:
                volume = df_wide.loc[(timestamp, metric), unit_id]
                
                # Get corresponding price
                try:
                    price = df_wide.loc[(timestamp, 'price'), unit_id]
                except KeyError:
                    price = np.nan
                
                if pd.notna(volume) and volume > 0:  # Only include non-zero volumes
                    results.append({
                        'timestamp': pd.Timestamp(timestamp),
                        'unit_id': unit_id,
                        'volume': volume,
                        'price': price,
                        'action_type': action_type
                    })
    
    return pd.DataFrame(results)

print(f"\n  Converting real bids to long format...")
real_bids = wide_to_long(real_bids_wide, 'bid')
print(f"  Real accepted bids (long): {len(real_bids)} actions")

print(f"  Converting real offers to long format...")
real_offers = wide_to_long(real_offers_wide, 'offer')
print(f"  Real accepted offers (long): {len(real_offers)} actions")

real_actions = pd.concat([real_bids, real_offers], ignore_index=True)
print(f"  Total real market actions: {len(real_actions)}")

# ============================================================================
# BASIC COMPARISON
# ============================================================================
print(f"\n[STEP 3] Basic Comparison: Your Algorithm vs Real Market")
print("-" * 80)

print(f"\nBIDS:")
print(f"  Real market:     {len(real_bids):5d} bids  | Volume: {real_bids['volume'].sum():10.2f} MWh")
print(f"  Your algorithm:  {len(your_bids):5d} bids  | Volume: {your_bids['volume_mwh'].sum():10.2f} MWh")
real_bid_vol = real_bids['volume'].sum()
your_bid_vol = your_bids['volume_mwh'].sum()
bid_diff_pct = (your_bid_vol - real_bid_vol) / real_bid_vol * 100 if real_bid_vol > 0 else 0
print(f"  Difference:      {bid_diff_pct:+6.1f}% (Your algo vs Real)")

print(f"\nOFFERS:")
print(f"  Real market:     {len(real_offers):5d} offers | Volume: {real_offers['volume'].sum():10.2f} MWh")
print(f"  Your algorithm:  {len(your_offers):5d} offers | Volume: {your_offers['volume_mwh'].sum():10.2f} MWh")
real_offer_vol = real_offers['volume'].sum()
your_offer_vol = your_offers['volume_mwh'].sum()
offer_diff_pct = (your_offer_vol - real_offer_vol) / real_offer_vol * 100 if real_offer_vol > 0 else 0
print(f"  Difference:      {offer_diff_pct:+6.1f}% (Your algo vs Real)")

print(f"\nTOTAL:")
real_total_vol = real_actions['volume'].sum()
your_total_vol = your_actions['volume_mwh'].sum()
total_diff_pct = (your_total_vol - real_total_vol) / real_total_vol * 100 if real_total_vol > 0 else 0
print(f"  Real market:     {real_total_vol:10.2f} MWh")
print(f"  Your algorithm:  {your_total_vol:10.2f} MWh")
print(f"  Difference:      {total_diff_pct:+6.1f}%")

# ============================================================================
# ZONE COMPARISON
# ============================================================================
print(f"\n[STEP 4] Comparison by Zone")
print("-" * 80)

zones = ['red', 'orange', 'green', 'blue', 'purple', 'yellow']

print(f"\n{'Zone':<10} {'Real (MWh)':<15} {'Your Algo (MWh)':<15} {'Difference %':<12} {'Your Actions':<12}")
print("-" * 70)

for zone in zones:
    zone_real = real_actions[real_actions.get('zone') == zone] if 'zone' in real_actions.columns else pd.DataFrame()
    zone_your = your_actions[your_actions['zone'] == zone]
    
    real_vol = real_actions[real_actions.get('zone') == zone]['volume'].sum() if len(zone_real) > 0 else 0
    your_vol = zone_your['volume_mwh'].sum()
    
    if real_vol > 0:
        diff = (your_vol - real_vol) / real_vol * 100
    else:
        diff = 0 if your_vol == 0 else 100
    
    print(f"{zone:<10} {real_vol:>13.2f} {your_vol:>14.2f} {diff:>10.1f}% {len(zone_your):>10d}")

# ============================================================================
# UNIT-LEVEL COMPARISON (Top units)
# ============================================================================
print(f"\n[STEP 5] Top 15 Units: Real Market vs Your Algorithm")
print("-" * 80)

print(f"\nReal market - Top units by volume:")
real_by_unit = real_actions.groupby('unit_id')['volume'].sum().sort_values(ascending=False)
for i, (unit, vol) in enumerate(real_by_unit.head(15).items(), 1):
    your_vol = your_actions[your_actions['unit_id'] == unit]['volume_mwh'].sum()
    diff_pct = (your_vol - vol) / vol * 100 if vol > 0 else 0
    print(f"  {i:2d}. {unit:<15} Real: {vol:8.2f} MWh | Your: {your_vol:8.2f} MWh ({diff_pct:+6.1f}%)")

# ============================================================================
# ACTION COUNT COMPARISON
# ============================================================================
print(f"\n[STEP 6] Action Distribution Comparison")
print("-" * 80)

action_comparison = pd.DataFrame({
    'Real Count': real_actions.groupby('action_type').size(),
    'Your Count': your_actions.groupby('action_type').size(),
})

action_comparison['Difference'] = action_comparison['Your Count'] - action_comparison['Real Count']
action_comparison['Difference %'] = (action_comparison['Your Count'] - action_comparison['Real Count']) / action_comparison['Real Count'] * 100

print(f"\n{action_comparison.to_string()}")

# ============================================================================
# PRICE RANGE COMPARISON
# ============================================================================
print(f"\n[STEP 7] Price Range Comparison")
print("-" * 80)

print(f"\nBIDS (Real market):")
if len(real_bids) > 0:
    print(f"  Price range: £{real_bids['price'].min():.2f} to £{real_bids['price'].max():.2f}")
    print(f"  Mean price:  £{real_bids['price'].mean():.2f}")

print(f"\nBIDS (Your algorithm):")
if len(your_bids) > 0:
    print(f"  Price range: £{your_bids['price_per_mwh'].min():.2f} to £{your_bids['price_per_mwh'].max():.2f}")
    print(f"  Mean price:  £{your_bids['price_per_mwh'].mean():.2f}")

print(f"\nOFFERS (Real market):")
if len(real_offers) > 0:
    print(f"  Price range: £{real_offers['price'].min():.2f} to £{real_offers['price'].max():.2f}")
    print(f"  Mean price:  £{real_offers['price'].mean():.2f}")

print(f"\nOFFERS (Your algorithm):")
if len(your_offers) > 0:
    print(f"  Price range: £{your_offers['price_per_mwh'].min():.2f} to £{your_offers['price_per_mwh'].max():.2f}")
    print(f"  Mean price:  £{your_offers['price_per_mwh'].mean():.2f}")

# ============================================================================
# TIME PERIOD ANALYSIS
# ============================================================================
print(f"\n[STEP 8] Hourly Volume Comparison")
print("-" * 80)

# Ensure both timestamps are naive (no timezone) for comparison
real_by_time = real_actions.copy()
real_by_time['timestamp'] = pd.to_datetime(real_by_time['timestamp']).dt.tz_localize(None)
real_by_time = real_by_time.groupby('timestamp')['volume'].sum()

your_by_time = your_actions.copy()
your_by_time['timestamp'] = pd.to_datetime(your_by_time['timestamp']).dt.tz_localize(None)
your_by_time = your_by_time.groupby('timestamp')['volume_mwh'].sum()

time_comparison = pd.DataFrame({
    'Real (MWh)': real_by_time,
    'Your Algo (MWh)': your_by_time,
})
time_comparison['Difference %'] = (time_comparison['Your Algo (MWh)'] - time_comparison['Real (MWh)']) / time_comparison['Real (MWh)'] * 100

print(f"\nFirst 12 settlement periods:")
print(time_comparison.head(12).to_string())

print(f"\n[STEP 9] Summary Statistics")
print("-" * 80)

print(f"\nAlignment Score (volume-based):")
total_real_vol = real_actions['volume'].sum()
total_your_vol = your_actions['volume_mwh'].sum()
alignment = 1 - abs(total_your_vol - total_real_vol) / max(total_real_vol, total_your_vol) * 100
alignment = max(0, alignment)
print(f"  Your algorithm captured: {alignment:.1f}% of real market volume")

print(f"\nVolume Distribution:")
print(f"  Real market total:  {total_real_vol:10.2f} MWh (100%)")
print(f"  Your algorithm:     {total_your_vol:10.2f} MWh ({total_your_vol/total_real_vol*100:5.1f}%)")

print(f"\n[STEP 10] Unit ID Matching Analysis")
print("-" * 80)

# Get unique unit IDs from both datasets
real_units = set(real_actions['unit_id'].unique())
your_units = set(your_actions['unit_id'].unique())

print(f"\nUnique Units - Comparison:")
print(f"  Real market units:        {len(real_units):3d}")
print(f"  Your algorithm units:     {len(your_units):3d}")

# Calculate overlaps
matched_units = real_units & your_units  # Intersection
only_in_real = real_units - your_units   # Only in real market
only_in_your = your_units - real_units   # Only in your algorithm

print(f"\nMatching Analysis:")
print(f"  Units in BOTH:            {len(matched_units):3d}  ({len(matched_units)/len(real_units)*100:5.1f}% of real market)")
print(f"  Units ONLY in real:       {len(only_in_real):3d}  ({len(only_in_real)/len(real_units)*100:5.1f}% of real market)")
print(f"  Units ONLY in your algo:  {len(only_in_your):3d}  ({len(only_in_your)/len(your_units)*100:5.1f}% of your algorithm)")

# Volume analysis by matched units
matched_real_vol = real_actions[real_actions['unit_id'].isin(matched_units)]['volume'].sum()
matched_your_vol = your_actions[your_actions['unit_id'].isin(matched_units)]['volume_mwh'].sum()
only_real_vol = real_actions[real_actions['unit_id'].isin(only_in_real)]['volume'].sum()
only_your_vol = your_actions[your_actions['unit_id'].isin(only_in_your)]['volume_mwh'].sum()

print(f"\nVolume by Unit Category:")
print(f"  Matched units (BOTH):     {matched_real_vol:10.2f} MWh (real) vs {matched_your_vol:10.2f} MWh (your)")
print(f"  Only in real market:      {only_real_vol:10.2f} MWh ({only_real_vol/total_real_vol*100:5.1f}% of total real)")
print(f"  Only in your algorithm:   {only_your_vol:10.2f} MWh ({only_your_vol/total_your_vol*100:5.1f}% of total your)")

print(f"\nUnit Matching Score:")
unit_coverage = len(matched_units) / len(real_units) * 100 if len(real_units) > 0 else 0
print(f"  Coverage: {unit_coverage:.1f}% of real market units are captured by your algorithm")

print(f"\nMissing Units (Top 10 by real market volume):")
missing_units_vol = real_actions[real_actions['unit_id'].isin(only_in_real)].groupby('unit_id')['volume'].sum().sort_values(ascending=False)
for i, (unit, vol) in enumerate(missing_units_vol.head(10).items(), 1):
    pct = vol / total_real_vol * 100
    print(f"  {i:2d}. {unit:<15} {vol:10.2f} MWh ({pct:5.2f}% of real market)")

print(f"\nExtra Units (Top 10 by your algorithm volume):")
extra_units_vol = your_actions[your_actions['unit_id'].isin(only_in_your)].groupby('unit_id')['volume_mwh'].sum().sort_values(ascending=False)
for i, (unit, vol) in enumerate(extra_units_vol.head(10).items(), 1):
    pct = vol / total_your_vol * 100
    print(f"  {i:2d}. {unit:<15} {vol:10.2f} MWh ({pct:5.2f}% of your algo)")