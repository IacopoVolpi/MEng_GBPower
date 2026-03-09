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
# LOAD ALL SUBMITTED ACTONS
# ============================================================================

# Load submitted bids and offers
submitted_bids_raw = pd.read_csv(DATA_DIR / 'submitted_bids.csv', parse_dates=['timestamp'])
submitted_offers_raw = pd.read_csv(DATA_DIR / 'submitted_offers.csv', parse_dates=['timestamp'])

print(f"\n[STEP 0] Submitted Actions Overview")
print("="*80)
print(f"  Submitted bids:   {len(submitted_bids_raw)}")
print(f"  Submitted offers: {len(submitted_offers_raw)}")
print(f"  Total submitted:  {len(submitted_bids_raw) + len(submitted_offers_raw)}")

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

print(f"\n  Acceptance rates (Real Market vs Submitted):")
print(f"  Bids accepted:   {len(real_bids):5d} / {len(submitted_bids_raw):5d} ({len(real_bids)/len(submitted_bids_raw)*100:.1f}%)")
print(f"  Offers accepted: {len(real_offers):5d} / {len(submitted_offers_raw):5d} ({len(real_offers)/len(submitted_offers_raw)*100:.1f}%)")

###################################################################################

# # Print the long format data
# print("\n" + "="*100)
# print("REAL BIDS (Long Format)")
# print("="*100)
# print(real_bids.to_string(index=False))

# print("\n" + "="*100)
# print("REAL OFFERS (Long Format)")
# print("="*100)
# print(real_offers.to_string(index=False))

###################################################################################

real_actions = pd.concat([real_bids, real_offers], ignore_index=True)
print(f"  Total real market actions: {len(real_actions)}")

# Map zones onto real actions using same BMU classification as the clearing algorithm
bmu_classification = pd.read_csv('data/prerun/bmu_constraint_classification.csv', index_col=0)

def classify_to_zone(unit_id):
    if unit_id not in bmu_classification.index:
        return 'unknown'
    row = bmu_classification.loc[unit_id]
    if row.get('SSE-SP_side') == 'north':
        return 'red'
    elif row.get('SCOTEX_side') == 'north':
        return 'orange'
    elif row.get('SSHARN_side') == 'north':
        return 'green'
    elif row.get('FLOWSTH_side') == 'north':
        return 'blue'
    elif row.get('SEIMP_side') == 'north':
        return 'purple'
    else:
        return 'yellow'

real_actions['zone'] = real_actions['unit_id'].map(classify_to_zone)
real_bids['zone'] = real_bids['unit_id'].map(classify_to_zone)
real_offers['zone'] = real_offers['unit_id'].map(classify_to_zone)

unclassified = (real_actions['zone'] == 'unknown').sum()
print(f"  Mapped zones onto real actions ({unclassified} actions could not be classified → 'unknown')")

# ============================================================================
# BASIC COMPARISON
# ============================================================================
print(f"\n[STEP 3] Basic Comparison: Your Algorithm Volume vs Real Market Volume")
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

print(f"\n{'Zone':<10} {'Real (MWh)':<15} {'Real Actions':<14} {'Your Algo (MWh)':<16} {'Your Actions':<14} {'Diff %':<10}")
print("-" * 80)

for zone in zones:
    zone_real = real_actions[real_actions['zone'] == zone]
    zone_your = your_actions[your_actions['zone'] == zone]
    
    real_vol = zone_real['volume'].sum()
    your_vol = zone_your['volume_mwh'].sum()
    
    if real_vol > 0:
        diff = (your_vol - real_vol) / real_vol * 100
    else:
        diff = 0 if your_vol == 0 else 100
    
    print(f"{zone:<10} {real_vol:>13.2f} {len(zone_real):>12d} {your_vol:>14.2f} {len(zone_your):>12d} {diff:>9.1f}%")

# ============================================================================
# UNIT-LEVEL COMPARISON (Top units)
# ============================================================================
print(f"\n[STEP 5] Top 50 Units: Real Market vs Your Algorithm")
print("-" * 80)

# Build carrier type lookup from bmu_classification (already loaded above)
carrier_lookup = bmu_classification['carrier'].to_dict() if 'carrier' in bmu_classification.columns else {}

def print_top_units(real_df, your_df, label):
    header = f"  {'#':>3}  {'Unit ID':<16} {'Carrier':<14} {'Zone':<8} {'Real (MWh)':>10} {'Your (MWh)':>10} {'Diff %':>8}"
    print(f"\n{label} - Top 50 units by volume:")
    print(header)
    print(f"  {'-'*72}")
    by_unit = real_df.groupby('unit_id')['volume'].sum().sort_values(ascending=False)
    for i, (unit, vol) in enumerate(by_unit.head(50).items(), 1):
        your_vol = your_df[your_df['unit_id'] == unit]['volume_mwh'].sum()
        diff_pct = (your_vol - vol) / vol * 100 if vol > 0 else 0
        carrier = carrier_lookup.get(unit, 'unknown')
        zone = real_df[real_df['unit_id'] == unit]['zone'].iloc[0] if len(real_df[real_df['unit_id'] == unit]) > 0 else 'unknown'
        print(f"  {i:3d}. {unit:<16} {carrier:<14} {zone:<8} {vol:>10.2f} {your_vol:>10.2f} {diff_pct:>+7.1f}%")

print_top_units(real_bids, your_bids, "BIDS (downward actions — generators turning down)")
print_top_units(real_offers, your_offers, "OFFERS (upward actions — generators turning up)")

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
alignment = (1 - abs(total_your_vol - total_real_vol) / max(total_real_vol, total_your_vol)) * 100
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

print(f"  Units called by BOTH:    Real: {matched_real_vol:10.2f} MWh | Yours: {matched_your_vol:10.2f} MWh  ← same BMUs, different volumes, your called more of the same BMUs than real market")
print(f"  Units called ONLY by real market :{only_real_vol:10.2f} MWh ({only_real_vol/total_real_vol*100:.1f}% of real total)  ← BMUs you completely ignored")
print(f"  Units called ONLY by your algorithm :    {only_your_vol:10.2f} MWh ({only_your_vol/total_your_vol*100:.1f}% of your total)  ← BMUs the real SO never used")



print(f"\nMissing Units (Top 30 by real market volume):")
print(f"  {'#':>3}  {'Unit ID':<16} {'Carrier':<14} {'Volume (MWh)':>12} {'% of Real':>10}")
print(f"  {'-'*58}")
missing_units_vol = real_actions[real_actions['unit_id'].isin(only_in_real)].groupby('unit_id')['volume'].sum().sort_values(ascending=False)
for i, (unit, vol) in enumerate(missing_units_vol.head(30).items(), 1):
    pct = vol / total_real_vol * 100
    carrier = carrier_lookup.get(unit, 'unknown')
    print(f"  {i:3d}. {unit:<16} {carrier:<14} {vol:12.2f} MWh ({pct:5.2f}%)")

print(f"\nExtra Units (Top 30 by your algorithm volume):")
print(f"  {'#':>3}  {'Unit ID':<16} {'Carrier':<14} {'Volume (MWh)':>12} {'% of Yours':>10}")
print(f"  {'-'*58}")
extra_units_vol = your_actions[your_actions['unit_id'].isin(only_in_your)].groupby('unit_id')['volume_mwh'].sum().sort_values(ascending=False)
for i, (unit, vol) in enumerate(extra_units_vol.head(30).items(), 1):
    pct = vol / total_your_vol * 100
    carrier = carrier_lookup.get(unit, 'unknown')
    print(f"  {i:3d}. {unit:<16} {carrier:<14} {vol:12.2f} MWh ({pct:5.2f}%)")

# ============================================================================
# FINANCIAL COMPARISON
# ============================================================================
print(f"\n[STEP 11] Financial Comparison: Real Market vs Your Algorithm")
print("=" * 80)

# --- Compute costs for real market actions ---
# Real market volumes are in MW (from ELEXON LevelFrom/LevelTo), x0.5 to get MWh
# Cost = price_per_mwh * volume_mwh
real_bids['cost_gbp']   = real_bids['price']   * real_bids['volume']   * 0.5
real_offers['cost_gbp'] = real_offers['price'] * real_offers['volume'] * 0.5
real_bids['carrier_type']   = real_bids['unit_id'].map(carrier_lookup).fillna('unknown')
real_offers['carrier_type'] = real_offers['unit_id'].map(carrier_lookup).fillna('unknown')
real_actions['cost_gbp']     = real_actions['price'] * real_actions['volume'] * 0.5
real_actions['carrier_type'] = real_actions['unit_id'].map(carrier_lookup).fillna('unknown')
real_actions['volume_mwh']   = real_actions['volume'] * 0.5  # normalise to MWh for comparisons

# --- Totals ---
real_total_cost  = real_actions['cost_gbp'].sum()
your_total_cost  = your_actions['cost_gbp'].sum()
real_bid_cost    = real_bids['cost_gbp'].sum()
real_offer_cost  = real_offers['cost_gbp'].sum()
your_bid_cost    = your_bids['cost_gbp'].sum()
your_offer_cost  = your_offers['cost_gbp'].sum()

print(f"\n--- TOTAL COST OVERVIEW ---")
print(f"  {'':40} {'Real Market':>14} {'Your Algorithm':>16} {'Diff':>10}")
print(f"  {'-'*82}")
print(f"  {'Total clearing cost':<40} £{real_total_cost:>12,.0f} £{your_total_cost:>14,.0f}  {(your_total_cost-real_total_cost)/real_total_cost*100:>+8.1f}%")
print(f"  {'  of which: Bid costs (turn-down payments)':<40} £{real_bid_cost:>12,.0f} £{your_bid_cost:>14,.0f}  {(your_bid_cost-real_bid_cost)/real_bid_cost*100 if real_bid_cost>0 else 0:>+8.1f}%")
print(f"  {'  of which: Offer costs (turn-up payments)':<40} £{real_offer_cost:>12,.0f} £{your_offer_cost:>14,.0f}  {'N/A' if real_offer_cost==0 else f'{(your_offer_cost-real_offer_cost)/real_offer_cost*100:>+8.1f}%':>10}")
print(f"  {'-'*82}")
real_avg_price = real_total_cost / (real_actions['volume_mwh'].sum()) if real_actions['volume_mwh'].sum() > 0 else 0
your_avg_price = your_total_cost / (your_actions['volume_mwh'].sum()) if your_actions['volume_mwh'].sum() > 0 else 0
print(f"  {'Avg price paid per MWh cleared':<40} £{real_avg_price:>12.2f} £{your_avg_price:>14.2f}  {(your_avg_price-real_avg_price)/real_avg_price*100 if real_avg_price>0 else 0:>+8.1f}%")

# --- Cost by technology ---
print(f"\n--- COST BREAKDOWN BY TECHNOLOGY ---")
print(f"  {'Technology':<14} {'Real Cost (£)':>14} {'% of Real':>10} {'Your Cost (£)':>14} {'% of Yours':>11} {'Diff (£)':>12}")
print(f"  {'-'*80}")
all_carriers = sorted(set(real_actions['carrier_type'].unique()) | set(your_actions['carrier_type'].unique()))
for carrier in all_carriers:
    r_cost = real_actions[real_actions['carrier_type'] == carrier]['cost_gbp'].sum()
    y_cost = your_actions[your_actions['carrier_type'] == carrier]['cost_gbp'].sum()
    r_pct  = r_cost / real_total_cost * 100 if real_total_cost > 0 else 0
    y_pct  = y_cost / your_total_cost * 100 if your_total_cost > 0 else 0
    diff   = y_cost - r_cost
    print(f"  {carrier:<14} £{r_cost:>12,.0f} {r_pct:>9.1f}% £{y_cost:>12,.0f} {y_pct:>10.1f}% £{diff:>+11,.0f}")
print(f"  {'-'*80}")
print(f"  {'TOTAL':<14} £{real_total_cost:>12,.0f} {'100.0%':>10} £{your_total_cost:>12,.0f} {'100.0%':>10} £{your_total_cost-real_total_cost:>+11,.0f}")

# --- Cost by zone ---
print(f"\n--- COST BREAKDOWN BY ZONE ---")
print(f"  {'Zone':<10} {'Direction':<10} {'Real Cost (£)':>14} {'Your Cost (£)':>14} {'Diff (£)':>12}")
print(f"  {'-'*62}")
for zone in ['red', 'orange', 'green', 'blue', 'purple', 'yellow']:
    r_cost = real_actions[real_actions['zone'] == zone]['cost_gbp'].sum()
    y_cost = your_actions[your_actions['zone'] == zone]['cost_gbp'].sum()
    # Determine direction from settlement summary
    direction = 'down' if zone in ['red', 'orange'] else 'up' if zone in ['blue', 'purple', 'yellow'] else 'both'
    print(f"  {zone:<10} {direction:<10} £{r_cost:>12,.0f} £{y_cost:>12,.0f} £{y_cost-r_cost:>+11,.0f}")
print(f"  {'-'*62}")
print(f"  {'TOTAL':<10} {'':<10} £{real_total_cost:>12,.0f} £{your_total_cost:>12,.0f} £{your_total_cost-real_total_cost:>+11,.0f}")

# --- Average price by technology (weighted average £/MWh) ---
print(f"\n--- AVERAGE PRICE PAID PER MWh BY TECHNOLOGY (weighted average) ---")
print(f"  {'Technology':<14} {'Real (£/MWh)':>14} {'Your (£/MWh)':>14} {'Real Vol (MWh)':>16} {'Your Vol (MWh)':>16}")
print(f"  {'-'*76}")
for carrier in all_carriers:
    r_sub  = real_actions[real_actions['carrier_type'] == carrier]
    y_sub  = your_actions[your_actions['carrier_type'] == carrier]
    r_vol  = r_sub['volume_mwh'].sum()
    y_vol  = y_sub['volume_mwh'].sum()
    r_wavg = r_sub['cost_gbp'].sum() / r_vol if r_vol > 0 else 0
    y_wavg = y_sub['cost_gbp'].sum() / y_vol if y_vol > 0 else 0
    print(f"  {carrier:<14} £{r_wavg:>12.2f} £{y_wavg:>12.2f} {r_vol:>16.1f} {y_vol:>16.1f}")

# ============================================================================
# TECHNOLOGY MIX COMPARISON
# ============================================================================
print(f"\n[STEP 12] Technology Mix: Contribution to Total Congestion Volume")
print("=" * 80)
print(f"  How much each technology type contributes to clearing congestion,")
print(f"  split by direction (bids=turn-down, offers=turn-up).")

real_total_vol_mwh = real_actions['volume_mwh'].sum()
your_total_vol_mwh = your_actions['volume_mwh'].sum()

# --- Volume by technology: BIDS (turn-down) ---
print(f"\n--- BIDS (Turn-Down): Technology Contribution ---")
print(f"  {'Technology':<14} {'Real Vol (MWh)':>15} {'% of Real Bids':>15} {'Your Vol (MWh)':>15} {'% of Your Bids':>15} {'Diff (MWh)':>12}")
print(f"  {'-'*80}")
real_bid_total_vol = real_bids['volume'].mul(0.5).sum()
your_bid_total_vol = your_bids['volume_mwh'].sum()
bid_carriers = sorted(set(real_bids['carrier_type'].unique()) | set(your_bids['carrier_type'].unique()))
for carrier in bid_carriers:
    r_vol = real_bids[real_bids['carrier_type'] == carrier]['volume'].sum() * 0.5
    y_vol = your_bids[your_bids['carrier_type'] == carrier]['volume_mwh'].sum()
    r_pct = r_vol / real_bid_total_vol * 100 if real_bid_total_vol > 0 else 0
    y_pct = y_vol / your_bid_total_vol * 100 if your_bid_total_vol > 0 else 0
    print(f"  {carrier:<14} {r_vol:>15.1f} {r_pct:>14.1f}% {y_vol:>15.1f} {y_pct:>14.1f}% {y_vol-r_vol:>+12.1f}")
print(f"  {'-'*80}")
print(f"  {'TOTAL':<14} {real_bid_total_vol:>15.1f} {'100.0%':>15} {your_bid_total_vol:>15.1f} {'100.0%':>15} {your_bid_total_vol-real_bid_total_vol:>+12.1f}")

# --- Volume by technology: OFFERS (turn-up) ---
print(f"\n--- OFFERS (Turn-Up): Technology Contribution ---")
print(f"  {'Technology':<14} {'Real Vol (MWh)':>15} {'% of Real Offers':>17} {'Your Vol (MWh)':>15} {'% of Your Offers':>17} {'Diff (MWh)':>12}")
print(f"  {'-'*84}")
real_offer_total_vol = real_offers['volume'].mul(0.5).sum()
your_offer_total_vol = your_offers['volume_mwh'].sum()
offer_carriers = sorted(set(real_offers['carrier_type'].unique()) | set(your_offers['carrier_type'].unique()))
for carrier in offer_carriers:
    r_vol = real_offers[real_offers['carrier_type'] == carrier]['volume'].sum() * 0.5
    y_vol = your_offers[your_offers['carrier_type'] == carrier]['volume_mwh'].sum()
    r_pct = r_vol / real_offer_total_vol * 100 if real_offer_total_vol > 0 else 0
    y_pct = y_vol / your_offer_total_vol * 100 if your_offer_total_vol > 0 else 0
    print(f"  {carrier:<14} {r_vol:>15.1f} {r_pct:>16.1f}% {y_vol:>15.1f} {y_pct:>16.1f}% {y_vol-r_vol:>+12.1f}")
print(f"  {'-'*84}")
print(f"  {'TOTAL':<14} {real_offer_total_vol:>15.1f} {'100.0%':>17} {your_offer_total_vol:>15.1f} {'100.0%':>17} {your_offer_total_vol-real_offer_total_vol:>+12.1f}")

# --- Overall volume by technology (bids + offers combined) ---
print(f"\n--- COMBINED (Bids + Offers): Total Technology Contribution to Congestion ---")
print(f"  {'Technology':<14} {'Real Vol (MWh)':>15} {'% of Real':>10} {'Your Vol (MWh)':>15} {'% of Yours':>11} {'Diff (MWh)':>12}")
print(f"  {'-'*80}")
for carrier in all_carriers:
    r_vol = real_actions[real_actions['carrier_type'] == carrier]['volume_mwh'].sum()
    y_vol = your_actions[your_actions['carrier_type'] == carrier]['volume_mwh'].sum()
    r_pct = r_vol / real_total_vol_mwh * 100 if real_total_vol_mwh > 0 else 0
    y_pct = y_vol / your_total_vol_mwh * 100 if your_total_vol_mwh > 0 else 0
    print(f"  {carrier:<14} {r_vol:>15.1f} {r_pct:>9.1f}% {y_vol:>15.1f} {y_pct:>10.1f}% {y_vol-r_vol:>+12.1f}")
print(f"  {'-'*80}")
print(f"  {'TOTAL':<14} {real_total_vol_mwh:>15.1f} {'100.0%':>10} {your_total_vol_mwh:>15.1f} {'100.0%':>10} {your_total_vol_mwh-real_total_vol_mwh:>+12.1f}")