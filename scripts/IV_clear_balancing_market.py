# SPDX-FileCopyrightText: 2024-2025 The PyPSA Authors
# SPDX-License-Identifier: MIT

"""
Explicit Balancing Market Clearing with Geographic Zone-Based Logic

This script clears the balancing market by:
1. Computing required balancing volumes per settlement period per geographic zone
2. Creating lookup tables for BMU zone/type classification
3. Clearing zones in order (red → orange → green → blue → purple → yellow)
4. For each zone, accepting bids/offers in merit order (price-based ranking)
5. Tracking uncleared volumes and flagging them in outputs

Geographic zones are defined by transmission constraint boundaries:
- Red: North of SSE-SP
- Orange: SSE-SP to SCOTEX
- Green: SCOTEX to SSHARN
- Blue: SSHARN to FLOWSTH
- Purple: FLOWSTH to SEIMP
- Yellow: South of all constraints
"""

import logging
logger = logging.getLogger(__name__)

import pandas as pd
import numpy as np
import pypsa
from _helpers import configure_logging


def get_unit_type(unit_id, n_redispatch):
    """
    Get the carrier type for a BMU (generator or storage unit).
    
    Parameters:
    -----------
    unit_id : str
        Unit ID (BMU)
    n_redispatch : pypsa.Network
        Network with generator/storage unit data
    
    Returns:
    --------
    str
        Carrier type (e.g., 'wind', 'gas', 'battery', 'hydro', etc.)
    """
    if unit_id in n_redispatch.generators.index:
        return n_redispatch.generators.loc[unit_id, 'carrier']
    elif unit_id in n_redispatch.storage_units.index:
        return n_redispatch.storage_units.loc[unit_id, 'carrier']
    else:
        return 'unknown'


def classify_unit_to_zone(unit_id, bmu_classification):
    """
    Classify a unit to its geographic zone based on constraint boundaries.
    
    Parameters:
    -----------
    unit_id : str
        Unit ID (BMU)
    bmu_classification : pd.DataFrame
        DataFrame with columns like 'SSE-SP_side', 'SCOTEX_side', etc.
    
    Returns:
    --------
    str
        Color zone: 'red', 'orange', 'green', 'blue', 'purple', 'yellow', or 'unknown'
    """
    if unit_id not in bmu_classification.index:
        return 'unknown'
    
    row = bmu_classification.loc[unit_id]
    
    # Check constraints in order from north to south
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


def calculate_zone_volumes_per_period(n_wholesale, n_redispatch, bmu_classification):
    """
    Calculate required balancing volume per settlement period per geographic zone.
    
    Uses dispatch change logic: compares wholesale vs redispatch dispatch
    at each timestamp for each zone.
    
    Parameters:
    -----------
    n_wholesale : pypsa.Network
        Network with wholesale market dispatch (before balancing)
    n_redispatch : pypsa.Network
        Network with redispatch (after balancing)
    bmu_classification : pd.DataFrame
        BMU classification with constraint boundaries
    
    Returns:
    --------
    pd.DataFrame
        Indexed by [timestamp, zone] with columns [flex_up_mwh, flex_down_mwh]
    """
    
    # Get dispatch time series
    wholesale_dispatch = n_wholesale.generators_t.p
    redispatch_dispatch = n_redispatch.generators_t.p
    
    # Get unique timestamps
    timestamps = wholesale_dispatch.index
    zones = ['red', 'orange', 'green', 'blue', 'purple', 'yellow', 'unknown']
    
    results = []
    
    for ts in timestamps:
        for zone in zones:
            # Find all generators in this zone
            zone_generators = []
            for gen_name in wholesale_dispatch.columns:
                if gen_name not in redispatch_dispatch.columns:
                    continue
                if classify_unit_to_zone(gen_name, bmu_classification) == zone:
                    zone_generators.append(gen_name)
            
            if not zone_generators:
                # No generators in this zone, skip
                continue
            
            # Calculate dispatch changes for this zone at this timestamp
            flex_up = 0.0
            flex_down = 0.0
            
            for gen_name in zone_generators:
                wholesale_dispatch_val = wholesale_dispatch.loc[ts, gen_name] * 0.5  # 30-min to MWh
                redispatch_dispatch_val = redispatch_dispatch.loc[ts, gen_name] * 0.5
                change = redispatch_dispatch_val - wholesale_dispatch_val
                
                if change > 0:
                    flex_up += change
                else:
                    flex_down += abs(change)
            
            results.append({
                'timestamp': ts,
                'zone': zone,
                'flex_up_mwh': flex_up,
                'flex_down_mwh': flex_down,
            })
    
    zone_volumes = pd.DataFrame(results).set_index(['timestamp', 'zone'])
    return zone_volumes


def create_unit_lookup(n_redispatch, bmu_classification):
    """
    Create lookup tables for unit zone and type classification.
    
    Parameters:
    -----------
    n_redispatch : pypsa.Network
        Network with generator/storage unit data
    bmu_classification : pd.DataFrame
        BMU classification with constraint boundaries
    
    Returns:
    --------
    dict with keys:
        - 'zone': dict mapping unit_id -> zone
        - 'type': dict mapping unit_id -> carrier type
    """
    
    # Get all units from both generators and storage_units
    all_units = set(n_redispatch.generators.index) | set(n_redispatch.storage_units.index)
    
    zone_lookup = {}
    type_lookup = {}
    
    for unit_id in all_units:
        zone_lookup[unit_id] = classify_unit_to_zone(unit_id, bmu_classification)
        type_lookup[unit_id] = get_unit_type(unit_id, n_redispatch)
    
    return {
        'zone': zone_lookup,
        'type': type_lookup,
    }


def annotate_bids_offers(bids_df, offers_df, unit_lookup):
    """
    Add zone and type information to bids and offers DataFrames.
    
    Parameters:
    -----------
    bids_df : pd.DataFrame
        Bids with columns [timestamp, unit_id, pair_id, price, volume_mw]
    offers_df : pd.DataFrame
        Offers with columns [timestamp, unit_id, pair_id, price, volume_mw]
    unit_lookup : dict
        Output from create_unit_lookup()
    
    Returns:
    --------
    tuple of (bids_df, offers_df) with added columns [zone, carrier_type]
    """
    
    bids = bids_df.copy()
    offers = offers_df.copy()
    
    # Add zone and type to bids
    bids['zone'] = bids['unit_id'].map(unit_lookup['zone'])
    bids['carrier_type'] = bids['unit_id'].map(unit_lookup['type'])
    
    # Add zone and type to offers
    offers['zone'] = offers['unit_id'].map(unit_lookup['zone'])
    offers['carrier_type'] = offers['unit_id'].map(unit_lookup['type'])
    
    return bids, offers


def clear_zone_upward(required_volume, available_offers, zone, settlement_timestamp):
    """
    Clear a zone requiring upward balancing (flex up) using offers.
    
    Accepts offers in ascending price order (cheapest first).
    """
    
    if available_offers.empty or required_volume <= 0:
        return {
            'cleared_volume': 0.0,
            'uncleared_volume': required_volume,
            'total_cost': 0.0,
            'accepted_actions': [],
        }
    
    # Sort offers by price ascending (cheapest first)
    sorted_offers = available_offers.sort_values('price', ascending=True).copy()
    
    accepted_actions = []  
    remaining_volume = required_volume
    total_cost = 0.0
    
    for idx, row in sorted_offers.iterrows():
        if remaining_volume <= 0.1:
            break
        
        accept_volume = min(row['volume_mw'], remaining_volume)
        
        if accept_volume <= 0:
            continue
        
        action_cost = row['price'] * accept_volume * 0.5
        
        accepted_actions.append({
            'timestamp': settlement_timestamp,
            'zone': zone,
            'unit_id': row['unit_id'],
            'carrier_type': row['carrier_type'],
            'pair_id': row['pair_id'],
            'action_type': 'offer',
            'volume_mwh': accept_volume,
            'price_per_mwh': row['price'],
            'cost_gbp': action_cost,
        })
        
        available_offers.loc[idx, 'volume_mw'] -= accept_volume
        remaining_volume -= accept_volume
        total_cost += action_cost
    
    return {
        'cleared_volume': required_volume - max(0, remaining_volume),
        'uncleared_volume': max(0, remaining_volume),
        'total_cost': total_cost,
        'accepted_actions': accepted_actions,
    }


def clear_zone_downward(required_volume, available_bids, zone, settlement_timestamp):
    if available_bids.empty or required_volume <= 0:
        return {
            'cleared_volume': 0.0,
            'uncleared_volume': required_volume,
            'total_cost': 0.0,
            'accepted_actions': [],
        }
    
    sorted_bids = available_bids.sort_values('price', ascending=False).copy()
    
    accepted_actions = []
    remaining_volume = required_volume
    total_cost = 0.0
    
    for idx, row in sorted_bids.iterrows():
        if remaining_volume <= 0.1:
            break
        
        accept_volume = min(row['volume_mw'], remaining_volume)
        
        if accept_volume <= 0:
            continue
        
        action_cost = row['price'] * accept_volume * 0.5
        
        accepted_actions.append({
            'timestamp': settlement_timestamp,
            'zone': zone,
            'unit_id': row['unit_id'],
            'carrier_type': row['carrier_type'],
            'pair_id': row['pair_id'],
            'action_type': 'bid',
            'volume_mwh': accept_volume,
            'price_per_mwh': row['price'],
            'cost_gbp': action_cost,
        })
        
        available_bids.loc[idx, 'volume_mw'] -= accept_volume
        remaining_volume -= accept_volume
        total_cost += action_cost
    
    return {
        'cleared_volume': required_volume - max(0, remaining_volume),
        'uncleared_volume': max(0, remaining_volume),
        'total_cost': total_cost,
        'accepted_actions': accepted_actions,
    }


def run_balancing_market_clearing(zone_volumes, bids_df, offers_df, unit_lookup):
    """
    Main balancing market clearing algorithm.
    
    Clears zones in order: red, orange, green, blue, purple, yellow
    
    Parameters:
    -----------
    zone_volumes : pd.DataFrame
        Output from calculate_zone_volumes_per_period()
    bids_df : pd.DataFrame
        Annotated bids with zone and type info
    offers_df : pd.DataFrame
        Annotated offers with zone and type info
    unit_lookup : dict
        Output from create_unit_lookup()
    
    Returns:
    --------
    tuple of (settlement_summary_df, accepted_actions_df, uncleared_summary_df)
    """
    
    zone_order = ['red', 'orange', 'green', 'blue', 'purple', 'yellow', 'unknown']
    zone_names = {
        'red': 'North of SSE-SP',
        'orange': 'SSE-SP to SCOTEX',
        'green': 'SCOTEX to SSHARN',
        'blue': 'SSHARN to FLOWSTH',
        'purple': 'FLOWSTH to SEIMP',
        'yellow': 'South of all constraints',
        'unknown': 'Not classified',
    }
    
    settlement_results = []
    all_accepted_actions = []
    uncleared_summary = []
    
    # Get unique timestamps
    timestamps = zone_volumes.index.get_level_values('timestamp').unique()
    
    for i, ts in enumerate(timestamps):
        logger.debug(f"Clearing settlement period {ts}")
        
        # Create mutable copies for this timestamp
        available_bids = bids_df[bids_df['timestamp'] == ts].copy()
        available_offers = offers_df[offers_df['timestamp'] == ts].copy()
        
        # Clear zones in order
        for zone in zone_order:
            
            # Get required volume for this zone at this timestamp
            try:
                zone_vol = zone_volumes.loc[(ts, zone)]
                flex_up_required = zone_vol['flex_up_mwh']
                flex_down_required = zone_vol['flex_down_mwh']
            except KeyError:
                # No volume required for this zone at this timestamp
                flex_up_required = 0.0
                flex_down_required = 0.0
            
            # Determine which direction balancing is needed
            if flex_down_required > 0.1:
                # Need to reduce output: accept bids
                direction = 'down'
                required_volume = flex_down_required
                
                # Filter bids to this zone only
                zone_bids = available_bids[available_bids['zone'] == zone]
                
                result = clear_zone_downward(required_volume, zone_bids, zone, ts)
                
                # Remove accepted bids from available pool
                for action in result['accepted_actions']:
                    matched = available_bids[
                        (available_bids['unit_id'] == action['unit_id']) &
                        (available_bids['pair_id'] == action['pair_id']) &
                        (available_bids['timestamp'] == ts)
                    ]
                    if not matched.empty:
                        available_bids = available_bids.drop(matched.index)
                
            elif flex_up_required > 0.1:
                # Need to increase output: accept offers
                direction = 'up'
                required_volume = flex_up_required
                
                # Filter offers to this zone only
                zone_offers = available_offers[available_offers['zone'] == zone]
                
                result = clear_zone_upward(required_volume, zone_offers, zone, ts)
                
                # Remove accepted offers from available pool
                for action in result['accepted_actions']:
                    matched = available_offers[
                        (available_offers['unit_id'] == action['unit_id']) &
                        (available_offers['pair_id'] == action['pair_id']) &
                        (available_offers['timestamp'] == ts)
                    ]
                    if not matched.empty:
                        available_offers = available_offers.drop(matched.index)
            
            else:
                # No balancing required for this zone
                direction = 'none'
                required_volume = 0.0
                result = {
                    'cleared_volume': 0.0,
                    'uncleared_volume': 0.0,
                    'total_cost': 0.0,
                    'accepted_actions': [],
                }
            
            # Log settlement result for this zone
            settlement_results.append({
                'timestamp': ts,
                'zone': zone,
                'zone_name': zone_names.get(zone, 'Unknown'),
                'direction': direction,
                'required_volume_mwh': required_volume,
                'cleared_volume_mwh': result['cleared_volume'],
                'uncleared_volume_mwh': result['uncleared_volume'],
                'total_cost_gbp': result['total_cost'],
                'uncleared_flag': 'YES' if result['uncleared_volume'] > 0.1 else 'NO',
            })
            
            # Collect accepted actions
            all_accepted_actions.extend(result['accepted_actions'])
            
            # Flag uncleared volumes
            if result['uncleared_volume'] > 0.1:
                uncleared_summary.append({
                    'timestamp': ts,
                    'zone': zone,
                    'zone_name': zone_names.get(zone, 'Unknown'),
                    'direction': direction,
                    'uncleared_volume_mwh': result['uncleared_volume'],
                })
                logger.warning(
                    f"UNCLEARED: {ts} | {zone} | {direction} | "
                    f"{result['uncleared_volume']:.2f} MWh"
                )
    
    return (
        pd.DataFrame(settlement_results),
        pd.DataFrame(all_accepted_actions),
        pd.DataFrame(uncleared_summary),
    )


if __name__ == '__main__':
    
    configure_logging(snakemake)
    
    logger.info("="*80)
    logger.info("BALANCING MARKET CLEARING - GEOGRAPHIC ZONE-BASED")
    logger.info("="*80)
    
    logger.info("\nStep 1: Loading networks and classification...")
    n_wholesale = pypsa.Network(snakemake.input['network_wholesale'])
    n_redispatch = pypsa.Network(snakemake.input['network_redispatch'])
    
    bmu_classification = pd.read_csv(
        snakemake.input['bmu_classification'],
        index_col=0
    )
    logger.info(f"Loaded {len(bmu_classification)} BMUs with constraint classification")
    
    logger.info("\nStep 2: Loading and preprocessing bids and offers...")
    bids_raw = pd.read_csv(snakemake.input['submitted_bids'], parse_dates=['timestamp'])
    offers_raw = pd.read_csv(snakemake.input['submitted_offers'], parse_dates=['timestamp'])

    # Rename columns to match expected format
    bids_df = bids_raw[['timestamp', 'NationalGridBmUnit', 'PairId', 'Bid', 'LevelFrom']].copy()
    bids_df.columns = ['timestamp', 'unit_id', 'pair_id', 'price', 'volume_mw']

    offers_df = offers_raw[['timestamp', 'NationalGridBmUnit', 'PairId', 'Offer', 'LevelFrom']].copy()
    offers_df.columns = ['timestamp', 'unit_id', 'pair_id', 'price', 'volume_mw']

    # Remove timezone info to match PyPSA network timestamps
    bids_df['timestamp'] = bids_df['timestamp'].dt.tz_localize(None)
    offers_df['timestamp'] = offers_df['timestamp'].dt.tz_localize(None)

    logger.info(f"Loaded {len(bids_df)} bids, {len(offers_df)} offers")
    
    logger.info("\nStep 2.5: Converting bid volumes to absolute values and filtering...")
    # CRITICAL FIX: Bids have negative volumes in the data. Convert to absolute values.
    bids_df['volume_mw'] = bids_df['volume_mw'].abs()
    logger.info("Converted bid volumes to absolute values (bids were stored as negative)")
    
    # Convert volume_mw to float64 to avoid dtype warnings during clearing
    bids_df['volume_mw'] = bids_df['volume_mw'].astype('float64')
    offers_df['volume_mw'] = offers_df['volume_mw'].astype('float64')
    logger.info("Converted volume_mw to float64 dtype")

    # Get all units in the network (generators + storage units)
    network_units = set(n_redispatch.generators.index) | set(n_redispatch.storage_units.index)
    logger.info(f"Network contains {len(network_units)} units")
    
    # Filter bids and offers to only network units
    bids_before = len(bids_df)
    offers_before = len(offers_df)
    
    bids_df = bids_df[bids_df['unit_id'].isin(network_units)].copy()
    offers_df = offers_df[offers_df['unit_id'].isin(network_units)].copy()
    
    logger.info(f"Filtered bids: {bids_before} --> {len(bids_df)} ({len(bids_df)/bids_before*100:.1f}% retained)")
    logger.info(f"Filtered offers: {offers_before} --> {len(offers_df)} ({len(offers_df)/offers_before*100:.1f}% retained)")
    
    removed_bids = bids_before - len(bids_df)
    removed_offers = offers_before - len(offers_df)
    if removed_bids > 0 or removed_offers > 0:
        logger.warning(f"Removed {removed_bids} bids and {removed_offers} offers from non-network units (virtual/demand units)")

    logger.info("\nStep 3: Creating unit lookup tables...")
    unit_lookup = create_unit_lookup(n_redispatch, bmu_classification)
    logger.info(f"Classified {len(unit_lookup['zone'])} units")
    
    logger.info("\nStep 4: Annotating bids and offers with zone/type info...")
    bids_df, offers_df = annotate_bids_offers(bids_df, offers_df, unit_lookup)
    logger.info("Annotation complete")
    
    logger.info("\nStep 4.5: Debugging zone assignment...")
    logger.info(f"Sample bids zones:\n{bids_df[['unit_id', 'zone']].drop_duplicates().head(10)}")
    logger.info(f"Sample offers zones:\n{offers_df[['unit_id', 'zone']].drop_duplicates().head(10)}")

    # Convert volume_mw to float64 after all filtering to avoid dtype warnings
    bids_df['volume_mw'] = bids_df['volume_mw'].astype('float64')
    offers_df['volume_mw'] = offers_df['volume_mw'].astype('float64')

    # Check how many bids/offers per zone
    logger.info(f"Bids per zone:\n{bids_df['zone'].value_counts()}")
    logger.info(f"Offers per zone:\n{offers_df['zone'].value_counts()}")

    # Check sample generator names
    logger.info(f"Sample generators from network: {list(n_redispatch.generators.index)[:5]}")
    logger.info(f"Sample bids unit_ids: {bids_df['unit_id'].unique()[:5]}")

    logger.info("\nStep 5: Calculating required balancing volumes per zone per period...")
    zone_volumes = calculate_zone_volumes_per_period(n_wholesale, n_redispatch, bmu_classification)
    logger.info(f"Calculated volumes for {len(zone_volumes)} zone-period combinations")
    
    logger.info("\nStep 5.5: Debug - Available bids and offers before clearing...")
    for zone in ['red', 'orange', 'green', 'blue', 'purple', 'yellow']:
        zone_bids = bids_df[bids_df['zone'] == zone]
        zone_offers = offers_df[offers_df['zone'] == zone]
        if not zone_bids.empty:
            logger.info(f"Zone {zone} bids: {len(zone_bids)} rows, total volume: {zone_bids['volume_mw'].sum():.2f} MW")
        if not zone_offers.empty:
            logger.info(f"Zone {zone} offers: {len(zone_offers)} rows, total volume: {zone_offers['volume_mw'].sum():.2f} MW")
    
    # Show sample bids/offers with all columns
    logger.info(f"\nSample bids for clearing:\n{bids_df[['timestamp', 'unit_id', 'zone', 'price', 'volume_mw']].head(20)}")
    logger.info(f"\nSample offers for clearing:\n{offers_df[['timestamp', 'unit_id', 'zone', 'price', 'volume_mw']].head(20)}")

    logger.info("\nStep 6: Running balancing market clearing (red > orange > green > blue > purple > yellow)...")
    settlement_summary, accepted_actions, uncleared_summary = run_balancing_market_clearing(
        zone_volumes, bids_df, offers_df, unit_lookup
    )
    logger.info(f"Clearing complete. Accepted {len(accepted_actions)} actions.")
    
    if not uncleared_summary.empty:
        logger.warning(f"\nWARNING: {len(uncleared_summary)} uncleared volumes detected")
        logger.warning(uncleared_summary.to_string())
    
    logger.info("\nStep 7: Saving outputs...")
    
    # Save settlement summary
    settlement_summary.to_csv(snakemake.output['settlement_summary'], index=False)
    logger.info(f"Settlement summary saved: {snakemake.output['settlement_summary']}")
    
    # Save accepted actions
    accepted_actions.to_csv(snakemake.output['accepted_actions'], index=False)
    logger.info(f"Accepted actions saved: {snakemake.output['accepted_actions']}")
    
    # Save uncleared summary
    uncleared_summary.to_csv(snakemake.output['uncleared_summary'], index=False)
    logger.info(f"Uncleared summary saved: {snakemake.output['uncleared_summary']}")
    
    logger.info("\n" + "="*80)
    logger.info("BALANCING MARKET CLEARING COMPLETE")
    logger.info("="*80)
    logger.info(f"Total settlement periods: {settlement_summary['timestamp'].nunique()}")
    logger.info(f"Total zones: {settlement_summary['zone'].nunique()}")
    logger.info(f"Total volume cleared: {accepted_actions['volume_mwh'].sum() if not accepted_actions.empty else 0:.2f} MWh")
    logger.info(f"Total cost: {accepted_actions['cost_gbp'].sum() if not accepted_actions.empty else 0:.2f}")
    logger.info(f"Uncleared periods: {len(uncleared_summary)}")
    logger.info("="*80 + "\n")