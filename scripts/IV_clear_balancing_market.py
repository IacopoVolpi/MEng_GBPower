# SPDX-FileCopyrightText: 2024-2025 The PyPSA Authors
# SPDX-License-Identifier: MIT

"""
Explicit Balancing Market Clearing with Geographic Zone-Based Logic

This script clears the balancing market by:
1. Loading pre-computed required balancing volumes per settlement period per geographic zone
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
from pathlib import Path
import sys

from _helpers import configure_logging

# Import shared function from distribute_balancing_volume script
sys.path.insert(0, str(Path(__file__).parent))
from IV_distribute_balancing_volume import classify_generator_to_zone


def create_unit_lookup(bmu_classification):
    """
    Create lookup tables for unit zone and type classification from BMU classification file.
    
    Parameters:
    -----------
    bmu_classification : pd.DataFrame
        BMU classification with constraint boundaries and carrier types
        Expected columns: carrier, SSE-SP_side, SCOTEX_side, SSHARN_side, FLOWSTH_side, SEIMP_side
    
    Returns:
    --------
    dict with keys:
        - 'zone': dict mapping unit_id -> zone
        - 'type': dict mapping unit_id -> carrier type
    """
    
    zone_lookup = {}
    type_lookup = {}
    
    for unit_id in bmu_classification.index:
        # Get zone using the imported function
        zone_lookup[unit_id] = classify_generator_to_zone(unit_id, bmu_classification)
        # Get carrier type directly from the classification file
        type_lookup[unit_id] = bmu_classification.loc[unit_id, 'carrier']
    
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
    #create a copy to avoid modifying original DataFrames
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
    """
    Clear a zone requiring downward balancing (flex down) using bids.
    
    Accepts bids in descending price order (highest price first).
    """
    
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
        
        # Negate price: submitted_bids.csv uses ELEXON BOD convention
        # (negative = SO pays generator, positive = generator pays SO)
        # We convert to SO cashflow convention (positive = cost to SO)
        # to match the sign convention used in bids.csv
        so_price = -row['price']
        action_cost = so_price * accept_volume * 0.5
        
        accepted_actions.append({
            'timestamp': settlement_timestamp,
            'zone': zone,
            'unit_id': row['unit_id'],
            'carrier_type': row['carrier_type'],
            'pair_id': row['pair_id'],
            'action_type': 'bid',
            'volume_mwh': accept_volume,
            'price_per_mwh': so_price,
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
        Pre-computed zone volumes with columns [timestamp, zone, flex_up_mwh, flex_down_mwh]
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
            
            # Process DOWNWARD balancing (accept bids)
            if flex_down_required > 0.1:
                zone_bids = available_bids[available_bids['zone'] == zone]
                result_down = clear_zone_downward(flex_down_required, zone_bids, zone, ts)
                
                # Remove accepted bids from available pool
                for action in result_down['accepted_actions']:
                    matched = available_bids[
                        (available_bids['unit_id'] == action['unit_id']) &
                        (available_bids['pair_id'] == action['pair_id']) &
                        (available_bids['timestamp'] == ts)
                    ]
                    if not matched.empty:
                        available_bids = available_bids.drop(matched.index)
                
                # Record settlement result for downward
                settlement_results.append({
                    'timestamp': ts,
                    'zone': zone,
                    'zone_name': zone_names.get(zone, 'Unknown'),
                    'direction': 'down',
                    'required_volume_mwh': flex_down_required,
                    'cleared_volume_mwh': result_down['cleared_volume'],
                    'uncleared_volume_mwh': result_down['uncleared_volume'],
                    'total_cost_gbp': result_down['total_cost'],
                    'uncleared_flag': 'YES' if result_down['uncleared_volume'] > 0.1 else 'NO',
                })
                all_accepted_actions.extend(result_down['accepted_actions'])
                
                if result_down['uncleared_volume'] > 0.1:
                    uncleared_summary.append({
                        'timestamp': ts,
                        'zone': zone,
                        'zone_name': zone_names.get(zone, 'Unknown'),
                        'direction': 'down',
                        'uncleared_volume_mwh': result_down['uncleared_volume'],
                    })
                    logger.warning(
                        f"UNCLEARED: {ts} | {zone} | down | "
                        f"{result_down['uncleared_volume']:.2f} MWh"
                    )
            
            # Process UPWARD balancing (accept offers) - INDEPENDENT of downward
            if flex_up_required > 0.1:
                zone_offers = available_offers[available_offers['zone'] == zone]
                result_up = clear_zone_upward(flex_up_required, zone_offers, zone, ts)
                
                # Remove accepted offers from available pool
                for action in result_up['accepted_actions']:
                    matched = available_offers[
                        (available_offers['unit_id'] == action['unit_id']) &
                        (available_offers['pair_id'] == action['pair_id']) &
                        (available_offers['timestamp'] == ts)
                    ]
                    if not matched.empty:
                        available_offers = available_offers.drop(matched.index)
                
                # Record settlement result for upward
                settlement_results.append({
                    'timestamp': ts,
                    'zone': zone,
                    'zone_name': zone_names.get(zone, 'Unknown'),
                    'direction': 'up',
                    'required_volume_mwh': flex_up_required,
                    'cleared_volume_mwh': result_up['cleared_volume'],
                    'uncleared_volume_mwh': result_up['uncleared_volume'],
                    'total_cost_gbp': result_up['total_cost'],
                    'uncleared_flag': 'YES' if result_up['uncleared_volume'] > 0.1 else 'NO',
                })
                all_accepted_actions.extend(result_up['accepted_actions'])
                
                if result_up['uncleared_volume'] > 0.1:
                    uncleared_summary.append({
                        'timestamp': ts,
                        'zone': zone,
                        'zone_name': zone_names.get(zone, 'Unknown'),
                        'direction': 'up',
                        'uncleared_volume_mwh': result_up['uncleared_volume'],
                    })
                    logger.warning(
                        f"UNCLEARED: {ts} | {zone} | up | "
                        f"{result_up['uncleared_volume']:.2f} MWh"
                    )
            
            # If neither direction needed, record a no-action result
            if flex_up_required <= 0.1 and flex_down_required <= 0.1:
                settlement_results.append({
                    'timestamp': ts,
                    'zone': zone,
                    'zone_name': zone_names.get(zone, 'Unknown'),
                    'direction': 'none',
                    'required_volume_mwh': 0.0,
                    'cleared_volume_mwh': 0.0,
                    'uncleared_volume_mwh': 0.0,
                    'total_cost_gbp': 0.0,
                    'uncleared_flag': 'NO',
                })
    
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
    
    logger.info("\nStep 1: Loading classification and pre-computed zone volumes...")
    
    bmu_classification = pd.read_csv(
        snakemake.input['bmu_classification'],
        index_col=0
    )
    logger.info(f"Loaded {len(bmu_classification)} BMUs with constraint classification")
    
    # Load pre-computed zone volumes from IV_distribute_balancing_volume
    zone_volumes = pd.read_csv(snakemake.input['zone_volumes'], parse_dates=['timestamp'])
    zone_volumes['timestamp'] = zone_volumes['timestamp'].dt.tz_localize(None)
    zone_volumes = zone_volumes.set_index(['timestamp', 'zone'])
    logger.info(f"Loaded pre-computed volumes for {len(zone_volumes)} zone-period combinations")
    
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

    # Convert bid volumes to absolute values (they're stored as negative)
    bids_df['volume_mw'] = bids_df['volume_mw'].abs()
    logger.info("Converted bid volumes to absolute values (bids were stored as negative)")

    # Filter bids and offers to only include BMUs that are in the classification file
    initial_bid_count = len(bids_df)
    initial_offer_count = len(offers_df)
    initial_bid_bmus = bids_df['unit_id'].nunique()
    initial_offer_bmus = offers_df['unit_id'].nunique()

    bids_filtered = bids_df[bids_df['unit_id'].isin(bmu_classification.index)]
    offers_filtered = offers_df[offers_df['unit_id'].isin(bmu_classification.index)]

    final_bid_count = len(bids_filtered)
    final_offer_count = len(offers_filtered)
    final_bid_bmus = bids_filtered['unit_id'].nunique()
    final_offer_bmus = offers_filtered['unit_id'].nunique()

    removed_bids = initial_bid_count - final_bid_count
    removed_offers = initial_offer_count - final_offer_count
    removed_bid_bmus = initial_bid_bmus - final_bid_bmus
    removed_offer_bmus = initial_offer_bmus - final_offer_bmus

    logger.info(f"Filtered bids: {initial_bid_count} --> {final_bid_count} ({final_bid_count/initial_bid_count*100:.1f}% retained)")
    logger.info(f"Filtered offers: {initial_offer_count} --> {final_offer_count} ({final_offer_count/initial_offer_count*100:.1f}% retained)")
    logger.info(f"Removed {removed_bids} bids and {removed_offers} offers from BMUs not in classification file (no geographic coordinates)")
    logger.info(f"Removed bids from {removed_bid_bmus}/{initial_bid_bmus} unique BMUs ({removed_bid_bmus/initial_bid_bmus*100:.1f}% of BMUs with bids)")
    logger.info(f"Removed offers from {removed_offer_bmus}/{initial_offer_bmus} unique BMUs ({removed_offer_bmus/initial_offer_bmus*100:.1f}% of BMUs with offers)")

    bids_df = bids_filtered
    offers_df = offers_filtered
    logger.info("")
    
    logger.info("\nStep 2.6: Excluding known problematic BMUs...")
    # These biomass units submit at £0/MWh and are never accepted by the real SO,
    # yet they absorb huge clearing volume in the IV algorithm — remove them.
    EXCLUDED_BMUS = ['DRAXX-5', 'DRAXX-6']
    
    n_bids_before = len(bids_df)
    n_offers_before = len(offers_df)
    bids_df = bids_df[~bids_df['unit_id'].isin(EXCLUDED_BMUS)]
    offers_df = offers_df[~offers_df['unit_id'].isin(EXCLUDED_BMUS)]
    n_bids_removed = n_bids_before - len(bids_df)
    n_offers_removed = n_offers_before - len(offers_df)
    logger.info(f"Excluded BMUs: {EXCLUDED_BMUS}")
    logger.info(f"Removed {n_bids_removed} bids and {n_offers_removed} offers from excluded BMUs")
    
    # Convert volume_mw to float64 to avoid dtype warnings during clearing
    bids_df['volume_mw'] = bids_df['volume_mw'].astype('float64')
    offers_df['volume_mw'] = offers_df['volume_mw'].astype('float64')

    logger.info("\nStep 3: Creating unit lookup tables...")
    unit_lookup = create_unit_lookup(bmu_classification)
    logger.info(f"Classified {len(unit_lookup['zone'])} units")
    
    logger.info("\nStep 4: Annotating bids and offers with zone/type info...")
    bids_df, offers_df = annotate_bids_offers(bids_df, offers_df, unit_lookup)
    logger.info("Annotation complete")
    
    # Convert volume_mw to float64 after all filtering to avoid dtype warnings
    bids_df['volume_mw'] = bids_df['volume_mw'].astype('float64')
    offers_df['volume_mw'] = offers_df['volume_mw'].astype('float64')

    # Check how many bids/offers per zone
    logger.info(f"Bids per zone:\n{bids_df['zone'].value_counts()}")
    logger.info(f"Offers per zone:\n{offers_df['zone'].value_counts()}")
    
    # ##########################################################################################################
    # #########################################################################################################
    # #Logging to visualise the distribution of bid and offer prices and volumes before clearing
    # # Preview sorted bids and offers
    # logger.info("\n" + "="*80)
    # logger.info("PREVIEW: Top 20 bids sorted by price (descending - most expensive first)")
    # logger.info("="*80)
    # top_bids = bids_df.sort_values('price', ascending=False).head(200)
    # logger.info(f"\n{top_bids[['timestamp', 'zone', 'unit_id', 'carrier_type', 'price', 'volume_mw']].to_string(index=False)}")
    
    # logger.info("\n" + "="*80)
    # logger.info("PREVIEW: Bottom 20 bids sorted by price (ascending - cheapest first)")
    # logger.info("="*80)
    # bottom_bids = bids_df.sort_values('price', ascending=True).head(200)
    # logger.info(f"\n{bottom_bids[['timestamp', 'zone', 'unit_id', 'carrier_type', 'price', 'volume_mw']].to_string(index=False)}")
    
    # logger.info("\n" + "="*80)
    # logger.info("PREVIEW: Top 20 offers sorted by price (ascending - cheapest first)")
    # logger.info("="*80)
    # top_offers = offers_df.sort_values('price', ascending=True).head(200)
    # logger.info(f"\n{top_offers[['timestamp', 'zone', 'unit_id', 'carrier_type', 'price', 'volume_mw']].to_string(index=False)}")
    
    # logger.info("\n" + "="*80)
    # logger.info("PREVIEW: Bottom 20 offers sorted by price (descending - most expensive first)")
    # logger.info("="*80)
    # bottom_offers = offers_df.sort_values('price', ascending=False).head(200)
    # logger.info(f"\n{bottom_offers[['timestamp', 'zone', 'unit_id', 'carrier_type', 'price', 'volume_mw']].to_string(index=False)}")
    

###########################################################################################################
#############################################################################################################

    logger.info("\nStep 5: Running balancing market clearing (red > orange > green > blue > purple > yellow)...")
    settlement_summary, accepted_actions, uncleared_summary = run_balancing_market_clearing(
        zone_volumes, bids_df, offers_df, unit_lookup
    )
    logger.info(f"Clearing complete. Accepted {len(accepted_actions)} actions.")
    
    if not uncleared_summary.empty:
        logger.warning(f"Uncleared volumes detected: {len(uncleared_summary)} zone-period combinations")
    
    logger.info("\nStep 6: Saving outputs...")
    
    # Save settlement summary
    settlement_summary.to_csv(snakemake.output['settlement_summary'], index=False)
    logger.info(f"Settlement summary saved: {snakemake.output['settlement_summary']}")
    
    # Save accepted actions (reorder columns, drop pair_id)
    if not accepted_actions.empty:
        accepted_actions = accepted_actions[['timestamp', 'zone', 'unit_id', 'carrier_type', 'action_type', 'price_per_mwh', 'cost_gbp', 'volume_mwh']]
        accepted_actions['cost_gbp'] = accepted_actions['cost_gbp'].round(2)
        accepted_actions['volume_mwh'] = accepted_actions['volume_mwh'].round(2)
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
    logger.info(f"Total cost: {accepted_actions['cost_gbp'].sum() if not accepted_actions.empty else 0:.2f} GBP")
    logger.info(f"Uncleared periods: {len(uncleared_summary)}")
    logger.info("="*80 + "\n")