# SPDX-FileCopyrightText: 2024-2025 Lukas Franken
# SPDX-License-Identifier: MIT

"""
Analyze dispatch changes across geographic zones during balancing.

This script compares wholesale dispatch (before balancing) with redispatch 
(after balancing) to understand how much generation in each geographic zone
had to flex up or down to relieve transmission constraints.

"""

import logging
logger = logging.getLogger(__name__)

import pandas as pd
import pypsa
from _helpers import configure_logging


def get_generator_type(gen_name, n_redispatch):

    if gen_name in n_redispatch.generators.index:
        return n_redispatch.generators.loc[gen_name, 'carrier']
    elif gen_name in n_redispatch.storage_units.index:
        return n_redispatch.storage_units.loc[gen_name, 'carrier']
    else:
        return 'unknown'


def classify_generator_to_zone(gen_name, bmu_classification):
   
    if gen_name not in bmu_classification.index:
        # Generator not in classification (likely not a BMU)
        return 'unknown'
    
    row = bmu_classification.loc[gen_name]
    
    # Check constraints in order from north to south
    # First constraint north of generator determines its zone
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


def calculate_dispatch_changes(n_wholesale, n_redispatch, bmu_classification):
    
    # Get dispatch time series
    wholesale_dispatch = n_wholesale.generators_t.p
    redispatch_dispatch = n_redispatch.generators_t.p
    
    changes = []
    zone_volumes_list = []
    
    # Aggregate per-timestamp per-zone volumes
    zone_volumes_dict = {}
    
    for gen_name in wholesale_dispatch.columns:
        
        # Only process generators present in both networks
        if gen_name not in redispatch_dispatch.columns:
            continue
        
        # Get generator metadata once
        gen_type = get_generator_type(gen_name, n_redispatch)
        zone = classify_generator_to_zone(gen_name, bmu_classification)
        
        # Calculate per-timestamp changes and aggregate by zone
        for ts in wholesale_dispatch.index:
            wholesale_val = wholesale_dispatch.loc[ts, gen_name] * 0.5
            redispatch_val = redispatch_dispatch.loc[ts, gen_name] * 0.5
            change = redispatch_val - wholesale_val
            
            # Skip unknown zone generators
            if zone == 'unknown':
                continue
            
            # Accumulate zone volumes
            key = (ts, zone)
            if key not in zone_volumes_dict:
                zone_volumes_dict[key] = {'flex_up': 0.0, 'flex_down': 0.0}
            
            if change > 0:
                zone_volumes_dict[key]['flex_up'] += change
            else:
                zone_volumes_dict[key]['flex_down'] += abs(change)
        
        # Calculate total change across all timestamps
        wholesale_sum = wholesale_dispatch[gen_name].sum() * 0.5
        redispatch_sum = redispatch_dispatch[gen_name].sum() * 0.5
        total_change = redispatch_sum - wholesale_sum
        
        # Determine direction based on total change
        if total_change > 0:
            direction = 'up'
        elif total_change < 0:
            direction = 'down'
        else:
            direction = 'none'
        
        changes.append({
            'generator': gen_name,
            'type': gen_type,
            'zone': zone,
            'dispatch_change_mwh': total_change,
            'direction': direction,
        })
    
    # Convert zone volumes dict to DataFrame
    for (ts, zone), vols in zone_volumes_dict.items():
        zone_volumes_list.append({
            'timestamp': ts,
            'zone': zone,
            'flex_up_mwh': vols['flex_up'],
            'flex_down_mwh': vols['flex_down'],
        })
    
    # Create dispatch changes DataFrame
    changes_df = pd.DataFrame(changes)
    
    # Sort by zone (north to south), then by type, then by generator name
    zone_order = {
        'red': 0,
        'orange': 1,
        'green': 2,
        'blue': 3,
        'purple': 4,
        'yellow': 5,
        'unknown': 6,
    }
    changes_df['zone_order'] = changes_df['zone'].map(zone_order)
    changes_df = changes_df.sort_values(['zone_order', 'type', 'generator'])
    changes_df = changes_df.drop('zone_order', axis=1)
    
    # Create zone volumes DataFrame
    zone_volumes_df = pd.DataFrame(zone_volumes_list)
    
    return changes_df.reset_index(drop=True), zone_volumes_df


def aggregate_by_zone(dispatch_changes_df):
    
    # Group by zone
    grouped = dispatch_changes_df.groupby('zone')
    
    # Separate up and down movements before grouping
    flex_up = dispatch_changes_df[dispatch_changes_df['direction'] == 'up'].groupby('zone')['dispatch_change_mwh'].sum()
    flex_down = dispatch_changes_df[dispatch_changes_df['direction'] == 'down'].groupby('zone')['dispatch_change_mwh'].sum()
    
    aggregated = pd.DataFrame({
        'count_generators': grouped.size(),
        'total_change_mwh': grouped['dispatch_change_mwh'].sum(),
        'avg_change_per_generator': grouped['dispatch_change_mwh'].mean(),
        'flex_up_mwh': flex_up,
        'flex_down_mwh': flex_down,
    }).fillna(0)
    
    # Reorder zones from north to south
    zone_order = ['red', 'orange', 'green', 'blue', 'purple', 'yellow', 'unknown']
    aggregated = aggregated.reindex([z for z in zone_order if z in aggregated.index])
    
    # Add zone names for clarity
    zone_names = {
        'red': 'North of SSE-SP',
        'orange': 'SSE-SP to SCOTEX',
        'green': 'SCOTEX to SSHARN',
        'blue': 'SSHARN to FLOWSTH',
        'purple': 'FLOWSTH to SEIMP',
        'yellow': 'South of all constraints',
        'unknown': 'Not classified',
    }
    aggregated['zone_name'] = [zone_names.get(z, 'Unknown') for z in aggregated.index]
    
    return aggregated


def aggregate_by_zone_and_type(dispatch_changes_df):
    
    # Group by zone and type
    grouped = dispatch_changes_df.groupby(['zone', 'type'])
    
    aggregated = pd.DataFrame({
        'generator_count': grouped.size(),
        'dispatch_change_mwh': grouped['dispatch_change_mwh'].sum(),
    }).reset_index()
    
    # Determine direction based on sign of dispatch_change_mwh
    aggregated['direction'] = aggregated['dispatch_change_mwh'].apply(
        lambda x: 'up' if x > 0 else ('down' if x < 0 else 'none')
    )
    
    # Define zone order (north to south)
    zone_order = {
        'red': 0,
        'orange': 1,
        'green': 2,
        'blue': 3,
        'purple': 4,
        'yellow': 5,
        'unknown': 6,
    }
    
    # Add zone order for sorting
    aggregated['zone_order'] = aggregated['zone'].map(zone_order)
    
    # Sort by zone, then by type
    aggregated = aggregated.sort_values(['zone_order', 'type']).reset_index(drop=True)
    
    # Add zone names for clarity
    zone_names = {
        'red': 'North of SSE-SP',
        'orange': 'SSE-SP to SCOTEX',
        'green': 'SCOTEX to SSHARN',
        'blue': 'SSHARN to FLOWSTH',
        'purple': 'FLOWSTH to SEIMP',
        'yellow': 'South of all constraints',
        'unknown': 'Not classified',
    }
    aggregated['zone_name'] = aggregated['zone'].map(zone_names)
    
    # Reorder columns
    aggregated = aggregated[['type', 'zone', 'zone_name', 'generator_count', 'dispatch_change_mwh', 'direction']]
    aggregated = aggregated.drop('zone_order', axis=1, errors='ignore')
    
    return aggregated


if __name__ == '__main__':
    
    configure_logging(snakemake)
    
    logger.info("Loading networks...")
    n_wholesale = pypsa.Network(snakemake.input['network_wholesale'])
    n_redispatch = pypsa.Network(snakemake.input['network_redispatch'])
    
    logger.info("Loading BMU constraint classification...")
    bmu_classification = pd.read_csv(
        snakemake.input['bmu_classification'],
        index_col=0
    )
    
    # Calculate dispatch changes and zone volumes in one pass
    logger.info("Calculating dispatch changes and zone volumes...")
    dispatch_changes, zone_volumes = calculate_dispatch_changes(
        n_wholesale,
        n_redispatch,
        bmu_classification
    )
    logger.info(f"Processed {len(dispatch_changes)} generators")
    logger.info(f"Generated {len(zone_volumes)} zone-timestamp combinations")
    
    # Use dispatch_changes for aggregations
    aggregated_by_zone = aggregate_by_zone(dispatch_changes)
    aggregated_by_zone_and_type = aggregate_by_zone_and_type(dispatch_changes)
    
    # Save zone volumes per timestamp (for market clearing)
    zone_volumes_output = zone_volumes.round(2)
    zone_volumes_output.to_csv(snakemake.output['zone_volumes_per_timestamp'], index=False)
    logger.info(f"Zone volumes per timestamp saved to {snakemake.output['zone_volumes_per_timestamp']}")

    # Save aggregated by zone
    aggregated_by_zone_output = aggregated_by_zone[['count_generators', 'total_change_mwh', 'avg_change_per_generator', 'flex_up_mwh', 'flex_down_mwh', 'zone_name']].round(2)
    aggregated_by_zone_output.to_csv(snakemake.output['aggregated_by_zone'])
    logger.info(f"Aggregated by zone results saved to {snakemake.output['aggregated_by_zone']}")
    

    # Save aggregated by zone and type
    aggregated_by_zone_and_type_output = aggregated_by_zone_and_type.round(2)
    aggregated_by_zone_and_type_output.to_csv(snakemake.output['aggregated_by_zone_and_type'], index=False)
    logger.info(f"Aggregated by zone and type results saved to {snakemake.output['aggregated_by_zone_and_type']}")
    
    logger.info("\nDispatch change analysis complete!")