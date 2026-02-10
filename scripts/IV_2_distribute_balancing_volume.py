# SPDX-FileCopyrightText: 2024-2025 Lukas Franken
# SPDX-License-Identifier: MIT

"""
Analyze dispatch changes across geographic zones during balancing.

This script compares wholesale dispatch (before balancing) with redispatch 
(after balancing) to understand how much generation in each geographic zone
had to flex up or down to relieve transmission constraints.

Geographic zones are defined by the constraint boundaries from 
IV_classify_bmu_constraints.py, creating 6 color-coded regions:
- Red: North of SSE-SP
- Orange: South of SSE-SP, North of SCOTEX
- Green: South of SCOTEX, North of SSHARN
- Blue: South of SSHARN, North of FLOWSTH
- Purple: South of FLOWSTH, North of SEIMP
- Yellow: South of all constraints
"""

import logging
logger = logging.getLogger(__name__)

import pandas as pd
import pypsa
from _helpers import configure_logging


def get_generator_type(gen_name, n_redispatch):
    """
    Get the carrier type for a generator.
    
    Parameters:
    -----------
    gen_name : str
        Generator name
    n_redispatch : pypsa.Network
        Network with generator data
    
    Returns:
    --------
    str
        Carrier type (e.g., 'wind', 'gas', 'battery', etc.)
    """
    if gen_name in n_redispatch.generators.index:
        return n_redispatch.generators.loc[gen_name, 'carrier']
    elif gen_name in n_redispatch.storage_units.index:
        return n_redispatch.storage_units.loc[gen_name, 'carrier']
    else:
        return 'unknown'


def classify_generator_to_zone(gen_name, bmu_classification):
    """
    Classify a generator to its geographic zone (color).
    
    Uses the constraint boundaries from IV_classify_bmu_constraints.py
    to determine which zone a generator belongs to.
    
    Parameters:
    -----------
    gen_name : str
        Generator name (BMU ID)
    bmu_classification : pd.DataFrame
        DataFrame with columns like 'SSE-SP_side', 'SCOTEX_side', etc.
    
    Returns:
    --------
    str
        Color zone: 'red', 'orange', 'green', 'blue', 'purple', or 'yellow'
    """
    
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
    """
    Calculate dispatch changes for each generator between wholesale and redispatch.
    
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
        Columns: generator, type, zone, dispatch_change_mwh, direction
    """
    
    # Get dispatch time series
    wholesale_dispatch = n_wholesale.generators_t.p
    redispatch_dispatch = n_redispatch.generators_t.p
    
    changes = []
    
    for gen_name in wholesale_dispatch.columns:
        
        # Only process generators present in both networks
        if gen_name not in redispatch_dispatch.columns:
            continue
        
        # Calculate change: redispatch - wholesale
        # Sum across all timesteps and convert to MWh (multiply by 0.5 for 30-min data)
        wholesale_sum = wholesale_dispatch[gen_name].sum() * 0.5
        redispatch_sum = redispatch_dispatch[gen_name].sum() * 0.5
        change = redispatch_sum - wholesale_sum
        
        # Get generator type
        gen_type = get_generator_type(gen_name, n_redispatch)
        
        # Classify to zone
        zone = classify_generator_to_zone(gen_name, bmu_classification)
        
        # Determine direction
        if change > 0:
            direction = 'up'
        elif change < 0:
            direction = 'down'
        else:
            direction = 'none'
        
        changes.append({
            'generator': gen_name,
            'type': gen_type,
            'zone': zone,
            'dispatch_change_mwh': change,
            'direction': direction,
        })
    
    return pd.DataFrame(changes)


def sort_generators_by_zone_and_type(dispatch_changes_df):
    """
    Sort generators first by zone (north to south), then by type (alphabetically).
    
    Parameters:
    -----------
    dispatch_changes_df : pd.DataFrame
        Output from calculate_dispatch_changes()
    
    Returns:
    --------
    pd.DataFrame
        Sorted by zone then type
    """
    
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
    
    # Create a copy and add sort keys
    df = dispatch_changes_df.copy()
    df['zone_order'] = df['zone'].map(zone_order)
    
    # Sort by zone order, then by type, then by generator name
    df = df.sort_values(['zone_order', 'type', 'generator'])
    df = df.drop('zone_order', axis=1)
    
    return df.reset_index(drop=True)


def aggregate_by_zone(dispatch_changes_df):
    """
    Aggregate dispatch changes by geographic zone.
    
    Parameters:
    -----------
    dispatch_changes_df : pd.DataFrame
        Output from calculate_dispatch_changes()
    
    Returns:
    --------
    pd.DataFrame
        Aggregated by zone with totals and statistics
    """
    
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
    """
    Aggregate dispatch changes by zone AND type (e.g., "red winds", "orange fossils").
    
    Parameters:
    -----------
    dispatch_changes_df : pd.DataFrame
        Output from calculate_dispatch_changes()
    
    Returns:
    --------
    pd.DataFrame
        Grouped by zone and type with counts and dispatch changes
    """
    
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
    logger.info(f"Loaded {len(bmu_classification)} BMUs with constraint classification")
    
    logger.info("\nCalculating dispatch changes...")
    dispatch_changes = calculate_dispatch_changes(
        n_wholesale,
        n_redispatch,
        bmu_classification
    )
    logger.info(f"Calculated dispatch changes for {len(dispatch_changes)} generators")
    
    logger.info("\nSorting by zone and type...")
    dispatch_changes_sorted = sort_generators_by_zone_and_type(dispatch_changes)
    
    logger.info("\nAggregating by zone...")
    aggregated_by_zone = aggregate_by_zone(dispatch_changes)
    
    logger.info("Aggregating by zone and type...")
    aggregated_by_zone_and_type = aggregate_by_zone_and_type(dispatch_changes)
    
    # Log results
    logger.info("\n" + "="*80)
    logger.info("DISPATCH CHANGES BY GEOGRAPHIC ZONE")
    logger.info("="*80)
    
    for zone, row in aggregated_by_zone.iterrows():
        logger.info(f"\n{row['zone_name']} ({zone}):")
        logger.info(f"  Generators: {int(row['count_generators'])}")
        logger.info(f"  Total change: {row['total_change_mwh']:10.2f} MWh (signed)")
        logger.info(f"  Flex up: {row['flex_up_mwh']:10.2f} MWh")
        logger.info(f"  Flex down: {row['flex_down_mwh']:10.2f} MWh")
        logger.info(f"  Avg per generator: {row['avg_change_per_generator']:10.2f} MWh")
    
    logger.info("\n" + "="*80)
    logger.info("DISPATCH CHANGES BY ZONE AND TYPE")
    logger.info("="*80)
    
    for idx, row in aggregated_by_zone_and_type.iterrows():
        logger.info(f"{row['zone_name']:25s} | {row['type']:15s} | {int(row['generator_count']):3d} units | {row['dispatch_change_mwh']:10.2f} MWh ({row['direction']})")
    
    logger.info("\n" + "="*80)
    logger.info(f"Total generators analyzed: {len(dispatch_changes)}")
    logger.info(f"Total net change: {dispatch_changes['dispatch_change_mwh'].sum():10.2f} MWh")
    logger.info("="*80 + "\n")
    
    # Save aggregated by zone results
    aggregated_by_zone_output = aggregated_by_zone[['count_generators', 'total_change_mwh', 'avg_change_per_generator', 'flex_up_mwh', 'flex_down_mwh', 'zone_name']].round(2)
    aggregated_by_zone_output.to_csv(snakemake.output['aggregated_by_zone'])
    logger.info(f"Aggregated by zone results saved to {snakemake.output['aggregated_by_zone']}")
    
    # Save detailed generator-level results
    dispatch_changes_output = dispatch_changes_sorted.round(2)
    dispatch_changes_output.to_csv(snakemake.output['detailed_by_generator'], index=False)
    logger.info(f"Detailed generator results saved to {snakemake.output['detailed_by_generator']}")
    
    # Save aggregated by zone and type results
    aggregated_by_zone_and_type_output = aggregated_by_zone_and_type.round(2)
    aggregated_by_zone_and_type_output.to_csv(snakemake.output['aggregated_by_zone_and_type'], index=False)
    logger.info(f"Aggregated by zone and type results saved to {snakemake.output['aggregated_by_zone_and_type']}")
    
    logger.info("\nDispatch change analysis complete!")