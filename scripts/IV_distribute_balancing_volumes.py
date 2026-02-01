# SPDX-FileCopyrightText: 2024-2025 Lukas Franken
# SPDX-License-Identifier: MIT

"""
Distribute total balancing volume across the 5 transmission constraint corridors.

This script calculates how much balancing (redispatch) volume is attributable to each
of the 5 critical transmission constraints (SSE-SP, SCOTEX, SSHARN, FLOWSTH, SEIMP).

Method:
--------
For each constraint corridor:
1. Sum absolute flows through all its lines in the wholesale (day-ahead) solution
2. Sum absolute flows through all its lines in the redispatch (balancing) solution
3. Calculate the flow change: |flow_redispatch - flow_wholesale|
4. This change represents how much redispatch was needed to manage that constraint
5. Normalize all constraint changes to sum to the total balancing volume

This ensures the per-constraint volumes are proportionally consistent with the
global balancing volume already calculated by the model.
"""

import logging
logger = logging.getLogger(__name__)

import yaml
import numpy as np
import pandas as pd
import pypsa

from _helpers import configure_logging


def get_constraint_flow(network, constraint_line_ids, stage='wholesale'):
    """
    Calculate total absolute flow through a constraint's lines.
    
    Parameters:
    -----------
    network : pypsa.Network
        Network with solved flows
    constraint_line_ids : List[str]
        Line IDs making up the constraint
    stage : str
        Stage label for logging ('wholesale' or 'redispatch')
    
    Returns:
    --------
    float
        Total MWh flowing through constraint over the day
    """
    total_flow_mwh = 0
    lines_found = 0
    
    for line_id in constraint_line_ids:
        # Check if line exists in network links (transmission lines are stored as links in DC model)
        if line_id in network.links_t.p0.columns:
            # Sum absolute flows across all timesteps, convert MW to MWh (0.5 for 30-min periods)
            flow_mwh = network.links_t.p0[line_id].abs().sum() * 0.5
            total_flow_mwh += flow_mwh
            lines_found += 1
    
    if lines_found == 0:
        logger.warning(f"No lines found for constraint in {stage} network")
    
    return total_flow_mwh


def calculate_per_constraint_balancing_volume(
    n_wholesale,
    n_redispatch,
    boundaries,
    total_balancing_volume,
):
    """
    Distribute total balancing volume across constraints based on flow changes.
    
    Parameters:
    -----------
    n_wholesale : pypsa.Network
        Solved wholesale (day-ahead) network
    n_redispatch : pypsa.Network
        Solved redispatch (balancing) network
    boundaries : dict
        Mapping of constraint names to line IDs
    total_balancing_volume : float
        Total balancing volume (MWh) calculated by the model
    
    Returns:
    --------
    pd.Series
        Per-constraint balancing volumes (index = constraint name, values = MWh)
    pd.DataFrame
        Detailed breakdown with wholesale flow, redispatch flow, and change
    """
    
    logger.info("\n" + "="*80)
    logger.info("Distributing total balancing volume across 5 transmission constraints")
    logger.info("="*80 + "\n")
    
    constraint_data = {}
    flow_changes = {}
    
    # For each constraint, calculate flow change
    for constraint_name in sorted(boundaries.keys()):
        line_ids = boundaries[constraint_name]
        
        # Get flows in both solutions
        wholesale_flow = get_constraint_flow(n_wholesale, line_ids, stage='wholesale')
        redispatch_flow = get_constraint_flow(n_redispatch, line_ids, stage='redispatch')
        
        # Flow change = how much redispatch changed the flow through this constraint
        flow_change = abs(redispatch_flow - wholesale_flow)
        
        constraint_data[constraint_name] = {
            'wholesale_flow_mwh': wholesale_flow,
            'redispatch_flow_mwh': redispatch_flow,
            'flow_change_mwh': flow_change,
        }
        
        flow_changes[constraint_name] = flow_change
        
        logger.info(f"{constraint_name:10s} | Wholesale: {wholesale_flow:10.1f} MWh | "
                   f"Redispatch: {redispatch_flow:10.1f} MWh | "
                   f"Change: {flow_change:10.1f} MWh")
    
    logger.info("\n")
    
    # Normalize flow changes to distribute total balancing volume proportionally
    total_flow_change = sum(flow_changes.values())
    
    if total_flow_change < 1e-6:  # Avoid division by zero
        logger.warning("Total flow change is near zero. Distributing equally across constraints.")
        # Fall back to equal distribution
        per_constraint = total_balancing_volume / len(boundaries)
        constraint_balancing = pd.Series(
            {name: per_constraint for name in boundaries.keys()}
        )
    else:
        # Distribute proportionally
        constraint_balancing = pd.Series({
            name: (flow_changes[name] / total_flow_change) * total_balancing_volume
            for name in boundaries.keys()
        })
    
    # Add balancing volume to the detailed breakdown
    for constraint_name in constraint_data.keys():
        constraint_data[constraint_name]['balancing_volume_mwh'] = constraint_balancing[constraint_name]
    
    # Create detailed dataframe
    detailed_df = pd.DataFrame(constraint_data).T
    detailed_df['proportion'] = detailed_df['balancing_volume_mwh'] / total_balancing_volume
    
    # Validation: ensure sum matches total
    sum_constraint_balancing = constraint_balancing.sum()
    error = abs(sum_constraint_balancing - total_balancing_volume)
    error_percent = (error / total_balancing_volume * 100) if total_balancing_volume > 0 else 0
    
    logger.info("Per-Constraint Balancing Volume Distribution:")
    logger.info("-" * 80)
    for constraint_name in sorted(constraint_balancing.index):
        vol = constraint_balancing[constraint_name]
        prop = vol / total_balancing_volume * 100 if total_balancing_volume > 0 else 0
        logger.info(f"{constraint_name:10s}: {vol:10.2f} MWh ({prop:5.1f}%)")
    
    logger.info("-" * 80)
    logger.info(f"{'Total':10s}: {sum_constraint_balancing:10.2f} MWh (100.0%)")
    logger.info(f"Model total: {total_balancing_volume:10.2f} MWh")
    logger.info(f"Difference:  {error:10.2f} MWh ({error_percent:.4f}%)")
    
    if error_percent > 0.01:  # Tolerance of 0.01%
        logger.warning(f"Distribution error exceeds tolerance! Error: {error_percent:.4f}%")
    else:
        logger.info("✓ Distribution validated: sum equals total balancing volume")
    
    logger.info("\n")
    
    return constraint_balancing, detailed_df


if __name__ == '__main__':
    
    configure_logging(snakemake)
    
    # Load networks
    logger.info("Loading networks...")
    n_wholesale = pypsa.Network(snakemake.input['network_wholesale'])
    n_redispatch = pypsa.Network(snakemake.input['network_redispatch'])
    
    # Load transmission boundaries
    with open(snakemake.input['transmission_boundaries']) as f:
        boundaries = yaml.safe_load(f)
    
    logger.info(f"Loaded {len(boundaries)} transmission constraints")
    
    # Get total balancing volume (already calculated by model)
    # This is stored in the network results - we need to recalculate it to ensure consistency
    from summarize_system_cost import get_bidding_volume
    
    total_balancing_volume = get_bidding_volume(n_wholesale, n_redispatch).sum()
    logger.info(f"Total balancing volume from model: {total_balancing_volume:.2f} MWh\n")
    
    # Calculate per-constraint distribution
    constraint_balancing, detailed_df = calculate_per_constraint_balancing_volume(
        n_wholesale,
        n_redispatch,
        boundaries,
        total_balancing_volume,
    )
    
    # Save results
    constraint_balancing.to_csv(snakemake.output['per_constraint_balancing'])
    logger.info(f"Per-constraint balancing volumes saved to {snakemake.output['per_constraint_balancing']}")
    
    detailed_df.to_csv(snakemake.output['detailed_breakdown'])
    logger.info(f"Detailed breakdown saved to {snakemake.output['detailed_breakdown']}")
    
    logger.info("\nPer-constraint balancing volume distribution complete!")