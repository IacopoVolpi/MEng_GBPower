# SPDX-FileCopyrightText: 2024-2025 Lukas Franken
# SPDX-License-Identifier: MIT

"""
Distribute total balancing volume across the 5 transmission constraint corridors.

This script calculates how much balancing (redispatch) volume is attributable to each
of the 5 critical transmission constraints (SSE-SP, SCOTEX, SSHARN, FLOWSTH, SEIMP).

Method:
--------
For each constraint corridor:
1. Calculate net flow through the constraint in redispatch solution (signed sum of line flows)
2. Distribute total balancing volume proportionally to absolute flow magnitudes

The total distributed volume equals the real submitted bid volume from the model.
"""

import logging
logger = logging.getLogger(__name__)

import yaml
import numpy as np
import pandas as pd
import pypsa

from _helpers import configure_logging


def get_constraint_flow(network, constraint_line_ids):
    """
    Calculate net flow through a constraint's lines (signed sum, then absolute value).
    
    Parameters:
    -----------
    network : pypsa.Network
        Network with solved flows
    constraint_line_ids : List[str]
        Line IDs making up the constraint
    
    Returns:
    --------
    float
        Net MWh flowing through constraint over the day
    """
    total_flow_mwh = 0
    
    for line_id in constraint_line_ids:
        if line_id in network.links_t.p0.columns:
            flow_mwh = network.links_t.p0[line_id].sum() * 0.5
            total_flow_mwh += flow_mwh
    
    return abs(total_flow_mwh)


def calculate_per_constraint_balancing_volume(
    n_redispatch,
    boundaries,
    total_balancing_volume,
):
    """
    Distribute total balancing volume across constraints based on flow magnitudes.
    
    Parameters:
    -----------
    n_redispatch : pypsa.Network
        Solved redispatch (balancing) network
    boundaries : dict
        Mapping of constraint names to line IDs
    total_balancing_volume : float
        Total balancing volume (MWh) from submitted bids
    
    Returns:
    --------
    pd.Series
        Per-constraint balancing volumes (index = constraint name, values = MWh)
    pd.DataFrame
        Detailed breakdown with flow, proportion, and balancing volume per constraint
    float
        Total flow change across all constraints
    """
    
    constraint_flows = {}
    
    # Calculate flow through each constraint
    for constraint_name, line_ids in boundaries.items():
        flow = get_constraint_flow(n_redispatch, line_ids)
        constraint_flows[constraint_name] = flow
    
    # Distribute proportionally by flow magnitude
    total_flow = sum(constraint_flows.values())
    
    if total_flow < 1e-6:
        logger.warning("No flow found. Distributing equally.")
        per_constraint = total_balancing_volume / len(boundaries)
        constraint_balancing = pd.Series(
            {name: per_constraint for name in boundaries.keys()}
        )
    else:
        constraint_balancing = pd.Series({
            name: (constraint_flows[name] / total_flow) * total_balancing_volume
            for name in boundaries.keys()
        })
    
    # Create detailed breakdown dataframe
    breakdown_data = {}
    for constraint_name in constraint_flows.keys():
        flow = constraint_flows[constraint_name]
        balancing_vol = constraint_balancing[constraint_name]
        proportion = (balancing_vol / total_balancing_volume * 100) if total_balancing_volume > 0 else 0
        
        breakdown_data[constraint_name] = {
            'constraint_flow_mwh': flow,
            'flow_proportion': flow / total_flow * 100 if total_flow > 0 else 0,
            'balancing_volume_mwh': balancing_vol,
            'balancing_proportion': proportion,
        }
    
    detailed_df = pd.DataFrame(breakdown_data).T
    
    # Validation
    sum_constraint_balancing = constraint_balancing.sum()
    error = abs(sum_constraint_balancing - total_balancing_volume)
    
    logger.info("Per-Constraint Balancing Volume Distribution:")
    for constraint_name in sorted(constraint_balancing.index):
        vol = constraint_balancing[constraint_name]
        prop = vol / total_balancing_volume * 100 if total_balancing_volume > 0 else 0
        logger.info(f"{constraint_name:10s}: {vol:10.2f} MWh ({prop:5.1f}%)")
    
    logger.info(f"{'Total':10s}: {sum_constraint_balancing:10.2f} MWh")
    logger.info(f"Model total: {total_balancing_volume:10.2f} MWh")
    logger.info(f"Difference:  {error:10.2f} MWh")
    
    return constraint_balancing, detailed_df, total_flow


if __name__ == '__main__':
    
    configure_logging(snakemake)
    
    # Load networks
    logger.info("Loading networks...")
    n_redispatch = pypsa.Network(snakemake.input['network_redispatch'])
    
    # Load transmission boundaries
    with open(snakemake.input['transmission_boundaries']) as f:
        boundaries = yaml.safe_load(f)
    
    logger.info(f"Loaded {len(boundaries)} transmission constraints")
    
    # Load bids and BMUs to calculate real daily balancing volume
    idx = pd.IndexSlice
    bids = pd.read_csv(snakemake.input['bids'], index_col=[0,1], parse_dates=True)
    bmus = pd.read_csv(snakemake.input['bmus'], index_col=0)

    # Clean BMU data
    bmus = bmus.loc[bmus['lat'] != 'distributed']
    bmus['lat'] = bmus['lat'].astype(float)

    # Reduce bids to total volume per unit
    bids = bids.loc[idx[:, 'vol'], :].sum()
    bids.index = bids.index.get_level_values(0)

    # Select BMUs likely to curtail
    renewable_bmus = bmus[bmus.carrier.isin(['onwind', 'offwind', 'hydro', 'cascade'])].index
    thermal_bmus = bmus[(bmus.carrier.isin(['fossil', 'biomass', 'coal'])) & (bmus['lat'] > 55.3)].index
    bid_counting_units = renewable_bmus.union(thermal_bmus)

    # Calculate total daily bidding volume
    total_balancing_volume = bids.loc[bids.index.intersection(bid_counting_units)].sum()

    logger.info(f"Total balancing volume from submitted bids: {total_balancing_volume:.2f} MWh\n")
    
    # Calculate per-constraint distribution
    constraint_balancing, detailed_df, total_flow = calculate_per_constraint_balancing_volume(
        n_redispatch,
        boundaries,
        total_balancing_volume,
    )
    
    # Save results
    constraint_order = ['SSE-SP', 'SCOTEX', 'SSHARN', 'FLOWSTH', 'SEIMP']
    ordered_balancing = constraint_balancing[constraint_order]
    
    output_series = pd.Series({
        'total_balancing_volume': total_balancing_volume,
        'total_constraint_flow_change_mwh': total_flow,
    })
    output_series = pd.concat([output_series, ordered_balancing])
    output_series.to_csv(snakemake.output['per_constraint_balancing'], header=False)
    logger.info(f"Per-constraint balancing volumes saved to {snakemake.output['per_constraint_balancing']}")
    
    # Save detailed breakdown
    detailed_df_reordered = detailed_df.reindex(constraint_order)
    detailed_df_reordered = detailed_df_reordered.round(2)
    detailed_df_reordered.to_csv(snakemake.output['detailed_breakdown'])
    logger.info(f"Detailed breakdown saved to {snakemake.output['detailed_breakdown']}")
    
    logger.info("\nPer-constraint balancing volume distribution complete!")