# SPDX-FileCopyrightText: 2024 The PyPSA Authors
# SPDX-License-Identifier: MIT

"""
Classify BMUs as North or South of each transmission constraint boundary.

This script determines whether each Balancing Mechanism Unit (BMU) is located
upstream (north) or downstream (south) of each of the 5 critical transmission
constraint boundaries (SSE-SP, SCOTEX, SSHARN, FLOWSTH, SEIMP).

The classification is based on fitting a linear boundary through the northernmost
points of each constraint's transmission lines, then determining which side of
that boundary each BMU falls on.

"""

import logging
logger = logging.getLogger(__name__)

import yaml
import numpy as np
import pandas as pd
import pypsa

from _helpers import configure_logging


def get_northernmost_points(constraint_line_ids, network):
    
    northernmost_points = []
    
    for line_id in constraint_line_ids:
        # Try n.lines first, then n.links (like the notebook does)
        if str(line_id) in network.lines.index:
            bus0 = network.lines.loc[str(line_id), 'bus0']
            bus1 = network.lines.loc[str(line_id), 'bus1']
        elif str(line_id) in network.links.index:
            bus0 = network.links.loc[str(line_id), 'bus0']
            bus1 = network.links.loc[str(line_id), 'bus1']
        else:
            logger.warning(f"Line {line_id} not found in network.lines or network.links")
            continue
        
        # Get their coordinates
        coords = []
        if bus0 in network.buses.index:
            lon0 = network.buses.loc[bus0, 'x']
            lat0 = network.buses.loc[bus0, 'y']
            coords.append((lon0, lat0, lat0))
        
        if bus1 in network.buses.index:
            lon1 = network.buses.loc[bus1, 'x']
            lat1 = network.buses.loc[bus1, 'y']
            coords.append((lon1, lat1, lat1))
        
        if coords:
            # Pick the northernmost (highest latitude) endpoint
            northernmost = max(coords, key=lambda x: x[2])
            northernmost_points.append((northernmost[0], northernmost[1]))
    
    return northernmost_points


def fit_constraint_boundary(northernmost_points):
    """
    Fit a linear boundary through the northernmost points of a constraint.
    Uses least-squares linear regression to find: lat = m * lon + b
    
    """
    if not northernmost_points:
        raise ValueError("No northernmost points provided for constraint")
    
    points_array = np.array(northernmost_points)
    lons = points_array[:, 0]
    lats = points_array[:, 1]
    
    # Fit linear regression: lat = m * lon + b
    coefficients = np.polyfit(lons, lats, 1)  # Returns [m, b]
    slope = coefficients[0]
    intercept = coefficients[1]
    
    # Calculate center point
    center_lon = lons.mean()
    center_lat = lats.mean()
    
    logger.info(f"Constraint boundary fitted: lat = {slope:.4f} * lon + {intercept:.4f}")
    logger.info(f"Center point: ({center_lon:.4f}, {center_lat:.4f})")
    
    return {
        'slope': slope,
        'intercept': intercept,
        'center_lon': center_lon,
        'center_lat': center_lat,
    }


def classify_bmu_relative_to_constraint(lon, lat, boundary_params):

    lon = float(lon)
    lat = float(lat)
    
    slope = boundary_params['slope']
    intercept = boundary_params['intercept']
    
    # Calculate the latitude of the boundary at this BMU's longitude
    boundary_lat_at_bmu = slope * lon + intercept
    
    if lat > boundary_lat_at_bmu:
        return 'north'
    else:
        return 'south'


if __name__ == '__main__':
    configure_logging(snakemake)
    

    # Load the network (use the nodal network which has all buses and links)
    logger.info("Classifying BMUs relative to transmission constraints")
    network = pypsa.Network(snakemake.input.network)
    logger.info(f"Loaded network with {len(network.buses)} buses and {len(network.links)} links")
    
    # Load transmission boundaries
    with open(snakemake.input.transmission_boundaries) as f:
        boundaries = yaml.safe_load(f)
    logger.info(f"Loaded {len(boundaries)} transmission constraints")
    
    # Load BMU data
    bmus = pd.read_csv(snakemake.input.bmus, index_col=0)
    logger.info(f"Loaded {len(bmus)} BMUs with coordinates")
    

    
    # Filter BMUs to only those with valid coordinates, so not the one which are 'distributed' (which means they don't have a specific location and can't be classified as north/south)
    bmus_valid = bmus[(bmus['lat'] != 'distributed') & (bmus['lon'] != 'distributed')].copy()
    bmus_valid['lat'] = pd.to_numeric(bmus_valid['lat'], errors='coerce')
    bmus_valid['lon'] = pd.to_numeric(bmus_valid['lon'], errors='coerce')
    bmus_valid = bmus_valid.dropna(subset=['lat', 'lon'])
    logger.info(f"{len(bmus_valid)} BMUs have valid coordinates")

    # Remove line 8009 from SSHARN and SCOTEX (it's an offshore interconnector that skews the boundary line plotting)
    if 'SSHARN' in boundaries:
        boundaries['SSHARN'] = [str(line) for line in boundaries['SSHARN'] if str(line) != '8009']
    if 'SCOTEX' in boundaries:
        boundaries['SCOTEX'] = [str(line) for line in boundaries['SCOTEX'] if str(line) != '8009']

    
    # For each constraint, compute boundary and classify BMUs
    constraint_boundaries = {}
    
    for constraint_name in sorted(boundaries.keys()):
        logger.info(f"\n--- Processing constraint: {constraint_name} ---")
        
        line_ids = boundaries[constraint_name]
        logger.info(f"Constraint contains {len(line_ids)} transmission lines")
        
        # Get northernmost points of the constraint
        northernmost_points = get_northernmost_points(line_ids, network)
        
        if not northernmost_points:
            logger.error(f"No valid northernmost points found for {constraint_name}")
            continue
        
        logger.info(f"Found {len(northernmost_points)} northernmost points")
        
        # Fit a boundary line through these points
        boundary_params = fit_constraint_boundary(northernmost_points)
        constraint_boundaries[constraint_name] = boundary_params
        
        # Classify each BMU relative to this constraint
        bmus_valid[f'{constraint_name}_side'] = bmus_valid.apply(
            lambda row: classify_bmu_relative_to_constraint(
                row['lon'], row['lat'], boundary_params
            ),
            axis=1
        )
        
    
    # Reorder columns: north to south (SSE-SP, SCOTEX, SSHARN, FLOWSTH, SEIMP)
    constraint_order = ['SSE-SP', 'SCOTEX', 'SSHARN', 'FLOWSTH', 'SEIMP']
    side_columns = [col for col in bmus_valid.columns if col.endswith('_side')]
    other_columns = [col for col in bmus_valid.columns if not col.endswith('_side')]
    # Order the side columns by constraint order
    ordered_side_columns = [f'{constraint}_side' for constraint in constraint_order if f'{constraint}_side' in side_columns]  
    # Reorder the dataframe
    bmus_valid = bmus_valid[other_columns + ordered_side_columns]
    # Save BMU classification to CSV
    bmus_valid.to_csv(snakemake.output.bmu_constraint_classification)
    logger.info(f"\nBMU classification saved to {snakemake.output.bmu_constraint_classification}")
    
    # Save constraint boundaries to YAML for reference
    boundary_dict = {}
    for constraint_name, params in constraint_boundaries.items():
        boundary_dict[constraint_name] = {
            'slope': float(params['slope']),
            'intercept': float(params['intercept']),
            'center_lon': float(params['center_lon']),
            'center_lat': float(params['center_lat']),
        }
    
    with open(snakemake.output.balancing_constraint_boundaries, 'w') as f:
        yaml.dump(boundary_dict, f, default_flow_style=False)
    logger.info(f"Constraint boundaries saved to {snakemake.output.balancing_constraint_boundaries}")
    
    logger.info("\nBMU constraint classification complete!")