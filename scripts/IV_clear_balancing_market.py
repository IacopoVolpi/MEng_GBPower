# SPDX-FileCopyrightText: : 2024 The PyPSA Authors
# SPDX-License-Identifier: MIT

"""
Explicit Balancing Market Clearing (Pay-as-you-bid)
"""

import logging
logger = logging.getLogger(__name__)

import pypsa
import pandas as pd
import numpy as np

from _helpers import configure_logging
from summarize_system_cost import get_bidding_volume


def load_submitted_bids(path: str) -> pd.DataFrame:
    """Load submitted bids from CSV."""
    df = pd.read_csv(path, parse_dates=['timestamp']) #reads the csv file and parses the timestamp column as dates
    df = df.rename(columns={
        'NationalGridBmUnit': 'unit_id', #changes the name of the column from NationalGridBmUnit to unit_id
        'Bid': 'price', #changes the name of the column from Bid to price
    })
    # LevelFrom/LevelTo = MW capacity available (they're equal)
    df['volume_mw'] = df['LevelFrom'].abs()
    return df[['timestamp', 'unit_id', 'PairId', 'price', 'volume_mw']]


def load_submitted_offers(path: str) -> pd.DataFrame:
    """Load submitted offers from CSV."""
    df = pd.read_csv(path, parse_dates=['timestamp']) #reads the csv file and parses the timestamp column as dates
    df = df.rename(columns={
        'NationalGridBmUnit': 'unit_id', #changes the name of the column from NationalGridBmUnit to unit_id
        'Offer': 'price', #changes the name of the column from Offer to price
    })
    df['volume_mw'] = df['LevelFrom'].abs()
    return df[['timestamp', 'unit_id', 'PairId', 'price', 'volume_mw']]


def clear_pay_as_bid(required_volume: float, stack: pd.DataFrame, direction: str) -> dict:
    """
    Clear a single period using pay-as-bid rules.
    
    Args:
        required_volume: MW needed (positive)
        stack: DataFrame with columns [unit_id, price, volume]
        direction: 'up' (use offers) or 'down' (use bids)
    
    Returns:
        dict with cleared_volume, total_cost, accepted_actions
    """
    if stack.empty or required_volume <= 0: #if there are no bids/offers or no volume is required no need to run balancing mechanism
        return {
            'cleared_volume': 0,
            'total_cost': 0,
            'accepted_actions': [],
        }
    
    if direction == 'up': #for upward balancing action the lowest the price the lowest the cost of the action, so sort from lowest to highest
        # Offers: sort ascending (cheapest first)
        sorted_stack = stack.sort_values('price', ascending=True)
        """#for downward balancing action the highest the price the lowest the cost of the action
        , so sort from highest to lowest because A Party can submit negative Bid Prices, this means that the Party will be paid to reduce their generation or increase demand."""
    else: 
        # Bids: sort descending (highest price first)
        sorted_stack = stack.sort_values('price', ascending=False)
    
    accepted = []
    remaining = required_volume
    total_cost = 0.0
    
    for _, row in sorted_stack.iterrows():
        if remaining <= 0:
            break
        
        accept_vol = min(row['volume_mw'], remaining)
        if accept_vol <= 0: #skip if there is no volume to accept
            continue
            
        action_cost = row['price'] * accept_vol * 0.5  # 30-min period
        
        accepted.append({
            'unit_id': row['unit_id'],
            'pair_id': row['PairId'],
            'volume': accept_vol,
            'price': row['price'],
            'cost': action_cost,
        })
        
        remaining -= accept_vol
        total_cost += action_cost
    
    return {
        'cleared_volume': required_volume - max(0, remaining),
        'total_cost': total_cost,
        'uncleared_volume': max(0, remaining),
        'accepted_actions': accepted,
    }


def run_explicit_balancing(n_wholesale, n_redispatch, offers_df, bids_df):
    """
    Main entry point: compute balancing volumes and clear auction.
    """
    volumes = get_bidding_volume(n_wholesale, n_redispatch)
    volumes.index = pd.to_datetime(volumes.index, utc=True)
    logger.info(f"Total balancing volume: {volumes.sum():.1f} MWh")
    
    results = []
    all_accepted_actions = []
    
    for timestamp, volume in volumes.items():
        if volume <= 0:
            continue
        
        period_offers = offers_df[offers_df['timestamp'] == timestamp]
        result = clear_pay_as_bid(volume, period_offers, direction='up')
        
        # Flatten accepted_actions and add timestamp
        for action in result['accepted_actions']:
            action['timestamp'] = timestamp
            all_accepted_actions.append(action)
        
        # Remove accepted_actions from result dict before appending
        result.pop('accepted_actions')
        result['timestamp'] = timestamp
        result['required_volume'] = volume
        results.append(result)
    
    return pd.DataFrame(results), pd.DataFrame(all_accepted_actions)


if __name__ == '__main__':
    configure_logging(snakemake)
    
    logger.info("Running explicit balancing market clearing")
    
    # Load networks
    n_wholesale = pypsa.Network(snakemake.input.network_wholesale)
    n_redispatch = pypsa.Network(snakemake.input.network_redispatch)
    
    # Load submitted bids/offers from your new data
    offers_df = load_submitted_offers(snakemake.input.submitted_offers)
    bids_df = load_submitted_bids(snakemake.input.submitted_bids)
    
    logger.info(f"Loaded {len(offers_df)} offers, {len(bids_df)} bids")
    
    # Run clearing
    results, accepted_actions = run_explicit_balancing(n_wholesale, n_redispatch, offers_df, bids_df)
    
    # Save summary
    summary = results[['timestamp', 'required_volume', 'cleared_volume', 'total_cost', 'uncleared_volume']]
    summary.to_csv(snakemake.output.clearing_results, index=False)
    
    # Save detailed accepted actions
    accepted_actions.to_csv(snakemake.output.accepted_actions, index=False)
    
    logger.info(f"Saved clearing results and accepted actions")